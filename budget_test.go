package loom_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// noSchedules makes c's schedule list empty.
func noSchedules(c *mocks.Client) {
	sc := &mocks.ScheduleClient{}
	it := &mocks.ScheduleListIterator{}
	it.On("HasNext").Return(false)
	sc.On("List", mock.Anything, mock.Anything).Return(it, nil)
	c.On("ScheduleClient").Return(sc)
}

// schedules stands in for Temporal's schedules.
type schedules struct {
	mu     sync.Mutex
	budget map[string]loom.ScheduleBudget
	asked  int
}

func (f *schedules) Budget(_ context.Context, name string) (loom.ScheduleBudget, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked++
	b, ok := f.budget[name]
	if !ok {
		return b, loom.ErrNoSchedule
	}
	return b, nil
}

func (f *schedules) Pause(_ context.Context, name, note string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.budget[name]
	b.Paused, b.Note = true, note
	f.budget[name] = b
	return nil
}

// budgetRun runs src as a run schedule started at (none: by hand), on a
// worker whose ask steps cost what out says and whose budget check
// asks sc; it returns the result, the error and how many steps ran.
func budgetRun(t *testing.T, s *warp.Store, sc loom.ScheduleControl, schedule string, at time.Time, out pirn.Answer) (loom.RunResult, error, int) {
	t.Helper()
	reg := claudeRegistry(t)
	src := `(pipe (ask "the news") echo)`
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(at)
	loom.RegisterWith(env, &loom.Checker{Registry: reg}, &loom.Recorder{Store: s, Schedules: sc})
	var mu sync.Mutex
	steps := 0
	env.RegisterActivityWithOptions(func(context.Context, plan.Input) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		steps++
		return jsonOf(out), nil
	}, activity.RegisterOptions{Name: pirn.ActivityName})
	if schedule != "" {
		env.SetTypedSearchAttributesOnStart(temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyKeyword("TemporalScheduledById").ValueSet(loom.SchedulePrefix + schedule)))
	}
	env.ExecuteWorkflow(loom.RunWorkflowName, loom.RunInput{Program: src, Plan: must(loom.Compile(ctx, reg, src)), Ref: loom.DefaultRef})
	var res loom.RunResult
	err := env.GetWorkflowError()
	if err == nil {
		env.GetWorkflowResult(&res)
	}
	return res, err, steps
}

func headOf(t *testing.T, s *warp.Store) warp.ID {
	id, _, err := s.Resolve(ctx, loom.DefaultRef)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Three cents of claude-sonnet-5-5: 10,000 tokens in, 1,000 out.
var threeCents = pirn.Answer{Pirn: 1, Model: "claude-sonnet-5-5", Text: "news", InputTokens: 10000, OutputTokens: 1000}

// TestInv_BudgetIsCheckedBeforeAnyStep: a run a schedule with a $0.01
// budget started runs while the month's runs of that schedule cost less,
// and records which schedule started it (Loom-Schedule); the next, the
// month now at three cents, starts no step and records nothing: the
// schedule is paused, the note saying so, and the run says it skipped.
// A run started by hand is never checked, and neither is a schedule
// with no budget. A worker that can't check budgets runs no scheduled
// run. (Roadmap 10.4.)
func TestInv_BudgetIsCheckedBeforeAnyStep(t *testing.T) {
	s := newStore(t)
	sc := &schedules{budget: map[string]loom.ScheduleBudget{"news": {Budget: loom.Budget{USD: 0.01}}, "free": {}}}
	nov := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)

	res, err, steps := budgetRun(t, s, sc, "news", nov, threeCents)
	if err != nil || res.Skipped != "" || steps != 1 || res.Commit == "" {
		t.Fatalf("first run: %+v %v, %d steps", res, err, steps)
	}
	log := must(s.Log(ctx, headOf(t, s), 1))
	if ru, _, _ := loom.RecordUsage(ctx, s, log[0]); ru.Schedule != "news" {
		t.Fatalf("record names schedule %q", ru.Schedule)
	}
	head := headOf(t, s)
	res, err, steps = budgetRun(t, s, sc, "news", nov.Add(time.Hour), threeCents)
	want := "paused: monthly budget $0.01 reached (est.) in November 2026"
	if err != nil || res.Skipped != want || steps != 0 || res.Commit != "" || headOf(t, s) != head {
		t.Fatalf("over budget: %+v %v, %d steps", res, err, steps)
	}
	if b := sc.budget["news"]; !b.Paused || b.Note != want {
		t.Fatalf("schedule %+v", b)
	}
	if month, ok := loom.BudgetPaused(want); !ok || month != "November 2026" {
		t.Errorf("note not read back: %q %v", month, ok)
	}

	asked := sc.asked
	if res, err, steps := budgetRun(t, s, sc, "", nov.Add(2*time.Hour), threeCents); err != nil || res.Skipped != "" || steps != 1 || sc.asked != asked {
		t.Errorf("by hand: %+v %v %d, asked %d", res, err, steps, sc.asked-asked)
	}
	for i := 0; i < 2; i++ {
		if res, err, steps := budgetRun(t, s, sc, "free", nov.Add(3*time.Hour), threeCents); err != nil || res.Skipped != "" || steps != 1 {
			t.Errorf("no budget: %+v %v %d", res, err, steps)
		}
	}
	if res, err, steps := budgetRun(t, s, nil, "news", nov, threeCents); appErrType(err) != "NoBudgetCheck" || steps != 0 {
		t.Errorf("worker with no budget check: %+v %v %d", res, err, steps)
	}
}

// TestInv_BudgetThatCantBeCheckedPauses: if a run of the schedule this
// month used a model with no price, or has no counts, the month can't be
// added up, so the next run doesn't start: the schedule is paused
// saying why, never run on a guess. (Roadmap 10.4.)
func TestInv_BudgetThatCantBeCheckedPauses(t *testing.T) {
	s := newStore(t)
	sc := &schedules{budget: map[string]loom.ScheduleBudget{"news": {Budget: loom.Budget{USD: 100}}}}
	nov := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	unpriced := pirn.Answer{Pirn: 1, Model: "claude-mythos-5-1", Text: "x", InputTokens: 1, OutputTokens: 1}
	if res, err, _ := budgetRun(t, s, sc, "news", nov, unpriced); err != nil || res.Skipped != "" {
		t.Fatal(res, err)
	}
	res, err, steps := budgetRun(t, s, sc, "news", nov.Add(time.Hour), threeCents)
	if err != nil || steps != 0 || !strings.HasPrefix(res.Skipped, "paused: monthly budget $100.00 can't be checked in November 2026: 1 of its steps this month have no estimate") ||
		!strings.Contains(res.Skipped, "no price for claude-mythos-5-1") || !sc.budget["news"].Paused {
		t.Fatalf("%+v %v %d", res, err, steps)
	}
	if _, ok := loom.BudgetPaused(res.Skipped); !ok {
		t.Errorf("not read as a budget's note: %q", res.Skipped)
	}
}

// TestInv_BudgetMonthFollowsTheTimeZone: a schedule's month is the
// calendar month in its time zone: a run recorded at 02:00 UTC on 1
// November is October's in New York, and one at 03:00 UTC on 1 December
// still November's; in UTC, the other way round. (Roadmap 10.4.)
func TestInv_BudgetMonthFollowsTheTimeZone(t *testing.T) {
	for _, tc := range []struct {
		zone       string
		at         time.Time // when the costly run is recorded
		then       time.Time // the next run
		overBudget bool
	}{
		{"America/New_York", time.Date(2026, 11, 1, 2, 0, 0, 0, time.UTC), time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC), false},
		{"", time.Date(2026, 11, 1, 2, 0, 0, 0, time.UTC), time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC), true},
		{"America/New_York", time.Date(2026, 11, 20, 2, 0, 0, 0, time.UTC), time.Date(2026, 12, 1, 3, 0, 0, 0, time.UTC), true},
		{"", time.Date(2026, 11, 20, 2, 0, 0, 0, time.UTC), time.Date(2026, 12, 1, 3, 0, 0, 0, time.UTC), false},
		{"Asia/Tokyo", time.Date(2026, 10, 31, 16, 0, 0, 0, time.UTC), time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC), true},
	} {
		s := newStore(t)
		sc := &schedules{budget: map[string]loom.ScheduleBudget{"news": {Budget: loom.Budget{USD: 0.01}, TimeZone: "x"}}}
		sc.budget["news"] = loom.ScheduleBudget{Budget: loom.Budget{USD: 0.01}, TimeZone: tc.zone}
		if _, err, _ := budgetRun(t, s, sc, "news", tc.at, threeCents); err != nil {
			t.Fatal(err)
		}
		sc.budget["news"] = loom.ScheduleBudget{Budget: loom.Budget{USD: 0.01}, TimeZone: tc.zone}
		res, err, _ := budgetRun(t, s, sc, "news", tc.then, threeCents)
		if err != nil || (res.Skipped != "") != tc.overBudget {
			t.Errorf("%s, spent %v, run %v: %+v %v", tc.zone, tc.at, tc.then, res, err)
		}
	}
}

// TestInv_BudgetPauseIsToldOnce: the watcher tells of a run its
// schedule's budget stopped once, as a pause of that schedule saying
// why, not as a run that finished. (Roadmap 10.4.)
func TestInv_BudgetPauseIsToldOnce(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	now := time.Now()
	by, _ := dc.ToPayload("loom-schedule-news")
	note, _ := dc.ToPayload("paused: monthly budget $0.01 reached (est.) in November 2026")
	l := &temporalList{ended: []*workflow.WorkflowExecutionInfo{{
		Execution: &common.WorkflowExecution{WorkflowId: "loom-x-2026", RunId: "r1"}, Status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		CloseTime:        timestamppb.New(now.Add(-10 * time.Second)),
		SearchAttributes: &common.SearchAttributes{IndexedFields: map[string]*common.Payload{"TemporalScheduledById": by}},
		Memo:             &common.Memo{Fields: map[string]*common.Payload{loom.MemoSkipped: note}},
	}}}
	c := &mocks.Client{}
	c.On("ListWorkflow", mock.Anything, mock.Anything).Return(l.list, nil)
	noSchedules(c)
	w := &loom.Watcher{Client: c, Queue: "loom", Since: now.Add(-time.Minute)}
	got, err := w.Check(ctx)
	if err != nil || len(got) != 1 || got[0].Title != "heedle: news paused" || got[0].Schedule != "news" ||
		got[0].Body != "Paused: monthly budget $0.01 reached (est.) in November 2026. It started nothing." {
		t.Fatalf("%+v %v", got, err)
	}
	if again, _ := w.Check(ctx); len(again) != 0 {
		t.Errorf("told twice: %+v", again)
	}
}

package loom_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// claudeStep is a stand-in ask step: on each attempt it makes the calls
// calls says (each noted on the meter, as pirn notes what Claude
// answers), then fails if fail says so, else answers with those calls'
// tokens, as pirn records them.
type claudeStep struct {
	calls func(node string, attempt int32) []pirn.Call
	fail  func(node string, attempt int32) bool
}

func (c claudeStep) run(ctx context.Context, in plan.Input) (string, error) {
	attempt := activity.GetInfo(ctx).Attempt
	var a pirn.Answer
	for _, call := range c.calls(in.NodeID, attempt) {
		pirn.MeterFrom(ctx).Add(call)
		a.Model, a.InputTokens, a.OutputTokens = call.Model, a.InputTokens+call.Input, a.OutputTokens+call.Output
	}
	if c.fail(in.NodeID, attempt) {
		return "", errors.New("overloaded")
	}
	if a.Model == "" {
		a.Model = "claude-sonnet-5-5"
	}
	a.Pirn, a.Text = 1, fmt.Sprintf("%s on attempt %d", in.NodeID, attempt)
	return jsonOf(a), nil
}

// runs numbers ledgerRun's runs.
var runs int

// ledgerRun runs src at at on a worker that counts its Claude steps in
// s's ledger (Counted), the steps being step; schedule, if set, started
// it, with its budget asked of sc.
func ledgerRun(t *testing.T, s *warp.Store, sc loom.ScheduleControl, src, schedule string, at time.Time, step claudeStep) (loom.RunResult, error) {
	t.Helper()
	reg := claudeRegistry(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(at)
	// Each run its own workflow ID, as Submit and schedules give them.
	runs++
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: fmt.Sprintf("loom-test-%d", runs)})
	loom.RegisterWith(env, &loom.Checker{Registry: reg}, &loom.Recorder{Store: s, Schedules: sc})
	loom.Counted(loom.Labelled(env), s).RegisterActivityWithOptions(shuttle.ArrowAsActivity(step.run), activity.RegisterOptions{Name: pirn.ActivityName})
	if schedule != "" {
		env.SetTypedSearchAttributesOnStart(temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyKeyword("TemporalScheduledById").ValueSet(loom.SchedulePrefix + schedule)))
	}
	env.ExecuteWorkflow(loom.RunWorkflowName, loom.RunInput{Program: src, Plan: must(loom.Compile(ctx, reg, src)), Ref: loom.DefaultRef})
	var res loom.RunResult
	err := env.GetWorkflowError()
	if err == nil {
		env.GetWorkflowResult(&res)
	}
	return res, err
}

// counted is everything the Costs page counts between from and to:
// records' usage and the ledger's extras, by kind ("record" for records).
func counted(t *testing.T, s *warp.Store, from, to time.Time) (loom.Tokens, map[string]loom.Tokens) {
	t.Helper()
	runs := must(loom.AllRunsBetween(ctx, s, from, to))
	by := map[string]loom.Tokens{}
	var total loom.Tokens
	for _, r := range runs {
		by["record"] = by["record"].Add(r.Usage.Total())
		total = total.Add(r.Usage.Total())
	}
	for _, x := range must(loom.ExtrasBetween(ctx, s, from, to, runs, "")) {
		by[x.Kind] = by[x.Kind].Add(x.Usage.Total())
		total = total.Add(x.Usage.Total())
	}
	return total, by
}

// now is the time the tests' runs start: Temporal's test environment
// gives activities the real clock, so runs are started on it too.
func now() time.Time { return time.Now().UTC().Truncate(time.Second) }

var hundred = pirn.Call{Model: "claude-sonnet-5-5", Input: 100, Output: 10}

// TestInv_RetriedAttemptsAreCounted: a step that fails twice, then
// succeeds, counts all three calls: the record holds the last, and the
// ledger the two retried attempts, each once (LOOM-320, roadmap 10.4.1).
func TestInv_RetriedAttemptsAreCounted(t *testing.T) {
	s := newStore(t)
	at := now()
	step := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(_ string, a int32) bool { return a < 3 }}
	if res, err := ledgerRun(t, s, nil, `(ask "the news")`, "", at, step); err != nil || res.Commit == "" {
		t.Fatalf("the run: %+v %v", res, err)
	}
	total, by := counted(t, s, at.Add(-time.Hour), at.Add(time.Hour))
	if total != (loom.Tokens{Input: 300, Output: 30}) || by["record"] != (loom.Tokens{Input: 100, Output: 10}) || by[loom.ExtraRetry] != (loom.Tokens{Input: 200, Output: 20}) {
		t.Errorf("counted %+v: %+v", total, by)
	}
}

// TestInv_FailedRunsAreCounted: a run that fails, with no record, counts
// every call its steps made: a step that succeeded before another
// failed, and every attempt of the one that failed (LOOM-321, roadmap
// 10.4.1).
func TestInv_FailedRunsAreCounted(t *testing.T) {
	s := newStore(t)
	at := now()
	reg := claudeRegistry(t)
	p := must(loom.Compile(ctx, reg, `(pipe (ask "a") (ask "b"))`))
	second := string(p.Nodes[1].ID)
	step := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(n string, _ int32) bool { return n == second }}
	if _, err := ledgerRun(t, s, nil, `(pipe (ask "a") (ask "b"))`, "", at, step); err == nil {
		t.Fatal("the run should fail")
	}
	if runs := must(loom.AllRunsBetween(ctx, s, at.Add(-time.Hour), at.Add(time.Hour))); len(runs) != 0 {
		t.Fatalf("a failed run was recorded: %+v", runs)
	}
	total, by := counted(t, s, at.Add(-time.Hour), at.Add(time.Hour))
	if total != (loom.Tokens{Input: 400, Output: 40}) || by[loom.ExtraFailedRun] != (loom.Tokens{Input: 200, Output: 20}) || by[loom.ExtraRetry] != (loom.Tokens{Input: 200, Output: 20}) {
		t.Errorf("counted %+v: %+v", total, by)
	}
}

// meteredAsker answers in turn like scripted, each answer one call
// noted on the meter, as pirn's Claude notes it.
type meteredAsker struct{ scripted }

func (m *meteredAsker) Ask(ctx context.Context, q pirn.Question) (pirn.Answer, error) {
	a, err := m.scripted.Ask(ctx, q)
	a.Model, a.InputTokens, a.OutputTokens = "claude-sonnet-5-5", 1000, 100
	pirn.MeterFrom(ctx).Add(pirn.Call{Model: a.Model, Input: a.InputTokens, Output: a.OutputTokens})
	return a, err
}

// TestInv_FrontDoorAsksAreCounted: the front door's asks count toward
// the month, every attempt, whether a proposal came of them or not
// (LOOM-322, roadmap 10.4.1).
func TestInv_FrontDoorAsksAreCounted(t *testing.T) {
	s := newStore(t)
	from := time.Now().UTC().Add(-time.Hour)
	w := writer(&meteredAsker{scripted{replies: []string{"no JSON here", "```json\n{\"summary\": \"Hi.\", \"program\": \"(echo \\\"hi\\\")\"}\n```"}}})
	w.Ledger = s
	if _, err := w.AskActivity(ctx, loom.AskRequest{Request: "say hi"}); err != nil {
		t.Fatal(err)
	}
	w = writer(&meteredAsker{scripted{replies: []string{"nothing", "nothing", "nothing"}}})
	w.Ledger = s
	if _, err := w.AskActivity(ctx, loom.AskRequest{Request: "say nothing"}); err == nil {
		t.Fatal("no proposal should come of three unusable answers")
	}
	total, by := counted(t, s, from, time.Now().UTC().Add(time.Hour))
	if want := (loom.Tokens{Input: 5000, Output: 500}); total != want || by[loom.ExtraFrontDoor] != want {
		t.Errorf("counted %+v: %+v", total, by)
	}
	start, end := loom.MonthOf(time.Now(), time.UTC)
	if xs := must(loom.ExtrasBetween(ctx, s, start, end, nil, "")); len(xs) != 2 || xs[0].Kind != loom.ExtraFrontDoor {
		t.Errorf("this month's extras: %+v", xs)
	}
}

// TestInv_BudgetsCountWhatRecordsDont: a schedule's budget counts the
// calls its runs made that no record holds: after a scheduled run that
// failed, having spent three cents in failed attempts, the next run of a
// $0.05 schedule is stopped before any step, the schedule paused; one
// that is another schedule's or a run by hand isn't charged to it
// (LOOM-323, roadmap 10.4.1).
func TestInv_BudgetsCountWhatRecordsDont(t *testing.T) {
	s := newStore(t)
	sc := &schedules{budget: map[string]loom.ScheduleBudget{"news": {Budget: loom.Budget{USD: 0.05}}, "other": {Budget: loom.Budget{USD: 0.05}}}}
	nov := now()
	cent := pirn.Call{Model: "claude-sonnet-5-5", Input: 10000 / 3, Output: 1000 / 3} // about a cent
	pricey := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{cent, cent, cent, cent, cent} }, fail: func(string, int32) bool { return true }}
	// A failed run by hand and one of another schedule: not news's.
	if _, err := ledgerRun(t, s, sc, `(ask "the news")`, "", nov, pricey); err == nil {
		t.Fatal("the run should fail")
	}
	if _, err := ledgerRun(t, s, sc, `(ask "the news")`, "other", nov.Add(time.Minute), pricey); err == nil {
		t.Fatal("the other schedule's run should fail")
	}
	cheap := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(string, int32) bool { return false }}
	if res, err := ledgerRun(t, s, sc, `(ask "the news")`, "news", nov.Add(2*time.Minute), cheap); err != nil || res.Skipped != "" {
		t.Fatalf("news's first run: %+v %v", res, err)
	}
	// news's own failed run: 3 attempts of about 5 cents' calls.
	if _, err := ledgerRun(t, s, sc, `(ask "the news")`, "news", nov.Add(3*time.Minute), pricey); err == nil {
		t.Fatal("the run should fail")
	}
	res, err := ledgerRun(t, s, sc, `(ask "the news")`, "news", nov.Add(4*time.Minute), cheap)
	if err != nil || !strings.HasPrefix(res.Skipped, "paused: monthly budget $0.05 reached") || !sc.budget["news"].Paused {
		t.Errorf("after a failed run over budget: %+v %v (schedule %+v)", res, err, sc.budget["news"])
	}
	if sc.budget["other"].Paused {
		t.Error("another schedule was paused")
	}
}

// TestInv_TheLedgerIsItsOwn: an entry is written once however often it
// is written, on its own ref outside refs/loom/ (where runs and branches
// are), and adding up runs never reads the ledger as runs (LOOM-324,
// roadmap 10.4.1).
func TestInv_TheLedgerIsItsOwn(t *testing.T) {
	s := newStore(t)
	at := now()
	e := loom.LedgerEntry{Kind: loom.LedgerFrontDoor, At: at, Workflow: "ask-1", Run: "r1", Calls: []pirn.Call{hundred}}
	for i := 0; i < 3; i++ {
		if err := loom.WriteLedger(ctx, s, e); err != nil {
			t.Fatal(err)
		}
	}
	refs := must(s.Refs(ctx, "refs/"))
	// refs/usage/<month>/<the entry's ID>/<entry.json's blob ID>: a month
	// is read from its refs' names and one batch of blobs.
	shape := regexp.MustCompile(`^refs/usage/2026-11/[0-9a-f]{40}/[0-9a-f]{40,64}$`)
	if len(refs) != 1 || !shape.MatchString(refs[0].Name) || strings.HasPrefix(refs[0].Name, "refs/loom/") {
		t.Errorf("refs: %+v", refs)
	}
	if es := must(loom.ReadLedger(ctx, s, at.Add(-time.Hour), at.Add(time.Hour))); len(es) != 1 || es[0].Calls[0] != hundred {
		t.Errorf("entries: %+v", es)
	}
	if runs := must(loom.AllRunsBetween(ctx, s, at.Add(-time.Hour), at.Add(time.Hour))); len(runs) != 0 {
		t.Errorf("the ledger was read as runs: %+v", runs)
	}
}

// TestLaw_EveryCallIsCountedOnce: with random failures and retries, what
// the Costs page and budgets count (records plus the ledger's extras)
// equals every call Claude answered, exactly once: steps that succeed
// first time, ones that fail and are tried again, runs that fail and are
// never recorded, and front-door asks. Checked on 40 random programs of
// ask steps under pipe and par, each attempt making 0 to 3 calls and
// failing at random (Soundness law 21, extended by roadmap 10.4.1).
func TestLaw_EveryCallIsCountedOnce(t *testing.T) {
	r := rand.New(rand.NewSource(1041))
	var leaf func(d int) string
	leaf = func(d int) string {
		if d == 0 || r.Intn(3) == 0 {
			return fmt.Sprintf(`(ask "q%d")`, r.Intn(1000))
		}
		return fmt.Sprintf("(%s %s %s)", []string{"pipe", "par"}[r.Intn(2)], leaf(d-1), leaf(d-1))
	}
	models := []string{"claude-sonnet-5-5", "claude-haiku-5-5"}
	at := now()
	all := map[string]loom.Tokens{}
	for i := 0; i < 40; i++ {
		// Each trial its own store: Temporal's test environment gives
		// every run the same run ID, which a store records once.
		s := newStore(t)
		var mu sync.Mutex
		var made loom.Tokens
		seed := r.Int63()
		step := claudeStep{
			calls: func(node string, attempt int32) []pirn.Call {
				rr := rand.New(rand.NewSource(seed ^ int64(len(node))<<8 ^ int64(attempt)))
				var cs []pirn.Call
				for n := rr.Intn(4); n > 0; n-- {
					c := pirn.Call{Model: models[rr.Intn(2)], Input: int64(rr.Intn(5000)), Output: int64(rr.Intn(500))}
					cs = append(cs, c)
					mu.Lock()
					made = made.Add(loom.Tokens{Input: c.Input, Output: c.Output})
					mu.Unlock()
				}
				return cs
			},
			fail: func(node string, attempt int32) bool {
				return rand.New(rand.NewSource(seed^int64(len(node))<<16^int64(attempt)<<1)).Intn(3) == 0
			},
		}
		src := leaf(3)
		ledgerRun(t, s, nil, src, "", at, step)
		if r.Intn(3) == 0 {
			a := &meteredAsker{scripted{replies: []string{"no JSON here", "```json\n{\"summary\": \"Hi.\", \"program\": \"(echo \\\"hi\\\")\"}\n```"}}}
			w := writer(a)
			w.Ledger = s
			w.AskActivity(ctx, loom.AskRequest{Request: "say hi"})
			made = made.Add(loom.Tokens{Input: 1000 * int64(len(a.asked)), Output: 100 * int64(len(a.asked))})
		}
		got, by := counted(t, s, at.Add(-time.Hour), time.Now().Add(time.Hour))
		if got != made {
			t.Fatalf("%s: counted %+v (%+v), but Claude answered %+v", src, got, by, made)
		}
		for k, v := range by {
			all[k] = all[k].Add(v)
		}
	}
	by := all
	if by["record"] == (loom.Tokens{}) || by[loom.ExtraRetry] == (loom.Tokens{}) || by[loom.ExtraFailedRun] == (loom.Tokens{}) || by[loom.ExtraFrontDoor] == (loom.Tokens{}) {
		t.Errorf("not every kind of call was tried: %+v", by)
	}
}

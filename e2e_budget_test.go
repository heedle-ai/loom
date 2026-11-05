package loom_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
)

// TestE2E_BudgetPausesASchedule: on a real Temporal dev server, a
// schedule made with a $0.01 budget runs once (three cents of Claude, its
// record naming the schedule), and its second run starts nothing: the
// schedule is paused with the budget's note, and the watcher tells of it
// once. Raising the budget resumes it; a new month resumes one paused by
// its budget only if it's to (ResumeNextMonth). Schedules lists each
// budget. CI's e2e job runs it with LOOM_E2E=1. (Roadmap 10.4.)
func TestE2E_BudgetPausesASchedule(t *testing.T) {
	if os.Getenv("LOOM_E2E") != "1" {
		t.Skip("set LOOM_E2E=1 to run against a real Temporal server")
	}
	c0, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	srv, err := testsuite.StartDevServer(c0, testsuite.DevServerOptions{LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	c := srv.Client()
	const queue = "e2e-budget"
	s := newStore(t)
	reg := claudeRegistry(t)
	w := worker.New(c, queue, worker.Options{})
	loom.RegisterWith(w, &loom.Checker{Registry: reg}, &loom.Recorder{Store: s, Schedules: loom.TemporalSchedules{Client: c}})
	steps := make(chan string, 10)
	w.RegisterActivityWithOptions(func(_ context.Context, in plan.Input) (string, error) {
		steps <- in.NodeID
		return jsonOf(threeCents), nil
	}, activity.RegisterOptions{Name: pirn.ActivityName})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	src := `(ask "the news")`
	in := loom.RunInput{Program: src, Plan: must(loom.Compile(c0, reg, src))}
	// Cron times far off: each run here is one asked for now.
	spec := loom.ScheduleSpec{Cron: "0 0 1 1 *"}
	if err := loom.CreateScheduleWithBudget(c0, c, queue, "news", in, spec, loom.Budget{USD: 0.01}); err != nil {
		t.Fatal(err)
	}
	if err := loom.CreateSchedule(c0, c, queue, "free", in, spec); err != nil {
		t.Fatal(err)
	}
	watcher := &loom.Watcher{Client: c, Queue: queue, Since: time.Now().Add(-time.Minute)}
	h := c.ScheduleClient().GetHandle(c0, loom.SchedulePrefix+"news")
	// runNow asks for a run of news now and waits for it to end.
	runNow := func(n int) loom.RunResult {
		t.Helper()
		if err := loom.ScheduleAction(c0, c, "news", "run"); err != nil {
			t.Fatal(err)
		}
		for {
			d, err := h.Describe(c0)
			if err != nil {
				t.Fatal(err)
			}
			if len(d.Info.RecentActions) >= n {
				a := d.Info.RecentActions[n-1].StartWorkflowResult
				var res loom.RunResult
				if err := c.GetWorkflow(c0, a.WorkflowID, a.FirstExecutionRunID).Get(c0, &res); err != nil {
					t.Fatal(err)
				}
				return res
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	first := runNow(1)
	if first.Skipped != "" || first.Commit == "" || len(steps) != 1 {
		t.Fatalf("first run: %+v, %d steps", first, len(steps))
	}
	head, _, _ := s.Resolve(c0, loom.DefaultRef)
	if ru, _, err := loom.RecordUsage(c0, s, must(s.Log(c0, head, 1))[0]); err != nil || ru.Schedule != "news" {
		t.Fatalf("record: %+v %v", ru, err)
	}
	second := runNow(2)
	month := time.Now().UTC().Format("January 2006")
	note := "paused: monthly budget $0.01 reached (est.) in " + month
	if second.Skipped != note || second.Commit != "" || len(steps) != 1 {
		t.Fatalf("second run: %+v, %d steps", second, len(steps))
	}
	if after, _, _ := s.Resolve(c0, loom.DefaultRef); after != head {
		t.Fatal("the skipped run recorded something")
	}
	d := must(h.Describe(c0))
	if !d.Schedule.State.Paused || d.Schedule.State.Note != note {
		t.Fatalf("schedule %+v", d.Schedule.State)
	}
	// Told once, once Temporal's list has it.
	var told []loom.Notice
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(time.Second) {
		ns, err := watcher.Check(c0)
		if err != nil {
			t.Fatal(err)
		}
		told = append(told, ns...)
		if len(told) >= 2 {
			break
		}
	}
	var paused []loom.Notice
	for _, n := range told {
		if strings.HasSuffix(n.Title, " paused") {
			paused = append(paused, n)
		}
	}
	if len(paused) != 1 || paused[0].Schedule != "news" || !strings.Contains(paused[0].Body, "monthly budget $0.01 reached (est.)") {
		t.Fatalf("told %+v", told)
	}
	if again, _ := watcher.Check(c0); len(again) != 0 {
		t.Errorf("told again: %+v", again)
	}

	ss := must(loom.Schedules(c0, c, queue))
	if len(ss) != 2 || ss[1].Name != "news" || ss[1].Budget == nil || ss[1].Budget.USD != 0.01 || ss[0].Budget != nil || !ss[1].Paused {
		t.Fatalf("schedules %+v", ss)
	}
	// Not to resume in a new month: it stays paused.
	if got := must(loom.ResumeBudgets(c0, c, queue, time.Now().AddDate(0, 1, 0))); len(got) != 0 {
		t.Errorf("resumed %v", got)
	}
	// Lowering the budget leaves it paused; raising it resumes it.
	if err := loom.SetBudget(c0, c, "news", loom.Budget{USD: 0.01, ResumeNextMonth: true}); err != nil || !must(h.Describe(c0)).Schedule.State.Paused {
		t.Fatalf("same budget: %v", err)
	}
	// This month, a schedule to resume in a new month stays paused; in
	// the next, it resumes.
	if got := must(loom.ResumeBudgets(c0, c, queue, time.Now())); len(got) != 0 {
		t.Errorf("resumed this month: %v", got)
	}
	if got := must(loom.ResumeBudgets(c0, c, queue, time.Now().AddDate(0, 1, 0))); len(got) != 1 || got[0] != "news" || must(h.Describe(c0)).Schedule.State.Paused {
		t.Fatalf("a new month: %v", got)
	}
	h.Pause(c0, client.SchedulePauseOptions{Note: note})
	if err := loom.SetBudget(c0, c, "news", loom.Budget{USD: 0.10}); err != nil {
		t.Fatal(err)
	}
	if d := must(h.Describe(c0)); d.Schedule.State.Paused {
		t.Errorf("raised budget, still paused: %+v", d.Schedule.State)
	}
	if b := must((loom.TemporalSchedules{Client: c}).Budget(c0, "news")); b.Budget != (loom.Budget{USD: 0.10}) {
		t.Errorf("budget now %+v", b)
	}
	// Under the new budget, it runs.
	if third := runNow(3); third.Skipped != "" || len(steps) != 2 {
		t.Errorf("third run: %+v", third)
	}
}

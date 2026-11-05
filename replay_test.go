package loom_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle"
	"github.com/heedle-ai/shuttle/plan"
)

// actResult is an act step's output that ended as ended, with a script of
// steps actions whose first says which run kept it.
func actResult(t *testing.T, ended string, steps int, mark string) string {
	t.Helper()
	r := pirn.ActResult{Answer: pirn.Answer{Pirn: 1, Model: "claude-sonnet-5-5", Text: mark}, Act: 1, Image: "img", Calls: []pirn.ActCall{}, Allowed: []string{"forms.example.org"},
		Blocked: []string{}, Inputs: []string{}, Held: []string{}, Ended: ended, Script: []pirn.ScriptStep{}}
	for i := 0; i < steps; i++ {
		r.Script = append(r.Script, pirn.ScriptStep{Tool: "browser_navigate", Input: fmt.Sprintf(`{"url":"https://forms.example.org/%s/%d"}`, mark, i), OutlineSHA: strings.Repeat("a", 64)})
	}
	b := must(json.Marshal(r))
	if _, ok := pirn.ParseAct(string(b)); !ok {
		t.Fatalf("not an act result: %s", b)
	}
	return string(b)
}

// actRegistry is draft's with act.
func actRegistry(t *testing.T) *registry.Registry {
	reg := draft.DefaultRegistry()
	if err := reg.Register(pirn.ActBuiltinSpec()); err != nil {
		t.Fatal(err)
	}
	return reg
}

// TestInv_ScriptIsTheProgramsLast: an act step's script is the one the
// last successful run of the same program (its Loom-Program) kept for
// that step: never another program's, even one with the same step at the
// same node; not a run whose step didn't finish or kept no script (an
// older one that did is used); not another ref's; and not a node that
// isn't an act step there. (Roadmap 9.9.)
func TestInv_ScriptIsTheProgramsLast(t *testing.T) {
	s := newStore(t)
	reg := actRegistry(t)
	rec := &loom.Recorder{Store: s}
	progA, progB := `(act "Sign me up." "https://forms.example.org/a")`, `(act "Sign me up." "https://forms.example.org/b")`
	at := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	record := func(program, ref, out, run string) {
		t.Helper()
		p := must(loom.Compile(ctx, reg, program))
		at = at.Add(time.Minute)
		if _, err := rec.Record(ctx, loom.RecordInput{Program: program, Plan: p, Ref: ref, Result: plan.Result{Outputs: map[string]string{"n0": out}}, WorkflowID: "w-" + run, RunID: run, At: at}); err != nil {
			t.Fatal(err)
		}
	}
	id := func(program, ref string) string {
		return must(loom.WorkflowID(ctx, loom.RunInput{Program: program, Plan: must(loom.Compile(ctx, reg, program)), Ref: ref}))
	}
	find := func(program, ref, node string) (pirn.ActScript, bool) {
		t.Helper()
		got, ok, err := loom.LastScript(ctx, s, ref, id(program, ref), node)
		if err != nil {
			t.Fatal(err)
		}
		return got, ok
	}
	if _, ok := find(progA, loom.DefaultRef, "n0"); ok {
		t.Error("a script before any run")
	}
	record(progA, loom.DefaultRef, actResult(t, pirn.EndedFinished, 2, "a1"), "a1")
	record(progB, loom.DefaultRef, actResult(t, pirn.EndedFinished, 3, "b1"), "b1")
	record(progA, loom.DefaultRef, actResult(t, pirn.EndedStopped, 1, "a2"), "a2")
	record(progA, loom.DefaultRef, actResult(t, pirn.EndedFinished, 0, "a3"), "a3")
	if got, ok := find(progA, loom.DefaultRef, "n0"); !ok || got.Run != "a1" || got.Result.Text != "a1" || len(got.Result.Script) != 2 || len(got.Record) != 40 {
		t.Errorf("program A: %+v %v", got, ok)
	}
	if got, ok := find(progB, loom.DefaultRef, "n0"); !ok || got.Run != "b1" || len(got.Result.Script) != 3 {
		t.Errorf("program B: %+v %v", got, ok)
	}
	record(progA, loom.DefaultRef, actResult(t, pirn.EndedFinished, 1, "a4"), "a4")
	if got, ok := find(progA, loom.DefaultRef, "n0"); !ok || got.Run != "a4" {
		t.Errorf("not the last: %+v", got)
	}
	if _, ok := find(progA, loom.DefaultRef, "n1"); ok {
		t.Error("a script for a node the plan hasn't")
	}
	if _, ok := find(progA, "refs/loom/other", "n0"); ok {
		t.Error("a script from another ref's runs")
	}
	// A step that isn't an act step: never read as one.
	echo := `(echo "Sign me up.")`
	record(echo, loom.DefaultRef, actResult(t, pirn.EndedFinished, 1, "e1"), "e1")
	if _, ok := find(echo, loom.DefaultRef, "n0"); ok {
		t.Error("an echo's output read as a script")
	}
	if _, _, err := loom.LastScript(ctx, s, loom.DefaultRef, "", "n0"); err == nil {
		t.Error("no program accepted")
	}
}

// TestInv_UsageSaysReplayed: an act step that replayed a script is
// counted with only Claude's tokens (none, for a whole replay), and says
// it replayed: "all" when Claude wasn't asked, "part" when it handed the
// rest to Claude. (Roadmap 9.9.)
func TestInv_UsageSaysReplayed(t *testing.T) {
	p := plan.Plan{Nodes: []plan.Node{{ID: "n0", Activity: pirn.ActActivityName}, {ID: "n1", Activity: pirn.ActActivityName}, {ID: "n2", Activity: pirn.ActActivityName}}}
	whole, part, none := &pirn.ActReplay{Steps: 3, Replayed: 3}, &pirn.ActReplay{Steps: 3, Replayed: 1, HandedOver: true, At: 2}, &pirn.ActReplay{Steps: 3, HandedOver: true, At: 1}
	// Review 51: one a "no" cut short is part of a replay, not all of it.
	cut := loom.UsageOf(plan.Plan{Nodes: p.Nodes[:1]}, map[string]string{"n0": func() string {
		res := pirn.ActResult{Answer: pirn.Answer{Pirn: 1, Model: "claude-sonnet-5-5"}, Act: 1, Calls: []pirn.ActCall{}, Allowed: []string{}, Blocked: []string{}, Inputs: []string{}, Held: []string{},
			Ended: pirn.EndedCancelled, Replay: &pirn.ActReplay{Steps: 4, Replayed: 2, At: 3, Why: "a send there wasn't confirmed"}}
		return string(must(json.Marshal(res)))
	}()})
	if cut.Steps[0].Replayed != "part" {
		t.Errorf("a replay cut short: %+v", cut.Steps[0])
	}
	out := func(r *pirn.ActReplay, tokens int64, turns int) string {
		res := pirn.ActResult{Answer: pirn.Answer{Pirn: 1, Model: "claude-sonnet-5-5", InputTokens: tokens, OutputTokens: tokens}, Act: 1, Turns: turns, Calls: []pirn.ActCall{}, Allowed: []string{},
			Blocked: []string{}, Inputs: []string{}, Held: []string{}, Ended: pirn.EndedFinished, Replay: r}
		return string(must(json.Marshal(res)))
	}
	u := loom.UsageOf(p, map[string]string{"n0": out(whole, 0, 0), "n1": out(part, 40, 2), "n2": out(none, 90, 3)})
	got := fmt.Sprintf("%s %v %d, %s %v, %q %d", u.Steps[0].Replayed, u.Steps[0].Tokens, u.Steps[0].Requests, u.Steps[1].Replayed, u.Steps[1].Tokens, u.Steps[2].Replayed, u.Steps[2].Requests)
	if got != `all {0 0} 0, part {40 40}, "" 3` || u.Steps[0].Missing != "" {
		t.Errorf("usage %s: %+v", got, u.Steps)
	}
}

// TestE2E_ScriptsFindTheRunsProgram: on a real Temporal server, an act
// step finds its script through Temporal itself (loom.Scripts reads its
// run's input from the run's history): the second run of a program finds
// the first's, a run its schedule starts finds the last one's, and a run
// of another program, with the same step at the same node, finds none. CI's e2e job runs it with LOOM_E2E=1. (Roadmap
// 9.9.)
func TestE2E_ScriptsFindTheRunsProgram(t *testing.T) {
	if os.Getenv("LOOM_E2E") != "1" {
		t.Skip("set LOOM_E2E=1 to run against a real Temporal server")
	}
	c0, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	opts := testsuite.DevServerOptions{LogLevel: "error"}
	opts.ExistingPath, _ = exec.LookPath("temporal")
	srv, err := testsuite.StartDevServer(c0, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	c := srv.Client()
	const queue = "e2e-scripts"
	reg := actRegistry(t)
	s := newStore(t)
	w := worker.New(c, queue, worker.Options{})
	loom.RegisterWith(w, &loom.Checker{Registry: reg}, &loom.Recorder{Store: s, Schedules: loom.TemporalSchedules{Client: c}})
	scripts := loom.Scripts(c, s)
	// A stand-in act step: it says what script it found, and keeps one.
	w.RegisterActivityWithOptions(shuttle.ArrowAsActivity(func(ctx context.Context, in plan.Input) (string, error) {
		found, ok, err := scripts(ctx, in.NodeID)
		if err != nil {
			return "", err
		}
		mark := "none"
		if ok {
			mark = "from " + found.Run
		}
		return actResult(t, pirn.EndedFinished, 1, mark), nil
	}), activity.RegisterOptions{Name: pirn.ActActivityName})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	run := func(program string) (string, string) {
		t.Helper()
		r, err := loom.Submit(c0, c, queue, loom.RunInput{Program: program, Plan: must(loom.Compile(c0, reg, program))})
		if err != nil {
			t.Fatal(err)
		}
		var res loom.RunResult
		if err := r.Get(c0, &res); err != nil {
			t.Fatal(err)
		}
		got, _ := pirn.ParseAct(res.Outputs["n0"])
		return got.Text, r.GetRunID()
	}
	a := `(act "Sign me up." "https://forms.example.org/a")`
	first, firstRun := run(a)
	second, secondRun := run(a)
	other, _ := run(`(act "Sign me up." "https://forms.example.org/b")`)
	if first != "none" || second != "from "+firstRun || other != "none" {
		t.Errorf("first %q, second %q (want from %s), another program %q", first, second, firstRun, other)
	}
	// A run a schedule started (its workflow ID has the time Temporal
	// adds) finds its program's last script too (review 51).
	in := loom.RunInput{Program: a, Plan: must(loom.Compile(c0, reg, a))}
	if err := loom.CreateSchedule(c0, c, queue, "replay", in, loom.ScheduleSpec{Cron: "0 0 1 1 *"}); err != nil {
		t.Fatal(err)
	}
	if err := loom.ScheduleAction(c0, c, "replay", "run"); err != nil {
		t.Fatal(err)
	}
	h := c.ScheduleClient().GetHandle(c0, loom.SchedulePrefix+"replay")
	for {
		d, err := h.Describe(c0)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Info.RecentActions) > 0 {
			act := d.Info.RecentActions[0].StartWorkflowResult
			var res loom.RunResult
			if err := c.GetWorkflow(c0, act.WorkflowID, act.FirstExecutionRunID).Get(c0, &res); err != nil {
				t.Fatal(err)
			}
			got, _ := pirn.ParseAct(res.Outputs["n0"])
			if !strings.HasPrefix(act.WorkflowID, must(loom.WorkflowID(c0, loom.RunInput{Program: a, Plan: in.Plan, Ref: loom.DefaultRef}))+"-") || got.Text != "from "+secondRun {
				t.Errorf("a scheduled run %s found %q, want from %s", act.WorkflowID, got.Text, secondRun)
			}
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, ok, _ := s.Resolve(c0, loom.DefaultRef); !ok {
		t.Error("nothing recorded")
	}
}

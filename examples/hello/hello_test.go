package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/loom/examples/hello/shipment"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

var ctx = context.Background()

// world is a temporary copy of the sources plus a warp store.
type world struct {
	t       *testing.T
	sources string
	store   *warp.Store
	runs    int
}

func newWorld(t *testing.T) *world {
	dir := t.TempDir()
	for _, name := range []string{"weather", "carrier", "factory"} {
		b, err := os.ReadFile(filepath.Join("sources", name+".txt"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".txt"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// HELLO_STORE keeps the store after the test, to inspect it with git.
	storeDir := filepath.Join(dir, "store.git")
	if keep := os.Getenv("HELLO_STORE"); keep != "" {
		storeDir = keep
	}
	s, err := warp.Init(ctx, storeDir)
	if err != nil {
		t.Fatal(err)
	}
	return &world{t: t, sources: dir, store: s}
}

func (w *world) set(name, signal, summary string) {
	if err := os.WriteFile(filepath.Join(w.sources, name+".txt"), []byte(signal+"\n"+summary+"\n"), 0o644); err != nil {
		w.t.Fatal(err)
	}
}

// run executes the hello program with shuttle and records it with loom's
// Recorder, as RunWorkflow does, giving each run its own run ID.
func (w *world) run() (plan.Result, warp.ID) {
	w.t.Helper()
	p, err := loom.Compile(ctx, shipment.Registry(), shipment.Program)
	if err != nil {
		w.t.Fatal(err)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	plan.Register(env)
	shipment.RegisterCommands(env, w.sources)
	env.ExecuteWorkflow(plan.WorkflowName, p)
	if err := env.GetWorkflowError(); err != nil {
		w.t.Fatal(err)
	}
	var res plan.Result
	if err := env.GetWorkflowResult(&res); err != nil {
		w.t.Fatal(err)
	}
	w.runs++
	in := loom.RunInput{Program: shipment.Program, Plan: p, Ref: loom.DefaultRef}
	id, _ := loom.WorkflowID(ctx, in)
	c, err := (&loom.Recorder{Store: w.store}).Record(ctx, loom.RecordInput{
		Program: shipment.Program, Plan: p, Result: res, Ref: loom.DefaultRef,
		WorkflowID: id, RunID: "run-" + string(rune('0'+w.runs)), At: time.Unix(1761900000+int64(w.runs)*60, 0),
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return res, c
}

func (w *world) git(args ...string) string {
	w.t.Helper()
	out, err := exec.Command("git", append([]string{"--git-dir", w.store.Dir()}, args...)...).CombinedOutput()
	if err != nil {
		w.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestHello_EvidenceChangesAndGitShowsWhy(t *testing.T) {
	w := newWorld(t)

	// Run 1: two sources signal a delay.
	res, c1 := w.run()
	want := map[string]string{
		"n0": "weather: delay: Storm closes the port of Long Beach for three days.\n",
		"n1": "carrier: on-time: Carrier confirms the container is loaded and sailing on schedule.\n",
		"n2": "factory: delay: Factory line 2 is down for maintenance until Friday.\n",
		"n3": "the shipment will be late: SUPPORTED\n  for:     factory, weather\n  against: carrier\n",
	}
	for id, v := range want {
		if res.Outputs[id] != v {
			t.Errorf("run 1 %s = %q, want %q", id, res.Outputs[id], v)
		}
	}

	// Run 2: the storm passes. Still supported, now by the factory alone.
	w.set("weather", "on-time", "Storm has passed; the port reopened this morning.")
	res, c2 := w.run()
	if got := res.Outputs["n3"]; got != "the shipment will be late: SUPPORTED\n  for:     factory\n  against: carrier, weather\n" {
		t.Errorf("run 2 conclusion = %q", got)
	}

	// Run 3: the factory is fixed. No source supports the claim any more.
	w.set("factory", "on-time", "Line 2 is repaired; production resumed.")
	res, c3 := w.run()
	if got := res.Outputs["n3"]; got != "the shipment will be late: NOT SUPPORTED\n  for:     (none)\n  against: carrier, factory, weather\n" {
		t.Errorf("run 3 conclusion = %q", got)
	}

	// History: three commits, each on top of the last.
	if log := w.git("rev-list", loom.DefaultRef); log != strings.Join([]string{string(c3), string(c2), string(c1)}, "\n") {
		t.Errorf("history = %q", log)
	}
	// Provenance with plain git: exactly the changed source and the
	// conclusion differ between consecutive runs, plus the beliefs index
	// entry of this question, which now names the newer run.
	p, err := loom.Compile(ctx, shipment.Registry(), shipment.Program)
	if err != nil {
		t.Fatal(err)
	}
	q, err := loom.QuestionID(ctx, shipment.Program, p)
	if err != nil {
		t.Fatal(err)
	}
	if d := w.git("diff", "--name-only", string(c1), string(c2)); d != "beliefs/"+q+"\noutputs/n0\noutputs/n3" {
		t.Errorf("run 1 -> 2 changed %q, want the belief, weather (n0) and the conclusion (n3)", d)
	}
	if d := w.git("diff", "--name-only", string(c2), string(c3), "--", "outputs"); d != "outputs/n2\noutputs/n3" {
		t.Errorf("run 2 -> 3 changed %q, want factory (n2) and the conclusion (n3)", d)
	}
	// The program and plan didn't change: the same blobs are reused.
	if w.git("rev-parse", string(c1)+":plan.json") != w.git("rev-parse", string(c3)+":plan.json") {
		t.Error("plan.json changed between runs of the same program")
	}
	w.git("fsck", "--strict")
}

func TestHello_RunsThroughLoom(t *testing.T) {
	// The full composition, RunWorkflow = PlanWorkflow >>> Record, with the
	// example's commands registered on the same worker.
	w := newWorld(t)
	p, err := loom.Compile(ctx, shipment.Registry(), shipment.Program)
	if err != nil {
		t.Fatal(err)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.Register(env, w.store, shipment.Registry())
	shipment.RegisterCommands(env, w.sources)
	env.ExecuteWorkflow(loom.RunWorkflowName, loom.RunInput{Program: shipment.Program, Plan: p})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var res loom.RunResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Outputs["n3"], "SUPPORTED\n  for:     factory, weather") {
		t.Errorf("conclusion = %q", res.Outputs["n3"])
	}
	if got := w.git("show", string(res.Commit)+":outputs/n3"); !strings.HasPrefix(got, "the shipment will be late: SUPPORTED") {
		t.Errorf("recorded conclusion = %q", got)
	}
}

func TestHello_BadSourceNameIsRejected(t *testing.T) {
	w := newWorld(t)
	_, err := shipment.Source(w.sources)(ctx, plan.Input{Args: []string{"../../etc/passwd"}})
	if err == nil {
		t.Fatal("path traversal accepted")
	}
}

func TestHello_SourcesAreStrict(t *testing.T) {
	w := newWorld(t)
	src := shipment.Source(w.sources)

	// A signal with no summary is still read correctly...
	w.set("weather", "delay", "")
	out, err := src(ctx, plan.Input{Args: []string{"weather"}})
	if err != nil || out != "weather: delay\n" {
		t.Fatalf("summary-less source = %q, %v", out, err)
	}
	// ...and counts for the claim.
	verdict, err := shipment.Assess()(ctx, plan.Input{Args: []string{"late"}, Upstream: map[string]string{"n0": out}})
	if err != nil || !strings.Contains(verdict, "SUPPORTED\n  for:     weather") {
		t.Fatalf("verdict = %q, %v", verdict, err)
	}

	// Unknown signals, missing files, symlinks and missing args are rejected.
	w.set("carrier", "Delay", "capitalised")
	if err := os.Symlink("/etc/hostname", filepath.Join(w.sources, "link.txt")); err != nil {
		t.Fatal(err)
	}
	for name, in := range map[string]plan.Input{
		"unknown signal": {Args: []string{"carrier"}},
		"missing file":   {Args: []string{"nowhere"}},
		"symlink":        {Args: []string{"link"}},
		"no args":        {},
	} {
		_, err := src(ctx, in)
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) || !appErr.NonRetryable() {
			t.Errorf("%s: err = %v, want a non-retryable error", name, err)
		}
	}
	if _, err := shipment.Assess()(ctx, plan.Input{Args: []string{"late"}, Upstream: map[string]string{"n0": "garbage"}}); err == nil {
		t.Error("assess accepted a non-report input")
	}
	if _, err := shipment.Assess()(ctx, plan.Input{}); err == nil {
		t.Error("assess accepted no claim")
	}
}

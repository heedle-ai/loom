package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/loom/examples/hello/shipment"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// slip is a second question, about the factory alone.
const slip = `(pipe (source "factory") (assess "production will slip"))
`

// runOn executes program with shuttle on Temporal's test environment and
// records it on ref with loom's Recorder, with its own run ID.
func (w *world) runOn(ref, program string) warp.ID {
	w.t.Helper()
	p, err := loom.Compile(ctx, shipment.Registry(), program)
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
	c, err := (&loom.Recorder{Store: w.store}).Record(ctx, loom.RecordInput{
		Program: program, Plan: p, Result: res, Ref: ref, WorkflowID: "wf",
		RunID: fmt.Sprintf("run-%d", w.runs), At: time.Unix(1761900000+int64(w.runs)*60, 0),
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return c
}

// workflow runs one of loom's workflows on Temporal's test environment.
func (w *world) workflow(name string, in, out any) error {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.Register(env, w.store, shipment.Registry())
	shipment.RegisterCommands(env, w.sources)
	env.ExecuteWorkflow(name, in)
	if err := env.GetWorkflowError(); err != nil {
		return err
	}
	return env.GetWorkflowResult(out)
}

// believed returns what ref believes about the shipment and the factory.
func (w *world) believed(ref string) (late, slips string) {
	w.t.Helper()
	head, _, err := w.store.Resolve(ctx, ref)
	if err != nil {
		w.t.Fatal(err)
	}
	bs, err := loom.Beliefs(ctx, w.store, head)
	if err != nil {
		w.t.Fatal(err)
	}
	for _, b := range bs {
		switch b.Program {
		case shipment.Program:
			late = b.Outputs["n3"]
		case slip:
			slips = b.Outputs["n1"]
		}
	}
	return late, slips
}

// TestHello_WhatIfBranchAndMerge is the Branching and Merging page's
// story: explore a what-if on a branch, keep working on main, merge. The
// question asked only on the branch joins main as is; the question both
// sides answered differently is a conflict, resolved by running it again.
func TestHello_WhatIfBranchAndMerge(t *testing.T) {
	w := newWorld(t)
	const main, whatif = loom.DefaultRef, "refs/loom/what-if"
	w.runOn(main, shipment.Program) // storm, factory down: SUPPORTED by factory and weather

	var head warp.ID
	if err := w.workflow(loom.BranchWorkflowName, loom.BranchInput{From: main, To: whatif}, &head); err != nil {
		t.Fatal(err)
	}

	// On the branch: what if the factory were fixed?
	w.set("factory", "on-time", "Line 2 is repaired; production resumed.")
	w.runOn(whatif, shipment.Program)
	w.runOn(whatif, slip)
	late, slips := w.believed(whatif)
	t.Logf("what-if believes:\n%s%s", late, slips)
	if late != "the shipment will be late: SUPPORTED\n  for:     weather\n  against: carrier, factory\n" ||
		slips != "production will slip: NOT SUPPORTED\n  for:     (none)\n  against: factory\n" {
		t.Fatalf("what-if: %q %q", late, slips)
	}

	// Meanwhile on main, in reality: the storm passes, the factory is still down.
	w.set("factory", "delay", "Factory line 2 is down for maintenance until Friday.")
	w.set("weather", "on-time", "Storm has passed; the port reopened this morning.")
	w.runOn(main, shipment.Program)
	late, _ = w.believed(main)
	if late != "the shipment will be late: SUPPORTED\n  for:     factory\n  against: carrier, weather\n" {
		t.Fatalf("main: %q", late)
	}

	// Merging without a resolution refuses: both sides changed the
	// shipment question. Nothing moves.
	before, _, _ := w.store.Resolve(ctx, main)
	var res loom.MergeResult
	err := w.workflow(loom.MergeWorkflowName, loom.MergeInput{Into: main, From: whatif}, &res)
	if err == nil || !strings.Contains(err.Error(), "MergeConflict") || !strings.Contains(err.Error(), `(pipe`) {
		t.Fatalf("merge without resolve: %v", err)
	}
	if after, _, _ := w.store.Resolve(ctx, main); after != before {
		t.Fatal("a refused merge moved main")
	}
	t.Logf("refused: %v", err)

	// The factory really is fixed now; merge, re-running the conflict on
	// today's evidence.
	w.set("factory", "on-time", "Line 2 is repaired; production resumed.")
	if err := w.workflow(loom.MergeWorkflowName, loom.MergeInput{Into: main, From: whatif, Resolve: loom.ResolveRerun}, &res); err != nil {
		t.Fatal(err)
	}
	late, slips = w.believed(main)
	t.Logf("main after merge:\n%s%s", late, slips)
	if late != "the shipment will be late: NOT SUPPORTED\n  for:     (none)\n  against: carrier, factory, weather\n" {
		t.Errorf("rerun belief: %q", late)
	}
	if slips != "production will slip: NOT SUPPORTED\n  for:     (none)\n  against: factory\n" {
		t.Errorf("the branch-only question didn't join main: %q", slips)
	}
	// Every run from both sides is in main's history: 2 on main, 2 on the
	// branch, the merge, and the rerun.
	if n := w.git("rev-list", "--count", main); n != "6" {
		t.Errorf("main's history has %s commits, want 6", n)
	}
	t.Logf("log:\n%s", w.git("log", "--graph", "--format=%s", main))
	w.git("fsck", "--strict")
}

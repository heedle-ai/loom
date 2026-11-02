package loom_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// pinned is a registry whose command "weather" is Bound to version v, as
// treadle registers a skill.
func pinned(v string) *registry.Registry {
	reg := draft.DefaultRegistry()
	reg.MustRegister(registry.BuiltinSpec{Name: "weather", AgentID: "Skill", Bound: []string{"weather", v},
		ArgSchema: registry.ArgSchema{Variadic: true, VariadicType: registry.StringT}, Backends: []registry.Backend{registry.BackendTemporal}})
	return reg
}

// TestLaw_PinnedRunsCheckAgainstTheirPins: a run is checked against the
// versions its plan pins, not the newest: a plan compiled with version 1
// still checks once the worker's newest is version 2, and a plan whose
// program doesn't compile to it with its own pins still fails. Without
// RegistryFor, the newest is used, and the old plan is refused.
func TestLaw_PinnedRunsCheckAgainstTheirPins(t *testing.T) {
	src := `(pipe (echo "a") (weather "Boston"))`
	old := must(loom.Compile(ctx, pinned("v1"), src))
	forPlan := func(_ context.Context, p plan.Plan) (*registry.Registry, error) {
		for _, n := range p.Nodes {
			if n.Activity == "Skill" {
				return pinned(n.Args[1]), nil
			}
		}
		return pinned("v2"), nil
	}
	in := loom.RunInput{Program: src, Plan: old, Ref: loom.DefaultRef}
	if err := (&loom.Checker{Registry: pinned("v2"), RegistryFor: forPlan}).Check(ctx, in); err != nil {
		t.Errorf("a run pinned to v1 failed its check after v2: %v", err)
	}
	if err := (&loom.Checker{Registry: pinned("v2")}).Check(ctx, in); !mismatch(err) {
		t.Errorf("without its pins, an old plan passed: %v", err)
	}
	forged := old
	forged.Nodes = append([]plan.Node(nil), old.Nodes...)
	forged.Nodes[1].Args = []string{"weather", "v1", "Paris"}
	if err := (&loom.Checker{Registry: pinned("v2"), RegistryFor: forPlan}).Check(ctx, loom.RunInput{Program: src, Plan: forged, Ref: loom.DefaultRef}); !mismatch(err) {
		t.Errorf("a plan the program doesn't compile to passed: %v", err)
	}
	refuse := func(context.Context, plan.Plan) (*registry.Registry, error) {
		return nil, fmt.Errorf("%w: no such version", warp.ErrInvalid)
	}
	if err := (&loom.Checker{Registry: pinned("v2"), RegistryFor: refuse}).Check(ctx, in); !mismatch(err) {
		t.Errorf("an unknown pin passed: %v", err)
	}
	// Review 13: a store that failed to answer failed the run for good.
	flaky := func(context.Context, plan.Plan) (*registry.Registry, error) {
		return nil, errors.New("git: resource busy")
	}
	var app *temporal.ApplicationError
	if err := (&loom.Checker{Registry: pinned("v2"), RegistryFor: flaky}).Check(ctx, in); err == nil || errors.As(err, &app) {
		t.Errorf("a transient failure wasn't retryable: %v", err)
	}
}

func mismatch(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && app.Type() == "ProgramPlanMismatch"
}

// TestInv_RecordsKeepWhatTheyUse: a record holds the trees its plan uses
// under uses/<name>, so its history keeps them wherever it goes; a plan
// that uses none has no uses/ directory.
func TestInv_RecordsKeepWhatTheyUse(t *testing.T) {
	s := newStore(t)
	skill := must(s.PutTree(ctx, []warp.Entry{{Name: "SKILL.md", Kind: warp.KindBlob, ID: must(s.PutBlob(ctx, []byte("x")))}}))
	src := `(weather "Boston")`
	p := must(loom.Compile(ctx, pinned(string(skill)), src))
	rec := &loom.Recorder{Store: s, Uses: func(_ context.Context, p plan.Plan) ([]warp.Entry, error) {
		return []warp.Entry{{Name: p.Nodes[0].Args[0], Kind: warp.KindTree, ID: warp.ID(p.Nodes[0].Args[1])}}, nil
	}}
	c := must(rec.Record(ctx, loom.RecordInput{Program: src, Plan: p, Result: plan.Result{Outputs: map[string]string{"n0": "sunny"}},
		Ref: loom.DefaultRef, WorkflowID: "wf", RunID: "r1", At: time.Unix(1700000000, 0)}))
	tree, _ := must2(s.Parents(ctx, c))
	if got := entry(t, s, entry(t, s, tree, loom.UsesDir).ID, "weather"); got.ID != skill || got.Kind != warp.KindTree {
		t.Errorf("uses/weather = %+v", got)
	}
	// The same run, recorded again, is the same record.
	if again := must(rec.Record(ctx, loom.RecordInput{Program: src, Plan: p, Result: plan.Result{Outputs: map[string]string{"n0": "sunny"}},
		Ref: loom.DefaultRef, WorkflowID: "wf", RunID: "r1", At: time.Unix(1700000000, 0)})); again != c {
		t.Errorf("recording again made %s, want %s", again, c)
	}
	plain := newStore(t)
	tree2, _ := must2(plain.Parents(ctx, must(record(plain, "run-1", time.Unix(1700000000, 0)))))
	for _, e := range must(plain.Tree(ctx, tree2)) {
		if e.Name == loom.UsesDir {
			t.Error("a plan without uses has a uses/ directory")
		}
	}
}

package loom_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

var ctx = context.Background()

const program = `(pipe (par (echo "a") (echo "b")) echo)`

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func newStore(t *testing.T) *warp.Store {
	t.Helper()
	return must(warp.Init(ctx, t.TempDir()+"/store.git"))
}

func compile(t *testing.T, src string) plan.Plan {
	t.Helper()
	return must(loom.Compile(ctx, draft.DefaultRegistry(), src))
}

// runLoom executes RunWorkflow, with shuttle's PlanWorkflow as its child,
// in Temporal's in-process test environment.
func runLoom(t *testing.T, s *warp.Store, src string) loom.RunResult {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.Register(env, s)
	env.ExecuteWorkflow(loom.RunWorkflowName, loom.RunInput{Program: src, Plan: compile(t, src), Ref: loom.DefaultRef})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	var res loom.RunResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	return res
}

// runShuttle executes the plan with shuttle alone, no loom.
func runShuttle(t *testing.T, p plan.Plan) plan.Result {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	plan.Register(env)
	env.ExecuteWorkflow(plan.WorkflowName, p)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var res plan.Result
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	return res
}

func entry(t *testing.T, s *warp.Store, tree warp.ID, name string) warp.Entry {
	t.Helper()
	for _, e := range must(s.Tree(ctx, tree)) {
		if e.Name == name {
			return e
		}
	}
	t.Fatalf("tree %s has no %q", tree, name)
	return warp.Entry{}
}

func TestRun_EndToEnd(t *testing.T) {
	s := newStore(t)
	res := runLoom(t, s, program)

	if res.Outputs["n2"] != "a b" {
		t.Errorf("n2 = %q, want %q", res.Outputs["n2"], "a b")
	}
	head, ok, _ := s.Resolve(ctx, loom.DefaultRef)
	if !ok || head != res.Commit {
		t.Fatalf("ref head = %s (exists %v), want the run's commit %s", head, ok, res.Commit)
	}

	tree, parents := must2(s.Parents(ctx, res.Commit))
	if len(parents) != 0 {
		t.Errorf("first run has parents %v", parents)
	}
	if got := string(must(s.Blob(ctx, entry(t, s, tree, "program.draft").ID))); got != program {
		t.Errorf("program.draft = %q", got)
	}
	if got := must(s.Blob(ctx, entry(t, s, tree, "plan.json").ID)); !bytes.Equal(got, must(loom.PlanJSON(compile(t, program)))) {
		t.Errorf("plan.json differs from the compiled plan")
	}
	outs := entry(t, s, tree, "outputs").ID
	for node, want := range res.Outputs {
		if got := string(must(s.Blob(ctx, entry(t, s, outs, node).ID))); got != want {
			t.Errorf("outputs/%s = %q, want %q", node, got, want)
		}
	}
	trailers := fmt.Sprint(must(s.Trailers(ctx, res.Commit)))
	for _, k := range []string{loom.TrailerWorkflow, loom.TrailerRun} {
		if !strings.Contains(trailers, k) {
			t.Errorf("trailers %s lack %s", trailers, k)
		}
	}
	if out, err := exec.Command("git", "--git-dir", s.Dir(), "fsck", "--strict").CombinedOutput(); err != nil {
		t.Fatalf("git fsck: %v\n%s", err, out)
	}
}

func must2[A, B any](a A, b B, err error) (A, B) {
	if err != nil {
		panic(err)
	}
	return a, b
}

func record(s *warp.Store, runID string, at time.Time) (warp.ID, error) {
	r := &loom.Recorder{Store: s}
	p := must(loom.Compile(ctx, draft.DefaultRegistry(), `(echo "x")`))
	return r.Record(ctx, loom.RecordInput{
		Program: `(echo "x")`, Plan: p, Result: plan.Result{Outputs: map[string]string{"n0": "x"}},
		Ref: loom.DefaultRef, WorkflowID: "wf", RunID: runID, At: at,
	})
}

func TestRecord_Idempotent(t *testing.T) {
	s := newStore(t)
	at := time.Unix(1700000000, 0)
	c1 := must(record(s, "run-1", at))
	c2 := must(record(s, "run-1", at)) // a Temporal retry after success
	if c1 != c2 {
		t.Fatalf("re-recording the same run made a new commit: %s %s", c1, c2)
	}
	if head, _, _ := s.Resolve(ctx, loom.DefaultRef); head != c1 {
		t.Errorf("ref moved on a repeated record")
	}
}

func TestRecord_SameRunDifferentContentIsRejected(t *testing.T) {
	s := newStore(t)
	at := time.Unix(1700000000, 0)
	first := must(record(s, "run-1", at))
	r := &loom.Recorder{Store: s}
	p := must(loom.Compile(ctx, draft.DefaultRegistry(), `(echo "y")`))
	_, err := r.Record(ctx, loom.RecordInput{
		Program: `(echo "y")`, Plan: p, Result: plan.Result{Outputs: map[string]string{"n0": "y"}},
		Ref: loom.DefaultRef, WorkflowID: "wf", RunID: "run-1", At: at,
	})
	if err == nil || !strings.Contains(err.Error(), "different content") {
		t.Fatalf("err = %v, want a RecordMismatch error", err)
	}
	if head, _, _ := s.Resolve(ctx, loom.DefaultRef); head != first {
		t.Error("ref moved on a mismatched record")
	}
}

func TestRecord_ConcurrentRunsFormOneLinearHistory(t *testing.T) {
	s := newStore(t)
	const n = 12
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for { // what Temporal's retry policy does on ErrConflict
				_, err := record(s, fmt.Sprintf("run-%d", i), time.Unix(1700000000+int64(i), 0))
				if err == nil {
					return
				}
				if !errors.Is(err, warp.ErrConflict) {
					t.Errorf("run-%d: %v", i, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	head, _, _ := s.Resolve(ctx, loom.DefaultRef)
	// Walk back: every commit has exactly one parent except the first.
	count := 0
	for c := head; ; count++ {
		_, ps := must2(s.Parents(ctx, c))
		if len(ps) == 0 {
			count++
			break
		}
		if len(ps) != 1 {
			t.Fatalf("commit %s has %d parents", c, len(ps))
		}
		c = ps[0]
	}
	if count != n {
		t.Errorf("history has %d commits, want %d", count, n)
	}
	for i := 0; i < n; i++ {
		c, ok, _ := s.FindByTrailer(ctx, loom.DefaultRef, loom.TrailerRun, fmt.Sprintf("run-%d", i))
		if !ok {
			t.Errorf("run-%d missing from history", i)
			continue
		}
		if anc, _ := s.IsAncestor(ctx, c, head); !anc {
			t.Errorf("run-%d's commit is not an ancestor of the head", i)
		}
	}
}

func TestWorkflowID_IsGitIDOfPlan(t *testing.T) {
	p := compile(t, program)
	id := must(loom.WorkflowID(ctx, p))
	cmd := exec.Command("git", "hash-object", "--stdin")
	cmd.Stdin = bytes.NewReader(must(loom.PlanJSON(p)))
	if want := "loom-" + strings.TrimSpace(string(must(cmd.Output()))); id != want {
		t.Errorf("WorkflowID = %s, want %s", id, want)
	}
	if other := must(loom.WorkflowID(ctx, compile(t, `(echo "a")`))); other == id {
		t.Error("different plans share a workflow ID")
	}
}

// === Soundness laws ========================================================

// Law 1: compiling is a function. The same source always gives the same
// plan, byte for byte.
func TestLaw_CompileIsAFunction(t *testing.T) {
	a := must(loom.PlanJSON(compile(t, program)))
	b := must(loom.PlanJSON(compile(t, program)))
	if !bytes.Equal(a, b) {
		t.Fatal("same source, different plans")
	}
}

// Law 2: loom adds nothing to execution. RunWorkflow = PlanWorkflow >>> Record,
// and Record doesn't change results, so loom's outputs equal running the
// plan with shuttle alone.
func TestLaw_LoomAgreesWithShuttle(t *testing.T) {
	for _, src := range []string{
		program,
		`(echo "solo")`,
		`(pipe (echo "x") (par echo echo))`,
		`(pipe (par (pipe (echo "x") echo) (echo "y")) echo)`,
	} {
		viaLoom := runLoom(t, newStore(t), src)
		viaShuttle := runShuttle(t, compile(t, src))
		if !reflect.DeepEqual(viaLoom.Outputs, viaShuttle.Outputs) || !reflect.DeepEqual(viaLoom.Leaves, viaShuttle.Leaves) {
			t.Errorf("%s: loom %v/%v, shuttle %v/%v", src, viaLoom.Outputs, viaLoom.Leaves, viaShuttle.Outputs, viaShuttle.Leaves)
		}
	}
}

// Law 3: every record is self-verifying. Recompiling the recorded program
// reproduces the recorded plan exactly, and re-running that plan
// reproduces the recorded outputs.
func TestLaw_RecordsAreReproducible(t *testing.T) {
	s := newStore(t)
	res := runLoom(t, s, program)
	tree, _ := must2(s.Parents(ctx, res.Commit))
	src := string(must(s.Blob(ctx, entry(t, s, tree, "program.draft").ID)))
	recordedPlan := must(s.Blob(ctx, entry(t, s, tree, "plan.json").ID))

	p := compile(t, src)
	if !bytes.Equal(must(loom.PlanJSON(p)), recordedPlan) {
		t.Fatal("recompiled plan differs from the recorded plan")
	}
	outs := entry(t, s, tree, "outputs").ID
	for node, out := range runShuttle(t, p).Outputs {
		if got := string(must(s.Blob(ctx, entry(t, s, outs, node).ID))); got != out {
			t.Errorf("rerun %s = %q, recorded %q", node, out, got)
		}
	}
}

// Law 4: history only grows. Each run's commit has the previous head as its
// parent, so the ref moves only forward in the commit order.
func TestLaw_RefOnlyMovesForward(t *testing.T) {
	s := newStore(t)
	var prev warp.ID
	for i := 0; i < 4; i++ {
		c := must(record(s, fmt.Sprintf("run-%d", i), time.Unix(1700000000+int64(i), 0)))
		if prev != "" {
			if anc, _ := s.IsAncestor(ctx, prev, c); !anc {
				t.Fatalf("run %d: previous head is not an ancestor", i)
			}
			if _, ps := must2(s.Parents(ctx, c)); len(ps) != 1 || ps[0] != prev {
				t.Fatalf("run %d: parent %v, want %s", i, ps, prev)
			}
		}
		prev = c
	}
}

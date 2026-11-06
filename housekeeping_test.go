package loom_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/heedle-ai/bobbin"
	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/reed"
	"github.com/heedle-ai/shuttle"
	"github.com/heedle-ai/shuttle/plan"
)

// TestInv_EveryStepIsLabelled: through Labelled, every plan step's
// activity runs with its run labels (the workflow that scheduled it, that
// workflow's run, and the step's node ID) for every container it starts;
// other activities are registered as they are; and the worker registers
// every command, skills included, through it (LOOM-500).
func TestInv_EveryStepIsLabelled(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestActivityEnvironment()
	step := shuttle.ArrowAsActivity(func(ctx context.Context, in plan.Input) (string, error) {
		b, err := json.Marshal(bobbin.LabelsFrom(ctx))
		return string(b), err
	})
	loom.Labelled(env).RegisterActivityWithOptions(step, activity.RegisterOptions{Name: "Step"})
	other := func(ctx context.Context) (bobbin.Labels, error) {
		e := activity.GetInfo(ctx).WorkflowExecution
		if bobbin.LabelsFrom(ctx) != (bobbin.Labels{}) {
			return bobbin.Labels{}, errors.New("an activity that isn't a step was labelled")
		}
		return bobbin.Labels{Workflow: e.ID, Run: e.RunID}, nil
	}
	loom.Labelled(env).RegisterActivityWithOptions(other, activity.RegisterOptions{Name: "Other"})
	v, err := env.ExecuteActivity("Other")
	if err != nil {
		t.Fatal(err)
	}
	var want bobbin.Labels
	v.Get(&want)
	want.Step = "n4"
	v, err = env.ExecuteActivity("Step", plan.Input{NodeID: "n4"})
	if err != nil {
		t.Fatal(err)
	}
	var out string
	v.Get(&out)
	var got bobbin.Labels
	json.Unmarshal([]byte(out), &got)
	if got != want || got.Workflow == "" || got.Run == "" {
		t.Errorf("a step's labels = %+v, want %+v", got, want)
	}

	// The worker's commands, and its skills, register through Labelled.
	src, err := os.ReadFile("cli/cli.go")
	if err != nil {
		t.Fatal(err)
	}
	cmds := regexp.MustCompile(`\b(?:bobbin|reed|pirn)\.Register\w*\(reg, (\w+)`).FindAllStringSubmatch(string(src), -1)
	if len(cmds) < 6 {
		t.Fatalf("found %d command registrations in cli.go", len(cmds))
	}
	for _, m := range cmds {
		if m[1] != "steps" {
			t.Errorf("cli.go: %s registers with %s, not steps (loom.Labelled)", m[0], m[1])
		}
	}
	if !strings.Contains(string(src), "steps := loom.Labelled(loom.Counted(w, s))") || !strings.Contains(string(src), "treadle.Register(ctx, reg, loom.Labelled(loom.Counted(w, s)),") {
		t.Error("cli.go doesn't register its commands and skills through loom.Labelled")
	}
}

// fakeTemporal is the part of a Temporal client housekeeping uses: it
// describes the executions it was given and records cancellations.
type fakeTemporal struct {
	client.Client
	runs      map[string]*workflowservice.DescribeWorkflowExecutionResponse // "workflow run" (run may be empty: the latest)
	asked     []string
	cancelled []string
	down      bool
}

func (f *fakeTemporal) DescribeWorkflowExecution(_ context.Context, wf, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.asked = append(f.asked, wf+" "+run)
	if f.down {
		return nil, errors.New("connection refused")
	}
	if d, ok := f.runs[wf+" "+run]; ok {
		return d, nil
	}
	return nil, serviceerror.NewNotFound("no such workflow")
}

func (f *fakeTemporal) CancelWorkflow(_ context.Context, wf, run string) error {
	f.cancelled = append(f.cancelled, wf+" "+run)
	return nil
}

func (f *fakeTemporal) CheckHealth(context.Context, *client.CheckHealthRequest) (*client.CheckHealthResponse, error) {
	if f.down {
		return nil, errors.New("connection refused")
	}
	return &client.CheckHealthResponse{}, nil
}

func execution(wf, run, typ string, status enums.WorkflowExecutionStatus, parent *commonpb.WorkflowExecution) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: wf, RunId: run}, Type: &commonpb.WorkflowType{Name: typ}, Status: status, ParentExecution: parent}}
}

// TestInv_CancelGoesThroughTemporal: Cancel asks Temporal to cancel a run
// and nothing else: another kind of workflow is refused, and a run that
// has ended is left as it is (LOOM-501).
func TestInv_CancelGoesThroughTemporal(t *testing.T) {
	f := &fakeTemporal{runs: map[string]*workflowservice.DescribeWorkflowExecutionResponse{
		"loom-a ":      execution("loom-a", "r1", loom.RunWorkflowName, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, nil),
		"loom-a/plan ": execution("loom-a/plan", "p1", plan.WorkflowName, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, &commonpb.WorkflowExecution{WorkflowId: "loom-a", RunId: "r1"}),
		"merge-x ":     execution("merge-x", "m1", loom.MergeWorkflowName, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, nil),
		"loom-b ":      execution("loom-b", "r2", loom.RunWorkflowName, enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, nil),
	}}
	if run, err := loom.Cancel(ctx, f, "loom-a", ""); err != nil || run != "r1" {
		t.Fatalf("Cancel(loom-a) = %q, %v", run, err)
	}
	for _, wf := range []string{"loom-a/plan", "merge-x"} {
		if _, err := loom.Cancel(ctx, f, wf, ""); !errors.Is(err, loom.ErrNotARun) {
			t.Errorf("Cancel(%s): %v, want ErrNotARun", wf, err)
		}
	}
	if _, err := loom.Cancel(ctx, f, "loom-b", ""); err == nil || !strings.Contains(err.Error(), "already ended (completed)") {
		t.Errorf("Cancel of an ended run: %v", err)
	}
	if _, err := loom.Cancel(ctx, f, "nope", ""); err == nil {
		t.Error("Cancel of no such run succeeded")
	}
	if strings.Join(f.cancelled, ",") != "loom-a r1" {
		t.Errorf("Temporal was asked to cancel %q", f.cancelled)
	}
}

// waitSpec is a command whose step waits, heartbeating, until it's
// cancelled, and says what labels and cancellation it saw.
var waitSpec = registry.BuiltinSpec{Name: "wait", AgentID: "Wait", Backends: []registry.Backend{registry.BackendTemporal},
	Options: plan.Options{Heartbeat: 2 * time.Second}}

// TestInv_CancelledRunsEndCanceled: a run cancelled through Temporal ends
// canceled, with no record, while its step runs with its run labels in its
// context (LOOM-501).
func TestInv_CancelledRunsEndCanceled(t *testing.T) {
	reg := draft.DefaultRegistry()
	if err := reg.Register(waitSpec); err != nil {
		t.Fatal(err)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.Register(env, newStore(t), reg)
	var told atomic.Bool
	var seen atomic.Value
	wait := shuttle.ArrowAsActivity(func(ctx context.Context, in plan.Input) (string, error) {
		seen.Store(bobbin.LabelsFrom(ctx))
		for {
			activity.RecordHeartbeat(ctx)
			select {
			case <-ctx.Done():
				told.Store(true)
				return "", ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	})
	loom.Labelled(env).RegisterActivityWithOptions(wait, activity.RegisterOptions{Name: "Wait"})
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	p := must(loom.Compile(ctx, reg, "(wait)"))
	env.ExecuteWorkflow(loom.RunWorkflowName, loom.RunInput{Program: "(wait)", Plan: p, Ref: loom.DefaultRef})
	if err := env.GetWorkflowError(); !temporal.IsCanceledError(err) {
		t.Fatalf("a cancelled run ended with %v, want canceled", err)
	}
	// Temporal's in-process test environment doesn't pass a cancel
	// through the plan's child workflow to its activity, so that the step
	// is told, and ends, is TestE2E_CancelStopsTheStep's, on a real server.
	_ = told.Load()
	if l, _ := seen.Load().(bobbin.Labels); l.Step != string(p.Nodes[0].ID) || !strings.HasSuffix(l.Workflow, "/plan") || l.Run == "" {
		t.Errorf("the step's labels = %+v", l)
	}
}

// containerPodman is a fake podman that lists ps and logs every call.
func containerPodman(t *testing.T, ps string) (bobbin.Runner, string) {
	dir := t.TempDir()
	log := filepath.Join(dir, "log")
	p := filepath.Join(dir, "podman")
	os.WriteFile(p, []byte("#!/bin/sh\necho \"$@\" >> "+log+"\ncase \"$1\" in ps) cat <<'EOF'\n"+ps+"\nEOF\n;; stats) echo '[]';; esac\n"), 0o755)
	return bobbin.Runner{Podman: p}, log
}

const (
	idRunning = "1000000000000000000000000000000000000000000000000000000000000001"
	idEnded   = "2000000000000000000000000000000000000000000000000000000000000002"
	idUnknown = "3000000000000000000000000000000000000000000000000000000000000003"
	idNoRun   = "4000000000000000000000000000000000000000000000000000000000000004"
	idWrite   = "5000000000000000000000000000000000000000000000000000000000000005"
	idSkill   = "6000000000000000000000000000000000000000000000000000000000000006"
	ps        = `[{"Id":"` + idRunning + `","Names":["a"],"State":"running","Created":1,"Labels":{"heedle.kind":"agent","heedle.workflow":"loom-a/plan","heedle.run":"p1","heedle.step":"n0"}},
 {"Id":"` + idEnded + `","Names":["b"],"State":"running","Created":2,"Labels":{"heedle.kind":"browse","heedle.workflow":"loom-b/plan","heedle.run":"p2","heedle.step":"n1"}},
 {"Id":"` + idUnknown + `","Names":["c"],"State":"running","Created":3,"Labels":{"heedle.kind":"container","heedle.workflow":"elsewhere/plan","heedle.run":"p9","heedle.step":"n0"}},
 {"Id":"` + idNoRun + `","Names":["d"],"State":"running","Created":4,"Labels":{"heedle.kind":"agent"}},
 {"Id":"` + idWrite + `","Names":["e"],"State":"running","Created":5,"Labels":{"heedle.kind":"connector","heedle.workflow":"loom-b","heedle.run":"r2"}},
 {"Id":"` + idSkill + `","Names":["f"],"State":"running","Created":6,"Labels":{"heedle.kind":"agent","heedle.workflow":"skills-x","heedle.run":"s1"}}]`
)

func temporalWithRuns() *fakeTemporal {
	return &fakeTemporal{runs: map[string]*workflowservice.DescribeWorkflowExecutionResponse{
		"loom-a/plan p1": execution("loom-a/plan", "p1", plan.WorkflowName, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, &commonpb.WorkflowExecution{WorkflowId: "loom-a", RunId: "r1"}),
		"loom-b/plan p2": execution("loom-b/plan", "p2", plan.WorkflowName, enums.WORKFLOW_EXECUTION_STATUS_CANCELED, &commonpb.WorkflowExecution{WorkflowId: "loom-b", RunId: "r2"}),
		"loom-a r1":      execution("loom-a", "r1", loom.RunWorkflowName, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, nil),
		"loom-b r2":      execution("loom-b", "r2", loom.RunWorkflowName, enums.WORKFLOW_EXECUTION_STATUS_FAILED, nil),
		"skills-x s1":    execution("skills-x", "s1", loom.SkillsWorkflowName, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, nil),
	}}
}

// TestInv_ContainersShowTheirRuns: each container is shown with the run
// it belongs to (a plan's step with the run that started the plan) and
// that step's status; one Temporal doesn't know, or can't be asked about,
// is unknown; only a container of a run (RunWorkflow) that is still
// running can be stopped; each execution is asked about once (LOOM-502).
func TestInv_ContainersShowTheirRuns(t *testing.T) {
	r, _ := containerPodman(t, ps)
	f := temporalWithRuns()
	cs, err := loom.Containers(ctx, f, r)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, c := range cs {
		got[c.ID] = fmt.Sprint(c.Workflow, " ", c.Run, " ", c.Status, " ", c.Stoppable)
	}
	// Review 50: the UI offered Stop run for any running workflow, a
	// skill's creation included, which Cancel then refused.
	want := map[string]string{idRunning: "loom-a r1 running true", idEnded: "loom-b r2 canceled false", idUnknown: "elsewhere/plan p9 unknown false",
		idNoRun: "   false", idWrite: "loom-b r2 failed false", idSkill: "skills-x s1 running false"}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("%s… = %q, want %q", id[:4], got[id], w)
		}
	}
	if len(f.asked) != 6 {
		t.Errorf("Temporal was asked %d times: %q", len(f.asked), f.asked)
	}
	f.down = true
	cs, err = loom.Containers(ctx, f, r)
	if err != nil || len(cs) != 6 || cs[0].Status != "unknown" || cs[0].Stoppable {
		t.Errorf("with Temporal down: %+v, %v", cs, err)
	}
}

// TestInv_CleanRemovesOnlyEndedRuns: Clean removes the containers of
// runs Temporal says have ended, and only those: a running run's, one
// Temporal doesn't know (another Temporal's, perhaps) and one with no run
// stay; with Temporal down it removes nothing (LOOM-503).
func TestInv_CleanRemovesOnlyEndedRuns(t *testing.T) {
	r, log := containerPodman(t, ps)
	gone, err := loom.Clean(ctx, temporalWithRuns(), r, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(gone) != 2 || gone[0].ID != idEnded || gone[1].ID != idWrite {
		t.Fatalf("Clean removed %+v", gone)
	}
	b, _ := os.ReadFile(log)
	if !strings.Contains(string(b), "rm --force --time 0 --ignore "+idEnded+" "+idWrite+"\n") {
		t.Errorf("podman calls:\n%s", b)
	}
	for _, id := range []string{idRunning, idUnknown, idNoRun} {
		if strings.Contains(string(b), "rm --force --time 0 --ignore") && strings.Contains(strings.SplitN(string(b), "rm --force", 2)[1], id) {
			t.Errorf("Clean removed %s…", id[:4])
		}
	}
	r, log = containerPodman(t, ps)
	down := temporalWithRuns()
	down.down = true
	if _, err := loom.Clean(ctx, down, r, ""); err == nil {
		t.Error("Clean with Temporal down succeeded")
	}
	if b, _ := os.ReadFile(log); strings.Contains(string(b), "rm") {
		t.Errorf("Clean with Temporal down ran podman:\n%s", b)
	}
	if _, err := loom.Clean(ctx, nil, r, ""); err == nil {
		t.Error("Clean without Temporal succeeded")
	}
}

// TestInv_WorkersSayTheirHome: a worker's activities start from
// WorkerContext(store), which carries the store's home for bobbin and reed
// alike, so every container a step starts says its home; and the worker
// command uses it (LOOM-520, roadmap 9.2.1).
func TestInv_WorkersSayTheirHome(t *testing.T) {
	store := filepath.Join(t.TempDir(), "store.git")
	wctx := loom.WorkerContext(store)
	home := bobbin.HomeOf(store)
	if !bobbin.ValidHome(home) || bobbin.HomeFrom(wctx) != home || reed.HomeFrom(wctx) != home || wctx.Err() != nil {
		t.Fatalf("WorkerContext: bobbin %q, reed %q, want %q", bobbin.HomeFrom(wctx), reed.HomeFrom(wctx), home)
	}
	if bobbin.HomeOf(filepath.Join(t.TempDir(), "store.git")) == home {
		t.Error("two stores share a home")
	}
	// A step under a worker so made labels its containers with the home.
	var s testsuite.WorkflowTestSuite
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{BackgroundActivityContext: wctx})
	step := shuttle.ArrowAsActivity(func(ctx context.Context, in plan.Input) (string, error) {
		b, err := json.Marshal(bobbin.LabelsFrom(ctx))
		return string(b), err
	})
	loom.Labelled(env).RegisterActivityWithOptions(step, activity.RegisterOptions{Name: "Step"})
	v, err := env.ExecuteActivity("Step", plan.Input{NodeID: "n4"})
	if err != nil {
		t.Fatal(err)
	}
	var out string
	v.Get(&out)
	var l bobbin.Labels
	json.Unmarshal([]byte(out), &l)
	if l.Home != home || l.Step != "n4" {
		t.Errorf("a step's labels under the worker: %+v", l)
	}
	src, err := os.ReadFile("cli/cli.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(src), "worker.New(") != 1 || !strings.Contains(string(src), "worker.New(c, *queue, reed.WorkerOptions(worker.Options{BackgroundActivityContext: loom.WorkerContext(*store), WorkerStopTimeout: loom.WorkerStopTimeout}))") {
		t.Error("cli.go's worker doesn't start its activities from loom.WorkerContext(*store)")
	}
}

// homePS lists containers of two homes and one from before homes.
const (
	homeA  = "aaaaaaaaaaaa"
	homeB  = "bbbbbbbbbbbb"
	idA    = "a000000000000000000000000000000000000000000000000000000000000001"
	idB    = "b000000000000000000000000000000000000000000000000000000000000002"
	idOld  = "c000000000000000000000000000000000000000000000000000000000000003"
	idBad  = "d000000000000000000000000000000000000000000000000000000000000004"
	idARun = "e000000000000000000000000000000000000000000000000000000000000005"
	homePS = `[{"Id":"` + idA + `","Names":["a"],"State":"exited","Created":1,"Labels":{"heedle.kind":"agent","heedle.workflow":"loom-b/plan","heedle.run":"p2","heedle.step":"n1","heedle.home":"` + homeA + `"}},
 {"Id":"` + idB + `","Names":["b"],"State":"exited","Created":2,"Labels":{"heedle.kind":"agent","heedle.workflow":"loom-b/plan","heedle.run":"p2","heedle.step":"n1","heedle.home":"` + homeB + `"}},
 {"Id":"` + idOld + `","Names":["c"],"State":"exited","Created":3,"Labels":{"heedle.kind":"agent","heedle.workflow":"loom-b/plan","heedle.run":"p2","heedle.step":"n1"}},
 {"Id":"` + idBad + `","Names":["d"],"State":"exited","Created":4,"Labels":{"heedle.kind":"agent","heedle.workflow":"loom-b/plan","heedle.run":"p2","heedle.step":"n1","heedle.home":"--x"}},
 {"Id":"` + idARun + `","Names":["e"],"State":"running","Created":5,"Labels":{"heedle.kind":"agent","heedle.workflow":"loom-a/plan","heedle.run":"p1","heedle.step":"n0","heedle.home":"` + homeA + `"}}]`
)

// TestInv_CleanTouchesOnlyItsHome: Clean for a home removes the ended
// runs' containers of that home and those with no home label, never
// another home's or one whose home label isn't one, and never a running
// run's; with no home it is as before (LOOM-522, roadmap 9.2.1).
func TestInv_CleanTouchesOnlyItsHome(t *testing.T) {
	r, log := containerPodman(t, homePS)
	gone, err := loom.Clean(ctx, temporalWithRuns(), r, homeA)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, g := range gone {
		ids = append(ids, g.ID)
	}
	if strings.Join(ids, " ") != idA+" "+idOld {
		t.Errorf("Clean for home A removed %q", ids)
	}
	b, _ := os.ReadFile(log)
	if !strings.Contains(string(b), "rm --force --time 0 --ignore "+idA+" "+idOld+"\n") || strings.Contains(string(b), idB) || strings.Contains(string(b), idBad) {
		t.Errorf("podman calls:\n%s", b)
	}
	for _, c := range []struct {
		l    bobbin.Labels
		home string
		ok   bool
	}{{bobbin.Labels{Home: homeA}, homeA, true}, {bobbin.Labels{}, homeA, true}, {bobbin.Labels{Home: homeB}, homeA, false},
		{bobbin.Labels{Home: bobbin.InvalidHome}, homeA, false}, {bobbin.Labels{Home: homeB}, "", true}} {
		if loom.OfHome(c.l, c.home) != c.ok {
			t.Errorf("OfHome(%+v, %q) = %v", c.l, c.home, !c.ok)
		}
	}
}

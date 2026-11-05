package loom_test

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/heedle-ai/bobbin"
	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
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
	if !strings.Contains(string(src), "steps := loom.Labelled(w)") || !strings.Contains(string(src), "treadle.Register(ctx, reg, loom.Labelled(w),") {
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
	ps        = `[{"Id":"` + idRunning + `","Names":["a"],"State":"running","Created":1,"Labels":{"heedle.kind":"agent","heedle.workflow":"loom-a/plan","heedle.run":"p1","heedle.step":"n0"}},
 {"Id":"` + idEnded + `","Names":["b"],"State":"running","Created":2,"Labels":{"heedle.kind":"browse","heedle.workflow":"loom-b/plan","heedle.run":"p2","heedle.step":"n1"}},
 {"Id":"` + idUnknown + `","Names":["c"],"State":"running","Created":3,"Labels":{"heedle.kind":"container","heedle.workflow":"elsewhere/plan","heedle.run":"p9","heedle.step":"n0"}},
 {"Id":"` + idNoRun + `","Names":["d"],"State":"running","Created":4,"Labels":{"heedle.kind":"agent"}},
 {"Id":"` + idWrite + `","Names":["e"],"State":"running","Created":5,"Labels":{"heedle.kind":"connector","heedle.workflow":"loom-b","heedle.run":"r2"}}]`
)

func temporalWithRuns() *fakeTemporal {
	return &fakeTemporal{runs: map[string]*workflowservice.DescribeWorkflowExecutionResponse{
		"loom-a/plan p1": execution("loom-a/plan", "p1", plan.WorkflowName, enums.WORKFLOW_EXECUTION_STATUS_RUNNING, &commonpb.WorkflowExecution{WorkflowId: "loom-a", RunId: "r1"}),
		"loom-b/plan p2": execution("loom-b/plan", "p2", plan.WorkflowName, enums.WORKFLOW_EXECUTION_STATUS_CANCELED, &commonpb.WorkflowExecution{WorkflowId: "loom-b", RunId: "r2"}),
		"loom-b r2":      execution("loom-b", "r2", loom.RunWorkflowName, enums.WORKFLOW_EXECUTION_STATUS_FAILED, nil),
	}}
}

// TestInv_ContainersShowTheirRuns: each container is shown with the run
// it belongs to (a plan's step with the run that started the plan) and
// that step's status; one Temporal doesn't know, or can't be asked about,
// is unknown; each execution is asked about once (LOOM-502).
func TestInv_ContainersShowTheirRuns(t *testing.T) {
	r, _ := containerPodman(t, ps)
	f := temporalWithRuns()
	cs, err := loom.Containers(ctx, f, r)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, c := range cs {
		got[c.ID] = c.Workflow + " " + c.Run + " " + c.Status
	}
	want := map[string]string{idRunning: "loom-a r1 running", idEnded: "loom-b r2 canceled", idUnknown: "elsewhere/plan p9 unknown", idNoRun: "  ", idWrite: "loom-b r2 failed"}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("%s… = %q, want %q", id[:4], got[id], w)
		}
	}
	if len(f.asked) != 4 {
		t.Errorf("Temporal was asked %d times: %q", len(f.asked), f.asked)
	}
	f.down = true
	cs, err = loom.Containers(ctx, f, r)
	if err != nil || len(cs) != 5 || cs[0].Status != "unknown" {
		t.Errorf("with Temporal down: %+v, %v", cs, err)
	}
}

// TestInv_CleanRemovesOnlyEndedRuns: Clean removes the containers of
// runs Temporal says have ended, and only those: a running run's, one
// Temporal doesn't know (another Temporal's, perhaps) and one with no run
// stay; with Temporal down it removes nothing (LOOM-503).
func TestInv_CleanRemovesOnlyEndedRuns(t *testing.T) {
	r, log := containerPodman(t, ps)
	gone, err := loom.Clean(ctx, temporalWithRuns(), r)
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
	if _, err := loom.Clean(ctx, down, r); err == nil {
		t.Error("Clean with Temporal down succeeded")
	}
	if b, _ := os.ReadFile(log); strings.Contains(string(b), "rm") {
		t.Errorf("Clean with Temporal down ran podman:\n%s", b)
	}
	if _, err := loom.Clean(ctx, nil, r); err == nil {
		t.Error("Clean without Temporal succeeded")
	}
}

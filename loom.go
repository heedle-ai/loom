// Package loom runs draft programs on shuttle and records every run in warp.
//
// A run is two existing pieces composed in sequence:
//
//	RunWorkflow = shuttle's PlanWorkflow (as a child workflow) >>> Record
//
// Record is an activity that writes the program, the plan and every node's
// output into warp as one commit on a ref, linked to the Temporal execution
// by Git trailers. loom adds this composition and nothing else: compiling is
// draft's, executing is shuttle's and Temporal's, storing is warp's and Git's.
package loom

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

const (
	// RunWorkflowName is the registered name of RunWorkflow.
	RunWorkflowName = "LoomRun"
	// RecordName is the registered name of the Record activity.
	RecordName = "LoomRecord"
	// DefaultRef is the warp ref runs are appended to unless told otherwise.
	DefaultRef = "refs/loom/runs"

	// Trailer keys on every run commit.
	TrailerWorkflow = "Shuttle-Workflow"
	TrailerRun      = "Shuttle-Run"
)

// RunInput is what RunWorkflow receives.
type RunInput struct {
	Program string    // draft source, recorded verbatim
	Plan    plan.Plan // the program, compiled
	Ref     string    // warp ref to append the run to
}

// RunResult is what RunWorkflow returns.
type RunResult struct {
	Commit  warp.ID           // the warp commit recording this run
	Outputs map[string]string // every node's output
	Leaves  []string          // nodes nothing depends on
}

// RecordInput is everything Record writes. At is the workflow's clock, so
// recording the same run twice produces the same commit.
type RecordInput struct {
	Program    string
	Plan       plan.Plan
	Result     plan.Result
	Ref        string
	WorkflowID string
	RunID      string
	At         time.Time
}

// RunWorkflow runs the plan as shuttle's PlanWorkflow, then records the
// result in warp. It contains no logic of its own beyond that sequence.
func RunWorkflow(ctx workflow.Context, in RunInput) (RunResult, error) {
	info := workflow.GetInfo(ctx)

	child := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: info.WorkflowExecution.ID + "/plan",
	})
	var res plan.Result
	if err := workflow.ExecuteChildWorkflow(child, plan.WorkflowName, in.Plan).Get(ctx, &res); err != nil {
		return RunResult{}, err
	}

	act := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		// A conflict means another run advanced the ref first; Temporal's
		// retry re-runs Record, which rebuilds on the new head.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Second,
			MaximumAttempts:    20,
		},
	})
	rec := RecordInput{
		Program:    in.Program,
		Plan:       in.Plan,
		Result:     res,
		Ref:        in.Ref,
		WorkflowID: info.WorkflowExecution.ID,
		RunID:      info.WorkflowExecution.RunID,
		At:         workflow.Now(ctx),
	}
	var commit warp.ID
	if err := workflow.ExecuteActivity(act, RecordName, rec).Get(ctx, &commit); err != nil {
		return RunResult{}, err
	}
	return RunResult{Commit: commit, Outputs: res.Outputs, Leaves: res.Leaves}, nil
}

// Recorder holds the warp store the Record activity writes to.
type Recorder struct {
	Store *warp.Store
}

// Record writes one run into warp and advances the ref to it.
//
// The commit's tree is:
//
//	program.draft   the source
//	plan.json       the compiled plan
//	outputs/<node>  each node's output
//
// Record is idempotent: if a commit for this Temporal run is already on the
// ref with the same tree, it returns that commit; with a different tree it
// fails with the non-retryable error type "RecordMismatch". If another run advanced the ref first, it
// returns warp.ErrConflict and Temporal retries it on the new head.
func (r *Recorder) Record(ctx context.Context, in RecordInput) (warp.ID, error) {
	s := r.Store

	program, err := s.PutBlob(ctx, []byte(in.Program))
	if err != nil {
		return "", err
	}
	planJSON, err := PlanJSON(in.Plan)
	if err != nil {
		return "", temporal.NewNonRetryableApplicationError(err.Error(), "InvalidInput", err)
	}
	planBlob, err := s.PutBlob(ctx, planJSON)
	if err != nil {
		return "", err
	}
	var outs []warp.Entry
	for node, out := range in.Result.Outputs {
		id, err := s.PutBlob(ctx, []byte(out))
		if err != nil {
			return "", err
		}
		outs = append(outs, warp.Entry{Name: node, Kind: warp.KindBlob, ID: id})
	}
	outTree, err := s.PutTree(ctx, outs)
	if err != nil {
		return "", err
	}
	tree, err := s.PutTree(ctx, []warp.Entry{
		{Name: "program.draft", Kind: warp.KindBlob, ID: program},
		{Name: "plan.json", Kind: warp.KindBlob, ID: planBlob},
		{Name: "outputs", Kind: warp.KindTree, ID: outTree},
	})
	if err != nil {
		return "", err
	}

	// Already recorded (a Temporal retry after a success)? Return that
	// commit, but only if it records exactly this run's content.
	if done, ok, err := s.FindByTrailer(ctx, in.Ref, TrailerRun, in.RunID); err != nil {
		return "", err
	} else if ok {
		doneTree, _, err := s.Parents(ctx, done)
		if err != nil {
			return "", err
		}
		if doneTree != tree {
			msg := fmt.Sprintf("run %s is already recorded as %s with different content", in.RunID, done)
			return "", temporal.NewNonRetryableApplicationError(msg, "RecordMismatch", nil)
		}
		return done, nil
	}

	head, exists, err := s.Resolve(ctx, in.Ref)
	if err != nil {
		return "", err
	}
	var parents []warp.ID
	if exists {
		parents = []warp.ID{head}
	}
	commit, err := s.Commit(ctx, warp.CommitSpec{
		Tree:    tree,
		Parents: parents,
		Message: "run " + in.WorkflowID,
		Trailers: []warp.Trailer{
			{Key: TrailerWorkflow, Value: in.WorkflowID},
			{Key: TrailerRun, Value: in.RunID},
		},
		Author: warp.Signature{Name: "loom", Email: "loom@heedle.ai", When: in.At},
	})
	if err != nil {
		return "", err
	}
	// On warp.ErrConflict the activity fails and Temporal retries it.
	if err := s.Advance(ctx, in.Ref, head, commit); err != nil {
		return "", err
	}
	return commit, nil
}

// PlanJSON is the canonical encoding of a plan: indented JSON with a
// trailing newline. encoding/json writes struct fields in declaration
// order and map keys sorted, so equal plans encode identically.
func PlanJSON(p plan.Plan) ([]byte, error) {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// Register adds shuttle's PlanWorkflow and Echo, RunWorkflow, and Record
// (writing to store) to a worker.
func Register(w plan.Registry, store *warp.Store) {
	plan.Register(w)
	w.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowName})
	rec := &Recorder{Store: store}
	w.RegisterActivityWithOptions(rec.Record, activity.RegisterOptions{Name: RecordName})
}

// Compile compiles a draft program against reg.
func Compile(ctx context.Context, reg *registry.Registry, program string) (plan.Plan, error) {
	return draft.Compile(ctx, reg, draft.Source(program))
}

// WorkflowID names the workflow after what it runs: "loom-" plus the Git
// blob ID of the plan's canonical JSON. Concurrent submissions of the same
// plan join one execution; each execution's run ID tells runs apart.
func WorkflowID(ctx context.Context, p plan.Plan) (string, error) {
	b, err := PlanJSON(p)
	if err != nil {
		return "", err
	}
	id, err := warp.HashBlob(ctx, b)
	if err != nil {
		return "", err
	}
	return "loom-" + string(id), nil
}

// Submit starts RunWorkflow for a compiled plan and returns the run handle
// without waiting. While a run of the same plan is in progress, Submit
// joins it instead of starting another.
func Submit(ctx context.Context, c client.Client, taskQueue string, in RunInput) (client.WorkflowRun, error) {
	if in.Ref == "" {
		in.Ref = DefaultRef
	}
	if err := in.Plan.Validate(); err != nil {
		return nil, fmt.Errorf("loom: %w", err)
	}
	id, err := WorkflowID(ctx, in.Plan)
	if err != nil {
		return nil, err
	}
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       id,
		TaskQueue:                taskQueue,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, RunWorkflowName, in)
}

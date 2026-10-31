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
	"errors"
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
	Program string    `json:"program"` // draft source, recorded verbatim
	Plan    plan.Plan `json:"plan"`    // the program, compiled
	Ref     string    `json:"ref"`     // warp ref to append the run to
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
	if in.Ref == "" {
		in.Ref = DefaultRef
	}
	// Every node's output becomes a tree entry; reject names that can't be
	// one before running anything. (ValidName does no I/O.)
	for _, n := range in.Plan.Nodes {
		if err := warp.ValidName(string(n.ID)); err != nil {
			return RunResult{}, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidInput", err)
		}
	}
	// Checking the ref needs git, so it's a local activity, run before the
	// plan so a bad ref costs nothing.
	check := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Second})
	if err := workflow.ExecuteLocalActivity(check, CheckRef, in.Ref).Get(ctx, nil); err != nil {
		return RunResult{}, err
	}

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
		// retry re-runs Record, which rebuilds on the new head. Retries are
		// bounded by time, not attempts, so heavy contention can't exhaust
		// them; invalid input is non-retryable and fails at once.
		ScheduleToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Second,
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

// CheckRef is a local activity: it fails with the non-retryable error type
// "InvalidInput" unless ref is a valid full ref name (git check-ref-format).
func CheckRef(ctx context.Context, ref string) error {
	if err := warp.CheckRef(ctx, ref); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "InvalidInput", err)
	}
	return nil
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
// fails with the non-retryable error type "RecordMismatch". Input that can
// never be recorded (an invalid ref or node name) fails with the
// non-retryable error type "InvalidInput". If another run advanced the ref first, it
// returns warp.ErrConflict and Temporal retries it on the new head.
func (r *Recorder) Record(ctx context.Context, in RecordInput) (warp.ID, error) {
	c, err := r.record(ctx, in)
	if errors.Is(err, warp.ErrInvalid) {
		return "", temporal.NewNonRetryableApplicationError(err.Error(), "InvalidInput", err)
	}
	return c, err
}

func (r *Recorder) record(ctx context.Context, in RecordInput) (warp.ID, error) {
	s := r.Store

	program, err := s.PutBlob(ctx, []byte(in.Program))
	if err != nil {
		return "", err
	}
	planJSON, err := PlanJSON(in.Plan)
	if err != nil {
		return "", fmt.Errorf("%w: %v", warp.ErrInvalid, err)
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

	// Read the head once. Both the "already recorded?" check and the
	// compare-and-swap use this same head, so if another attempt records
	// this run in between, the swap fails and the retry finds its commit.
	head, exists, err := s.Resolve(ctx, in.Ref)
	if err != nil {
		return "", err
	}
	var parents []warp.ID
	if exists {
		done, ok, err := s.FindByTrailer(ctx, head, TrailerRun, in.RunID)
		if err != nil {
			return "", err
		}
		if ok {
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

// InputJSON is the canonical encoding of a run's input: indented JSON of
// the program, plan and ref, with a trailing newline.
func InputJSON(in RunInput) ([]byte, error) {
	b, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// WorkflowID names a run's workflow after everything it was asked to do:
// "loom-" plus the Git blob ID of InputJSON. Submitting the exact same
// program, plan and ref while a run is in progress joins that run; any
// difference, even a comment in the program, is a different workflow.
func WorkflowID(ctx context.Context, in RunInput) (string, error) {
	b, err := InputJSON(in)
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
// without waiting. While a run of exactly the same input (program, plan
// and ref) is in progress, Submit joins it instead of starting another.
func Submit(ctx context.Context, c client.Client, taskQueue string, in RunInput) (client.WorkflowRun, error) {
	if in.Ref == "" {
		in.Ref = DefaultRef
	}
	if err := in.Plan.Validate(); err != nil {
		return nil, fmt.Errorf("loom: %w", err)
	}
	if err := warp.CheckRef(ctx, in.Ref); err != nil {
		return nil, err
	}
	for _, n := range in.Plan.Nodes {
		if err := warp.ValidName(string(n.ID)); err != nil {
			return nil, err
		}
	}
	id, err := WorkflowID(ctx, in)
	if err != nil {
		return nil, err
	}
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       id,
		TaskQueue:                taskQueue,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, RunWorkflowName, in)
}

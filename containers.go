package loom

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"

	"github.com/heedle-ai/bobbin"
	"github.com/heedle-ai/shuttle/plan"
)

// Housekeeping (roadmap 9.2): every step's containers are labelled with
// its run, so heedle can list them, stop a run through Temporal, and
// clean up after runs that have ended. loom never starts a process: the
// containers are bobbin's to list and remove.

// Activities is the part of a Temporal worker that commands register
// their activities with.
type Activities interface {
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// Labelled is w, with every plan step's activity (a command's, as
// shuttle.ArrowAsActivity makes it) wrapped so that each container it
// starts is labelled with its run and step: the workflow that scheduled
// it and that workflow's run, from Temporal's activity info, and the
// step's node ID (bobbin.StepLabels). Commands register through it, so a
// container started by any command, a skill's included, carries them.
func Labelled(w Activities) Activities { return labelled{w} }

type labelled struct{ w Activities }

func (l labelled) RegisterActivityWithOptions(a any, o activity.RegisterOptions) {
	if f, ok := a.(func(context.Context, plan.Input) (string, error)); ok {
		a = func(ctx context.Context, in plan.Input) (string, error) {
			return f(bobbin.WithLabels(ctx, bobbin.StepLabels(ctx, in.NodeID)), in)
		}
	}
	l.w.RegisterActivityWithOptions(a, o)
}

// ErrNotARun is a workflow loom won't cancel: one that isn't a run.
var ErrNotARun = errors.New("loom: not a run")

// Cancel asks Temporal to cancel a run (RunWorkflow; runID empty means its
// latest). Its steps are told through their heartbeats, end, and Podman
// removes their containers; the run ends as canceled, with no record, as
// a run that fails has none. Anything that isn't a run is refused, and a
// run that has already ended is left as it is: Cancel says how it ended.
// It returns the run ID it cancelled.
func Cancel(ctx context.Context, c client.Client, workflowID, runID string) (string, error) {
	d, err := c.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return "", err
	}
	info := d.GetWorkflowExecutionInfo()
	if t := info.GetType().GetName(); t != RunWorkflowName {
		return "", fmt.Errorf("%w: %s is a %s workflow", ErrNotARun, workflowID, t)
	}
	run := info.GetExecution().GetRunId()
	if s := info.GetStatus(); s != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return run, fmt.Errorf("run %s has already ended (%s)", workflowID, statusName(s))
	}
	return run, c.CancelWorkflow(ctx, workflowID, run)
}

// statusName is a workflow status as people say it: completed, failed,
// canceled, terminated, timed out.
func statusName(s enums.WorkflowExecutionStatus) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(s.String(), "WORKFLOW_EXECUTION_STATUS_")), "_", " ")
}

// Container is one of heedle's containers, with the run it belongs to as
// Temporal knows it.
type Container struct {
	bobbin.Container
	// Workflow and Run are the run the container's step belongs to: for a
	// plan's step, the run that started the plan; empty outside a run.
	Workflow string `json:"workflow,omitempty"`
	Run      string `json:"run,omitempty"`
	// Status is that run's: running, completed, failed, canceled,
	// terminated, timed out; "unknown" when Temporal doesn't know it or
	// can't be asked.
	Status string `json:"status,omitempty"`
}

// Containers lists heedle's containers (bobbin's listing) with the run
// each belongs to: Temporal describes the workflow its labels name, and a
// plan workflow's run is its parent. Each workflow is asked about once.
func Containers(ctx context.Context, c client.Client, r bobbin.Runner) ([]Container, error) {
	bs, err := r.Containers(ctx)
	if err != nil {
		return nil, err
	}
	type key struct{ wf, run string }
	known := map[key]Container{}
	out := make([]Container, 0, len(bs))
	for _, b := range bs {
		ct := Container{Container: b}
		l := b.Labels
		if l.Workflow != "" && l.Run != "" && c != nil {
			k := key{l.Workflow, l.Run}
			got, ok := known[k]
			if !ok {
				got = describeRun(ctx, c, l.Workflow, l.Run)
				known[k] = got
			}
			ct.Workflow, ct.Run, ct.Status = got.Workflow, got.Run, got.Status
		}
		out = append(out, ct)
	}
	return out, nil
}

// describeRun is the run a container labelled with workflow and run
// belongs to: that execution, or for a plan workflow its parent, with the
// labelled execution's status (the step's, which is what decides whether
// its container is still needed).
func describeRun(ctx context.Context, c client.Client, workflow, run string) Container {
	ct := Container{Workflow: workflow, Run: run, Status: "unknown"}
	d, err := c.DescribeWorkflowExecution(ctx, workflow, run)
	if err != nil {
		return ct
	}
	info := d.GetWorkflowExecutionInfo()
	ct.Status = statusName(info.GetStatus())
	if p := info.GetParentExecution(); p != nil && info.GetType().GetName() == plan.WorkflowName {
		ct.Workflow, ct.Run = p.GetWorkflowId(), p.GetRunId()
	}
	return ct
}

// Clean removes heedle's containers whose run has ended: Temporal says
// the workflow their labels name has closed. A container whose run is
// still running, unknown to Temporal (another Temporal's, perhaps) or not
// labelled with a run is left, so it can never remove one still in use.
// Without Temporal it removes nothing. It returns what it removed.
func Clean(ctx context.Context, c client.Client, r bobbin.Runner) ([]Container, error) {
	if c == nil {
		return nil, errors.New("loom: cleaning up needs Temporal, to know which runs have ended")
	}
	if _, err := c.CheckHealth(ctx, &client.CheckHealthRequest{}); err != nil {
		return nil, fmt.Errorf("Temporal isn't answering, so nothing was removed: %w", err)
	}
	cs, err := Containers(ctx, c, r)
	if err != nil {
		return nil, err
	}
	ended := map[string]Container{}
	for _, ct := range cs {
		if ct.Status != "" && ct.Status != "running" && ct.Status != "unknown" {
			ended[ct.ID] = ct
		}
	}
	gone, err := r.Remove(ctx, func(b bobbin.Container) bool {
		_, ok := ended[b.ID]
		return ok
	})
	var out []Container
	for _, b := range gone {
		out = append(out, ended[b.ID])
	}
	return out, err
}

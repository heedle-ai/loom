package loom

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	"github.com/heedle-ai/pirn"
)

// Forms waiting to be sent (roadmap 9.7). An act step stops before each
// send and asks: its heartbeat carries the pending send (pirn.ActBeat),
// a person decides on the run page or with `loom forms`, the decision is
// a signal to the run (FormSignal), and the step asks its run for it
// (FormsQuery) until it comes.

const (
	// FormSignal decides a pending send: FormDecision.
	FormSignal = "loom-form"
	// FormsQuery answers the decisions a run has had: pending send ID to
	// whether to send it.
	FormsQuery = "loom-forms"
	// FormPoll is how often a waiting step asks its run.
	FormPoll = 2 * time.Second
)

// FormDecision says whether to send the pending send ID.
type FormDecision struct {
	ID   string `json:"id"`
	Send bool   `json:"send"`
}

// PendingForm is a send waiting for a person in a running run.
type PendingForm struct {
	Node    string `json:"node"`
	ID      string `json:"id"`
	Summary string `json:"summary"`
	URL     string `json:"url"`
	JPEG    []byte `json:"-"` // the screenshot of the filled form
}

// formDecisions keeps a run's decisions on pending sends, answering
// FormsQuery: the first decision on a send is the one that counts.
func formDecisions(ctx workflow.Context) error {
	decided := map[string]bool{}
	if err := workflow.SetQueryHandler(ctx, FormsQuery, func() (map[string]bool, error) { return decided, nil }); err != nil {
		return err
	}
	ch := workflow.GetSignalChannel(ctx, FormSignal)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var d FormDecision
			ch.Receive(ctx, &d)
			if _, ok := decided[d.ID]; !ok && d.ID != "" && len(decided) < 1000 {
				decided[d.ID] = d.Send
			}
		}
	})
	return nil
}

// Confirmer is pirn's Act.Confirm for a worker: inside an act activity it
// asks the step's run (its plan's parent, "<run>/plan") every FormPoll
// for the decision on p, until one comes or ctx ends.
func Confirmer(c client.Client) func(context.Context, pirn.ActPending) (bool, error) {
	return func(ctx context.Context, p pirn.ActPending) (bool, error) {
		if !activity.IsActivity(ctx) {
			return false, errors.New("loom: no run to ask: not in an activity")
		}
		run, ok := strings.CutSuffix(activity.GetInfo(ctx).WorkflowExecution.ID, "/plan")
		if !ok {
			return false, errors.New("loom: no run to ask: the step isn't in a loom run")
		}
		for {
			v, err := c.QueryWorkflow(ctx, run, "", FormsQuery)
			if err == nil {
				var decided map[string]bool
				if err := v.Get(&decided); err == nil {
					if send, ok := decided[p.ID]; ok {
						return send, nil
					}
				}
			}
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(FormPoll):
			}
		}
	}
}

// PendingForms are the sends waiting for a person in run workflowID, read
// from its act steps' heartbeats (its plan, "<run>/plan"), less those the
// run has a decision on already: a heartbeat can lag a decision by up to
// a minute (review 41).
func PendingForms(ctx context.Context, c client.Client, workflowID string) ([]PendingForm, error) {
	d, err := c.DescribeWorkflowExecution(ctx, workflowID+"/plan", "")
	if err != nil {
		return nil, err
	}
	decided, err := decidedForms(ctx, c, workflowID)
	if err != nil {
		return nil, err
	}
	var out []PendingForm
	for _, a := range d.PendingActivities {
		if a.GetActivityType().GetName() != pirn.ActActivityName || a.GetHeartbeatDetails() == nil {
			continue
		}
		var b pirn.ActBeat
		if converter.GetDefaultDataConverter().FromPayloads(a.GetHeartbeatDetails(), &b) != nil || b.Pending == nil {
			continue
		}
		if _, ok := decided[b.Pending.ID]; ok {
			continue
		}
		out = append(out, PendingForm{Node: b.Pending.Node, ID: b.Pending.ID, Summary: b.Pending.Summary, URL: b.Pending.URL, JPEG: b.JPEG})
	}
	return out, nil
}

// decidedForms are the decisions run workflowID has had (FormsQuery).
func decidedForms(ctx context.Context, c client.Client, workflowID string) (map[string]bool, error) {
	v, err := c.QueryWorkflow(ctx, workflowID, "", FormsQuery)
	if err != nil {
		return nil, err
	}
	var decided map[string]bool
	if err := v.Get(&decided); err != nil {
		return nil, err
	}
	return decided, nil
}

// DecideForm sends or cancels a pending send in run workflowID: only one
// that is pending there now and not decided already, so a decision is on
// a send someone saw, and a second one is refused rather than ignored
// (review 41).
func DecideForm(ctx context.Context, c client.Client, workflowID string, d FormDecision) error {
	pending, err := PendingForms(ctx, c, workflowID)
	if err != nil {
		return err
	}
	for _, p := range pending {
		if p.ID == d.ID {
			return c.SignalWorkflow(ctx, workflowID, "", FormSignal, d)
		}
	}
	return fmt.Errorf("%w: no form %q waits to be sent in %s", pirn.ErrInvalid, d.ID, workflowID)
}

// PendingWheels are the act steps of run workflowID waiting for a person
// to take the wheel (roadmap 9.10), read from their heartbeats (its plan,
// "<run>/plan"): each with Claude's reason. Driving the browser isn't
// through Temporal: only the machine whose worker runs the step can.
func PendingWheels(ctx context.Context, c client.Client, workflowID string) ([]pirn.ActWheel, error) {
	d, err := c.DescribeWorkflowExecution(ctx, workflowID+"/plan", "")
	if err != nil {
		return nil, err
	}
	var out []pirn.ActWheel
	for _, a := range d.PendingActivities {
		if a.GetActivityType().GetName() != pirn.ActActivityName || a.GetHeartbeatDetails() == nil {
			continue
		}
		var b pirn.ActBeat
		if converter.GetDefaultDataConverter().FromPayloads(a.GetHeartbeatDetails(), &b) != nil || b.Wheel == nil {
			continue
		}
		out = append(out, *b.Wheel)
	}
	return out, nil
}

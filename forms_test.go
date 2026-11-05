package loom_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/reed"
	"github.com/heedle-ai/shuttle/plan"
)

// TestInv_ActStepsAskTheirRun: a run keeps the first decision a person
// signalled on each pending send, and answers them (FormsQuery): a
// decision on another send, or a second on the same, changes nothing.
// An act step waiting to send asks its run (its plan's parent, never the
// plan) every FormPoll until a decision on its own send comes
// (Confirmer); outside a run there is no one to ask. (Roadmap 9.7.)
func TestInv_ActStepsAskTheirRun(t *testing.T) {
	reg := draft.DefaultRegistry()
	reg.MustRegister(pirn.ActBuiltinSpec())
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(time.Minute) // the step asks every FormPoll, in real time
	loom.Register(env, newStore(t), reg)
	// The step's view of its run: no decision, then its own.
	c := &mocks.Client{}
	var mu sync.Mutex
	var asked []string
	decided := []map[string]bool{{}, {"other": true}, {"other": true, "p1": false}}
	c.On("QueryWorkflow", mock.Anything, mock.Anything, "", loom.FormsQuery).Return(
		func(_ context.Context, wf, _ string, _ string, _ ...any) converter.EncodedValue {
			mu.Lock()
			defer mu.Unlock()
			asked = append(asked, wf)
			v, _ := converter.GetDefaultDataConverter().ToPayloads(decided[min(len(asked), len(decided))-1])
			return encoded{v}
		}, nil)
	confirm := loom.Confirmer(c)
	var got string
	env.RegisterActivityWithOptions(func(ctx context.Context, in plan.Input) (string, error) {
		send, err := confirm(ctx, pirn.ActPending{ID: "p1"})
		mu.Lock()
		got = map[bool]string{true: "send", false: "cancel"}[send]
		mu.Unlock()
		return "done", err
	}, activity.RegisterOptions{Name: pirn.ActActivityName})
	// The person, on the run itself.
	var answered map[string]bool
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(loom.FormSignal, loom.FormDecision{ID: "other", Send: true})
		env.SignalWorkflow(loom.FormSignal, loom.FormDecision{ID: "p1", Send: false})
		env.SignalWorkflow(loom.FormSignal, loom.FormDecision{ID: "p1", Send: true})
		env.SignalWorkflow(loom.FormSignal, loom.FormDecision{ID: "", Send: true})
	}, time.Second)
	env.RegisterDelayedCallback(func() { // once the run has taken the signals in
		v, err := env.QueryWorkflow(loom.FormsQuery)
		if err == nil {
			v.Get(&answered)
		}
	}, 2*time.Second)
	src := `(act "x" "https://forms.example.org/")`
	env.ExecuteWorkflow(loom.RunWorkflowName, loom.RunInput{Program: src, Plan: must(loom.Compile(ctx, reg, src)), Ref: loom.DefaultRef})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(answered) != 2 || answered["p1"] != false || answered["other"] != true {
		t.Errorf("the run answered %v", answered)
	}
	mu.Lock()
	defer mu.Unlock()
	if got != "cancel" || len(asked) != 3 || strings.Contains(asked[0], "/plan") || asked[0] == "" {
		t.Errorf("got %q, asked %v", got, asked)
	}
	// Outside a run there is no one to ask.
	if _, err := confirm(ctx, pirn.ActPending{ID: "p1"}); err == nil {
		t.Error("asked with no run")
	}
}

// encoded is a query's answer, as a Temporal client returns it.
type encoded struct{ p *common.Payloads }

func (e encoded) HasValue() bool { return e.p != nil }
func (e encoded) Get(v any) error {
	return converter.GetDefaultDataConverter().FromPayloads(e.p, v)
}

// TestInv_FormsAreDecidedOnlyWhileWaiting: the forms a run waits to send
// are read from its act steps' heartbeats (summary, step, page and
// screenshot); a decision is signalled only for a send waiting now, so a
// person decides what they saw. (Roadmap 9.7.)
func TestInv_FormsAreDecidedOnlyWhileWaiting(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	beat, _ := dc.ToPayloads(pirn.ActBeat{Frame: reed.Frame{URL: "https://forms.example.org/x", JPEG: []byte{0xff, 0xd8, 0xff}},
		Pending: &pirn.ActPending{ID: "f00d", Node: "n0", Summary: "Name Ada.", URL: "https://forms.example.org/x"}})
	plain, _ := dc.ToPayloads(pirn.ActBeat{Frame: reed.Frame{URL: "https://forms.example.org/y", JPEG: []byte{0xff}}})
	c := &mocks.Client{}
	c.On("DescribeWorkflowExecution", mock.Anything, "wf/plan", "").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		PendingActivities: []*workflow.PendingActivityInfo{
			{ActivityType: &common.ActivityType{Name: pirn.ActActivityName}, HeartbeatDetails: beat},
			{ActivityType: &common.ActivityType{Name: pirn.ActActivityName}, HeartbeatDetails: plain},
			{ActivityType: &common.ActivityType{Name: "Browse"}, HeartbeatDetails: beat},
		},
	}, nil)
	var signalled []loom.FormDecision
	c.On("SignalWorkflow", mock.Anything, "wf", "", loom.FormSignal, mock.Anything).Run(func(a mock.Arguments) {
		signalled = append(signalled, a.Get(4).(loom.FormDecision))
	}).Return(nil)
	forms, err := loom.PendingForms(ctx, c, "wf")
	if err != nil || len(forms) != 1 || forms[0].ID != "f00d" || forms[0].Node != "n0" || forms[0].Summary != "Name Ada." || len(forms[0].JPEG) != 3 {
		t.Fatalf("%+v %v", forms, err)
	}
	if err := loom.DecideForm(ctx, c, "wf", loom.FormDecision{ID: "f00d", Send: true}); err != nil || len(signalled) != 1 {
		t.Errorf("deciding a waiting form: %v %v", err, signalled)
	}
	if err := loom.DecideForm(ctx, c, "wf", loom.FormDecision{ID: "beef", Send: true}); !errors.Is(err, pirn.ErrInvalid) || len(signalled) != 1 {
		t.Errorf("deciding a form that isn't waiting: %v %v", err, signalled)
	}
}

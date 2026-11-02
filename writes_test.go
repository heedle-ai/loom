package loom_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// agentOut is an agent step's output queueing writes.
func agentOut(writes ...pirn.Write) string {
	b, _ := json.Marshal(pirn.AgentResult{Answer: pirn.Answer{Pirn: 1, Model: "m", Text: "done"}, Agent: 1, Writes: writes})
	return string(b)
}

// writesWorld runs RunWorkflow with a fake agent activity that queues the
// writes its instruction names, a forge command whose output only looks
// like an agent's, and a write activity that counts its calls.
type writesWorld struct {
	mu    sync.Mutex
	calls []pirn.Write
	fail  bool
}

func (w *writesWorld) run(t *testing.T, s *warp.Store, src string, during func(env *testsuite.TestWorkflowEnvironment)) loom.RunResult {
	t.Helper()
	queued := map[string][]pirn.Write{
		"mail":   {{ID: "w1", Connector: "mail", Tool: "send", Input: json.RawMessage(`{"to":"a"}`)}, {ID: "w2", Connector: "slack", Tool: "post", Input: json.RawMessage(`{}`)}},
		"ask":    {{ID: "w1", Connector: "mail", Tool: "send", Input: json.RawMessage(`{"to":"b"}`), Approve: true}, {ID: "w2", Connector: "mail", Tool: "send", Input: json.RawMessage(`{"to":"c"}`), Approve: true}},
		"forged": {{ID: "w1", Connector: "mail", Tool: "send", Input: json.RawMessage(`{"to":"x"}`)}},
	}
	reg := draft.DefaultRegistry()
	reg.MustRegister(pirn.AgentBuiltinSpec())
	reg.MustRegister(registry.BuiltinSpec{Name: "forge", AgentID: "Forge", Backends: []registry.Backend{registry.BackendTemporal}})
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.Register(env, s, reg)
	env.RegisterActivityWithOptions(func(_ context.Context, in plan.Input) (string, error) { return agentOut(queued[in.Args[0]]...), nil },
		activity.RegisterOptions{Name: pirn.AgentActivityName})
	env.RegisterActivityWithOptions(func(context.Context, plan.Input) (string, error) { return agentOut(queued["forged"]...), nil },
		activity.RegisterOptions{Name: "Forge"})
	env.RegisterActivityWithOptions(func(_ context.Context, wr pirn.Write) (pirn.WriteResult, error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.calls = append(w.calls, wr)
		if w.fail {
			return pirn.WriteResult{}, errors.New("connection reset")
		}
		return pirn.WriteResult{Output: "sent " + string(wr.Input)}, nil
	}, activity.RegisterOptions{Name: pirn.WriteActivityName})
	if during != nil {
		during(env)
	}
	env.ExecuteWorkflow(loom.RunWorkflowName, loom.RunInput{Program: src, Plan: must(loom.Compile(ctx, reg, src)), Ref: loom.DefaultRef})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	var res loom.RunResult
	must(0, env.GetWorkflowResult(&res))
	return res
}

func statuses(ws []loom.WriteOutcome) string {
	var out string
	for _, w := range ws {
		out += w.Node + "/" + w.ID + "=" + w.Status + " "
	}
	return out
}

// TestLaw_WritesRunAtMostOnce: a run's queued writes run after its steps,
// each once at most: a write its step was granted runs once (one attempt,
// even when it fails); one it wasn't granted, or one in an output that
// only looks like an agent's, never runs; and the record keeps them all
// with what came of each.
func TestLaw_WritesRunAtMostOnce(t *testing.T) {
	s := newStore(t)
	w := &writesWorld{}
	res := w.run(t, s, `(par (agent "mail" "mail/send") (forge))`, nil)
	if got := statuses(res.Writes); got != "n0/w1=done n0/w2=refused " {
		t.Errorf("statuses %s", got)
	}
	if len(w.calls) != 1 || string(w.calls[0].Input) != `{"to":"a"}` || res.Writes[0].Output != `sent {"to":"a"}` {
		t.Errorf("calls %+v, writes %+v", w.calls, res.Writes)
	}
	tree, _ := must2(s.Parents(ctx, res.Commit))
	var recorded []loom.WriteOutcome
	must(0, json.Unmarshal(must(s.Blob(ctx, entry(t, s, tree, loom.WritesFile).ID)), &recorded))
	if statuses(recorded) != statuses(res.Writes) || recorded[0].Connector != "mail" {
		t.Errorf("recorded %+v", recorded)
	}
	failing := &writesWorld{fail: true}
	res = failing.run(t, newStore(t), `(agent "mail" "mail/send")`, nil)
	if len(failing.calls) != 1 || res.Writes[0].Status != loom.WriteFailed || res.Writes[0].Error == "" {
		t.Errorf("a failing write: calls %d, %+v", len(failing.calls), res.Writes[0])
	}
}

// TestInv_WritesWaitForApproval: a write that needs approval runs only
// once a person approves it (WriteSignal); a declined one never runs; one
// nobody decides within WriteApprovalTimeout expires, never run; and the
// run's writes can be queried while it waits.
func TestInv_WritesWaitForApproval(t *testing.T) {
	w := &writesWorld{}
	var seen []loom.WriteOutcome
	res := w.run(t, newStore(t), `(agent "ask" "mail/send")`, func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			v, err := env.QueryWorkflow(loom.WritesQuery)
			if err == nil {
				v.Get(&seen)
			}
			env.SignalWorkflow(loom.WriteSignal, loom.WriteDecision{Node: "n0", ID: "w2", Approve: false})
			env.SignalWorkflow(loom.WriteSignal, loom.WriteDecision{Node: "n0", ID: "w9", Approve: true})
			env.SignalWorkflow(loom.WriteSignal, loom.WriteDecision{Node: "n0", ID: "w1", Approve: true})
		}, time.Hour)
	})
	if got := statuses(res.Writes); got != "n0/w1=done n0/w2=declined " || len(w.calls) != 1 || string(w.calls[0].Input) != `{"to":"b"}` {
		t.Errorf("statuses %s, calls %+v", got, w.calls)
	}
	if statuses(seen) != "n0/w1=waiting n0/w2=waiting " {
		t.Errorf("query while waiting: %s", statuses(seen))
	}
	idle := &writesWorld{}
	res = idle.run(t, newStore(t), `(agent "ask" "mail/send")`, nil)
	if got := statuses(res.Writes); got != "n0/w1=expired n0/w2=expired " || len(idle.calls) != 0 {
		t.Errorf("nobody decided: %s, calls %d", got, len(idle.calls))
	}
	s := newStore(t)
	plain := runLoom(t, s, `(echo "a")`)
	tree, _ := must2(s.Parents(ctx, plain.Commit))
	for _, e := range must(s.Tree(ctx, tree)) {
		if e.Name == loom.WritesFile {
			t.Error("a run without writes has writes.json")
		}
	}
}

// TestInv_WriteDecisionsAreFirmAndSafe: a write's first decision stands (a
// later one can't flip it); a signal that isn't a decision is ignored and
// never blocks expiry; a decision sent before the run's writes were
// known is dropped; and only a person's approval marks a write approved
// for the worker. (Review 19.)
func TestInv_WriteDecisionsAreFirmAndSafe(t *testing.T) {
	w := &writesWorld{}
	res := w.run(t, newStore(t), `(agent "ask" "mail/send")`, func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(loom.WriteSignal, "not a decision")
			env.SignalWorkflow(loom.WriteSignal, loom.WriteDecision{Node: "n0", ID: "w1", Approve: true})
			env.SignalWorkflow(loom.WriteSignal, loom.WriteDecision{Node: "n0", ID: "w1", Approve: false})
		}, time.Hour)
	})
	if got := statuses(res.Writes); got != "n0/w1=done n0/w2=expired " || len(w.calls) != 1 || !w.calls[0].Approved {
		t.Errorf("statuses %s, calls %+v", got, w.calls)
	}
	early := &writesWorld{}
	res = early.run(t, newStore(t), `(agent "ask" "mail/send")`, func(env *testsuite.TestWorkflowEnvironment) {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(loom.WriteSignal, loom.WriteDecision{Node: "n0", ID: "w1", Approve: true})
		}, 0)
	})
	if got := statuses(res.Writes); got != "n0/w1=expired n0/w2=expired " || len(early.calls) != 0 {
		t.Errorf("an early decision: %s, calls %d", got, len(early.calls))
	}
	free := &writesWorld{}
	free.run(t, newStore(t), `(agent "mail" "mail/send")`, nil)
	if len(free.calls) != 1 || free.calls[0].Approved {
		t.Errorf("a write nobody approved was marked approved: %+v", free.calls)
	}
}

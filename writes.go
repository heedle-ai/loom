package loom

import (
	"context"
	"encoding/json"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
)

// A run's writes: tools that change something outside the run, such as
// sending an email, which agent steps queue rather than run (pirn's
// Write). After the run's steps, RunWorkflow runs each queued write once,
// in the plan's order, waiting first for a person's decision on any that
// needs approval; then it records them all, with what came of each, in
// the record's WritesFile.
const (
	// WriteSignal decides a write that waits for approval: a
	// WriteDecision.
	WriteSignal = "loom-write"
	// WritesQuery answers a run's writes and their status so far.
	WritesQuery = "loom-writes"
	// WriteApprovalTimeout is how long a write waits for a decision; one
	// still waiting then is "expired", never run.
	WriteApprovalTimeout = 7 * 24 * time.Hour
	// WritesFile is where a record keeps its writes.
	WritesFile = "writes.json"
)

// A write's status.
const (
	WriteWaiting  = "waiting"  // for a person's decision
	WriteApproved = "approved" // approved, not yet run
	WriteDeclined = "declined" // a person said no: never run
	WriteExpired  = "expired"  // nobody decided within WriteApprovalTimeout: never run
	WriteDone     = "done"     // ran once; Output is what it said
	WriteFailed   = "failed"   // ran or tried once and failed; never tried again
	WriteRefused  = "refused"  // its step wasn't granted it: never run
)

// WriteDecision approves or declines one write: the node that queued it
// and its ID there.
type WriteDecision struct {
	Node    string `json:"node"`
	ID      string `json:"id"`
	Approve bool   `json:"approve"`
}

// WriteOutcome is one write of a run, as recorded.
type WriteOutcome struct {
	Node string `json:"node"`
	pirn.Write
	Status  string `json:"status"`
	Output  string `json:"output,omitempty"`
	IsError bool   `json:"isError,omitempty"`
	Error   string `json:"error,omitempty"`
}

// QueuedWrites are the writes a run's agent steps queued, in plan order.
// Only an agent step's output can queue one, and only a write its own
// arguments granted; any other is refused, never run.
func QueuedWrites(p plan.Plan, res plan.Result) []WriteOutcome {
	var out []WriteOutcome
	for _, n := range p.Nodes {
		if n.Activity != pirn.AgentActivityName {
			continue
		}
		r, ok := pirn.ParseAgent(res.Outputs[string(n.ID)])
		if !ok {
			continue
		}
		var args []string
		if len(n.Args) > 0 {
			args = n.Args[1:]
		}
		g, err := pirn.ParseGrant(args)
		for _, w := range r.Writes {
			w.Approved = false // only a person's decision approves a write
			o := WriteOutcome{Node: string(n.ID), Write: w, Status: WriteApproved}
			switch {
			case err != nil || !g.Allows(w.Connector, w.Tool):
				o.Status, o.Error = WriteRefused, "the step wasn't granted "+w.Connector+"/"+w.Tool
			case w.Approve:
				o.Status = WriteWaiting
			}
			out = append(out, o)
		}
	}
	return out
}

// queuedWrites is QueuedWrites as RunWorkflow's local activity.
func queuedWrites(_ context.Context, p plan.Plan, res plan.Result) ([]WriteOutcome, error) {
	return QueuedWrites(p, res), nil
}

// runWrites waits for decisions on the writes that need them, then runs
// every approved write once, in order.
func runWrites(ctx workflow.Context, writes []WriteOutcome) []WriteOutcome {
	waiting := func() int {
		n := 0
		for _, w := range writes {
			if w.Status == WriteWaiting {
				n++
			}
		}
		return n
	}
	decisions := workflow.GetSignalChannel(ctx, WriteSignal)
	// A decision sent before the run's writes were known is dropped: a
	// person decides a write they could see (review 19).
	for {
		var early any
		if !decisions.ReceiveAsync(&early) {
			break
		}
	}
	if waiting() > 0 {
		tctx, cancel := workflow.WithCancel(ctx)
		timer := workflow.NewTimer(tctx, WriteApprovalTimeout)
		expired := false
		for waiting() > 0 && !expired {
			sel := workflow.NewSelector(ctx)
			sel.AddReceive(decisions, func(c workflow.ReceiveChannel, _ bool) {
				// ReceiveAsync skips a payload that isn't a WriteDecision
				// and never blocks, so expiry still comes (review 19:
				// Receive waited on past a malformed one).
				var d WriteDecision
				if !c.ReceiveAsync(&d) {
					return
				}
				for i := range writes {
					if writes[i].Node == d.Node && writes[i].ID == d.ID && writes[i].Status == WriteWaiting {
						writes[i].Status = map[bool]string{true: WriteApproved, false: WriteDeclined}[d.Approve]
						writes[i].Approved = d.Approve
					}
				}
			})
			sel.AddFuture(timer, func(workflow.Future) { expired = true })
			sel.Select(ctx)
		}
		cancel()
		for i := range writes {
			if writes[i].Status == WriteWaiting {
				writes[i].Status = WriteExpired
			}
		}
	}
	once := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	for i := range writes {
		if writes[i].Status != WriteApproved {
			continue
		}
		var r pirn.WriteResult
		if err := workflow.ExecuteActivity(once, pirn.WriteActivityName, writes[i].Write).Get(ctx, &r); err != nil {
			writes[i].Status, writes[i].Error = WriteFailed, err.Error()
			continue
		}
		writes[i].Status, writes[i].Output, writes[i].IsError = WriteDone, r.Output, r.IsError
	}
	return writes
}

// writesJSON is a record's WritesFile.
func writesJSON(writes []WriteOutcome) ([]byte, error) {
	b, err := json.MarshalIndent(writes, "", "  ")
	return append(b, '\n'), err
}

// Writes asks a run (RunWorkflow's workflow ID) for its writes so far.
func Writes(ctx context.Context, c client.Client, workflowID string) ([]WriteOutcome, error) {
	v, err := c.QueryWorkflow(ctx, workflowID, "", WritesQuery)
	if err != nil {
		return nil, err
	}
	var ws []WriteOutcome
	return ws, v.Get(&ws)
}

// DecideWrite approves or declines one of a run's writes waiting for
// approval. Deciding one that isn't waiting changes nothing.
func DecideWrite(ctx context.Context, c client.Client, workflowID string, d WriteDecision) error {
	return c.SignalWorkflow(ctx, workflowID, "", WriteSignal, d)
}

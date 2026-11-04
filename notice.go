package loom

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// Notices are what to tell a person who isn't watching: a scheduled run
// that ended, and a write that waits for their approval. A Watcher finds
// them in Temporal, each once; heedle up shows them as the operating
// system's notifications.

// Notice is one thing to tell. Key is the same for the same thing.
type Notice struct {
	Key      string `json:"key"`
	Title    string `json:"title"`
	Body     string `json:"body"`
	Workflow string `json:"workflowId"`
	Run      string `json:"runId"`
}

var taskQueueName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

// MaxNotices bounds the notices one Check returns; the rest wait for the
// next.
const MaxNotices = 20

// Watcher finds new notices on a task queue: scheduled runs that ended
// after Since, and writes waiting for approval in runs going now.
type Watcher struct {
	Client client.Client
	Queue  string
	Since  time.Time // runs that ended before this aren't news
	told   map[string]bool
}

// Check returns what there is to tell that it hasn't told before.
func (w *Watcher) Check(ctx context.Context) ([]Notice, error) {
	if !taskQueueName.MatchString(w.Queue) {
		return nil, fmt.Errorf("loom: task queue %q: use letters, digits, '.', '_' and '-'", w.Queue)
	}
	if w.told == nil {
		w.told = map[string]bool{}
	}
	var out []Notice
	add := func(n Notice) {
		if !w.told[n.Key] && len(out) < MaxNotices {
			w.told[n.Key] = true
			out = append(out, n)
		}
	}
	// Runs a schedule started that ended since: a minute back, since
	// Temporal's list catches up in a moment; told keeps each once.
	since := w.Since
	ended, err := w.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("WorkflowType = '%s' AND TaskQueue = '%s' AND ExecutionStatus != 'Running' AND CloseTime > '%s'",
			RunWorkflowName, w.Queue, since.UTC().Format(time.RFC3339)),
		PageSize: 100,
	})
	if err != nil {
		return nil, err
	}
	dc := converter.GetDefaultDataConverter()
	for _, e := range ended.Executions {
		var by string
		if p := e.GetSearchAttributes().GetIndexedFields()["TemporalScheduledById"]; p != nil {
			dc.FromPayload(p, &by)
		}
		if !strings.HasPrefix(by, SchedulePrefix) {
			continue // not a schedule's: you started it, you know
		}
		name, wf, run := strings.TrimPrefix(by, SchedulePrefix), e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId()
		n := Notice{Key: "run:" + wf + "/" + run, Workflow: wf, Run: run, Title: "heedle: " + name + " finished", Body: "Its run is recorded."}
		if s := e.GetStatus(); s != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			n.Title = "heedle: " + name + " didn't finish"
			n.Body = "Its run ended " + strings.ToLower(strings.TrimPrefix(s.String(), "WORKFLOW_EXECUTION_STATUS_")) + ", and its schedule is paused until you look."
		}
		add(n)
	}
	if t := time.Now().Add(-time.Minute); t.After(w.Since) {
		w.Since = t
	}
	// Writes waiting in runs going now.
	running, err := w.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query:    fmt.Sprintf("WorkflowType = '%s' AND TaskQueue = '%s' AND ExecutionStatus = 'Running'", RunWorkflowName, w.Queue),
		PageSize: 100,
	})
	if err != nil {
		return out, err
	}
	for _, e := range running.Executions {
		wf, run := e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId()
		writes, err := Writes(ctx, w.Client, wf)
		if err != nil {
			continue // a run that just ended, or a worker that's busy: next time
		}
		for _, o := range writes {
			if o.Status == WriteWaiting {
				add(Notice{Key: "write:" + wf + "/" + o.Node + "/" + o.ID, Workflow: wf, Run: run,
					Title: "heedle: a write waits for you", Body: fmt.Sprintf("%s/%s, from step %s. Approve or decline it.", o.Connector, o.Tool, o.Node)})
			}
		}
	}
	return out, nil
}

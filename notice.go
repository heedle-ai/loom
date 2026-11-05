package loom

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// Notices are what to tell a person who isn't watching: a scheduled run
// that ended, a write that waits for their approval, and a form an act
// step waits to send. A Watcher finds
// them in Temporal, each once; heedle up shows them as the operating
// system's notifications.

// Notice is one thing to tell. Key is the same for the same thing.
type Notice struct {
	Key      string `json:"key"`
	Title    string `json:"title"`
	Body     string `json:"body"`
	Workflow string `json:"workflowId"`
	Run      string `json:"runId"`
	// Schedule is set on a notice about a schedule itself, such as its
	// budget pausing it: the schedule's name.
	Schedule string `json:"schedule,omitempty"`
}

var taskQueueName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

// MaxRunsAsked bounds how many runs going now a Check asks for their
// writes (each a query to its worker), and AskWithin how long each may
// take; MaxPages how many pages of 100 runs it reads.
const (
	MaxRunsAsked = 25
	AskWithin    = 3 * time.Second
	MaxPages     = 10
)

// Watcher finds new notices on a task queue: scheduled runs that ended
// after Since, and writes waiting for approval in runs going now.
type Watcher struct {
	Client client.Client
	Queue  string
	Since  time.Time // runs that ended before this aren't news
	// told is what's been told: a run by when it ended, a write by when
	// it was last seen waiting. Runs that ended well before Since, and
	// writes no longer waiting, are forgotten, so it stays small.
	told map[string]time.Time
}

// Check returns everything there is to tell that it hasn't told before,
// reading every page; it tells each thing once.
func (w *Watcher) Check(ctx context.Context) ([]Notice, error) {
	if !taskQueueName.MatchString(w.Queue) {
		return nil, fmt.Errorf("loom: task queue %q: use letters, digits, '.', '_' and '-'", w.Queue)
	}
	if w.told == nil {
		w.told = map[string]time.Time{}
	}
	now := time.Now()
	var out []Notice
	// Runs a schedule started that ended since; told keeps each once.
	ended, err := w.list(ctx, fmt.Sprintf("WorkflowType = '%s' AND TaskQueue = '%s' AND ExecutionStatus != 'Running' AND CloseTime > '%s'",
		RunWorkflowName, w.Queue, w.Since.UTC().Format(time.RFC3339)))
	if err != nil {
		return nil, err
	}
	dc := converter.GetDefaultDataConverter()
	for _, e := range ended {
		var by string
		if p := e.GetSearchAttributes().GetIndexedFields()["TemporalScheduledById"]; p != nil {
			dc.FromPayload(p, &by)
		}
		if !strings.HasPrefix(by, SchedulePrefix) {
			continue // not a schedule's: you started it, you know
		}
		name, wf, run := strings.TrimPrefix(by, SchedulePrefix), e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId()
		key := "run:" + wf + "/" + run
		if _, ok := w.told[key]; ok {
			continue
		}
		closed := now
		if t := e.GetCloseTime(); t != nil {
			closed = t.AsTime()
		}
		w.told[key] = closed
		n := Notice{Key: key, Workflow: wf, Run: run, Title: "heedle: " + name + " finished", Body: "Its run is recorded."}
		var skipped string
		if p := e.GetMemo().GetFields()[MemoSkipped]; p != nil {
			dc.FromPayload(p, &skipped)
		}
		switch s := e.GetStatus(); {
		case s == enums.WORKFLOW_EXECUTION_STATUS_COMPLETED && skipped != "":
			// Its budget paused the schedule before the run did anything
			// (roadmap 10.4).
			n.Title, n.Body, n.Schedule = "heedle: "+name+" paused", strings.ToUpper(skipped[:1])+skipped[1:]+". It started nothing.", name
		case s == enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		case s == enums.WORKFLOW_EXECUTION_STATUS_FAILED, s == enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
			// A schedule pauses when a run fails (PauseOnFailure).
			n.Title, n.Body = "heedle: "+name+" didn't finish", "Its run "+map[bool]string{true: "failed", false: "timed out"}[s == enums.WORKFLOW_EXECUTION_STATUS_FAILED]+", and its schedule is paused until you look."
		default:
			n.Title, n.Body = "heedle: "+name+" didn't finish", "Its run was "+strings.ToLower(strings.TrimPrefix(s.String(), "WORKFLOW_EXECUTION_STATUS_"))+"."
		}
		out = append(out, n)
	}
	// Looking a minute back next time, since Temporal's list catches up
	// in a moment; runs that ended before that are forgotten.
	if t := now.Add(-time.Minute); t.After(w.Since) {
		w.Since = t
	}
	for k, at := range w.told {
		if strings.HasPrefix(k, "run:") && at.Before(w.Since.Add(-time.Minute)) {
			delete(w.told, k)
		}
	}
	// Writes waiting in runs going now.
	running, err := w.list(ctx, fmt.Sprintf("WorkflowType = '%s' AND TaskQueue = '%s' AND ExecutionStatus = 'Running'", RunWorkflowName, w.Queue))
	if err != nil {
		return out, err
	}
	waiting := map[string]bool{}
	for i, e := range running {
		if i == MaxRunsAsked {
			break
		}
		wf, run := e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId()
		qctx, cancel := context.WithTimeout(ctx, AskWithin)
		writes, err := Writes(qctx, w.Client, wf)
		cancel()
		if err != nil {
			continue // a run that just ended, or a worker that's busy: next time
		}
		for _, o := range writes {
			if o.Status != WriteWaiting {
				continue
			}
			key := "write:" + wf + "/" + o.Node + "/" + o.ID
			waiting[key] = true
			if _, ok := w.told[key]; !ok {
				out = append(out, Notice{Key: key, Workflow: wf, Run: run,
					Title: "heedle: a write waits for you", Body: fmt.Sprintf("%s/%s, from step %s. Approve or decline it.", o.Connector, o.Tool, o.Node)})
			}
			w.told[key] = now
		}
	}
	// Forms waiting to be sent in runs going now (roadmap 9.7).
	for i, e := range running {
		if i == MaxRunsAsked {
			break
		}
		wf, run := e.GetExecution().GetWorkflowId(), e.GetExecution().GetRunId()
		qctx, cancel := context.WithTimeout(ctx, AskWithin)
		forms, err := PendingForms(qctx, w.Client, wf)
		cancel()
		if err != nil {
			continue
		}
		for _, f := range forms {
			key := "form:" + wf + "/" + f.ID
			waiting[key] = true
			if _, ok := w.told[key]; !ok {
				out = append(out, Notice{Key: key, Workflow: wf, Run: run,
					Title: "heedle: a form is ready to send", Body: fmt.Sprintf("%s From step %s. Send or cancel it.", clipNotice(f.Summary), f.Node)})
			}
			w.told[key] = now
		}
	}
	for k := range w.told {
		if (strings.HasPrefix(k, "write:") || strings.HasPrefix(k, "form:")) && !waiting[k] {
			delete(w.told, k)
		}
	}
	return out, nil
}

// clipNotice keeps a notification's text short: the system shows little.
func clipNotice(s string) string {
	if r := []rune(s); len(r) > 160 {
		return string(r[:159]) + "…"
	}
	return s
}

// list reads every page (up to MaxPages) of runs matching query.
func (w *Watcher) list(ctx context.Context, query string) ([]*workflow.WorkflowExecutionInfo, error) {
	var out []*workflow.WorkflowExecutionInfo
	var token []byte
	for page := 0; page < MaxPages; page++ {
		res, err := w.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: query, PageSize: 100, NextPageToken: token})
		if err != nil {
			return nil, err
		}
		out = append(out, res.Executions...)
		if token = res.NextPageToken; len(token) == 0 {
			break
		}
	}
	return out, nil
}

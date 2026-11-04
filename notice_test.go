package loom_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
)

// TestInv_NoticesAreToldOnce: the Watcher tells of each run a loom
// schedule started that ended since it began watching (finished, or not,
// saying its schedule is paused), never of a run someone started by hand,
// and of each write waiting for approval in a run going now; each thing
// once, however often it looks, and at most MaxNotices at a time.
// (Roadmap 8.5.)
func TestInv_NoticesAreToldOnce(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	exec := func(wf, run, by string, s enums.WorkflowExecutionStatus) *workflow.WorkflowExecutionInfo {
		e := &workflow.WorkflowExecutionInfo{Execution: &common.WorkflowExecution{WorkflowId: wf, RunId: run}, Status: s}
		if by != "" {
			p, _ := dc.ToPayload(by)
			e.SearchAttributes = &common.SearchAttributes{IndexedFields: map[string]*common.Payload{"TemporalScheduledById": p}}
		}
		return e
	}
	c := &mocks.Client{}
	var queries []string
	ended := []*workflow.WorkflowExecutionInfo{
		exec("loom-a-2026", "r1", "loom-schedule-lisbon-weekly", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED),
		exec("loom-b-2026", "r2", "loom-schedule-digest", enums.WORKFLOW_EXECUTION_STATUS_FAILED),
		exec("loom-c", "r3", "", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED),                       // started by hand
		exec("loom-d", "r4", "someone-elses-schedule", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED), // not loom's
	}
	running := []*workflow.WorkflowExecutionInfo{exec("loom-e", "r5", "", enums.WORKFLOW_EXECUTION_STATUS_RUNNING)}
	c.On("ListWorkflow", mock.Anything, mock.Anything).Return(func(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
		queries = append(queries, req.Query)
		if strings.Contains(req.Query, "ExecutionStatus = 'Running'") {
			return &workflowservice.ListWorkflowExecutionsResponse{Executions: running}
		}
		return &workflowservice.ListWorkflowExecutionsResponse{Executions: ended}
	}, nil)
	v := &mocks.Value{}
	v.On("Get", mock.Anything).Run(func(args mock.Arguments) {
		*(args.Get(0).(*[]loom.WriteOutcome)) = []loom.WriteOutcome{
			{Node: "n1", Status: loom.WriteWaiting, Write: pirn.Write{ID: "w1", Connector: "mail", Tool: "send"}},
			{Node: "n1", Status: loom.WriteDone, Write: pirn.Write{ID: "w2", Connector: "mail", Tool: "send"}},
		}
	}).Return(nil)
	c.On("QueryWorkflow", mock.Anything, "loom-e", "", loom.WritesQuery).Return(v, nil)

	since := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	w := &loom.Watcher{Client: c, Queue: "loom", Since: since}
	got, err := w.Check(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var titles []string
	for _, n := range got {
		titles = append(titles, n.Title+" | "+n.Body)
	}
	want := []string{
		"heedle: lisbon-weekly finished | Its run is recorded.",
		"heedle: digest didn't finish | Its run ended failed, and its schedule is paused until you look.",
		"heedle: a write waits for you | mail/send, from step n1. Approve or decline it.",
	}
	if strings.Join(titles, "\n") != strings.Join(want, "\n") || got[0].Workflow != "loom-a-2026" || got[0].Run != "r1" || got[2].Workflow != "loom-e" {
		t.Errorf("notices:\n%s", strings.Join(titles, "\n"))
	}
	if !strings.Contains(queries[0], "TaskQueue = 'loom'") || !strings.Contains(queries[0], "CloseTime > '2026-11-04T09:00:00Z'") {
		t.Errorf("query %q", queries[0])
	}
	if again, _ := w.Check(ctx); len(again) != 0 {
		t.Errorf("told twice: %+v", again)
	}
	if !w.Since.After(since) {
		t.Errorf("Since stayed at %v", w.Since)
	}
	many := &loom.Watcher{Client: c, Queue: "loom"}
	ended = nil
	for i := 0; i < loom.MaxNotices+5; i++ {
		ended = append(ended, exec("loom-m", strings.Repeat("x", i+1), "loom-schedule-m", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED))
	}
	first, _ := many.Check(ctx)
	rest, _ := many.Check(ctx)
	if len(first) != loom.MaxNotices || len(rest) != 5+1 { // the rest, and the waiting write
		t.Errorf("at most %d at a time: %d then %d", loom.MaxNotices, len(first), len(rest))
	}
	if _, err := (&loom.Watcher{Client: c, Queue: "a' OR '1'='1"}).Check(ctx); err == nil {
		t.Error("a queue name that would change the query")
	}
}

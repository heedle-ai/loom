package loom_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/reed"
)

// temporalList stands in for Temporal's list: it keeps to the query's
// CloseTime and status, and pages by 100, as Temporal does.
type temporalList struct {
	ended, running []*workflow.WorkflowExecutionInfo
	queries        []string
}

var closeAfter = regexp.MustCompile(`CloseTime > '([^']+)'`)

func (l *temporalList) list(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
	l.queries = append(l.queries, req.Query)
	all := l.running
	if !strings.Contains(req.Query, "ExecutionStatus = 'Running'") {
		after, _ := time.Parse(time.RFC3339, closeAfter.FindStringSubmatch(req.Query)[1])
		all = nil
		for _, e := range l.ended {
			if e.CloseTime.AsTime().After(after) {
				all = append(all, e)
			}
		}
	}
	from, _ := strconv.Atoi(string(req.NextPageToken))
	to := min(from+100, len(all))
	res := &workflowservice.ListWorkflowExecutionsResponse{Executions: all[from:to]}
	if to < len(all) {
		res.NextPageToken = []byte(strconv.Itoa(to))
	}
	return res
}

// TestInv_NoticesAreToldOnce: the Watcher tells of each run a loom
// schedule started that ended since it began watching (finished; or not,
// saying its schedule is paused only when a failure pauses it), never of
// a run someone started by hand, and of each write waiting for approval
// in a run going now; each thing once, however often it looks, every one
// of them, on however many pages; and what it remembers stays small.
// (Roadmap 8.5; review 33: past 20 at once, or 100 a page, notices were
// lost.)
func TestInv_NoticesAreToldOnce(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	now := time.Now()
	exec := func(wf, run, by string, s enums.WorkflowExecutionStatus, closed time.Time) *workflow.WorkflowExecutionInfo {
		e := &workflow.WorkflowExecutionInfo{Execution: &common.WorkflowExecution{WorkflowId: wf, RunId: run}, Status: s, CloseTime: timestamppb.New(closed)}
		if by != "" {
			p, _ := dc.ToPayload(by)
			e.SearchAttributes = &common.SearchAttributes{IndexedFields: map[string]*common.Payload{"TemporalScheduledById": p}}
		}
		return e
	}
	l := &temporalList{
		ended: []*workflow.WorkflowExecutionInfo{
			exec("loom-a-2026", "r1", "loom-schedule-lisbon-weekly", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, now.Add(-30*time.Second)),
			exec("loom-b-2026", "r2", "loom-schedule-digest", enums.WORKFLOW_EXECUTION_STATUS_FAILED, now.Add(-20*time.Second)),
			exec("loom-f-2026", "r6", "loom-schedule-digest", enums.WORKFLOW_EXECUTION_STATUS_CANCELED, now.Add(-10*time.Second)),
			exec("loom-c", "r3", "", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, now.Add(-10*time.Second)),                       // by hand
			exec("loom-d", "r4", "someone-elses-schedule", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, now.Add(-10*time.Second)), // not loom's
			exec("loom-old", "r0", "loom-schedule-old", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, now.Add(-time.Hour)),         // before watching
		},
		running: []*workflow.WorkflowExecutionInfo{exec("loom-e", "r5", "", enums.WORKFLOW_EXECUTION_STATUS_RUNNING, time.Time{})},
	}
	c := &mocks.Client{}
	c.On("ListWorkflow", mock.Anything, mock.Anything).Return(l.list, nil)
	waiting := true
	v := &mocks.Value{}
	v.On("Get", mock.Anything).Run(func(args mock.Arguments) {
		status := loom.WriteWaiting
		if !waiting {
			status = loom.WriteApproved
		}
		*(args.Get(0).(*[]loom.WriteOutcome)) = []loom.WriteOutcome{
			{Node: "n1", Status: status, Write: pirn.Write{ID: "w1", Connector: "mail", Tool: "send"}},
			{Node: "n1", Status: loom.WriteDone, Write: pirn.Write{ID: "w2", Connector: "mail", Tool: "send"}},
		}
	}).Return(nil)
	c.On("QueryWorkflow", mock.Anything, "loom-e", "", loom.WritesQuery).Return(v, nil)
	// An act step waits to send a form (roadmap 9.7).
	beat, _ := dc.ToPayloads(pirn.ActBeat{Frame: reed.Frame{URL: "https://forms.example.org/x", JPEG: []byte{0xff, 0xd8}},
		Pending: &pirn.ActPending{ID: "f00d", Node: "n2", Summary: "Name Ada, email ada@example.org.", URL: "https://forms.example.org/x"}})
	c.On("DescribeWorkflowExecution", mock.Anything, "loom-e/plan", "").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		PendingActivities: []*workflow.PendingActivityInfo{{ActivityType: &common.ActivityType{Name: pirn.ActActivityName}, HeartbeatDetails: beat}},
	}, nil)
	none, _ := dc.ToPayloads(map[string]bool{})
	c.On("QueryWorkflow", mock.Anything, "loom-e", "", loom.FormsQuery).Return(encoded{none}, nil)

	w := &loom.Watcher{Client: c, Queue: "loom", Since: now.Add(-5 * time.Minute)}
	got, err := w.Check(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, n := range got {
		lines = append(lines, n.Title+" | "+n.Body)
	}
	want := []string{
		"heedle: lisbon-weekly finished | Its run is recorded.",
		"heedle: digest didn't finish | Its run failed, and its schedule is paused until you look.",
		"heedle: digest didn't finish | Its run was canceled.",
		"heedle: a write waits for you | mail/send, from step n1. Approve or decline it.",
		"heedle: a form is ready to send | Name Ada, email ada@example.org. From step n2. Send or cancel it.",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") || got[0].Workflow != "loom-a-2026" || got[0].Run != "r1" || got[3].Workflow != "loom-e" {
		t.Errorf("notices:\n%s", strings.Join(lines, "\n"))
	}
	if !strings.Contains(l.queries[0], "TaskQueue = 'loom'") {
		t.Errorf("query %q", l.queries[0])
	}
	if again, _ := w.Check(ctx); len(again) != 0 {
		t.Errorf("told twice: %+v", again)
	}
	// A write decided, then (another) waiting again: told again.
	waiting = false
	w.Check(ctx)
	waiting = true
	if again, _ := w.Check(ctx); len(again) != 1 {
		t.Errorf("a write waiting anew: %+v", again)
	}

	// Many at once, over several pages: every one told, once.
	l.ended = nil
	for i := 0; i < 250; i++ {
		l.ended = append(l.ended, exec("loom-m", fmt.Sprint(i), "loom-schedule-m", enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, now.Add(-30*time.Second)))
	}
	many := &loom.Watcher{Client: c, Queue: "loom", Since: now.Add(-time.Minute)}
	first, _ := many.Check(ctx)
	rest, _ := many.Check(ctx)
	if len(first) != 252 || len(rest) != 0 { // 250 runs, the waiting write and the waiting form
		t.Errorf("all of them, once: %d then %d", len(first), len(rest))
	}
	// What it remembers of runs is forgotten once they're well before Since.
	many.Since = now.Add(time.Hour)
	l.ended = nil
	many.Check(ctx)
	if n := loom.ToldOf(many); n != 2 { // only the waiting write and form
		t.Errorf("remembers %d things", n)
	}
	if _, err := (&loom.Watcher{Client: c, Queue: "a' OR '1'='1"}).Check(ctx); err == nil {
		t.Error("a queue name that would change the query")
	}
}

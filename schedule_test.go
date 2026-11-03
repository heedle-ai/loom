package loom_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/heedle-ai/loom"
)

// TestInv_SchedulesStartRunsLikeSubmit: a schedule starts RunWorkflow
// with exactly the input Submit would, under the same workflow ID (to
// which Temporal adds each run's time), on the same queue and history, one
// run at a time, a missed run catching up for at most ScheduleCatchup; it
// is refused, before Temporal is asked, for what Submit refuses and for a
// name, interval, cron expression or time zone that makes no sense.
// (Roadmap 7.3.)
func TestInv_SchedulesStartRunsLikeSubmit(t *testing.T) {
	ctx := context.Background()
	src := `(pipe (echo "a") echo)`
	in := loom.RunInput{Program: src, Plan: compile(t, src)}
	o, err := loom.ScheduleOptions(ctx, "loom", "daily-digest", in, loom.ScheduleSpec{Every: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	want := in
	want.Ref = loom.DefaultRef
	id, _ := loom.WorkflowID(ctx, want)
	a := o.Action.(*client.ScheduleWorkflowAction)
	if o.ID != "loom-schedule-daily-digest" || a.Workflow != loom.RunWorkflowName || a.ID != id || a.TaskQueue != "loom" ||
		a.Args[0].(loom.RunInput).Ref != loom.DefaultRef || a.Memo[loom.MemoRef] != loom.DefaultRef ||
		o.Overlap != enums.SCHEDULE_OVERLAP_POLICY_SKIP || o.CatchupWindow != loom.ScheduleCatchup ||
		len(o.Spec.Intervals) != 1 || o.Spec.Intervals[0].Every != 24*time.Hour || o.Memo["program"] != src {
		t.Errorf("options %+v action %+v", o, a)
	}
	c, err := loom.ScheduleOptions(ctx, "loom", "weekdays", in, loom.ScheduleSpec{Cron: "0 9 * * 1-5", TimeZone: "Europe/Lisbon"})
	if err != nil || c.Spec.CronExpressions[0] != "0 9 * * 1-5" || c.Spec.TimeZoneName != "Europe/Lisbon" {
		t.Errorf("cron: %+v %v", c.Spec, err)
	}
	bad := in
	bad.Ref = "refs/heads/../x"
	for name, tc := range map[string]struct {
		name string
		in   loom.RunInput
		spec loom.ScheduleSpec
	}{
		"no time":      {"x", in, loom.ScheduleSpec{}},
		"both":         {"x", in, loom.ScheduleSpec{Every: time.Hour, Cron: "0 9 * * *"}},
		"too often":    {"x", in, loom.ScheduleSpec{Every: 30 * time.Second}},
		"four fields":  {"x", in, loom.ScheduleSpec{Cron: "0 9 * *"}},
		"odd field":    {"x", in, loom.ScheduleSpec{Cron: "0 9 * * $(id)"}},
		"bad zone":     {"x", in, loom.ScheduleSpec{Cron: "0 9 * * *", TimeZone: "Mars/Olympus"}},
		"bad name":     {"Daily Digest", in, loom.ScheduleSpec{Every: time.Hour}},
		"long name":    {strings.Repeat("a", 41), in, loom.ScheduleSpec{Every: time.Hour}},
		"bad ref":      {"x", bad, loom.ScheduleSpec{Every: time.Hour}},
		"invalid plan": {"x", loom.RunInput{Program: "x"}, loom.ScheduleSpec{Every: time.Hour}},
	} {
		if _, err := loom.ScheduleOptions(ctx, "loom", tc.name, tc.in, tc.spec); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := loom.ScheduleAction(ctx, nil, "Bad Name", "pause"); err == nil {
		t.Error("an action on a name no schedule can have")
	}
}

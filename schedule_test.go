package loom_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
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
		o.Overlap != enums.SCHEDULE_OVERLAP_POLICY_SKIP || o.CatchupWindow != loom.ScheduleCatchup || !o.PauseOnFailure ||
		loom.TriggerOverlap != enums.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE || o.Memo["queue"] != "loom" ||
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
		"no time":        {"x", in, loom.ScheduleSpec{}},
		"both":           {"x", in, loom.ScheduleSpec{Every: time.Hour, Cron: "0 9 * * *"}},
		"too often":      {"x", in, loom.ScheduleSpec{Every: 30 * time.Second}},
		"four fields":    {"x", in, loom.ScheduleSpec{Cron: "0 9 * *"}},
		"odd field":      {"x", in, loom.ScheduleSpec{Cron: "0 9 * * $(id)"}},
		"bad zone":       {"x", in, loom.ScheduleSpec{Cron: "0 9 * * *", TimeZone: "Mars/Olympus"}},
		"server's zone":  {"x", in, loom.ScheduleSpec{Cron: "0 9 * * *", TimeZone: "Local"}},
		"zone, interval": {"x", in, loom.ScheduleSpec{Every: time.Hour, TimeZone: "Europe/Lisbon"}},
		"step of zero":   {"x", in, loom.ScheduleSpec{Cron: "*/0 * * * *"}},
		"out of range":   {"x", in, loom.ScheduleSpec{Cron: "61 25 * * *"}},
		"backwards":      {"x", in, loom.ScheduleSpec{Cron: "0 9 * * 5-1"}},
		"a name":         {"x", in, loom.ScheduleSpec{Cron: "@daily * * * *"}},
		"seconds":        {"x", in, loom.ScheduleSpec{Every: 90 * time.Second}},
		"bad name":       {"Daily Digest", in, loom.ScheduleSpec{Every: time.Hour}},
		"long name":      {strings.Repeat("a", 41), in, loom.ScheduleSpec{Every: time.Hour}},
		"bad ref":        {"x", bad, loom.ScheduleSpec{Every: time.Hour}},
		"invalid plan":   {"x", loom.RunInput{Program: "x"}, loom.ScheduleSpec{Every: time.Hour}},
	} {
		if _, err := loom.ScheduleOptions(ctx, "loom", tc.name, tc.in, tc.spec); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, ok := range []string{"*/15 9-17 * * 1-5", "0,30 * 1 1,6 0", "5 4 * * 7", "0 0 31 12 *"} {
		if _, err := loom.ScheduleOptions(ctx, "loom", "x", in, loom.ScheduleSpec{Cron: ok}); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	// An interval counts from when it's made (review 30): made at 14:25,
	// "every day" runs at 14:25.
	at := time.Date(2026, 11, 3, 14, 25, 0, 0, time.UTC)
	d, _ := loom.ScheduleOptionsAt(ctx, "loom", "x", in, loom.ScheduleSpec{Every: 24 * time.Hour}, at)
	iv := d.Spec.Intervals[0]
	if first := time.Unix(0, 0).UTC().Add(iv.Offset); first.Hour() != 14 || first.Minute() != 25 || iv.Offset >= iv.Every {
		t.Errorf("offset %v puts the first run at %v", iv.Offset, first)
	}
	if err := loom.ScheduleAction(ctx, nil, "Bad Name", "pause"); err == nil {
		t.Error("an action on a name no schedule can have")
	}
}

// TestInv_RunsNameTheirProgram: every run's commit names its program's
// own workflow ID (TrailerProgram), the same for a run Submit started
// and one a schedule did, whose workflow ID also has the time Temporal
// adds; so a run finds the previous run of its program either way.
// (Review 30.)
func TestInv_RunsNameTheirProgram(t *testing.T) {
	ctx := context.Background()
	s, err := warp.Init(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	src := `(echo "a")`
	in := loom.RunInput{Program: src, Plan: compile(t, src), Ref: loom.DefaultRef}
	want, _ := loom.WorkflowID(ctx, in)
	res := plan.Result{Outputs: map[string]string{"n0": "a"}}
	for i, wf := range []string{want, want + "-2026-11-03T09:00:00Z"} {
		c, err := (&loom.Recorder{Store: s}).Record(ctx, loom.RecordInput{Program: src, Plan: in.Plan, Result: res, Ref: in.Ref, WorkflowID: wf, RunID: fmt.Sprintf("r%d", i), At: time.Unix(int64(i), 0)})
		if err != nil {
			t.Fatal(err)
		}
		tr, _ := s.Trailers(ctx, c)
		if !strings.Contains(fmt.Sprint(tr), loom.TrailerProgram) {
			t.Errorf("%s: trailers %v", wf, tr)
		}
		got, ok, err := s.FindByTrailer(ctx, c, loom.TrailerProgram, want)
		if err != nil || !ok || got != c {
			t.Errorf("%s: found %v %v %v", wf, got, ok, err)
		}
	}
}

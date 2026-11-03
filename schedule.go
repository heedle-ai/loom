package loom

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/heedle-ai/warp"
)

// Schedules run a program again and again: Temporal Schedules, each
// starting RunWorkflow with the same input, so every run is recorded like
// any other. loom adds only names and checks.

// SchedulePrefix begins the Temporal schedule ID of every loom schedule:
// loom-schedule-<name>.
const SchedulePrefix = "loom-schedule-"

// MinScheduleEvery is the shortest interval a schedule may have.
const MinScheduleEvery = time.Minute

// ScheduleCatchup is how late a run may start, when the machine was asleep
// or Temporal down at its time; a run missed by more is skipped.
const ScheduleCatchup = 10 * time.Minute

var scheduleName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$`)

// cronRanges are the values each of a cron expression's five fields may
// take: minute, hour, day of the month, month, day of the week (0 or 7
// Sunday).
var cronRanges = [5][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}

// checkCronField reports whether f is a list of *, N, N-M, each perhaps
// with /STEP (STEP at least 1), within [lo, hi]: the cron Temporal and
// people both read the same way (review 30: Temporal refused what this
// let through).
func checkCronField(f string, lo, hi int) bool {
	num := func(s string) (int, bool) {
		n, err := strconv.Atoi(s)
		return n, err == nil && s != "" && n >= lo && n <= hi && !strings.HasPrefix(s, "+")
	}
	for _, item := range strings.Split(f, ",") {
		base, step, stepped := strings.Cut(item, "/")
		if stepped {
			n, err := strconv.Atoi(step)
			if err != nil || n < 1 || n > hi-lo+1 {
				return false
			}
		}
		switch a, b, ranged := strings.Cut(base, "-"); {
		case base == "*":
		case ranged:
			x, ok1 := num(a)
			y, ok2 := num(b)
			if !ok1 || !ok2 || x > y {
				return false
			}
		default:
			if _, ok := num(base); !ok {
				return false
			}
		}
	}
	return true
}

// ScheduleSpec is when a schedule runs: every so often, or at the times a
// cron expression (five fields) names, in TimeZone (an IANA name; UTC if
// empty).
type ScheduleSpec struct {
	Every    time.Duration `json:"every,omitempty"`
	Cron     string        `json:"cron,omitempty"`
	TimeZone string        `json:"timeZone,omitempty"`
}

// Check reports whether s says when, exactly one way, sensibly.
func (s ScheduleSpec) Check() error {
	switch {
	case (s.Every == 0) == (s.Cron == ""):
		return errors.New("loom: a schedule runs every so often or at cron times: give exactly one")
	case s.Every != 0 && s.Every < MinScheduleEvery:
		return fmt.Errorf("loom: a schedule runs at most every %v", MinScheduleEvery)
	case s.Every%time.Minute != 0:
		return errors.New("loom: a schedule runs every whole number of minutes")
	case s.Every != 0 && s.TimeZone != "":
		return errors.New("loom: a time zone goes with cron times; an interval counts from when the schedule was made")
	case s.Cron != "":
		f := strings.Fields(s.Cron)
		if len(f) != 5 {
			return fmt.Errorf("loom: a cron expression has five fields (minute hour day month weekday): %q", s.Cron)
		}
		for i, x := range f {
			if !checkCronField(x, cronRanges[i][0], cronRanges[i][1]) {
				return fmt.Errorf("loom: cron field %q: use *, numbers, ranges N-M, lists and /STEP, within %d-%d", x, cronRanges[i][0], cronRanges[i][1])
			}
		}
	}
	if s.TimeZone != "" {
		// An IANA name, never "Local" (the Temporal server's zone).
		if _, err := time.LoadLocation(s.TimeZone); err != nil || s.TimeZone == "Local" || strings.ContainsAny(s.TimeZone, " \t") {
			return fmt.Errorf("loom: time zone %q: use an IANA name such as Europe/Lisbon", s.TimeZone)
		}
	}
	return nil
}

// TriggerOverlap is how a run asked for now ("run") meets a run still
// going: it waits for it, rather than being dropped (review 30).
const TriggerOverlap = enums.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE

// ScheduleRun is one run a schedule started.
type ScheduleRun struct {
	WorkflowID string    `json:"workflowId"`
	RunID      string    `json:"runId"`
	At         time.Time `json:"at"`
}

// Schedule is a loom schedule as Temporal has it.
type Schedule struct {
	Name    string        `json:"name"`
	Program string        `json:"program"`
	Ref     string        `json:"ref"`
	Spec    ScheduleSpec  `json:"spec"`
	Paused  bool          `json:"paused"`
	Note    string        `json:"note,omitempty"` // why it's paused, such as a run that failed
	Next    []time.Time   `json:"next"`
	Recent  []ScheduleRun `json:"recent"`
}

// ScheduleOptions are the Temporal options for a schedule named name that
// starts RunWorkflow with in, as Submit would, when spec says: one run at
// a time (a run still going when the next is due skips it), each run's
// workflow ID being in's (WorkflowID) with the time Temporal adds, and
// its record naming the program (TrailerProgram). An interval counts from
// now, the time it's made (Temporal's would count from 1970, so "every
// day" ran at midnight UTC; review 30). A run that fails pauses the
// schedule, saying so, rather than failing again and again.
func ScheduleOptions(ctx context.Context, taskQueue, name string, in RunInput, spec ScheduleSpec) (client.ScheduleOptions, error) {
	return scheduleOptions(ctx, taskQueue, name, in, spec, time.Now())
}

func scheduleOptions(ctx context.Context, taskQueue, name string, in RunInput, spec ScheduleSpec, now time.Time) (client.ScheduleOptions, error) {
	if !scheduleName.MatchString(name) {
		return client.ScheduleOptions{}, fmt.Errorf("loom: a schedule's name is lower-case letters, digits and hyphens, at most 40: %q", name)
	}
	if err := spec.Check(); err != nil {
		return client.ScheduleOptions{}, err
	}
	if in.Ref == "" {
		in.Ref = DefaultRef
	}
	if err := checkRun(ctx, in); err != nil {
		return client.ScheduleOptions{}, err
	}
	id, err := WorkflowID(ctx, in)
	if err != nil {
		return client.ScheduleOptions{}, err
	}
	s := client.ScheduleSpec{TimeZoneName: spec.TimeZone}
	if spec.Every != 0 {
		s.Intervals = []client.ScheduleIntervalSpec{{Every: spec.Every, Offset: time.Duration(now.UnixNano() % int64(spec.Every))}}
	} else {
		s.CronExpressions = []string{spec.Cron}
	}
	memo := map[string]any{"program": in.Program, "ref": in.Ref, "every": spec.Every.String(), "cron": spec.Cron, "timeZone": spec.TimeZone, "queue": taskQueue}
	return client.ScheduleOptions{
		ID:             SchedulePrefix + name,
		Spec:           s,
		Overlap:        enums.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow:  ScheduleCatchup,
		PauseOnFailure: true,
		Memo:           memo,
		Action: &client.ScheduleWorkflowAction{
			ID: id, Workflow: RunWorkflowName, Args: []any{in}, TaskQueue: taskQueue,
			Memo: map[string]any{MemoRef: in.Ref},
		},
	}, nil
}

// checkRun refuses what Submit refuses.
func checkRun(ctx context.Context, in RunInput) error {
	if err := in.Plan.Validate(); err != nil {
		return fmt.Errorf("loom: %w", err)
	}
	if err := warp.CheckRef(ctx, in.Ref); err != nil {
		return err
	}
	for _, n := range in.Plan.Nodes {
		if err := warp.ValidName(string(n.ID)); err != nil {
			return err
		}
	}
	return nil
}

// CreateSchedule creates a schedule (ScheduleOptions).
func CreateSchedule(ctx context.Context, c client.Client, taskQueue, name string, in RunInput, spec ScheduleSpec) error {
	o, err := ScheduleOptions(ctx, taskQueue, name, in, spec)
	if err != nil {
		return err
	}
	_, err = c.ScheduleClient().Create(ctx, o)
	return err
}

// Schedules lists loom's schedules on taskQueue, by name: only those, as
// the runs list shows only that queue's runs (review 30).
func Schedules(ctx context.Context, c client.Client, taskQueue string) ([]Schedule, error) {
	it, err := c.ScheduleClient().List(ctx, client.ScheduleListOptions{PageSize: 100})
	if err != nil {
		return nil, err
	}
	dc := converter.GetDefaultDataConverter()
	var out []Schedule
	for it.HasNext() && len(out) < 500 {
		e, err := it.Next()
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(e.ID, SchedulePrefix) {
			continue
		}
		s := Schedule{Name: strings.TrimPrefix(e.ID, SchedulePrefix), Paused: e.Paused, Note: e.Note, Next: e.NextActionTimes, Recent: []ScheduleRun{}}
		queue := ""
		if e.Memo != nil {
			get := func(k string) string {
				var v string
				if p := e.Memo.Fields[k]; p != nil {
					dc.FromPayload(p, &v)
				}
				return v
			}
			s.Program, s.Ref, s.Spec.Cron, s.Spec.TimeZone, queue = get("program"), get("ref"), get("cron"), get("timeZone"), get("queue")
			if d, err := time.ParseDuration(get("every")); err == nil {
				s.Spec.Every = d
			}
		}
		if queue != taskQueue {
			continue
		}
		for _, a := range e.RecentActions {
			if a.StartWorkflowResult != nil {
				s.Recent = append(s.Recent, ScheduleRun{WorkflowID: a.StartWorkflowResult.WorkflowID, RunID: a.StartWorkflowResult.FirstExecutionRunID, At: a.ActualTime})
			}
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ScheduleAction pauses, resumes, deletes or runs at once ("run") the
// schedule named name.
func ScheduleAction(ctx context.Context, c client.Client, name, action string) error {
	if !scheduleName.MatchString(name) {
		return fmt.Errorf("loom: no schedule %q", name)
	}
	h := c.ScheduleClient().GetHandle(ctx, SchedulePrefix+name)
	switch action {
	case "pause":
		return h.Pause(ctx, client.SchedulePauseOptions{Note: "paused from heedle"})
	case "resume":
		return h.Unpause(ctx, client.ScheduleUnpauseOptions{Note: "resumed from heedle"})
	case "delete":
		return h.Delete(ctx)
	case "run":
		return h.Trigger(ctx, client.ScheduleTriggerOptions{Overlap: TriggerOverlap})
	}
	return fmt.Errorf("loom: a schedule can pause, resume, delete or run, not %q", action)
}

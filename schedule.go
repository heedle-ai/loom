package loom

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
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

var (
	scheduleName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$`)
	cronField    = regexp.MustCompile(`^[0-9*/,\-A-Za-z?#LW]+$`)
)

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
	case s.Cron != "":
		f := strings.Fields(s.Cron)
		if len(f) != 5 {
			return fmt.Errorf("loom: a cron expression has five fields (minute hour day month weekday): %q", s.Cron)
		}
		for _, x := range f {
			if !cronField.MatchString(x) {
				return fmt.Errorf("loom: cron field %q", x)
			}
		}
	}
	if s.TimeZone != "" {
		if _, err := time.LoadLocation(s.TimeZone); err != nil || strings.ContainsAny(s.TimeZone, " \t") {
			return fmt.Errorf("loom: time zone %q", s.TimeZone)
		}
	}
	return nil
}

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
	Next    []time.Time   `json:"next"`
	Recent  []ScheduleRun `json:"recent"`
}

// ScheduleOptions are the Temporal options for a schedule named name that
// starts RunWorkflow with in, as Submit would, when spec says: one run at
// a time (a run still going when the next is due skips it), each run's
// workflow ID being in's (WorkflowID) and the time Temporal adds.
func ScheduleOptions(ctx context.Context, taskQueue, name string, in RunInput, spec ScheduleSpec) (client.ScheduleOptions, error) {
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
		s.Intervals = []client.ScheduleIntervalSpec{{Every: spec.Every}}
	} else {
		s.CronExpressions = []string{spec.Cron}
	}
	memo := map[string]any{"program": in.Program, "ref": in.Ref, "every": spec.Every.String(), "cron": spec.Cron, "timeZone": spec.TimeZone}
	return client.ScheduleOptions{
		ID:            SchedulePrefix + name,
		Spec:          s,
		Overlap:       enums.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow: ScheduleCatchup,
		Memo:          memo,
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

// Schedules lists loom's schedules, by name.
func Schedules(ctx context.Context, c client.Client) ([]Schedule, error) {
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
		s := Schedule{Name: strings.TrimPrefix(e.ID, SchedulePrefix), Paused: e.Paused, Next: e.NextActionTimes, Recent: []ScheduleRun{}}
		if e.Memo != nil {
			get := func(k string) string {
				var v string
				if p := e.Memo.Fields[k]; p != nil {
					dc.FromPayload(p, &v)
				}
				return v
			}
			s.Program, s.Ref, s.Spec.Cron, s.Spec.TimeZone = get("program"), get("ref"), get("cron"), get("timeZone")
			if d, err := time.ParseDuration(get("every")); err == nil {
				s.Spec.Every = d
			}
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
		return h.Trigger(ctx, client.ScheduleTriggerOptions{Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP})
	}
	return fmt.Errorf("loom: a schedule can pause, resume, delete or run, not %q", action)
}

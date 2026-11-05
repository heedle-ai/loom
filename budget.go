package loom

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/heedle-ai/warp"
)

// Budgets (roadmap 10.4). A schedule may have a monthly budget in USD,
// kept in its action's memo (Temporal can change an action, never a
// schedule's own memo). Before a run a schedule started does anything
// else, the LoomBudget activity adds up that schedule's runs this month,
// in its time zone, as recorded on the run's ref; at or over the budget,
// or if they can't all be priced, it pauses the schedule with a note
// saying so and the run ends without starting a step or recording
// anything. A budget can't stop a run already going.

const (
	// TrailerSchedule names, on a run's record, the schedule that started
	// it: the name in Temporal's TemporalScheduledById.
	TrailerSchedule = "Loom-Schedule"
	// BudgetName is the registered name of the budget check activity.
	BudgetName = "LoomBudget"
	// MemoBudget and MemoBudgetResume hold a schedule's budget (USD, two
	// decimals) and whether a new month resumes it ("true"), in its
	// action's memo.
	MemoBudget       = "loom-budget"
	MemoBudgetResume = "loom-budget-resume"
	// MemoSkipped is set on a run its schedule's budget stopped: the note.
	MemoSkipped = "loom-skipped"
	// MaxBudget bounds a budget.
	MaxBudget = 1e6
	// ResumeBudgetsLook is how often a worker looks for schedules a new
	// month resumes.
	ResumeBudgetsLook = 5 * time.Minute
	// budgetChange is RunWorkflow's version marker for the check, so
	// scheduled runs started before it replay as they ran.
	budgetChange = "loom-budget"
)

// scheduledByID is Temporal's search attribute on a run a schedule
// started: the schedule's ID.
var scheduledByID = temporal.NewSearchAttributeKeyKeyword("TemporalScheduledById")

// scheduleOf is the loom schedule a TemporalScheduledById names, if any.
func scheduleOf(id string) string {
	if name, ok := strings.CutPrefix(id, SchedulePrefix); ok && scheduleName.MatchString(name) {
		return name
	}
	return ""
}

// Budget is a schedule's monthly budget: USD per calendar month in the
// schedule's time zone (UTC for one that runs every so often), and
// whether a new month resumes a schedule it paused.
type Budget struct {
	USD             float64 `json:"usd"`
	ResumeNextMonth bool    `json:"resumeNextMonth,omitempty"`
}

// ParseBudget reads a budget in dollars, such as 5 or 0.01: a number from
// 0 (none) to MaxBudget, in whole cents.
func ParseBudget(s string) (float64, error) {
	v, err := strconv.ParseFloat(strings.TrimPrefix(strings.TrimSpace(s), "$"), 64)
	if err != nil || math.IsNaN(v) || v < 0 || v > MaxBudget || math.Abs(v*100-math.Round(v*100)) > 1e-6 {
		return 0, fmt.Errorf("loom: a budget is dollars from 0 to %.0f, in whole cents, such as 5 or 0.01: %q", MaxBudget, s)
	}
	return math.Round(v*100) / 100, nil
}

// budgetNote is the pause note: at or over budget, or, if why isn't
// empty, why the month couldn't be added up. It names the month, so a
// new month knows it may resume the schedule.
func budgetNote(usd float64, month time.Time, why string) string {
	if strings.HasPrefix(why, "at most ") {
		// Reached by a step that can only be bounded (review 48: the
		// page showed it under budget, with nothing to say why).
		return fmt.Sprintf("paused: monthly budget $%.2f reached (%s, est.) in %s", usd, why, month.Format("January 2006"))
	}
	if why != "" {
		return fmt.Sprintf("paused: monthly budget $%.2f can't be checked in %s: %s", usd, month.Format("January 2006"), why)
	}
	return fmt.Sprintf("paused: monthly budget $%.2f reached (est.) in %s", usd, month.Format("January 2006"))
}

var budgetNoteMonth = regexp.MustCompile(`^paused: monthly budget \$[0-9]+\.[0-9]{2} (?:reached \((?:at most \$[0-9]+\.[0-9]{2}, )?est\.\)|can't be checked) in ([A-Z][a-z]+ [0-9]{4})`)

// BudgetPaused reports whether note is a budget's pause note, and the
// month it was paused in (January 2006).
func BudgetPaused(note string) (month string, ok bool) {
	m := budgetNoteMonth.FindStringSubmatch(note)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// ScheduleBudget is what the budget check needs to know of a schedule.
type ScheduleBudget struct {
	Budget   Budget
	TimeZone string
	Paused   bool
	Note     string
}

// ErrNoSchedule is a schedule that isn't there.
var ErrNoSchedule = errors.New("loom: no such schedule")

// ScheduleControl reads a schedule's budget and pauses it: Temporal's
// (TemporalSchedules), or a stand-in in tests.
type ScheduleControl interface {
	Budget(ctx context.Context, name string) (ScheduleBudget, error)
	Pause(ctx context.Context, name, note string) error
}

// TemporalSchedules is ScheduleControl with Temporal's schedules.
type TemporalSchedules struct{ Client client.Client }

// Budget describes the schedule.
func (t TemporalSchedules) Budget(ctx context.Context, name string) (ScheduleBudget, error) {
	d, err := t.Client.ScheduleClient().GetHandle(ctx, SchedulePrefix+name).Describe(ctx)
	var nf *serviceerror.NotFound
	if errors.As(err, &nf) {
		return ScheduleBudget{}, fmt.Errorf("%w %q", ErrNoSchedule, name)
	}
	if err != nil {
		return ScheduleBudget{}, err
	}
	out := ScheduleBudget{Paused: d.Schedule.State.Paused, Note: d.Schedule.State.Note}
	if d.Schedule.Spec != nil {
		out.TimeZone = d.Schedule.Spec.TimeZoneName
	}
	if a, ok := d.Schedule.Action.(*client.ScheduleWorkflowAction); ok {
		out.Budget, err = budgetOf(a.Memo)
	}
	return out, err
}

// Pause pauses the schedule with note.
func (t TemporalSchedules) Pause(ctx context.Context, name, note string) error {
	return t.Client.ScheduleClient().GetHandle(ctx, SchedulePrefix+name).Pause(ctx, client.SchedulePauseOptions{Note: note})
}

// budgetOf reads a budget from an action's memo, as Temporal gives it
// back (encoded) or as it was set.
func budgetOf(memo map[string]any) (Budget, error) {
	get := func(k string) (string, error) {
		switch v := memo[k].(type) {
		case nil:
			return "", nil
		case string:
			return v, nil
		case *commonpb.Payload:
			var s string
			if err := converter.GetDefaultDataConverter().FromPayload(v, &s); err != nil {
				return "", fmt.Errorf("loom: memo %s: %v", k, err)
			}
			return s, nil
		default:
			return "", fmt.Errorf("loom: memo %s isn't a string", k)
		}
	}
	b, err := get(MemoBudget)
	if err != nil || b == "" {
		return Budget{}, err
	}
	usd, err := ParseBudget(b)
	if err != nil {
		return Budget{}, err
	}
	r, err := get(MemoBudgetResume)
	return Budget{USD: usd, ResumeNextMonth: r == "true"}, err
}

// BudgetInput is what the budget check is asked about: a run of
// Schedule recorded on Ref, starting At (the workflow's clock).
type BudgetInput struct {
	Schedule string
	Ref      string
	At       time.Time
}

// Budget is the budget check (activity BudgetName): "" if the run may
// go on, or the note it paused the schedule with. Its schedule's runs
// this month, in the schedule's time zone, are added up from their
// records (RunsBetween) and priced with the worker's prices (ReadPrices
// of PricesFile; the built-in ones if that file can't be used); at or
// over the budget it pauses the schedule, and if any of those runs can't
// be priced, or there are too many to add up, so the budget can't be
// checked, it pauses it saying why. A schedule with no budget, or one
// deleted, never pauses. A worker with no Schedules fails every
// scheduled run rather than run one unchecked.
func (r *Recorder) Budget(ctx context.Context, in BudgetInput) (string, error) {
	if r.Schedules == nil {
		return "", temporal.NewNonRetryableApplicationError("this worker can't check schedule budgets", "NoBudgetCheck", nil)
	}
	b, err := r.Schedules.Budget(ctx, in.Schedule)
	if errors.Is(err, ErrNoSchedule) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if b.Budget.USD == 0 {
		return "", nil
	}
	loc := time.UTC
	if b.TimeZone != "" {
		if loc, err = time.LoadLocation(b.TimeZone); err != nil {
			return r.pause(ctx, in.Schedule, budgetNote(b.Budget.USD, in.At.UTC(), "its time zone "+strconv.Quote(b.TimeZone)+" isn't known here"))
		}
	}
	start, end := MonthOf(in.At, loc)
	if err := warp.CheckRef(ctx, in.Ref); err != nil {
		return r.pause(ctx, in.Schedule, budgetNote(b.Budget.USD, start, "its runs' ref "+strconv.Quote(in.Ref)+" isn't one"))
	}
	runs, err := RunsBetween(ctx, r.Store, in.Ref, start, end, in.Schedule)
	if errors.Is(err, ErrTooManyRuns) {
		return r.pause(ctx, in.Schedule, budgetNote(b.Budget.USD, start, err.Error()))
	}
	if err != nil {
		return "", err
	}
	prices, err := ReadPrices(r.PricesFile)
	if err != nil && activity.IsActivity(ctx) {
		activity.GetLogger(ctx).Warn(err.Error())
	}
	var u Usage
	for _, run := range runs {
		u = u.Add(run.Usage)
	}
	c := Cost(u, prices)
	// A step that can't be priced exactly but can be bounded counts at
	// its bound: a budget may stop a run early, never let one through.
	most, bounded := c.AtMost()
	switch {
	case !bounded:
		x := c.Unpriced[0]
		for _, y := range c.Unpriced {
			if y.AtMost == nil {
				x = y
				break
			}
		}
		return r.pause(ctx, in.Schedule, budgetNote(b.Budget.USD, start, fmt.Sprintf("%d of its steps this month have no estimate (step %s: %s)", len(c.Unpriced), x.Node, x.Why)))
	case most >= b.Budget.USD && !c.Complete:
		return r.pause(ctx, in.Schedule, budgetNote(b.Budget.USD, start, fmt.Sprintf("at most $%.2f", math.Ceil(most*100-1e-9)/100)))
	case most >= b.Budget.USD:
		return r.pause(ctx, in.Schedule, budgetNote(b.Budget.USD, start, ""))
	}
	return "", nil
}

func (r *Recorder) pause(ctx context.Context, name, note string) (string, error) {
	if err := r.Schedules.Pause(ctx, name, note); err != nil {
		return "", err
	}
	return note, nil
}

// scheduleTrailer is the record's TrailerSchedule, if a schedule started
// the run.
func scheduleTrailer(name string) []warp.Trailer {
	if name == "" {
		return nil
	}
	return []warp.Trailer{{Key: TrailerSchedule, Value: name}}
}

// budgetMemo is memo with budget b in place of any it had.
func budgetMemo(memo map[string]any, b Budget) map[string]any {
	out := map[string]any{}
	for k, v := range memo {
		if k != MemoBudget && k != MemoBudgetResume {
			out[k] = v
		}
	}
	if b.USD > 0 {
		out[MemoBudget] = fmt.Sprintf("%.2f", b.USD)
		if b.ResumeNextMonth {
			out[MemoBudgetResume] = "true"
		}
	}
	return out
}

// CreateScheduleWithBudget is CreateSchedule with a monthly budget from
// the start, so no run of it is ever unchecked.
func CreateScheduleWithBudget(ctx context.Context, c client.Client, taskQueue, name string, in RunInput, spec ScheduleSpec, b Budget) error {
	if _, err := ParseBudget(strconv.FormatFloat(b.USD, 'f', -1, 64)); err != nil {
		return err
	}
	o, err := ScheduleOptions(ctx, taskQueue, name, in, spec)
	if err != nil {
		return err
	}
	a := o.Action.(*client.ScheduleWorkflowAction)
	a.Memo = budgetMemo(a.Memo, b)
	_, err = c.ScheduleClient().Create(ctx, o)
	return err
}

// SetBudget sets the schedule's monthly budget (USD 0: none). If its
// budget paused it and the new budget is none or higher, it resumes it:
// its next run is checked against the new budget.
func SetBudget(ctx context.Context, c client.Client, name string, b Budget) error {
	if !scheduleName.MatchString(name) {
		return fmt.Errorf("%w %q", ErrNoSchedule, name)
	}
	if _, err := ParseBudget(strconv.FormatFloat(b.USD, 'f', -1, 64)); err != nil {
		return err
	}
	h := c.ScheduleClient().GetHandle(ctx, SchedulePrefix+name)
	// One update sets the budget and, if its budget paused the schedule
	// and the new one is none or higher, resumes it: a separate unpause
	// could reach Temporal's scheduler before the update and be undone
	// by it (review 48's end-to-end test caught it).
	err := h.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
		s := in.Description.Schedule
		a, ok := s.Action.(*client.ScheduleWorkflowAction)
		if !ok {
			return nil, fmt.Errorf("loom: schedule %q doesn't start a run", name)
		}
		was, err := budgetOf(a.Memo)
		if err != nil {
			return nil, err
		}
		a.Memo = budgetMemo(a.Memo, b)
		if s.State != nil {
			if _, byBudget := BudgetPaused(s.State.Note); s.State.Paused && byBudget && (b.USD == 0 || b.USD > was.USD) {
				st := *s.State
				st.Paused, st.Note = false, fmt.Sprintf("resumed: monthly budget changed to $%.2f", b.USD)
				s.State = &st
			}
		}
		return &client.ScheduleUpdate{Schedule: &s}, nil
	}})
	var nf *serviceerror.NotFound
	if errors.As(err, &nf) {
		return fmt.Errorf("%w %q", ErrNoSchedule, name)
	}
	return err
}

// ResumeBudgets resumes the schedules on taskQueue its budget paused in
// an earlier month than now's (in the schedule's time zone) that are to
// resume in a new month (Budget.ResumeNextMonth), and returns their
// names.
func ResumeBudgets(ctx context.Context, c client.Client, taskQueue string, now time.Time) ([]string, error) {
	it, err := c.ScheduleClient().List(ctx, client.ScheduleListOptions{PageSize: 100})
	if err != nil {
		return nil, err
	}
	var out []string
	for n := 0; it.HasNext() && n < 500; n++ {
		e, err := it.Next()
		if err != nil {
			return out, err
		}
		name := scheduleOf(e.ID)
		if name == "" || !e.Paused {
			continue
		}
		if _, ok := BudgetPaused(e.Note); !ok {
			continue
		}
		if ok, err := resumeBudget(ctx, c, name, taskQueue, now); err != nil {
			return out, err
		} else if ok {
			out = append(out, name)
		}
	}
	return out, nil
}

func resumeBudget(ctx context.Context, c client.Client, name, taskQueue string, now time.Time) (bool, error) {
	h := c.ScheduleClient().GetHandle(ctx, SchedulePrefix+name)
	d, err := h.Describe(ctx)
	if err != nil {
		return false, err
	}
	a, ok := d.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok || a.TaskQueue != taskQueue || !d.Schedule.State.Paused {
		return false, nil
	}
	b, err := budgetOf(a.Memo)
	month, paused := BudgetPaused(d.Schedule.State.Note)
	if err != nil || !paused || !b.ResumeNextMonth {
		return false, nil
	}
	loc := time.UTC
	if z := d.Schedule.Spec.TimeZoneName; z != "" {
		if loc, err = time.LoadLocation(z); err != nil {
			return false, nil
		}
	}
	if now.In(loc).Format("January 2006") == month {
		return false, nil
	}
	return true, h.Unpause(ctx, client.ScheduleUnpauseOptions{Note: "resumed: a new month"})
}

// ResumeBudgetsEvery runs ResumeBudgets every so often until ctx ends:
// the worker does, so a new month resumes a schedule whether or not
// anyone is watching for notices (review 48). logf hears of failures.
func ResumeBudgetsEvery(ctx context.Context, c client.Client, taskQueue string, every time.Duration, logf func(string, ...any)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if names, err := ResumeBudgets(ctx, c, taskQueue, time.Now()); err != nil && ctx.Err() == nil {
			logf("budgets: can't look for schedules to resume: %v", err)
		} else if len(names) > 0 {
			logf("budgets: a new month resumed %v", names)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

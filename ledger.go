package loom

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// The usage ledger (roadmap 10.4.1). A run's record holds what its steps
// used of Claude, but only a run that succeeded has a record, and only a
// step's last attempt is in it. The ledger holds the rest: every attempt
// of a Claude step (whatever became of it), every front-door ask, and
// which schedule started each scheduled run. It is written by activities,
// never by workflow code, and even when the attempt fails. A successful
// step's usage is still counted only from its record: the ledger's entry
// for that attempt is set aside whenever a record holds its exact output,
// so nothing is counted twice (Extras).
//
// Each entry is its own ref, LedgerPrefix + "YYYY-MM/" + an ID made of
// what the entry is about + "/" + its entry.json's blob ID, so writers
// never race for one ref (and a conflict never sends a step back to
// Claude), writing an entry twice is writing it once, and a month is read
// with two git commands however many entries it has (the refs, then the
// blobs they name). Two kinds aren't by month, but by the run they're
// about, so they're found however long the run goes on: which schedule
// started a run is under LedgerPrefix + "runs/", and which outputs its
// record holds under LedgerPrefix + "recorded/" (written by Record, so a
// successful attempt is set aside by its run, never by when it was
// made). The ledger grows by one ref and three small objects per Claude
// step attempt, front-door request, scheduled run and record, and
// nothing prunes it.

// LedgerPrefix is where the ledger's entries are, one ref each, by the
// month they were made in (UTC). It is outside refs/loom/, where runs and
// branches are.
const LedgerPrefix = "refs/usage/"

// Ledger entry kinds.
const (
	LedgerAttempt   = "attempt"    // one attempt of a Claude step
	LedgerFrontDoor = "front-door" // one front-door request, every attempt at a proposal
	LedgerRun       = "run"        // a scheduled run: which schedule started it
	LedgerRecorded  = "recorded"   // a run's record: its Claude steps' outputs
)

// LedgerEntry is one entry. Workflow and Run are the execution that made
// the calls: for an attempt, the plan's workflow (<run>/plan) and its run;
// for a front-door ask, the ask's; for LedgerRun, the run that Schedule
// started; for LedgerRecorded, the run Record recorded, with Outputs its
// Claude steps' outputs as blob IDs. An attempt carries its run's
// Schedule, read from the run's LedgerRun entry when it is written.
type LedgerEntry struct {
	Ledger   int                `json:"ledger"` // 1
	Kind     string             `json:"kind"`
	At       time.Time          `json:"at"`
	Workflow string             `json:"workflowId,omitempty"`
	Run      string             `json:"runId,omitempty"`
	Activity string             `json:"activityId,omitempty"`
	Node     string             `json:"node,omitempty"`
	Step     string             `json:"step,omitempty"` // ask, agent, act or skill
	Attempt  int32              `json:"attempt,omitempty"`
	Failed   bool               `json:"failed,omitempty"`
	Output   warp.ID            `json:"output,omitempty"` // a successful attempt's output, as a blob ID
	Schedule string             `json:"schedule,omitempty"`
	Calls    []pirn.Call        `json:"calls,omitempty"`
	Outputs  map[string]warp.ID `json:"outputs,omitempty"` // LedgerRecorded's, by node

	// until is where an unreadable entry's month ends (ReadLedger): it
	// may have been made any time before.
	until time.Time
}

// id names e by what it is about: the same attempt, ask or run is the
// same entry.
func (e LedgerEntry) id() string {
	key, _ := json.Marshal([]any{e.Kind, e.Workflow, e.Run, e.Activity, e.Node, e.Attempt})
	if e.Kind == LedgerRun {
		key, _ = json.Marshal([]any{e.Kind, e.Workflow})
	} else if e.Kind == LedgerRecorded {
		key, _ = json.Marshal([]any{e.Kind, e.Workflow, e.Run})
	} else if e.Activity == "" {
		// Not from an activity (a test, a tool): only its time tells it
		// from another.
		key, _ = json.Marshal([]any{e.Kind, e.Workflow, e.Run, e.Node, e.At.UnixNano(), e.Calls})
	}
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:20])
}

// ledgerDir is the folder of e's ref: its month (a run's: "runs"; a
// record's: "recorded" and its workflow's) and its ID.
func (e LedgerEntry) ledgerDir() string {
	switch e.Kind {
	case LedgerRun:
		return LedgerPrefix + "runs/" + e.id() + "/"
	case LedgerRecorded:
		return recordedDir(e.Workflow) + e.id() + "/"
	}
	return LedgerPrefix + e.At.UTC().Format("2006-01") + "/" + e.id() + "/"
}

// WriteLedger writes e: a commit of entry.json, made at e.At, on its own
// ref. Writing an entry already there (the same attempt, ask or run) is
// done. It is for activities only.
func WriteLedger(ctx context.Context, s *warp.Store, e LedgerEntry) error {
	e.Ledger = 1
	// Already written, with this content or another: written.
	if have, err := s.Refs(ctx, e.ledgerDir()); err != nil {
		return err
	} else if len(have) > 0 {
		return nil
	}
	b, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err
	}
	blob, err := s.PutBlob(ctx, append(b, '\n'))
	if err != nil {
		return err
	}
	tree, err := s.PutTree(ctx, []warp.Entry{{Name: "entry.json", Kind: warp.KindBlob, ID: blob}})
	if err != nil {
		return err
	}
	commit, err := s.Commit(ctx, warp.CommitSpec{Tree: tree, Message: "usage: " + e.Kind,
		Author: warp.Signature{Name: "loom", Email: "loom@heedle.invalid", When: e.At}})
	if err != nil {
		return err
	}
	ref := e.ledgerDir() + string(blob)
	if err := s.Advance(ctx, ref, "", commit); err != nil {
		if _, ok, rerr := s.Resolve(ctx, ref); rerr == nil && ok {
			return nil
		}
		return err
	}
	return nil
}

// scheduleOfRun is the schedule that started the run of workflow, as its
// LedgerRun entry says; "" if none does.
func scheduleOfRun(ctx context.Context, s Records, workflow string) (string, error) {
	refs, err := s.Refs(ctx, LedgerEntry{Kind: LedgerRun, Workflow: workflow}.ledgerDir())
	if err != nil || len(refs) == 0 {
		return "", err
	}
	b := warp.ID(refs[0].Name[strings.LastIndex(refs[0].Name, "/")+1:])
	if !ledgerBlob.MatchString(string(b)) {
		return "", nil
	}
	bs, err := s.Blobs(ctx, []warp.ID{b})
	if err != nil {
		return "", err
	}
	var e LedgerEntry
	if json.Unmarshal(bs[0], &e) != nil || e.Kind != LedgerRun {
		return "", nil
	}
	return e.Schedule, nil
}

// recordedDir is where the LedgerRecorded entries of workflow's runs are
// (a workflow ID is reused by every run of the same input).
func recordedDir(workflow string) string {
	sum := sha256.Sum256([]byte(workflow))
	return LedgerPrefix + "recorded/" + hex.EncodeToString(sum[:20]) + "/"
}

// NoteRecorded writes which outputs of the Claude steps of p a record of
// run (of workflow, started by schedule) holds, so their attempts in the
// ledger are set aside (Extras): the record counts them. Writing it twice
// is writing it once. Record calls it.
func NoteRecorded(ctx context.Context, s *warp.Store, workflow, run, schedule string, at time.Time, p plan.Plan, outputs map[string]string) error {
	e := LedgerEntry{Kind: LedgerRecorded, At: at, Workflow: workflow, Run: run, Schedule: schedule, Outputs: map[string]warp.ID{}}
	for node, out := range outputs {
		n, ok := p.Node(plan.NodeID(node))
		if _, claude := claudeKinds[n.Activity]; !ok || !claude {
			continue
		}
		id, err := warp.HashBlob(ctx, []byte(out))
		if err != nil {
			return err
		}
		e.Outputs[node] = id
	}
	if len(e.Outputs) == 0 {
		return nil
	}
	return WriteLedger(ctx, s, e)
}

// readRecorded is the LedgerRecorded entries of the runs of workflows:
// one listing of each workflow's own folder (never of every record's,
// which grows with every run ever recorded: review 56 round 3), then one
// read of their blobs. One that can't be read is left out, so its run's
// attempts aren't set aside and count (more, never less).
func readRecorded(ctx context.Context, s Records, workflows map[string]bool) ([]LedgerEntry, error) {
	names := make([]string, 0, len(workflows))
	for w := range workflows {
		names = append(names, w)
	}
	sort.Strings(names)
	var ids []warp.ID
	for _, w := range names {
		refs, err := s.Refs(ctx, recordedDir(w))
		if err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		for _, r := range refs {
			i := strings.LastIndex(r.Name, "/")
			dir, b := r.Name[:i], r.Name[i+1:]
			if seen[dir] || !ledgerBlob.MatchString(b) {
				continue
			}
			seen[dir] = true
			ids = append(ids, warp.ID(b))
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	bs, err := s.Blobs(ctx, ids)
	if err != nil {
		return nil, err
	}
	var out []LedgerEntry
	for _, b := range bs {
		var e LedgerEntry
		if json.Unmarshal(b, &e) == nil && e.Ledger == 1 && e.Kind == LedgerRecorded && workflows[e.Workflow] {
			out = append(out, e)
		}
	}
	return out, nil
}

// ledgerBlob is what a ledger ref's last part must be: a blob ID.
var ledgerBlob = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

// LedgerUnreadable is the Kind ReadLedger gives an entry it can't read:
// its calls are unknown, so it counts as a step with no counts, and a
// budget that meets one can't be checked (as for a record that can't be
// read).
const LedgerUnreadable = "unreadable"

// ReadLedger is the entries made at or after from and before to, oldest
// first, the first written under each ID. One it can't read (not JSON,
// another version, a ref that doesn't name a blob) is LedgerUnreadable,
// with Workflow naming its ref, at the start of its (UTC) month or from,
// whichever is later: it may have been made any time in that month, so
// every window that overlaps the month has it, in any time zone.
func ReadLedger(ctx context.Context, s Records, from, to time.Time) ([]LedgerEntry, error) {
	var out []LedgerEntry
	for m := time.Date(from.UTC().Year(), from.UTC().Month(), 1, 0, 0, 0, 0, time.UTC); m.Before(to); m = m.AddDate(0, 1, 0) {
		refs, err := s.Refs(ctx, LedgerPrefix+m.Format("2006-01")+"/")
		if err != nil {
			return nil, err
		}
		var ids []warp.ID
		var names []string
		seen := map[string]bool{}
		bad := func(name string) {
			at := m
			if at.Before(from) {
				at = from
			}
			out = append(out, LedgerEntry{Kind: LedgerUnreadable, At: at, Workflow: name, until: m.AddDate(0, 1, 0)})
		}
		for _, r := range refs {
			// .../<month>/<id>/<entry.json's blob ID>: the first under
			// each ID (two writers racing may have left two).
			dir, b := r.Name[:strings.LastIndex(r.Name, "/")], r.Name[strings.LastIndex(r.Name, "/")+1:]
			if seen[dir] {
				continue
			}
			seen[dir] = true
			if !ledgerBlob.MatchString(b) {
				bad(r.Name)
				continue
			}
			ids, names = append(ids, warp.ID(b)), append(names, r.Name)
		}
		if len(ids) == 0 {
			continue
		}
		bs, err := s.Blobs(ctx, ids)
		if err != nil {
			return nil, err
		}
		for i, b := range bs {
			var e LedgerEntry
			if err := json.Unmarshal(b, &e); err != nil || e.Ledger != 1 || e.At.IsZero() {
				bad(names[i])
				continue
			}
			if !e.At.Before(from) && e.At.Before(to) {
				out = append(out, e)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out, nil
}

// Kinds of Extra, as the Costs page names them.
const (
	ExtraFrontDoor = "front door"  // the front door's own asks
	ExtraRetry     = "retries"     // attempts tried again: failed, or whose success came too late
	ExtraFailedRun = "failed runs" // calls of runs with no record of them: failed, or still going
)

// Extra is usage the ledger adds to what records hold: one entry's calls.
// Workflow is the run they belong to (for front-door asks, the ask's), and
// Schedule the schedule that started it, when the ledger knows. An
// unreadable entry is Unreadable: one step with no counts, charged to
// every schedule, so a budget that meets it pauses rather than guess; it
// may have been made any time from At until Until, so a page shows it in
// every month that overlaps.
type Extra struct {
	Kind       string    `json:"kind"`
	Workflow   string    `json:"workflowId,omitempty"`
	Schedule   string    `json:"schedule,omitempty"`
	At         time.Time `json:"at"`
	Usage      Usage     `json:"usage"`
	Unreadable bool      `json:"unreadable,omitempty"`
	Until      time.Time `json:"until,omitzero"`
}

// runOf is the run a plan's workflow belongs to.
func runOf(workflow string) string { return strings.TrimSuffix(workflow, "/plan") }

// Extras is what entries add to the usage records hold, each call once:
// every front-door ask; every failed attempt, as a retry if the same step
// was tried again and else as a failed run's; and every successful
// attempt no record holds (its exact output, at that node of a run of
// that workflow, isn't in recorded), as a retry if the step was tried
// again and else as a failed run's. A successful attempt a record holds
// is set aside: the record counts it. recorded is the LedgerRecorded
// entries of the entries' runs, whenever they were recorded; later is
// entries made after them, which only tell a retry from a failed run.
// Pure: a function of what is written.
func Extras(entries, recorded, later []LedgerEntry) []Extra {
	last := map[string]int32{} // the last attempt of each step
	stepKey := func(e LedgerEntry) string { return e.Workflow + "\x00" + e.Run + "\x00" + e.Node }
	for _, e := range append(append([]LedgerEntry(nil), entries...), later...) {
		if e.Kind == LedgerAttempt && e.Attempt > last[stepKey(e)] {
			last[stepKey(e)] = e.Attempt
		}
	}
	// Each recorded output may set aside one successful attempt. Keyed by
	// workflow, not run (an attempt's run is its plan's, not the record's):
	// a failed rerun of the same input whose successful step answered
	// byte for byte as a recorded run's did is set aside too, counting
	// short by that step; outputs carry Claude's text, so that is all but
	// impossible (review 56 round 3).
	held := map[string]int{}
	schedule := map[string]string{}
	for _, r := range recorded {
		for node, id := range r.Outputs {
			held[r.Workflow+"\x00"+node+"\x00"+string(id)]++
		}
		if r.Schedule != "" {
			schedule[r.Workflow] = r.Schedule
		}
	}
	var out []Extra
	for _, e := range entries {
		x := Extra{Workflow: runOf(e.Workflow), At: e.At}
		switch e.Kind {
		case LedgerUnreadable:
			x.Kind, x.Workflow, x.Unreadable, x.Until = ExtraFailedRun, e.Workflow, true, e.until
			x.Usage = Usage{Steps: []StepUsage{{Node: e.Workflow, Kind: "ledger", Missing: "this usage-ledger entry can't be read, so its calls aren't known"}}}
			out = append(out, x)
			continue
		case LedgerFrontDoor:
			x.Kind, x.Workflow = ExtraFrontDoor, e.Workflow
		case LedgerAttempt:
			x.Schedule = e.Schedule
			if x.Schedule == "" {
				x.Schedule = schedule[x.Workflow]
			}
			k := x.Workflow + "\x00" + e.Node + "\x00" + string(e.Output)
			switch {
			case !e.Failed && e.Output != "" && held[k] > 0:
				held[k]--
				continue // the record counts it
			case e.Attempt < last[stepKey(e)]:
				x.Kind = ExtraRetry
			default:
				x.Kind = ExtraFailedRun
			}
		default:
			continue
		}
		if len(e.Calls) == 0 {
			continue
		}
		x.Usage = callsUsage(e)
		out = append(out, x)
	}
	return out
}

// callsUsage is an entry's calls as Usage: one step per model.
func callsUsage(e LedgerEntry) Usage {
	u := Usage{Steps: []StepUsage{}}
	at := map[string]int{}
	kind := e.Step
	if e.Kind == LedgerFrontDoor {
		kind = "front-door"
	}
	for _, c := range e.Calls {
		i, ok := at[c.Model]
		if !ok {
			i = len(u.Steps)
			at[c.Model] = i
			u.Steps = append(u.Steps, StepUsage{Node: e.Node, Kind: kind, Model: c.Model})
		}
		s := &u.Steps[i]
		s.Tokens = s.Tokens.Add(Tokens{Input: c.Input, Output: c.Output})
		s.Requests++
	}
	return u
}

// ExtrasBetween is the ledger's Extras for calls made at or after from
// and before to, and, given schedule, only that schedule's (and any
// entry that can't be read). Successful attempts are set aside by their
// run's LedgerRecorded entries, whenever the run was recorded, so a run
// that spans a month's end is counted once: in the month of its record.
// It reads the ledger only (never a record), so a record that can't be
// read never stops it; entries made a day after to only tell retries.
func ExtrasBetween(ctx context.Context, s Records, from, to time.Time, schedule string) ([]Extra, error) {
	all, err := ReadLedger(ctx, s, from, to.Add(recordSlack))
	if err != nil {
		return nil, err
	}
	var entries, later []LedgerEntry
	runs := map[string]bool{}
	for _, e := range all {
		if !e.At.Before(to) {
			later = append(later, e)
			continue
		}
		entries = append(entries, e)
		// Only a success can be set aside (a failure carries its
		// schedule): its run's record is looked up, one git command each.
		if e.Kind == LedgerAttempt && !e.Failed && e.Output != "" {
			runs[runOf(e.Workflow)] = true
		}
	}
	if len(entries) == 0 {
		return nil, nil
	}
	recorded, err := readRecorded(ctx, s, runs)
	if err != nil {
		return nil, err
	}
	var out []Extra
	for _, x := range Extras(entries, recorded, later) {
		if schedule == "" || x.Schedule == schedule || x.Unreadable {
			out = append(out, x)
		}
	}
	return out, nil
}

// Counted is w with every Claude step's activity (ask, agent, act and
// skill steps, by their activity names) counting each attempt in the
// ledger of store: the calls Claude answered during it (pirn's Meter),
// whether it succeeded, and if it did, its output's blob ID, so the
// record that holds that output is what counts it. The entry is written
// even when the attempt fails or is cancelled. A write that fails is
// logged, and never fails the step; a successful attempt's write never
// runs past the activity's deadline (it is skipped, and logged, if too
// little time is left), nor does a failed one's, so neither sends the
// step back to Claude. A
// Claude step registered as anything but func(context.Context,
// plan.Input) (string, error) can't be counted, and panics.
func Counted(w Activities, store *warp.Store) Activities { return counted{w, store} }

// IsCounted reports whether a, as Counted registered it, counts its
// attempts in the ledger.
func IsCounted(a any) bool {
	f, ok := a.(countedStep)
	return ok && f != nil
}

// countedStep is a Claude step's activity as Counted registers it.
type countedStep func(context.Context, plan.Input) (string, error)

// WorkerStopTimeout is how long a worker that is stopping (heedle down,
// Ctrl-C) lets its running steps go on before it cancels them: long
// enough for most to finish and note their calls in the ledger. A step
// still running then is cancelled, and notes its calls if the worker is
// still there to; a worker that is killed notes nothing of the attempts
// it was running.
const WorkerStopTimeout = 45 * time.Second

// LedgerMargin is how much of a successful attempt's time a ledger write
// leaves before its deadline; with less left, the write is skipped.
var LedgerMargin = 5 * time.Second

type counted struct {
	w     Activities
	store *warp.Store
}

func (c counted) RegisterActivityWithOptions(a any, o activity.RegisterOptions) {
	kind, claude := claudeKinds[o.Name]
	if claude && c.store != nil {
		f, ok := a.(func(context.Context, plan.Input) (string, error))
		if !ok {
			panic(fmt.Sprintf("loom: Counted can't count %s, registered as %T", o.Name, a))
		}
		a = countedStep(func(ctx context.Context, in plan.Input) (string, error) {
			m := &pirn.Meter{}
			out, err := f(pirn.WithMeter(ctx, m), in)
			calls := m.Calls()
			if len(calls) == 0 {
				return out, err
			}
			e := LedgerEntry{Kind: LedgerAttempt, At: time.Now().UTC(), Node: in.NodeID, Step: kind, Calls: calls, Failed: err != nil}
			if activity.IsActivity(ctx) {
				info := activity.GetInfo(ctx)
				e.Workflow, e.Run, e.Activity, e.Attempt = info.WorkflowExecution.ID, info.WorkflowExecution.RunID, info.ActivityID, info.Attempt
				if !info.StartedTime.IsZero() {
					e.At = info.StartedTime.UTC() // the attempt's start, on Temporal's clock
				}
			}
			wctx, cancel, ok := ledgerContext(ctx)
			defer cancel()
			if !ok {
				logLedger(ctx, fmt.Errorf("too little time left to note %d calls of step %s", len(calls), in.NodeID))
				return out, err
			}
			if err == nil {
				id, herr := hashBlob(wctx, []byte(out))
				if herr != nil {
					// Without its output it couldn't be set aside, and would
					// count twice: not written (review 56).
					logLedger(ctx, herr)
					return out, err
				}
				e.Output = id
			}
			if e.Workflow != "" {
				var serr error
				if e.Schedule, serr = scheduleOfRun(wctx, c.store, runOf(e.Workflow)); serr != nil {
					// Its schedule is then found by its run's record, if it
					// has one (Extras); logged, as it may not (review 56).
					logLedger(ctx, fmt.Errorf("finding the schedule of %s: %w", e.Workflow, serr))
				}
			}
			if werr := WriteLedger(wctx, c.store, e); werr != nil {
				logLedger(ctx, werr)
			}
			return out, err
		})
	}
	c.w.RegisterActivityWithOptions(a, o)
}

// hashBlob is warp.HashBlob; a test makes it fail.
var hashBlob = warp.HashBlob

// ledgerContext is the context a ledger write runs in: not cancelled with
// ctx (a cancelled attempt is still noted), for at most 30 seconds, and
// ending LedgerMargin before the activity's deadline, so a late write
// never turns its result (a success, or an error Temporal mustn't retry)
// into a timeout that Temporal tries again, asking Claude again (review
// 56). ok is false when that leaves no time.
func ledgerContext(ctx context.Context) (context.Context, context.CancelFunc, bool) {
	end := time.Now().Add(30 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Add(-LedgerMargin).Before(end) {
		end = d.Add(-LedgerMargin)
	}
	wctx, cancel := context.WithDeadline(context.WithoutCancel(ctx), end)
	return wctx, cancel, time.Now().Before(end)
}

func logLedger(ctx context.Context, err error) {
	if activity.IsActivity(ctx) {
		activity.GetLogger(ctx).Warn("usage ledger: " + err.Error())
	}
}

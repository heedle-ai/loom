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
// blobs they name).

// LedgerPrefix is where the ledger's entries are, one ref each, by the
// month they were made in (UTC). It is outside refs/loom/, where runs and
// branches are.
const LedgerPrefix = "refs/usage/"

// Ledger entry kinds.
const (
	LedgerAttempt   = "attempt"    // one attempt of a Claude step
	LedgerFrontDoor = "front-door" // one front-door request, every attempt at a proposal
	LedgerRun       = "run"        // a scheduled run: which schedule started it
)

// LedgerEntry is one entry. Workflow and Run are the execution that made
// the calls: for an attempt, the plan's workflow (<run>/plan) and its run;
// for a front-door ask, the ask's; for LedgerRun, the run that Schedule
// started.
type LedgerEntry struct {
	Ledger   int         `json:"ledger"` // 1
	Kind     string      `json:"kind"`
	At       time.Time   `json:"at"`
	Workflow string      `json:"workflowId,omitempty"`
	Run      string      `json:"runId,omitempty"`
	Activity string      `json:"activityId,omitempty"`
	Node     string      `json:"node,omitempty"`
	Step     string      `json:"step,omitempty"` // ask, agent, act or skill
	Attempt  int32       `json:"attempt,omitempty"`
	Failed   bool        `json:"failed,omitempty"`
	Output   warp.ID     `json:"output,omitempty"` // a successful attempt's output, as a blob ID
	Schedule string      `json:"schedule,omitempty"`
	Calls    []pirn.Call `json:"calls,omitempty"`
}

// id names e by what it is about: the same attempt, ask or run is the
// same entry.
func (e LedgerEntry) id() string {
	key, _ := json.Marshal([]any{e.Kind, e.Workflow, e.Run, e.Activity, e.Node, e.Attempt})
	if e.Activity == "" && e.Kind != LedgerRun {
		// Not from an activity (a test, a tool): only its time tells it
		// from another.
		key, _ = json.Marshal([]any{e.Kind, e.Workflow, e.Run, e.Node, e.At.UnixNano(), e.Calls})
	}
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:20])
}

// ledgerDir is the folder of e's ref: its month and its ID.
func (e LedgerEntry) ledgerDir() string {
	return LedgerPrefix + e.At.UTC().Format("2006-01") + "/" + e.id() + "/"
}

// WriteLedger writes e: a commit of entry.json, made at e.At, on its own
// ref. Writing an entry already there (the same attempt, ask or run) is
// done. It is for activities only.
func WriteLedger(ctx context.Context, s *warp.Store, e LedgerEntry) error {
	e.Ledger = 1
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
	// Already written, with this content or another: written.
	if have, err := s.Refs(ctx, e.ledgerDir()); err != nil {
		return err
	} else if len(have) > 0 {
		return nil
	}
	ref := e.ledgerDir() + string(blob)
	if err := s.Advance(ctx, ref, "", commit); err != nil {
		// Its ID is what it is about: already there is already written
		// (a retried budget check's marker, say).
		if _, ok, rerr := s.Resolve(ctx, ref); rerr == nil && ok {
			return nil
		}
		return err
	}
	return nil
}

// ledgerBlob is what a ledger ref's last part must be: a blob ID.
var ledgerBlob = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

// ReadLedger is the entries made at or after from and before to, oldest
// first.
func ReadLedger(ctx context.Context, s Records, from, to time.Time) ([]LedgerEntry, error) {
	var out []LedgerEntry
	for m := time.Date(from.UTC().Year(), from.UTC().Month(), 1, 0, 0, 0, 0, time.UTC); m.Before(to); m = m.AddDate(0, 1, 0) {
		refs, err := s.Refs(ctx, LedgerPrefix+m.Format("2006-01")+"/")
		if err != nil {
			return nil, err
		}
		var ids []warp.ID
		for _, r := range refs {
			// .../<month>/<id>/<entry.json's blob ID>
			if b := warp.ID(r.Name[strings.LastIndex(r.Name, "/")+1:]); ledgerBlob.MatchString(string(b)) {
				ids = append(ids, b)
			}
		}
		if len(ids) == 0 {
			continue
		}
		bs, err := s.Blobs(ctx, ids)
		if err != nil {
			return nil, err
		}
		for _, b := range bs {
			var e LedgerEntry
			if err := json.Unmarshal(b, &e); err != nil || e.Ledger != 1 {
				continue // not one this loom wrote
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
	ExtraRetry     = "retries"     // attempts that failed and were tried again
	ExtraFailedRun = "failed runs" // calls of runs with no record of them: failed (or not yet recorded)
)

// Extra is usage the ledger adds to what records hold: one entry's calls.
// Workflow is the run they belong to (for front-door asks, the ask's), and
// Schedule the schedule that started it, when the ledger knows.
type Extra struct {
	Kind     string    `json:"kind"`
	Workflow string    `json:"workflowId,omitempty"`
	Schedule string    `json:"schedule,omitempty"`
	At       time.Time `json:"at"`
	Usage    Usage     `json:"usage"`
}

// runOf is the run a plan's workflow belongs to.
func runOf(workflow string) string { return strings.TrimSuffix(workflow, "/plan") }

// Extras is what entries add to the usage runs' records hold, each call
// once: every front-door ask; every failed attempt, as a retry if the
// same step was tried again and else as a failed run's; and every
// successful attempt no record holds (its exact output, at that node of
// that run, isn't in runs), as a failed run's. A successful attempt a
// record holds is set aside: the record counts it. Pure: a function of
// what is written.
func Extras(entries []LedgerEntry, runs []RunUsage) []Extra {
	schedule := map[string]string{}
	later := map[string]int32{} // the last attempt of each step
	type stepKey struct{ wf, run, node string }
	for _, e := range entries {
		switch e.Kind {
		case LedgerRun:
			schedule[e.Workflow] = e.Schedule
		case LedgerAttempt:
			k := fmt.Sprint(stepKey{e.Workflow, e.Run, e.Node})
			if e.Attempt > later[k] {
				later[k] = e.Attempt
			}
		}
	}
	// Each recorded output may set aside one successful attempt.
	recorded := map[string]int{}
	for _, r := range runs {
		for node, id := range r.Outputs {
			recorded[r.Workflow+"\x00"+node+"\x00"+string(id)]++
		}
		if r.Schedule != "" {
			schedule[r.Workflow] = r.Schedule
		}
	}
	var out []Extra
	for _, e := range entries {
		if len(e.Calls) == 0 {
			continue
		}
		x := Extra{Workflow: runOf(e.Workflow), At: e.At, Usage: callsUsage(e)}
		switch e.Kind {
		case LedgerFrontDoor:
			x.Kind, x.Workflow = ExtraFrontDoor, e.Workflow
		case LedgerAttempt:
			x.Schedule = schedule[x.Workflow]
			k := x.Workflow + "\x00" + e.Node + "\x00" + string(e.Output)
			switch {
			case !e.Failed && recorded[k] > 0:
				recorded[k]--
				continue // the record counts it
			case e.Failed && e.Attempt < later[fmt.Sprint(stepKey{e.Workflow, e.Run, e.Node})]:
				x.Kind = ExtraRetry
			default:
				x.Kind = ExtraFailedRun
			}
		default:
			continue
		}
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
// and before to, given the runs recorded then (AllRunsBetween's, or one
// schedule's RunsBetween), and, given schedule, only that schedule's.
func ExtrasBetween(ctx context.Context, s Records, from, to time.Time, runs []RunUsage, schedule string) ([]Extra, error) {
	// A run's marker is written when it starts, so look a day further
	// back for those of runs that started before from.
	entries, err := ReadLedger(ctx, s, from.Add(-recordSlack), to)
	if err != nil {
		return nil, err
	}
	var out []Extra
	for _, x := range Extras(entries, runs) {
		if !x.At.Before(from) && (schedule == "" || x.Schedule == schedule) {
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
// even when the attempt fails or is cancelled; a write that fails is
// logged, and never fails the step or sends it back to Claude.
func Counted(w Activities, store *warp.Store) Activities { return counted{w, store} }

type counted struct {
	w     Activities
	store *warp.Store
}

func (c counted) RegisterActivityWithOptions(a any, o activity.RegisterOptions) {
	kind, claude := claudeKinds[o.Name]
	if f, ok := a.(func(context.Context, plan.Input) (string, error)); ok && claude && c.store != nil {
		a = func(ctx context.Context, in plan.Input) (string, error) {
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
			wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if err == nil {
				id, herr := warp.HashBlob(wctx, []byte(out))
				if herr != nil {
					logLedger(ctx, herr)
				}
				e.Output = id
			}
			if werr := WriteLedger(wctx, c.store, e); werr != nil {
				logLedger(ctx, werr)
			}
			return out, err
		}
	}
	c.w.RegisterActivityWithOptions(a, o)
}

func logLedger(ctx context.Context, err error) {
	if activity.IsActivity(ctx) {
		activity.GetLogger(ctx).Warn("usage ledger: " + err.Error())
	}
}

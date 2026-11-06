package loom_test

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// claudeStep is a stand-in ask step: on each attempt it makes the calls
// calls says (each noted on the meter, as pirn notes what Claude
// answers), then fails if fail says so, else answers with those calls'
// tokens, as pirn records them.
type claudeStep struct {
	calls func(node string, attempt int32) []pirn.Call
	fail  func(node string, attempt int32) bool
}

func (c claudeStep) run(ctx context.Context, in plan.Input) (string, error) {
	attempt := activity.GetInfo(ctx).Attempt
	var a pirn.Answer
	for _, call := range c.calls(in.NodeID, attempt) {
		pirn.MeterFrom(ctx).Add(call)
		a.Model, a.InputTokens, a.OutputTokens = call.Model, a.InputTokens+call.Input, a.OutputTokens+call.Output
	}
	if c.fail(in.NodeID, attempt) {
		return "", errors.New("overloaded")
	}
	if a.Model == "" {
		a.Model = "claude-sonnet-5-5"
	}
	a.Pirn, a.Text = 1, fmt.Sprintf("%s on attempt %d", in.NodeID, attempt)
	return jsonOf(a), nil
}

// runs numbers ledgerRun's runs.
var runs int

// ledgerRun runs src at at on a worker that counts its Claude steps in
// s's ledger (Counted), the steps being step; schedule, if set, started
// it, with its budget asked of sc.
func ledgerRun(t *testing.T, s *warp.Store, sc loom.ScheduleControl, src, schedule string, at time.Time, step claudeStep) (loom.RunResult, error) {
	t.Helper()
	reg := claudeRegistry(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(at)
	// Each run its own workflow ID, as Submit and schedules give them.
	runs++
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: fmt.Sprintf("loom-test-%d", runs)})
	loom.RegisterWith(env, &loom.Checker{Registry: reg}, &loom.Recorder{Store: s, Schedules: sc})
	loom.Labelled(loom.Counted(env, s)).RegisterActivityWithOptions(shuttle.ArrowAsActivity(step.run), activity.RegisterOptions{Name: pirn.ActivityName})
	if schedule != "" {
		env.SetTypedSearchAttributesOnStart(temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyKeyword("TemporalScheduledById").ValueSet(loom.SchedulePrefix + schedule)))
	}
	env.ExecuteWorkflow(loom.RunWorkflowName, loom.RunInput{Program: src, Plan: must(loom.Compile(ctx, reg, src)), Ref: loom.DefaultRef})
	var res loom.RunResult
	err := env.GetWorkflowError()
	if err == nil {
		env.GetWorkflowResult(&res)
	}
	return res, err
}

// counted is everything the Costs page counts between from and to:
// records' usage and the ledger's extras, by kind ("record" for records).
func counted(t *testing.T, s *warp.Store, from, to time.Time) (loom.Tokens, map[string]loom.Tokens) {
	t.Helper()
	runs := must(loom.AllRunsBetween(ctx, s, from, to))
	by := map[string]loom.Tokens{}
	var total loom.Tokens
	for _, r := range runs {
		by["record"] = by["record"].Add(r.Usage.Total())
		total = total.Add(r.Usage.Total())
	}
	for _, x := range must(loom.ExtrasBetween(ctx, s, from, to, "")) {
		by[x.Kind] = by[x.Kind].Add(x.Usage.Total())
		total = total.Add(x.Usage.Total())
	}
	return total, by
}

// nodeSeed is a node's own number, for seeding what its attempts do.
func nodeSeed(node string) int64 {
	h := fnv.New64a()
	h.Write([]byte(node))
	return int64(h.Sum64())
}

// now is the time the tests' runs start: Temporal's test environment
// gives activities the real clock, so runs are started on it too.
func now() time.Time { return time.Now().UTC().Truncate(time.Second) }

var hundred = pirn.Call{Model: "claude-sonnet-5-5", Input: 100, Output: 10}

// TestInv_RetriedAttemptsAreCounted: a step that fails twice, then
// succeeds, counts all three calls: the record holds the last, and the
// ledger the two retried attempts, each once (LOOM-320, roadmap 10.4.1).
func TestInv_RetriedAttemptsAreCounted(t *testing.T) {
	s := newStore(t)
	at := now()
	step := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(_ string, a int32) bool { return a < 3 }}
	if res, err := ledgerRun(t, s, nil, `(ask "the news")`, "", at, step); err != nil || res.Commit == "" {
		t.Fatalf("the run: %+v %v", res, err)
	}
	total, by := counted(t, s, at.Add(-time.Hour), at.Add(time.Hour))
	if total != (loom.Tokens{Input: 300, Output: 30}) || by["record"] != (loom.Tokens{Input: 100, Output: 10}) || by[loom.ExtraRetry] != (loom.Tokens{Input: 200, Output: 20}) {
		t.Errorf("counted %+v: %+v", total, by)
	}
}

// TestInv_FailedRunsAreCounted: a run that fails, with no record, counts
// every call its steps made: a step that succeeded before another
// failed, and every attempt of the one that failed (LOOM-321, roadmap
// 10.4.1).
func TestInv_FailedRunsAreCounted(t *testing.T) {
	s := newStore(t)
	at := now()
	reg := claudeRegistry(t)
	p := must(loom.Compile(ctx, reg, `(pipe (ask "a") (ask "b"))`))
	second := string(p.Nodes[1].ID)
	step := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(n string, _ int32) bool { return n == second }}
	if _, err := ledgerRun(t, s, nil, `(pipe (ask "a") (ask "b"))`, "", at, step); err == nil {
		t.Fatal("the run should fail")
	}
	if runs := must(loom.AllRunsBetween(ctx, s, at.Add(-time.Hour), at.Add(time.Hour))); len(runs) != 0 {
		t.Fatalf("a failed run was recorded: %+v", runs)
	}
	total, by := counted(t, s, at.Add(-time.Hour), at.Add(time.Hour))
	if total != (loom.Tokens{Input: 400, Output: 40}) || by[loom.ExtraFailedRun] != (loom.Tokens{Input: 200, Output: 20}) || by[loom.ExtraRetry] != (loom.Tokens{Input: 200, Output: 20}) {
		t.Errorf("counted %+v: %+v", total, by)
	}
}

// meteredAsker answers in turn like scripted, each answer one call
// noted on the meter, as pirn's Claude notes it.
type meteredAsker struct{ scripted }

func (m *meteredAsker) Ask(ctx context.Context, q pirn.Question) (pirn.Answer, error) {
	a, err := m.scripted.Ask(ctx, q)
	a.Model, a.InputTokens, a.OutputTokens = "claude-sonnet-5-5", 1000, 100
	pirn.MeterFrom(ctx).Add(pirn.Call{Model: a.Model, Input: a.InputTokens, Output: a.OutputTokens})
	return a, err
}

// TestInv_FrontDoorAsksAreCounted: the front door's asks count toward
// the month, every attempt, whether a proposal came of them or not
// (LOOM-322, roadmap 10.4.1).
func TestInv_FrontDoorAsksAreCounted(t *testing.T) {
	s := newStore(t)
	from := time.Now().UTC().Add(-time.Hour)
	w := writer(&meteredAsker{scripted{replies: []string{"no JSON here", "```json\n{\"summary\": \"Hi.\", \"program\": \"(echo \\\"hi\\\")\"}\n```"}}})
	w.Ledger = s
	if _, err := w.AskActivity(ctx, loom.AskRequest{Request: "say hi"}); err != nil {
		t.Fatal(err)
	}
	w = writer(&meteredAsker{scripted{replies: []string{"nothing", "nothing", "nothing"}}})
	w.Ledger = s
	if _, err := w.AskActivity(ctx, loom.AskRequest{Request: "say nothing"}); err == nil {
		t.Fatal("no proposal should come of three unusable answers")
	}
	total, by := counted(t, s, from, time.Now().UTC().Add(time.Hour))
	if want := (loom.Tokens{Input: 5000, Output: 500}); total != want || by[loom.ExtraFrontDoor] != want {
		t.Errorf("counted %+v: %+v", total, by)
	}
	// The month the asks were made in (review 56: not the month after).
	es := must(loom.ReadLedger(ctx, s, from, time.Now().UTC().Add(time.Hour)))
	start, end := loom.MonthOf(es[0].At, time.UTC)
	if xs := must(loom.ExtrasBetween(ctx, s, start, end, "")); len(xs) != 2 || xs[0].Kind != loom.ExtraFrontDoor {
		t.Errorf("this month's extras: %+v", xs)
	}
}

// TestInv_BudgetsCountWhatRecordsDont: a schedule's budget counts the
// calls its runs made that no record holds: after a scheduled run that
// failed, having spent three cents in failed attempts, the next run of a
// $0.05 schedule is stopped before any step, the schedule paused; one
// that is another schedule's or a run by hand isn't charged to it
// (LOOM-323, roadmap 10.4.1).
func TestInv_BudgetsCountWhatRecordsDont(t *testing.T) {
	s := newStore(t)
	sc := &schedules{budget: map[string]loom.ScheduleBudget{"news": {Budget: loom.Budget{USD: 0.05}}, "other": {Budget: loom.Budget{USD: 0.05}}}}
	nov := now()
	cent := pirn.Call{Model: "claude-sonnet-5-5", Input: 10000 / 3, Output: 1000 / 3} // about a cent
	pricey := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{cent, cent, cent, cent, cent} }, fail: func(string, int32) bool { return true }}
	// A failed run by hand and one of another schedule: not news's.
	if _, err := ledgerRun(t, s, sc, `(ask "the news")`, "", nov, pricey); err == nil {
		t.Fatal("the run should fail")
	}
	if _, err := ledgerRun(t, s, sc, `(ask "the news")`, "other", nov.Add(time.Minute), pricey); err == nil {
		t.Fatal("the other schedule's run should fail")
	}
	cheap := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(string, int32) bool { return false }}
	if res, err := ledgerRun(t, s, sc, `(ask "the news")`, "news", nov.Add(2*time.Minute), cheap); err != nil || res.Skipped != "" {
		t.Fatalf("news's first run: %+v %v", res, err)
	}
	// news's own failed run: 3 attempts of about 5 cents' calls.
	if _, err := ledgerRun(t, s, sc, `(ask "the news")`, "news", nov.Add(3*time.Minute), pricey); err == nil {
		t.Fatal("the run should fail")
	}
	res, err := ledgerRun(t, s, sc, `(ask "the news")`, "news", nov.Add(4*time.Minute), cheap)
	if err != nil || !strings.HasPrefix(res.Skipped, "paused: monthly budget $0.05 reached") || !sc.budget["news"].Paused {
		t.Errorf("after a failed run over budget: %+v %v (schedule %+v)", res, err, sc.budget["news"])
	}
	if sc.budget["other"].Paused {
		t.Error("another schedule was paused")
	}
}

// TestInv_TheLedgerIsItsOwn: an entry is written once however often it
// is written, on its own ref outside refs/loom/ (where runs and branches
// are), and adding up runs never reads the ledger as runs (LOOM-324,
// roadmap 10.4.1).
func TestInv_TheLedgerIsItsOwn(t *testing.T) {
	s := newStore(t)
	at := now()
	e := loom.LedgerEntry{Kind: loom.LedgerFrontDoor, At: at, Workflow: "ask-1", Run: "r1", Calls: []pirn.Call{hundred}}
	for i := 0; i < 3; i++ {
		if err := loom.WriteLedger(ctx, s, e); err != nil {
			t.Fatal(err)
		}
	}
	refs := must(s.Refs(ctx, "refs/"))
	// refs/usage/<month>/<the entry's ID>/<entry.json's blob ID>: a month
	// is read from its refs' names and one batch of blobs.
	shape := regexp.MustCompile(`^refs/usage/` + at.Format("2006-01") + `/[0-9a-f]{40}/[0-9a-f]{40,64}$`)
	if len(refs) != 1 || !shape.MatchString(refs[0].Name) || strings.HasPrefix(refs[0].Name, "refs/loom/") {
		t.Errorf("refs: %+v", refs)
	}
	if es := must(loom.ReadLedger(ctx, s, at.Add(-time.Hour), at.Add(time.Hour))); len(es) != 1 || es[0].Calls[0] != hundred {
		t.Errorf("entries: %+v", es)
	}
	if runs := must(loom.AllRunsBetween(ctx, s, at.Add(-time.Hour), at.Add(time.Hour))); len(runs) != 0 {
		t.Errorf("the ledger was read as runs: %+v", runs)
	}
	// Two entries under one ID (writers racing): the first is read.
	dir := refs[0].Name[:strings.LastIndex(refs[0].Name, "/")+1]
	other := e
	other.Ledger, other.Calls = 1, []pirn.Call{hundred, hundred}
	blob := must(s.PutBlob(ctx, []byte(jsonOf(other))))
	tree := must(s.PutTree(ctx, []warp.Entry{{Name: "entry.json", Kind: warp.KindBlob, ID: blob}}))
	commit := must(s.Commit(ctx, warp.CommitSpec{Tree: tree, Message: "usage", Author: warp.Signature{Name: "x", Email: "x@y", When: at}}))
	if err := s.Advance(ctx, dir+string(blob), "", commit); err != nil {
		t.Fatal(err)
	}
	refs = must(s.Refs(ctx, dir))
	es := must(loom.ReadLedger(ctx, s, at.Add(-time.Hour), at.Add(time.Hour)))
	if len(refs) != 2 || len(es) != 1 || len(es[0].Calls) != map[bool]int{true: 2, false: 1}[refs[0].Name == dir+string(blob)] {
		t.Errorf("two under one ID: %+v read as %+v", refs, es)
	}
}

// TestLaw_EveryCallIsCountedOnce: with random failures and retries, what
// the Costs page and budgets count (records plus the ledger's extras)
// equals every call Claude answered, exactly once: steps that succeed
// first time, ones that fail and are tried again, runs that fail and are
// never recorded, and front-door asks. Checked on 40 random programs of
// ask steps under pipe and par, each attempt making 0 to 3 calls and
// failing at random (Soundness law 21, extended by roadmap 10.4.1).
func TestLaw_EveryCallIsCountedOnce(t *testing.T) {
	r := rand.New(rand.NewSource(1041))
	var leaf func(d int) string
	leaf = func(d int) string {
		if d == 0 || r.Intn(3) == 0 {
			return fmt.Sprintf(`(ask "q%d")`, r.Intn(1000))
		}
		return fmt.Sprintf("(%s %s %s)", []string{"pipe", "par"}[r.Intn(2)], leaf(d-1), leaf(d-1))
	}
	models := []string{"claude-sonnet-5-5", "claude-haiku-5-5"}
	at := now()
	all := map[string]loom.Tokens{}
	partly, succeeded := 0, 0
	for i := 0; i < 40; i++ {
		// Each trial its own store: Temporal's test environment gives
		// every run the same run ID, which a store records once.
		s := newStore(t)
		var mu sync.Mutex
		var made loom.Tokens
		seed := r.Int63()
		step := claudeStep{
			calls: func(node string, attempt int32) []pirn.Call {
				rr := rand.New(rand.NewSource(seed ^ nodeSeed(node)<<8 ^ int64(attempt)))
				var cs []pirn.Call
				for n := rr.Intn(4); n > 0; n-- {
					c := pirn.Call{Model: models[rr.Intn(2)], Input: int64(rr.Intn(5000)), Output: int64(rr.Intn(500))}
					cs = append(cs, c)
					mu.Lock()
					made = made.Add(loom.Tokens{Input: c.Input, Output: c.Output})
					mu.Unlock()
				}
				return cs
			},
			fail: func(node string, attempt int32) bool {
				failed := rand.New(rand.NewSource(seed^nodeSeed(node)<<16^int64(attempt)<<1)).Intn(3) == 0
				mu.Lock()
				if !failed {
					succeeded++
				}
				mu.Unlock()
				return failed
			},
		}
		src := leaf(3)
		// Every fifth run is recorded three days into the next month
		// (Temporal's test clock), more than a day after its attempts
		// (on the real clock) this month, as a run whose act step waits
		// for a person may be (review 56 rounds 1, 2 and 4).
		start := at
		if i%5 == 0 {
			_, start = loom.MonthOf(at, time.UTC)
			start = start.AddDate(0, 0, 3)
		}
		succeeded = 0
		if _, err := ledgerRun(t, s, nil, src, "", start, step); err != nil && succeeded > 0 {
			partly++
		}
		if r.Intn(3) == 0 {
			a := &meteredAsker{scripted{replies: []string{"no JSON here", "```json\n{\"summary\": \"Hi.\", \"program\": \"(echo \\\"hi\\\")\"}\n```"}}}
			w := writer(a)
			w.Ledger = s
			w.AskActivity(ctx, loom.AskRequest{Request: "say hi"})
			made = made.Add(loom.Tokens{Input: 1000 * int64(len(a.asked)), Output: 100 * int64(len(a.asked))})
		}
		// Each month on its own, as the Costs page and budgets count:
		// a run that spans a month's end is still counted once.
		this, next := loom.MonthOf(at, time.UTC)
		got, by := counted(t, s, this, next)
		got2, by2 := counted(t, s, next, next.AddDate(0, 1, 0))
		if got.Add(got2) != made {
			t.Fatalf("%s: counted %+v + %+v (%+v, %+v), but Claude answered %+v", src, got, got2, by, by2, made)
		}
		for _, b := range []map[string]loom.Tokens{by, by2} {
			for k, v := range b {
				all[k] = all[k].Add(v)
			}
		}
	}
	by := all
	if partly == 0 {
		t.Error("no run failed after some of its steps succeeded")
	}
	if by["record"] == (loom.Tokens{}) || by[loom.ExtraRetry] == (loom.Tokens{}) || by[loom.ExtraFailedRun] == (loom.Tokens{}) || by[loom.ExtraFrontDoor] == (loom.Tokens{}) {
		t.Errorf("not every kind of call was tried: %+v", by)
	}
}

// recordingActivities keeps what was registered, by name.
type recordingActivities map[string]any

func (r recordingActivities) RegisterActivityWithOptions(a any, o activity.RegisterOptions) {
	r[o.Name] = a
}

// TestInv_SuccessNeverWaitsOnTheLedger: a successful attempt with too
// little of its time left returns at once, its calls not noted, rather
// than run past its deadline and be tried again (asking Claude again); a
// failed one is still noted; a Claude step Counted can't wrap panics
// rather than go uncounted (LOOM-327, review 56).
func TestInv_SuccessNeverWaitsOnTheLedger(t *testing.T) {
	s := newStore(t)
	reg := recordingActivities{}
	step := func(fail bool) func(context.Context, plan.Input) (string, error) {
		return func(ctx context.Context, in plan.Input) (string, error) {
			pirn.MeterFrom(ctx).Add(hundred)
			if fail {
				return "", errors.New("overloaded")
			}
			return `{"pirn":1}`, nil
		}
	}
	loom.Counted(reg, s).RegisterActivityWithOptions(step(false), activity.RegisterOptions{Name: pirn.ActivityName})
	loom.Counted(reg, s).RegisterActivityWithOptions(step(true), activity.RegisterOptions{Name: pirn.AgentActivityName})
	short, cancel := context.WithTimeout(ctx, loom.LedgerMargin/2)
	defer cancel()
	start := time.Now()
	ask := reflectCall(reg[pirn.ActivityName])
	if out, err := ask(short, plan.Input{NodeID: "n0"}); err != nil || out == "" || time.Since(start) > time.Second {
		t.Errorf("a success near its deadline: %q %v after %v", out, err, time.Since(start))
	}
	if es := must(loom.ReadLedger(ctx, s, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))); len(es) != 0 {
		t.Errorf("noted with no time left: %+v", es)
	}
	agent := reflectCall(reg[pirn.AgentActivityName])
	if _, err := agent(short, plan.Input{NodeID: "n1"}); err == nil {
		t.Fatal("the failing step succeeded")
	}
	// Nor a failed one's: its error mustn't become a timeout Temporal
	// tries again (review 56 round 2). With time left, it is noted.
	if es := must(loom.ReadLedger(ctx, s, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))); len(es) != 0 {
		t.Errorf("a failed attempt noted with no time left: %+v", es)
	}
	if _, err := agent(ctx, plan.Input{NodeID: "n1"}); err == nil {
		t.Fatal("the failing step succeeded")
	}
	if es := must(loom.ReadLedger(ctx, s, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))); len(es) != 1 || !es[0].Failed {
		t.Errorf("a failed attempt wasn't noted: %+v", es)
	}
	// A success whose output can't be hashed couldn't be set aside: it
	// isn't noted, so its record alone counts it.
	restore := loom.SetHashBlob(func(context.Context, []byte) (warp.ID, error) { return "", errors.New("no git") })
	if out, err := ask(ctx, plan.Input{NodeID: "n2"}); err != nil || out == "" {
		t.Errorf("a success that can't be hashed: %q %v", out, err)
	}
	restore()
	if es := must(loom.ReadLedger(ctx, s, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))); len(es) != 1 {
		t.Errorf("a success that couldn't be hashed was noted: %+v", es)
	}
	if !loom.IsCounted(reg[pirn.ActivityName]) || loom.IsCounted(step(false)) {
		t.Error("IsCounted doesn't tell a counted step")
	}
	defer func() {
		if recover() == nil {
			t.Error("a Claude step of another type was registered uncounted")
		}
	}()
	type named func(context.Context, plan.Input) (string, error)
	loom.Counted(reg, s).RegisterActivityWithOptions(named(step(false)), activity.RegisterOptions{Name: pirn.ActActivityName})
}

// reflectCall calls a counted step whatever its named type.
func reflectCall(a any) func(context.Context, plan.Input) (string, error) {
	v := reflect.ValueOf(a)
	return func(ctx context.Context, in plan.Input) (string, error) {
		out := v.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(in)})
		err, _ := out[1].Interface().(error)
		return out[0].String(), err
	}
}

// TestInv_LongRunsKeepTheirSchedule: a scheduled run's calls are its
// schedule's however long after its start they're made, in another month
// even: the schedule is found by the run (LOOM-328, review 56: a run
// longer than a day lost its schedule, and its budget failed open).
func TestInv_LongRunsKeepTheirSchedule(t *testing.T) {
	s := newStore(t)
	sc := &schedules{budget: map[string]loom.ScheduleBudget{"news": {Budget: loom.Budget{USD: 100}}}}
	started := now().AddDate(0, 0, -40)
	step := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(string, int32) bool { return true }}
	if _, err := ledgerRun(t, s, sc, `(ask "the news")`, "news", started, step); err == nil {
		t.Fatal("the run should fail")
	}
	this, next := loom.MonthOf(time.Now(), time.UTC)
	xs := must(loom.ExtrasBetween(ctx, s, this, next, "news"))
	if len(xs) != 3 || xs[0].Schedule != "news" {
		t.Errorf("this month's calls of news: %+v", xs)
	}
}

// TestInv_UnreadableEntriesFailClosed: a ledger entry that can't be read
// is a step with no counts, charged to every schedule, so a budget that
// meets one pauses its schedule saying so, rather than leave its calls
// out (LOOM-329, review 56).
func TestInv_UnreadableEntriesFailClosed(t *testing.T) {
	// In UTC, and west of it, where a month starts hours after UTC's
	// (review 56 round 2: there it was dropped).
	for _, zone := range []string{"UTC", "America/Los_Angeles"} {
		loc := must(time.LoadLocation(zone))
		s := newStore(t)
		at := now()
		blob := must(s.PutBlob(ctx, []byte("not an entry")))
		tree := must(s.PutTree(ctx, []warp.Entry{{Name: "entry.json", Kind: warp.KindBlob, ID: blob}}))
		commit := must(s.Commit(ctx, warp.CommitSpec{Tree: tree, Message: "usage: ?", Author: warp.Signature{Name: "x", Email: "x@y", When: at}}))
		if err := s.Advance(ctx, loom.LedgerPrefix+at.Format("2006-01")+"/"+strings.Repeat("a", 40)+"/"+string(blob), "", commit); err != nil {
			t.Fatal(err)
		}
		this, next := loom.MonthOf(at, loc)
		xs := must(loom.ExtrasBetween(ctx, s, this, next, "news"))
		if len(xs) != 1 || !xs[0].Unreadable || len(xs[0].Usage.Missing()) != 1 {
			t.Fatalf("%s: an unreadable entry: %+v", zone, xs)
		}
		sc := &schedules{budget: map[string]loom.ScheduleBudget{"news": {Budget: loom.Budget{USD: 100}, TimeZone: zone}}}
		cheap := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(string, int32) bool { return false }}
		res, err := ledgerRun(t, s, sc, `(ask "the news")`, "news", at, cheap)
		if err != nil || !strings.Contains(res.Skipped, "no estimate") || !strings.Contains(res.Skipped, loom.LedgerPrefix) || !sc.budget["news"].Paused {
			t.Errorf("%s: a budget meeting an unreadable entry: %+v %v", zone, res, err)
		}
	}
}

// TestInv_AProposalRunIsCountedOnce: asking the front door and running
// what it proposed counts every call once: the asks from the ledger, the
// run's from its record; the proposal's own usage (shown on its card) is
// in no record (LOOM-330, review 56).
func TestInv_AProposalRunIsCountedOnce(t *testing.T) {
	s := newStore(t)
	from := now().Add(-time.Hour)
	a := &meteredAsker{scripted{replies: []string{"```json\n{\"summary\": \"The news.\", \"program\": \"(ask \\\"the news\\\")\"}\n```"}}}
	w := writer(a)
	w.Ledger = s
	w.Commands = func(context.Context) (*registry.Registry, []loom.Command, error) {
		reg := claudeRegistry(t)
		return reg, loom.Commands(reg, loom.BuiltinCommands), nil
	}
	p, err := w.AskActivity(ctx, loom.AskRequest{Request: "the news"})
	if err != nil || p.Usage == nil {
		t.Fatalf("%+v %v", p, err)
	}
	step := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(string, int32) bool { return false }}
	if _, err := ledgerRun(t, s, nil, p.Program, "", now(), step); err != nil {
		t.Fatal(err)
	}
	if got, by := counted(t, s, from, time.Now().Add(time.Hour)); got != (loom.Tokens{Input: 1100, Output: 110}) {
		t.Errorf("counted %+v: %+v", got, by)
	}
}

// TestInv_RetriesAcrossAMonthEnd: a failed attempt whose step was tried
// again in the next month is a retry in its own month, not a failed
// run's (LOOM-335, review 56 round 2).
func TestInv_RetriesAcrossAMonthEnd(t *testing.T) {
	s := newStore(t)
	end := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	for i, at := range []time.Time{end.Add(-time.Minute), end.Add(time.Minute)} {
		e := loom.LedgerEntry{Kind: loom.LedgerAttempt, At: at, Workflow: "loom-x/plan", Run: "p", Activity: "1", Node: "n0", Step: "ask", Attempt: int32(i + 1), Failed: true, Calls: []pirn.Call{hundred}}
		if err := loom.WriteLedger(ctx, s, e); err != nil {
			t.Fatal(err)
		}
	}
	nov := must(loom.ExtrasBetween(ctx, s, end.AddDate(0, -1, 0), end, ""))
	dec := must(loom.ExtrasBetween(ctx, s, end, end.AddDate(0, 1, 0), ""))
	if len(nov) != 1 || nov[0].Kind != loom.ExtraRetry || len(dec) != 1 || dec[0].Kind != loom.ExtraFailedRun {
		t.Errorf("November %+v, December %+v", nov, dec)
	}
}

// noRecords is a store whose records can't be read: every Log fails.
type noRecords struct{ *warp.Store }

func (noRecords) Log(context.Context, warp.ID, int) ([]warp.CommitInfo, error) {
	return nil, errors.New("no record may be read")
}

// TestInv_TheLedgerReadsNoRecord: the ledger's extras are found from the
// ledger alone: a successful attempt is set aside by its run's
// LedgerRecorded entry, never by reading records, so a record that can't
// be read (on any ref) never stops a budget check, and the pages read
// each record once (LOOM-333, review 56 round 2).
func TestInv_TheLedgerReadsNoRecord(t *testing.T) {
	s := newStore(t)
	at := now()
	step := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(_ string, a int32) bool { return a < 2 }}
	if res, err := ledgerRun(t, s, nil, `(ask "the news")`, "", at, step); err != nil || res.Commit == "" {
		t.Fatalf("the run: %+v %v", res, err)
	}
	xs, err := loom.ExtrasBetween(ctx, noRecords{s}, at.Add(-time.Hour), at.Add(time.Hour), "")
	if err != nil || len(xs) != 1 || xs[0].Kind != loom.ExtraRetry {
		t.Errorf("extras %+v %v", xs, err)
	}
}

// TestInv_ScheduleLookupsAreLogged: Counted never drops an error finding
// an attempt's schedule unsaid (LOOM-334, review 56 round 2).
func TestInv_ScheduleLookupsAreLogged(t *testing.T) {
	b := must(os.ReadFile("ledger.go"))
	if regexp.MustCompile(`, _ = scheduleOfRun\(`).Match(b) || !strings.Contains(string(b), `logLedger(ctx, fmt.Errorf("finding the schedule of %s: %w", e.Workflow, serr))`) {
		t.Error("Counted drops scheduleOfRun's error")
	}
}

// listingStore counts the listings of refs/usage/recorded/.
type listingStore struct {
	*warp.Store
	calls int
}

func (l *listingStore) Refs(ctx context.Context, prefix string) ([]warp.RefInfo, error) {
	if strings.HasPrefix(prefix, loom.LedgerPrefix+"recorded/") {
		l.calls++
	}
	return l.Store.Refs(ctx, prefix)
}

// TestInv_RecordsAreFoundByTheirRun: finding the records that set
// attempts aside takes one listing a month the window's records may be
// in, however many runs there are (LOOM-336, review 56 rounds 3, 4).
func TestInv_RecordsAreFoundByTheirRun(t *testing.T) {
	s := newStore(t)
	at := now()
	p := must(loom.Compile(ctx, claudeRegistry(t), `(ask "old news")`))
	for i := 0; i < 20; i++ {
		if err := loom.NoteRecorded(ctx, s, fmt.Sprintf("loom-old-%d", i), "r", "", at.AddDate(-1, 0, 0), p, map[string]string{"n0": fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Eight runs this month, each a step tried twice, then recorded.
	for i := 0; i < 8; i++ {
		wf, out := fmt.Sprintf("loom-news-%d", i), fmt.Sprintf(`{"pirn":1,"text":"%d"}`, i)
		id := must(warp.HashBlob(ctx, []byte(out)))
		for a := int32(1); a <= 2; a++ {
			e := loom.LedgerEntry{Kind: loom.LedgerAttempt, At: at, Workflow: wf + "/plan", Run: "p", Activity: "1", Node: "n0", Step: "ask", Attempt: a, Failed: a == 1, Calls: []pirn.Call{hundred}}
			if a == 2 {
				e.Output = id
			}
			if err := loom.WriteLedger(ctx, s, e); err != nil {
				t.Fatal(err)
			}
		}
		if err := loom.NoteRecorded(ctx, s, wf, "r", "", at, p, map[string]string{"n0": out}); err != nil {
			t.Fatal(err)
		}
	}
	ls := &listingStore{Store: s}
	this, _ := loom.MonthOf(at, time.UTC)
	xs := must(loom.ExtrasBetween(ctx, ls, this, at.Add(time.Hour), ""))
	if len(xs) != 8 || xs[0].Kind != loom.ExtraRetry || ls.calls > 2 {
		t.Errorf("%d extras %+v; %d listings of records", len(xs), xs, ls.calls)
	}
}

// TestInv_ALedgerProblemNeverFailsARun: when noting what a record holds
// keeps failing, Record tries again a few times, then records the run
// all the same (logged; its successful attempts count twice, never
// less), so the run ends recorded, never failed (LOOM-337, review 56
// round 4).
func TestInv_ALedgerProblemNeverFailsARun(t *testing.T) {
	s := newStore(t)
	tries := 0
	defer loom.SetNoteRecorded(func(context.Context, *warp.Store, string, string, string, time.Time, plan.Plan, map[string]string) error {
		tries++
		return errors.New("disk full")
	})()
	step := claudeStep{calls: func(string, int32) []pirn.Call { return []pirn.Call{hundred} }, fail: func(string, int32) bool { return false }}
	res, err := ledgerRun(t, s, nil, `(ask "the news")`, "", now(), step)
	if err != nil || res.Commit == "" || tries != loom.NoteAttempts {
		t.Errorf("a run whose note fails: %+v %v after %d tries", res, err, tries)
	}
}

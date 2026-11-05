package loom

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/treadle"
	"github.com/heedle-ai/warp"
)

// Costs (roadmap 10.4). Tokens are facts: every Claude step records its
// model and its input and output tokens (pirn's Answer, which agent, act
// and skill results embed, summed over all their turns), and UsageOf only
// adds up what a record holds. Dollars are estimates: Cost prices tokens
// with a table that can go out of date, says which date, and gives no
// figure for a model it has no price for.

// Tokens are a step's or a run's tokens: what the record says, exactly.
// pirn records no cache tokens (it doesn't use prompt caching), so there
// are none to count.
type Tokens struct {
	Input  int64 `json:"input"`
	Output int64 `json:"output"`
}

// Add is the sum of t and u.
func (t Tokens) Add(u Tokens) Tokens {
	return Tokens{Input: t.Input + u.Input, Output: t.Output + u.Output}
}

// StepUsage is one Claude step's usage, as recorded: its model, its
// tokens and how many requests it made (an ask one; an agent, act or
// skill step one per turn). Missing says why a Claude step has no counts
// (its output isn't a pirn result), and then the rest is empty: it is
// never counted as zero.
type StepUsage struct {
	Node     string `json:"node"`
	Kind     string `json:"kind"` // ask, agent, act, skill, or front-door for the front door's own asks
	Model    string `json:"model,omitempty"`
	Tokens   Tokens `json:"tokens"`
	Requests int    `json:"requests,omitempty"`
	Missing  string `json:"missing,omitempty"`
}

// Usage is the Claude usage of some steps: a run's, a proposal's, a
// month's. Usages form a monoid under Add (concatenation), with the zero
// Usage as its unit; UsageOf is a homomorphism from runs to it (Soundness
// law 21).
type Usage struct {
	Steps []StepUsage `json:"steps"`
}

// Add is u's steps, then v's.
func (u Usage) Add(v Usage) Usage {
	steps := make([]StepUsage, 0, len(u.Steps)+len(v.Steps))
	return Usage{Steps: append(append(steps, u.Steps...), v.Steps...)}
}

// ByModel is u's tokens, summed per model; steps with no counts aren't
// in it (Missing lists them).
func (u Usage) ByModel() map[string]Tokens {
	out := map[string]Tokens{}
	for _, s := range u.Steps {
		if s.Missing == "" {
			out[s.Model] = out[s.Model].Add(s.Tokens)
		}
	}
	return out
}

// Total is all of u's counted tokens.
func (u Usage) Total() Tokens {
	var t Tokens
	for _, s := range u.Steps {
		if s.Missing == "" {
			t = t.Add(s.Tokens)
		}
	}
	return t
}

// Missing are u's Claude steps whose counts aren't recorded.
func (u Usage) Missing() []StepUsage {
	var out []StepUsage
	for _, s := range u.Steps {
		if s.Missing != "" {
			out = append(out, s)
		}
	}
	return out
}

// claudeKinds are the activities that ask Claude, by the kind of step.
var claudeKinds = map[string]string{
	pirn.ActivityName:      "ask",
	pirn.AgentActivityName: "agent",
	pirn.ActActivityName:   "act",
	treadle.ActivityName:   "skill",
}

// UsageOf is the Claude usage of a run of p that produced outputs: one
// StepUsage per step of p that asks Claude (ask, agent, act and skill
// steps, by their activity: another step's output is never read as
// usage, whatever it holds), in p's order. It is a pure function of what
// is recorded; nothing is asked of Anthropic.
func UsageOf(p plan.Plan, outputs map[string]string) Usage {
	u := Usage{Steps: []StepUsage{}}
	for _, n := range p.Nodes {
		kind, ok := claudeKinds[n.Activity]
		if !ok {
			continue
		}
		s := stepUsage(outputs[string(n.ID)], kind == "ask")
		s.Node, s.Kind = string(n.ID), kind
		u.Steps = append(u.Steps, s)
	}
	return u
}

// counts are the fields of a pirn result UsageOf reads.
type counts struct {
	Pirn         int    `json:"pirn"`
	Model        string `json:"model"`
	InputTokens  *int64 `json:"inputTokens"`
	OutputTokens *int64 `json:"outputTokens"`
	Turns        *int   `json:"turns"`
}

// stepUsage reads one Claude step's output.
func stepUsage(out string, ask bool) StepUsage {
	var c counts
	switch err := json.Unmarshal([]byte(out), &c); {
	case out == "":
		return StepUsage{Missing: "no output recorded"}
	case err != nil, c.Pirn != 1:
		return StepUsage{Missing: "its output isn't a pirn result, so it has no token counts"}
	case c.Model == "" || c.InputTokens == nil || c.OutputTokens == nil || *c.InputTokens < 0 || *c.OutputTokens < 0:
		return StepUsage{Missing: "its output has no model or token counts"}
	}
	s := StepUsage{Model: c.Model, Tokens: Tokens{Input: *c.InputTokens, Output: *c.OutputTokens}, Requests: 1}
	if !ask {
		s.Requests = 0 // not known unless the result says how many turns
		if c.Turns != nil && *c.Turns > 0 {
			s.Requests = *c.Turns
		}
	}
	return s
}

// ProposalUsage is the usage of the front door's own asks for one
// proposal: one step per attempt, each one request.
func ProposalUsage(answers []pirn.Answer) Usage {
	u := Usage{Steps: []StepUsage{}}
	for i, a := range answers {
		s := StepUsage{Node: fmt.Sprintf("attempt-%d", i+1), Kind: "front-door"}
		if a.Model == "" {
			s.Missing = "Claude's answer named no model"
		} else {
			s.Model, s.Tokens, s.Requests = a.Model, Tokens{Input: a.InputTokens, Output: a.OutputTokens}, 1
		}
		u.Steps = append(u.Steps, s)
	}
	return u
}

// Price is a model's price in USD per million tokens. A model priced by
// prompt size (UpTo) has Above for a request whose prompt is longer.
type Price struct {
	Input        float64 `json:"input"`
	Output       float64 `json:"output"`
	CacheWrite5m float64 `json:"cacheWrite5m,omitempty"`
	CacheWrite1h float64 `json:"cacheWrite1h,omitempty"`
	CacheRead    float64 `json:"cacheRead,omitempty"`
	UpTo         int64   `json:"upTo,omitempty"` // these prices are for a request whose prompt is at most UpTo tokens
	Above        *Price  `json:"above,omitempty"`
}

// Prices is a price table: per model, as of a date, from a source.
// Yours are the models whose prices came from your prices file.
type Prices struct {
	AsOf   string           `json:"asOf"`
	Source string           `json:"source"`
	Models map[string]Price `json:"models"`
	Yours  []string         `json:"yours,omitempty"`
}

//go:embed prices.json
var builtinPrices []byte

// ErrPrices is a prices file heedle can't use.
var ErrPrices = errors.New("loom: prices file")

// MaxPricesFile bounds a prices file.
const MaxPricesFile = 1 << 20

var modelName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,99}$`)

// BuiltinPrices is the table heedle was built with (prices.json).
func BuiltinPrices() Prices {
	p, err := decodePrices(builtinPrices)
	if err != nil {
		panic(err) // TestInv_BuiltinPricesAreChecked keeps it valid
	}
	return p
}

// decodePrices reads a price table strictly: no unknown fields, model
// names as Anthropic writes them, and every price a finite number, not
// negative.
func decodePrices(b []byte) (Prices, error) {
	var p Prices
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return Prices{}, err
	}
	if _, err := d.Token(); err != io.EOF {
		return Prices{}, errors.New("more than one JSON value")
	}
	if len(p.Yours) > 0 {
		return Prices{}, errors.New(`"yours" isn't for a file to say`)
	}
	for name, m := range p.Models {
		if !modelName.MatchString(name) {
			return Prices{}, fmt.Errorf("model %q: use the model's ID, such as claude-sonnet-5-5", name)
		}
		if err := m.check(true); err != nil {
			return Prices{}, fmt.Errorf("model %s: %v", name, err)
		}
	}
	return p, nil
}

func (m Price) check(top bool) error {
	for _, v := range []float64{m.Input, m.Output, m.CacheWrite5m, m.CacheWrite1h, m.CacheRead} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1e6 {
			return errors.New("a price is USD per million tokens: a number from 0 to 1000000")
		}
	}
	switch {
	case m.UpTo < 0:
		return errors.New("upTo can't be negative")
	case !top && (m.UpTo != 0 || m.Above != nil):
		return errors.New(`"above" prices have no "upTo" or "above" of their own`)
	case (m.UpTo == 0) != (m.Above == nil):
		return errors.New(`"upTo" and "above" go together: the prices for a prompt longer than upTo tokens`)
	case m.Above != nil:
		return m.Above.check(false)
	}
	return nil
}

// ReadPrices is the built-in table with the models path names replaced
// by its prices, one by one: a model the file doesn't name keeps its
// built-in price. No file (path empty or not there) is the built-in
// table. A file heedle can't use (too big, not strict JSON, an unknown
// field, a model name or a price that isn't one) is refused with
// ErrPrices saying why, and the built-in table is returned with it, so a
// caller can say so and go on.
func ReadPrices(path string) (Prices, error) {
	p := BuiltinPrices()
	if path == "" {
		return p, nil
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return p, nil
	}
	if err != nil {
		return p, fmt.Errorf("%w %s: %v; using the built-in prices", ErrPrices, path, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxPricesFile+1))
	if err != nil {
		return p, fmt.Errorf("%w %s: %v; using the built-in prices", ErrPrices, path, err)
	}
	if len(b) > MaxPricesFile {
		return p, fmt.Errorf("%w %s: more than %d bytes; using the built-in prices", ErrPrices, path, MaxPricesFile)
	}
	yours, err := decodePrices(b)
	if err != nil {
		return p, fmt.Errorf("%w %s: %v; using the built-in prices", ErrPrices, path, err)
	}
	if yours.AsOf != "" || yours.Source != "" {
		return p, fmt.Errorf(`%w %s: give only "models"; the as-of date and source are the built-in table's; using the built-in prices`, ErrPrices, path)
	}
	for name, m := range yours.Models {
		p.Models[name] = m
		p.Yours = append(p.Yours, name)
	}
	sort.Strings(p.Yours)
	return p, nil
}

// Unpriced is a step Cost gives no dollar figure for, and why.
type Unpriced struct {
	Node   string `json:"node"`
	Model  string `json:"model,omitempty"`
	Tokens Tokens `json:"tokens"`
	Why    string `json:"why"`
	// AtMost bounds what the step can have cost, when that much is
	// known: a step of several requests of a model priced by prompt
	// size, priced all at its higher prices. It is never shown as an
	// estimate; a budget compares against it, so it can only stop a run
	// early, never let one through (review 48).
	AtMost *float64 `json:"atMost,omitempty"`
}

// Costs are dollar estimates of a Usage: USD for the steps that could be
// priced, per model in ByModel, and Unpriced for the rest. Complete is
// whether every step was priced; only then is USD the whole estimate.
// Every figure is an estimate from the table of AsOf.
type Costs struct {
	USD      float64            `json:"usd"`
	ByModel  map[string]float64 `json:"byModel"`
	Unpriced []Unpriced         `json:"unpriced"`
	Complete bool               `json:"complete"`
	AsOf     string             `json:"asOf"`
	Source   string             `json:"source"`
	Yours    []string           `json:"yours,omitempty"` // models priced by your file
}

// Cost prices u with prices. A step whose model has no price, or whose
// counts are missing, gets no dollar figure: it is in Unpriced, by name,
// and ByModel has no entry for its model unless another step of that
// model was priced. A model priced by prompt size (UpTo) is priced at its
// lower prices for a step whose whole input is at most UpTo (so is each
// request's), at Above for a single request that is longer, and not at
// all for a step of several requests whose input together is longer,
// since how it was split among them isn't recorded.
func Cost(u Usage, prices Prices) Costs {
	c := Costs{ByModel: map[string]float64{}, Unpriced: []Unpriced{}, AsOf: prices.AsOf, Source: prices.Source}
	used := map[string]bool{}
	for _, s := range u.Steps {
		if s.Missing != "" {
			c.Unpriced = append(c.Unpriced, Unpriced{Node: s.Node, Why: s.Missing})
			continue
		}
		m, ok := prices.Models[s.Model]
		if !ok {
			c.Unpriced = append(c.Unpriced, Unpriced{Node: s.Node, Model: s.Model, Tokens: s.Tokens, Why: "no price for " + s.Model})
			continue
		}
		if m.UpTo > 0 && s.Tokens.Input > m.UpTo {
			if s.Requests != 1 {
				// Each request was at one price or the other, so the higher
				// of the two bounds it, whichever your prices file made
				// higher (review 48).
				most := (float64(s.Tokens.Input)*math.Max(m.Input, m.Above.Input) + float64(s.Tokens.Output)*math.Max(m.Output, m.Above.Output)) / 1e6
				c.Unpriced = append(c.Unpriced, Unpriced{Node: s.Node, Model: s.Model, Tokens: s.Tokens, AtMost: &most,
					Why: fmt.Sprintf("%s is priced by each request's prompt, and this step's requests together passed %d input tokens", s.Model, m.UpTo)})
				continue
			}
			m = *m.Above
		}
		usd := (float64(s.Tokens.Input)*m.Input + float64(s.Tokens.Output)*m.Output) / 1e6
		c.ByModel[s.Model] += usd
		c.USD += usd
		used[s.Model] = true
	}
	for _, y := range prices.Yours {
		if used[y] {
			c.Yours = append(c.Yours, y)
		}
	}
	c.Complete = len(c.Unpriced) == 0
	return c
}

// AtMost is an upper bound on what c's usage cost, if one is known: USD
// plus each unpriced step's AtMost. ok is false if some step has none
// (a model with no price, or no counts).
func (c Costs) AtMost() (usd float64, ok bool) {
	usd = c.USD
	for _, u := range c.Unpriced {
		if u.AtMost == nil {
			return 0, false
		}
		usd += *u.AtMost
	}
	return usd, true
}

// RunUsage is one recorded run's usage.
type RunUsage struct {
	Commit   warp.ID   `json:"commit"`
	Workflow string    `json:"workflowId"`
	Run      string    `json:"runId"`
	Schedule string    `json:"schedule,omitempty"`
	At       time.Time `json:"at"`
	Usage    Usage     `json:"usage"`
}

// Records is what reading runs' usage needs of a store: a *warp.Store,
// or drawdown's read-only view of one.
type Records interface {
	Resolve(ctx context.Context, ref string) (warp.ID, bool, error)
	Log(ctx context.Context, from warp.ID, limit int) ([]warp.CommitInfo, error)
	Tree(ctx context.Context, id warp.ID) ([]warp.Entry, error)
	Blob(ctx context.Context, id warp.ID) ([]byte, error)
	Blobs(ctx context.Context, ids []warp.ID) ([][]byte, error)
	Refs(ctx context.Context, prefix string) ([]warp.RefInfo, error)
}

// RecordUsage reads the usage of the run recorded in commit: UsageOf its
// plan.json and outputs/. A commit that records no run (a branch or a
// merge) has none: ok is false.
func RecordUsage(ctx context.Context, s Records, commit warp.CommitInfo) (u RunUsage, ok bool, err error) {
	u = RunUsage{Commit: commit.ID, At: commit.Time}
	for _, t := range commit.Trailers {
		switch t.Key {
		case TrailerWorkflow:
			u.Workflow = t.Value
		case TrailerRun:
			u.Run = t.Value
		case TrailerSchedule:
			u.Schedule = t.Value
		}
	}
	if u.Run == "" {
		return u, false, nil
	}
	es, err := s.Tree(ctx, commit.Tree)
	if err != nil {
		return u, false, err
	}
	var planID, outsID warp.ID
	for _, e := range es {
		switch {
		case e.Name == "plan.json" && e.Kind == warp.KindBlob:
			planID = e.ID
		case e.Name == "outputs" && e.Kind == warp.KindTree:
			outsID = e.ID
		}
	}
	if planID == "" || outsID == "" {
		return u, false, fmt.Errorf("%w: run %s in %s has no plan.json or outputs", warp.ErrInvalid, u.Run, commit.ID)
	}
	b, err := s.Blob(ctx, planID)
	if err != nil {
		return u, false, err
	}
	var p plan.Plan
	if err := json.Unmarshal(b, &p); err != nil {
		return u, false, fmt.Errorf("%w: run %s's plan.json: %v", warp.ErrInvalid, u.Run, err)
	}
	outs, err := s.Tree(ctx, outsID)
	if err != nil {
		return u, false, err
	}
	var names []string
	var ids []warp.ID
	for _, e := range outs {
		n, ok := p.Node(plan.NodeID(e.Name))
		if _, claude := claudeKinds[n.Activity]; ok && claude && e.Kind == warp.KindBlob {
			names, ids = append(names, e.Name), append(ids, e.ID)
		}
	}
	outputs := map[string]string{}
	if len(ids) > 0 {
		bs, err := s.Blobs(ctx, ids)
		if err != nil {
			return u, false, err
		}
		for i, b := range bs {
			outputs[names[i]] = string(b)
		}
	}
	u.Usage = UsageOf(p, outputs)
	return u, true, nil
}

// MaxCostWalk bounds how many commits RunsBetween reads back from a ref.
const MaxCostWalk = 20000

// recordSlack is how long before a record's parent it may say it was
// made. Records are appended in the order they are made, each saying
// when its run asked for it (workflow.Now), and Record gives up 10
// minutes after it's asked (writeOptions), so a record retried after a
// conflict lands after a later one by less than that; a day is ample.
const recordSlack = 24 * time.Hour

// ErrTooManyRuns is a walk that reached MaxCostWalk before from.
var ErrTooManyRuns = errors.New("loom: too many records to add up")

// RunsBetween is the usage of every run recorded on ref at or after from
// and before to, newest first, reading back along first parents. Given
// schedules, only those schedules' runs (TrailerSchedule), and no other
// run's outputs are read.
func RunsBetween(ctx context.Context, s Records, ref string, from, to time.Time, schedules ...string) ([]RunUsage, error) {
	only := map[string]bool{}
	for _, n := range schedules {
		if n == "" {
			// "" names no schedule: never read as "runs no schedule
			// started", which would count short (review 48 round 3).
			return nil, fmt.Errorf("%w: RunsBetween given an empty schedule name", warp.ErrInvalid)
		}
		only[n] = true
	}
	head, ok, err := s.Resolve(ctx, ref)
	if err != nil || !ok {
		return nil, err
	}
	log, err := s.Log(ctx, head, MaxCostWalk)
	if err != nil {
		return nil, err
	}
	var out []RunUsage
	for i, c := range log {
		if c.Time.Before(from.Add(-recordSlack)) {
			return out, nil
		}
		if i == MaxCostWalk-1 {
			return nil, fmt.Errorf("%w: more than %d on %s since %s", ErrTooManyRuns, MaxCostWalk, ref, from.Format(time.DateOnly))
		}
		if c.Time.Before(from) || !c.Time.Before(to) || (len(only) > 0 && !only[trailer(c.Trailers, TrailerSchedule)]) {
			continue
		}
		u, ok, err := RecordUsage(ctx, s, c)
		if errors.Is(err, warp.ErrInvalid) {
			// A record that can't be read has usage nobody knows: one
			// step with no counts, so a budget can't be checked and the
			// pages say so, rather than every page failing (review 48).
			u, ok = RunUsage{Commit: c.ID, Workflow: u.Workflow, Run: u.Run, Schedule: u.Schedule, At: c.Time,
				Usage: Usage{Steps: []StepUsage{{Node: "record", Kind: "record", Missing: "its record can't be read: " + err.Error()}}}}, true
		} else if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, u)
		}
	}
	return out, nil
}

func trailer(ts []warp.Trailer, key string) string {
	for _, t := range ts {
		if t.Key == key {
			return t.Value
		}
	}
	return ""
}

// MonthOf is the calendar month t is in, in loc: its first instant and
// the next month's.
func MonthOf(t time.Time, loc *time.Location) (start, end time.Time) {
	t = t.In(loc)
	start = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc)
	return start, start.AddDate(0, 1, 0)
}

// AllRunsBetween is RunsBetween on every ref the store's runs could be
// on (all but treadle's), each run once, newest first.
func AllRunsBetween(ctx context.Context, s Records, from, to time.Time) ([]RunUsage, error) {
	refs, err := s.Refs(ctx, "refs/")
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []RunUsage
	for _, r := range refs {
		if strings.HasPrefix(r.Name, "refs/treadle/") {
			continue
		}
		runs, err := RunsBetween(ctx, s, r.Name, from, to)
		if err != nil {
			return nil, err
		}
		for _, u := range runs {
			if !seen[u.Run] {
				seen[u.Run] = true
				out = append(out, u)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	return out, nil
}

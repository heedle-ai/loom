package loom_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/treadle"
	"github.com/heedle-ai/warp"
)

// claudeRegistry has every kind of step that asks Claude (ask, agent,
// act and a skill), and echo.
func claudeRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg := draft.DefaultRegistry()
	for _, s := range []registry.BuiltinSpec{pirn.BuiltinSpec(), pirn.AgentBuiltinSpec(), pirn.ActBuiltinSpec(),
		treadle.Spec(treadle.Skill{Name: "trip", Version: warp.ID(strings.Repeat("a", 40))})} {
		reg.MustRegister(s)
	}
	return reg
}

func jsonOf(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// stepsOf is u's steps without their node names, sorted: what a law
// compares, since composing renames nodes.
func stepsOf(u loom.Usage) []string {
	var out []string
	for _, s := range u.Steps {
		s.Node = ""
		out = append(out, jsonOf(s))
	}
	sort.Strings(out)
	return out
}

// TestInv_UsageIsExact: a run with every kind of Claude step (ask,
// agent, act and a skill, which record their tokens summed over every
// turn) is recorded, and UsageOf what was recorded has exactly the
// recorded counts, model by model and step by step, with each step's
// requests (one per turn). An echo step whose output looks like a pirn
// answer isn't counted: only steps that ask Claude are. The front door's
// own asks count one step per attempt. (Roadmap 10.4.)
func TestInv_UsageIsExact(t *testing.T) {
	reg := claudeRegistry(t)
	src := `(pipe (par (ask "plan Lisbon") (agent "find tiles") (act "book" "https://book.example.org/") (trip "3 days")) (echo "{\"pirn\":1,\"model\":\"claude-opus-5-5\",\"inputTokens\":999999,\"outputTokens\":999999}"))`
	p := must(loom.Compile(ctx, reg, src))
	ask := pirn.Answer{Pirn: 1, Model: "claude-sonnet-5-5", Text: "ok", InputTokens: 1234, OutputTokens: 56}
	agent := pirn.AgentResult{Answer: pirn.Answer{Pirn: 1, Model: "claude-sonnet-5-5", InputTokens: 40000, OutputTokens: 3000}, Agent: 1, Turns: 7}
	act := pirn.ActResult{Answer: pirn.Answer{Pirn: 1, Model: "claude-haiku-5-5", InputTokens: 9000, OutputTokens: 800}, Act: 1, Turns: 3}
	skill := pirn.AgentResult{Answer: pirn.Answer{Pirn: 1, Model: "claude-opus-5-5", InputTokens: 70001, OutputTokens: 4999}, Agent: 1, Turns: 12}
	outs := map[string]string{pirn.ActivityName: jsonOf(ask), pirn.AgentActivityName: jsonOf(agent), pirn.ActActivityName: jsonOf(act), treadle.ActivityName: jsonOf(skill)}

	s := newStore(t)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.Register(env, s, reg)
	for name, out := range outs {
		env.RegisterActivityWithOptions(func(context.Context, plan.Input) (string, error) { return out, nil }, activity.RegisterOptions{Name: name})
	}
	env.ExecuteWorkflow(loom.RunWorkflowName, loom.RunInput{Program: src, Plan: p, Ref: loom.DefaultRef})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	head, _, _ := s.Resolve(ctx, loom.DefaultRef)
	log := must(s.Log(ctx, head, 1))
	ru, ok, err := loom.RecordUsage(ctx, s, log[0])
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	want := map[string]loom.Tokens{
		"claude-sonnet-5-5": {Input: 1234 + 40000, Output: 56 + 3000},
		"claude-haiku-5-5":  {Input: 9000, Output: 800},
		"claude-opus-5-5":   {Input: 70001, Output: 4999},
	}
	if got := ru.Usage.ByModel(); !reflect.DeepEqual(got, want) {
		t.Errorf("by model %v, want %v", got, want)
	}
	if got := ru.Usage.Total(); got != (loom.Tokens{Input: 1234 + 40000 + 9000 + 70001, Output: 56 + 3000 + 800 + 4999}) {
		t.Errorf("total %v", got)
	}
	kinds := map[string]int{}
	for _, st := range ru.Usage.Steps {
		kinds[st.Kind] = st.Requests
	}
	if !reflect.DeepEqual(kinds, map[string]int{"ask": 1, "agent": 7, "act": 3, "skill": 12}) || len(ru.Usage.Steps) != 4 || len(ru.Usage.Missing()) != 0 {
		t.Errorf("steps %+v", ru.Usage.Steps)
	}
	if ru.Workflow == "" || ru.Run == "" || ru.Schedule != "" {
		t.Errorf("run %+v", ru)
	}

	front := loom.ProposalUsage([]pirn.Answer{{Model: "claude-sonnet-5-5", InputTokens: 9000, OutputTokens: 200}, {Model: "claude-sonnet-5-5", InputTokens: 9500, OutputTokens: 150}})
	if got := front.ByModel()["claude-sonnet-5-5"]; got != (loom.Tokens{Input: 18500, Output: 350}) || len(front.Steps) != 2 || front.Steps[1].Kind != "front-door" {
		t.Errorf("front door %+v", front)
	}
}

// TestInv_UsageSaysWhatIsMissing: a Claude step whose output has no
// counts (not a pirn result, or no model or tokens) is listed as missing
// with why, never counted as zero tokens of some model, and its cost is
// no dollar figure. (Roadmap 10.4.)
func TestInv_UsageSaysWhatIsMissing(t *testing.T) {
	p := must(loom.Compile(ctx, claudeRegistry(t), `(par (ask "a") (ask "b") (ask "c") (ask "d"))`))
	u := loom.UsageOf(p, map[string]string{
		"n0": "plain text",
		"n1": `{"pirn":1,"model":"claude-sonnet-5-5"}`,
		"n2": `{"pirn":1,"model":"claude-sonnet-5-5","inputTokens":10,"outputTokens":2}`,
	})
	if len(u.Steps) != 4 || len(u.Missing()) != 3 || u.Total() != (loom.Tokens{Input: 10, Output: 2}) {
		t.Fatalf("usage %+v", u)
	}
	for _, s := range u.Missing() {
		if s.Model != "" || s.Tokens != (loom.Tokens{}) || s.Missing == "" {
			t.Errorf("missing step counted: %+v", s)
		}
	}
	c := loom.Cost(u, loom.BuiltinPrices())
	if c.Complete || len(c.Unpriced) != 3 || math.Abs(c.USD-(10*2+2*10)/1e6) > 1e-12 {
		t.Errorf("cost %+v", c)
	}
}

// TestInv_NoPriceNoDollars: a model with no price gets no dollar figure:
// no ByModel entry, and the step is in Unpriced naming the model, with
// its tokens. A model priced by prompt size is priced at its lower or
// higher prices for one request, and not at all for several requests
// whose prompts together pass the size, since the split isn't recorded.
// (Roadmap 10.4.)
func TestInv_NoPriceNoDollars(t *testing.T) {
	prices := loom.BuiltinPrices()
	step := func(model string, in, out int64, requests int) loom.StepUsage {
		return loom.StepUsage{Node: model, Kind: "ask", Model: model, Tokens: loom.Tokens{Input: in, Output: out}, Requests: requests}
	}
	c := loom.Cost(loom.Usage{Steps: []loom.StepUsage{step("claude-sonnet-5-5", 1e6, 1e6, 1), step("claude-mythos-5-1", 5, 5, 1)}}, prices)
	if _, ok := c.ByModel["claude-mythos-5-1"]; ok || c.Complete || len(c.Unpriced) != 1 || c.Unpriced[0].Model != "claude-mythos-5-1" ||
		!strings.Contains(c.Unpriced[0].Why, "no price for claude-mythos-5-1") || c.Unpriced[0].Tokens.Input != 5 || c.USD != 12 {
		t.Errorf("cost %+v", c)
	}
	if c.AsOf != "2026-11-05" || !strings.HasPrefix(c.Source, "https://") {
		t.Errorf("no date or source: %+v", c)
	}
	for _, tc := range []struct {
		in       int64
		requests int
		usd      float64 // -1: unpriced
	}{
		{100000, 1, (100000*0.1 + 1e6*0.5) / 1e6},
		{100001, 1, (100001*0.5 + 1e6*2.5) / 1e6},
		{100000, 9, (100000*0.1 + 1e6*0.5) / 1e6},
		{100001, 9, -1},
		{100001, 0, -1},
	} {
		c := loom.Cost(loom.Usage{Steps: []loom.StepUsage{step("claude-haiku-5-5", tc.in, 1e6, tc.requests)}}, prices)
		switch {
		case tc.usd < 0 && (c.Complete || len(c.ByModel) != 0 || c.USD != 0):
			t.Errorf("%d tokens in %d requests priced: %+v", tc.in, tc.requests, c)
		case tc.usd >= 0 && (!c.Complete || math.Abs(c.USD-tc.usd) > 1e-9):
			t.Errorf("%d tokens in %d requests: $%v, want $%v", tc.in, tc.requests, c.USD, tc.usd)
		}
	}
}

// TestInv_PricesFileOverridesOnlyWhatItNames: your prices file replaces
// the prices of the models it names and no others, and says which; a
// file heedle can't use is refused with an error saying what's wrong and
// the built-in table is used instead. (Roadmap 10.4.)
func TestInv_PricesFileOverridesOnlyWhatItNames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "prices.json")
	builtin := loom.BuiltinPrices()
	if p, err := loom.ReadPrices(path); err != nil || !reflect.DeepEqual(p, builtin) {
		t.Fatalf("no file: %v", err)
	}
	os.WriteFile(path, []byte(`{"models":{"claude-sonnet-5-5":{"input":1,"output":2},"my-model":{"input":3,"output":4}}}`), 0o600)
	p, err := loom.ReadPrices(path)
	if err != nil || p.Models["claude-sonnet-5-5"] != (loom.Price{Input: 1, Output: 2}) || p.Models["my-model"].Output != 4 ||
		!reflect.DeepEqual(p.Models["claude-opus-5-5"], builtin.Models["claude-opus-5-5"]) || len(p.Models) != len(builtin.Models)+1 ||
		!reflect.DeepEqual(p.Yours, []string{"claude-sonnet-5-5", "my-model"}) || p.AsOf != builtin.AsOf {
		t.Fatalf("override: %+v %v", p, err)
	}
	c := loom.Cost(loom.Usage{Steps: []loom.StepUsage{{Model: "claude-sonnet-5-5", Tokens: loom.Tokens{Input: 1e6}, Requests: 1}}}, p)
	if c.USD != 1 || !reflect.DeepEqual(c.Yours, []string{"claude-sonnet-5-5"}) {
		t.Errorf("cost with your prices %+v", c)
	}
	for _, bad := range []string{
		`not json`,
		`{"models":{"claude-sonnet-5-5":{"input":1,"output":2,"discount":1}}}`,
		`{"models":{"claude-sonnet-5-5":{"input":-1,"output":2}}}`,
		`{"models":{"claude-sonnet-5-5":{"input":"1","output":2}}}`,
		`{"models":{"Claude Sonnet":{"input":1,"output":2}}}`,
		`{"models":{"claude-sonnet-5-5":{"input":1,"output":2,"upTo":5}}}`,
		`{"models":{"claude-sonnet-5-5":{"input":1,"output":2,"upTo":5,"above":{"input":1,"output":1,"upTo":9,"above":{}}}}}`,
		`{"asOf":"2030-01-01","models":{}}`,
		`{"yours":["x"],"models":{}}`,
		`{"models":{}} {"models":{}}`,
		`{"models":{"claude-sonnet-5-5":{"input":1e300,"output":2}}}`,
		`{"models":{"x":{"input":1,"output":2}}}` + strings.Repeat(" ", loom.MaxPricesFile),
	} {
		os.WriteFile(path, []byte(bad), 0o600)
		p, err := loom.ReadPrices(path)
		if !errors.Is(err, loom.ErrPrices) || !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "using the built-in prices") || !reflect.DeepEqual(p, builtin) {
			t.Errorf("%.60s: %v", bad, err)
		}
	}
	os.Mkdir(filepath.Join(dir, "d"), 0o700)
	if _, err := loom.ReadPrices(filepath.Join(dir, "d")); !errors.Is(err, loom.ErrPrices) {
		t.Errorf("a folder: %v", err)
	}
}

// TestInv_BuiltinPricesAreChecked: the built-in table passes the same
// strict reading as your file, says its date and source, and prices the
// model ask and agent steps use unless told otherwise. (Roadmap 10.4:
// prices from https://platform.claude.com/docs/en/about-claude/pricing,
// read 2026-11-05.)
func TestInv_BuiltinPricesAreChecked(t *testing.T) {
	b := must(os.ReadFile("prices.json"))
	path := filepath.Join(t.TempDir(), "prices.json")
	var m map[string]any
	json.Unmarshal(b, &m)
	delete(m, "asOf")
	delete(m, "source")
	os.WriteFile(path, []byte(jsonOf(m)), 0o600)
	if _, err := loom.ReadPrices(path); err != nil {
		t.Fatalf("the built-in table fails the check: %v", err)
	}
	p := loom.BuiltinPrices()
	if p.AsOf != "2026-11-05" || p.Source != "https://platform.claude.com/docs/en/about-claude/pricing" {
		t.Errorf("%s from %s", p.AsOf, p.Source)
	}
	if got := p.Models[pirn.DefaultModel]; got.Input != 2 || got.Output != 10 {
		t.Errorf("%s: %+v", pirn.DefaultModel, got)
	}
}

// fakeOutput is the output a step of p could have recorded, as a
// function of the step alone (its activity and arguments): what a law
// over compositions needs, since composing renames nodes but keeps steps.
func fakeOutput(n plan.Node) string {
	h := fnv.New64a()
	fmt.Fprint(h, n.Activity, n.Args)
	x := h.Sum64()
	if x%7 == 0 {
		return "no counts here"
	}
	models := []string{"claude-sonnet-5-5", "claude-haiku-5-5", "claude-mythos-5-1"}
	return jsonOf(map[string]any{"pirn": 1, "model": models[x%3], "inputTokens": int64(x % 200000), "outputTokens": int64(x / 7 % 9000), "turns": int(x%5) + 1})
}

func fakeRun(p plan.Plan) map[string]string {
	outs := map[string]string{}
	for _, n := range p.Nodes {
		outs[string(n.ID)] = fakeOutput(n)
	}
	return outs
}

// TestLaw_UsageIsAHomomorphism: what a run uses of Claude is a monoid
// homomorphism from programs to usages: usage(f ; g) = usage(f) +
// usage(g) = usage(f ⊗ g), and a step that asks nothing (echo) is the
// unit. Usages form a monoid under +; their tokens per model, their
// steps with no counts and their cost are commutative, so a program's
// usage depends only on its steps, never on bracketing or order. Checked
// on 300 random programs with every kind of step, some outputs with no
// counts and a model with no price. (Soundness law 21; roadmap 10.4.)
func TestLaw_UsageIsAHomomorphism(t *testing.T) {
	reg := lawRegistry(t)
	r := rand.New(rand.NewSource(21))
	usage := func(src string) loom.Usage {
		t.Helper()
		p, err := loom.Compile(ctx, reg, src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		return loom.UsageOf(p, fakeRun(p))
	}
	prices := loom.BuiltinPrices()
	same := func(a, b loom.Usage) bool {
		ca, cb := loom.Cost(a, prices), loom.Cost(b, prices)
		return reflect.DeepEqual(stepsOf(a), stepsOf(b)) && reflect.DeepEqual(a.ByModel(), b.ByModel()) &&
			len(a.Missing()) == len(b.Missing()) && math.Abs(ca.USD-cb.USD) < 1e-9 && len(ca.Unpriced) == len(cb.Unpriced)
	}
	if u := usage(`(echo "unit")`); len(u.Steps) != 0 || u.Total() != (loom.Tokens{}) {
		t.Fatalf("echo uses %+v", u)
	}
	for i := 0; i < 300; i++ {
		f, g, h := randomProgram(r, 3), randomProgram(r, 3), randomProgram(r, 2)
		uf, ug, uh := usage(f), usage(g), usage(h)
		for _, src := range []string{"(pipe " + f + " " + g + ")", "(par " + f + " " + g + ")"} {
			if got := usage(src); !same(got, uf.Add(ug)) {
				t.Fatalf("usage(%s) = %v, want usage(f) + usage(g) = %v", src, stepsOf(got), stepsOf(uf.Add(ug)))
			}
		}
		if !same(usage("(pipe "+f+` (echo "u"))`), uf) || !same(uf.Add(loom.Usage{}), uf) || !same(loom.Usage{}.Add(uf), uf) {
			t.Fatalf("unit: usage(f ; echo) isn't usage(f) for %s", f)
		}
		if !same(uf.Add(ug).Add(uh), uf.Add(ug.Add(uh))) || !same(uf.Add(ug), ug.Add(uf)) {
			t.Fatalf("+ isn't associative and commutative on %s, %s, %s", f, g, h)
		}
		if a, b := usage("(pipe (pipe "+f+" "+g+") "+h+")"), usage("(par "+h+" (par "+g+" "+f+"))"); !same(a, b) {
			t.Fatalf("(f;g);h uses %v but h⊗(g⊗f) uses %v", stepsOf(a), stepsOf(b))
		}
	}
}

// costly answers with replies in turn, each counting tokens.
type costly struct{ scripted }

func (c *costly) Ask(ctx context.Context, q pirn.Question) (pirn.Answer, error) {
	a, err := c.scripted.Ask(ctx, q)
	a.Model, a.InputTokens, a.OutputTokens = "claude-sonnet-5-5", int64(1000*len(c.asked)), int64(10*len(c.asked))
	return a, err
}

// TestInv_ProposalSaysWhatItUsed: a proposal carries what the front
// door's own asks used of Claude, every attempt (one a failed one), as
// Claude counted it. (Roadmap 10.4.)
func TestInv_ProposalSaysWhatItUsed(t *testing.T) {
	a := &costly{scripted{replies: []string{"no JSON here", "```json\n{\"summary\": \"Hi.\", \"program\": \"(echo \\\"hi\\\")\"}\n```"}}}
	p, err := writer(a).Ask(ctx, loom.AskRequest{Request: "say hi"})
	if err != nil || p.Attempts != 2 || p.Usage == nil {
		t.Fatalf("%+v %v", p, err)
	}
	if got := p.Usage.ByModel()["claude-sonnet-5-5"]; got != (loom.Tokens{Input: 3000, Output: 30}) || len(p.Usage.Steps) != 2 || p.Usage.Steps[0].Kind != "front-door" {
		t.Errorf("usage %+v", p.Usage)
	}
}

// TestInv_CostsSayWhatTheyMiss: the wiki says plainly what costs and
// budgets still can't see: a skill Claude wrote, a program loom write
// writes, an attempt whose worker died mid-call; and that a budget can't
// stop a run already going. (Review 48: failed runs were invisible to
// budgets, unsaid; roadmap 10.4.1 counts them, and retries and the front
// door, from the usage ledger.)
func TestInv_CostsSayWhatTheyMiss(t *testing.T) {
	b, err := os.ReadFile("docs/wiki/How-It-Works.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"**What still isn't counted:** a skill Claude writes", "a program `loom write` writes", "an attempt whose worker died mid-call", "A budget can't stop a run already going"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("How-It-Works doesn't say %q", want)
		}
	}
}

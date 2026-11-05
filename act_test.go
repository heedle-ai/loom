package loom_test

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
)

// TestInv_ActNeedsClaudeAndABrowserAgent: an act step needs Claude and a
// browser-agent image, and a worker without either can't run it, saying
// which and how to give it; a browser image (browse's) isn't enough.
// (Roadmap 9.5.)
func TestInv_ActNeedsClaudeAndABrowserAgent(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	act := plan.Plan{Nodes: []plan.Node{{ID: "n0", Activity: pirn.ActActivityName, Args: []string{"sign up", "https://forms.example.org/v", "cdn.example.org"}}}}
	r, err := loom.Needs(ctx, act, nil)
	if err != nil || !r.Claude || !r.BrowserAgent || r.Browser || r.Agent || strings.Join(r.ActHosts, " ") != "cdn.example.org forms.example.org" {
		t.Fatalf("act needs %+v %v", r, err)
	}
	for name, c := range map[string]struct {
		w     loom.Worker
		ready bool
		why   func(loom.Readiness) bool
	}{
		"no image":             {loom.Worker{Keys: memVault{"ANTHROPIC_API_KEY": "k"}, Browsers: true, Agents: true}, false, func(r loom.Readiness) bool { return strings.Contains(r.BrowserAgent, "-browser-agent-image") }},
		"no Claude":            {loom.Worker{Keys: memVault{}, BrowserAgents: true}, false, func(r loom.Readiness) bool { return r.Claude == "key" && r.BrowserAgent == "" }},
		"Claude and the image": {loom.Worker{Keys: memVault{"ANTHROPIC_API_KEY": "k"}, BrowserAgents: true}, true, func(r loom.Readiness) bool { return r.Claude == "" && r.BrowserAgent == "" }},
		"neither":              {loom.Worker{Keys: memVault{}}, false, func(r loom.Readiness) bool { return r.Claude == "key" && r.BrowserAgent != "" }},
	} {
		r, err := loom.CheckReady(ctx, act, nil, c.w)
		if err != nil || r.Ready != c.ready || !c.why(r) {
			t.Errorf("%s: %+v %v", name, r, err)
		}
	}
	// A browse step doesn't need the browser-agent image.
	browse := plan.Plan{Nodes: []plan.Node{{ID: "n0", Activity: "Browse", Args: []string{"example.org"}}}}
	if r, err := loom.CheckReady(ctx, browse, nil, loom.Worker{Browsers: true}); err != nil || !r.Ready || r.BrowserAgent != "" {
		t.Errorf("browse: %+v %v", r, err)
	}
}

// TestInv_ActHostsAreDisclosed: the hosts a plan's act steps may submit
// forms on are every act step's start host and further hosts, sorted and
// once each; other steps' hosts aren't among them. (Roadmap 9.5.)
func TestInv_ActHostsAreDisclosed(t *testing.T) {
	p := plan.Plan{Nodes: []plan.Node{
		{ID: "n0", Activity: pirn.ActActivityName, Args: []string{"a", "https://b.example.org/x"}},
		{ID: "n1", Activity: pirn.ActActivityName, Args: []string{"b", "https://a.example.org/", "b.example.org"}},
		{ID: "n2", Activity: "Browse", Args: []string{"news.example.org"}},
	}}
	if got := strings.Join(loom.ActHosts(p), " "); got != "a.example.org b.example.org" {
		t.Errorf("act hosts %q", got)
	}
	if got := loom.ActHosts(plan.Plan{Nodes: p.Nodes[2:]}); got != nil {
		t.Errorf("no act step, hosts %q", got)
	}
}

// TestInv_WritersKnowAct: the front door and the program writer are told
// what act does and that it can't sign in, upload or download, and the
// front door is told to say so when a request needs a sign-in.
// (Roadmap 9.5.)
func TestInv_WritersKnowAct(t *testing.T) {
	c, ok := loom.BuiltinCommands["act"]
	if !ok || !strings.HasPrefix(c.Args, "(act ") {
		t.Fatalf("act: %+v", c)
	}
	// Roadmap 9.7, review 41: they're told it stops before sending, and
	// never to opt out unless the request says so.
	for _, want := range []string{"fills in a form", "stops before each send to ask", "may reach only", "can't sign in", "say so in the summary", "Never add :send-without-asking", ":uses", "never write the details themselves"} {
		if !strings.Contains(c.About, want) {
			t.Errorf("act's description lacks %q: %s", want, c.About)
		}
	}
	if !strings.Contains(loom.AskFormat, "act can't, so say that") {
		t.Error("the front door isn't told to say when act can't sign in")
	}
}

// TestInv_MergeRerunsActOnlyWhenAllowed: merging with "rerun" runs each
// conflicting question again, so one whose program has act steps would
// submit its form again: the merge is refused, before anything runs,
// unless AllowAct says the person allowed it. (Review 39.)
func TestInv_MergeRerunsActOnlyWhenAllowed(t *testing.T) {
	h := newHist(t)
	reg := draft.DefaultRegistry()
	reg.MustRegister(pirn.ActBuiltinSpec())
	src := `(act "Join as Ada" "https://forms.example.org/")`
	p := must(loom.Compile(ctx, reg, src))
	answer := func(ref, out string) {
		id, at := h.next()
		if _, err := h.rec.Record(ctx, loom.RecordInput{Program: src, Plan: p, Result: plan.Result{Outputs: map[string]string{"n0": out}}, Ref: ref, WorkflowID: "wf-" + id, RunID: id, At: at}); err != nil {
			t.Fatal(err)
		}
	}
	answer("refs/loom/a", "base")
	h.branch("refs/loom/a", "refs/loom/b")
	answer("refs/loom/a", "yes")
	answer("refs/loom/b", "no")
	before := h.head("refs/loom/a")
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.Register(env, h.s, reg)
	env.ExecuteWorkflow(loom.MergeWorkflowName, loom.MergeInput{Into: "refs/loom/a", From: "refs/loom/b", Resolve: loom.ResolveRerun})
	err := env.GetWorkflowError()
	if appErrType(err) != "InvalidInput" || !strings.Contains(err.Error(), "click and type on forms.example.org") || h.head("refs/loom/a") != before {
		t.Fatalf("merge rerunning act: %v", err)
	}
	// Allowed: it goes on to re-run it (here, on a worker without act's
	// activity, which fails: nothing is merged).
	env = suite.NewTestWorkflowEnvironment()
	loom.Register(env, h.s, reg)
	env.ExecuteWorkflow(loom.MergeWorkflowName, loom.MergeInput{Into: "refs/loom/a", From: "refs/loom/b", Resolve: loom.ResolveRerun, AllowAct: true})
	if err := env.GetWorkflowError(); err == nil || strings.Contains(err.Error(), "click and type") {
		t.Fatalf("allowed: %v", err)
	}
	// One that sends without asking needs AllowSendFreely too (review 41).
	free := `(act "Join as Ada" "https://forms.example.org/" :send-without-asking)`
	fp := must(loom.Compile(ctx, reg, free))
	for _, ref := range []string{"refs/loom/c", "refs/loom/d"} {
		id, at := h.next()
		if _, err := h.rec.Record(ctx, loom.RecordInput{Program: free, Plan: fp, Result: plan.Result{Outputs: map[string]string{"n0": ref}}, Ref: ref, WorkflowID: "wf-" + id, RunID: id, At: at}); err != nil {
			t.Fatal(err)
		}
	}
	env = suite.NewTestWorkflowEnvironment()
	loom.Register(env, h.s, reg)
	env.ExecuteWorkflow(loom.MergeWorkflowName, loom.MergeInput{Into: "refs/loom/c", From: "refs/loom/d", Resolve: loom.ResolveRerun, AllowAct: true})
	if err := env.GetWorkflowError(); appErrType(err) != "InvalidInput" || !strings.Contains(err.Error(), "without asking again") {
		t.Fatalf("merge rerunning act that sends without asking: %v", err)
	}
}

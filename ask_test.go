package loom_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/treadle"
)

func reply(fields string) string { return "Here you are:\n```json\n{" + fields + "}\n```\n" }

// TestInv_AskProposesOnlyWhatChecks: a proposal is returned only when
// Claude's answer is the JSON asked for, with a summary and exactly one
// of a program that compiles and a skill to create, and a schedule only
// with a program, given one way, sensibly, and named; anything else goes
// back to Claude saying what's wrong, up to MaxAskAttempts. Claude is
// told the request, today, the time zone, the commands (skills too) and
// the conversation so far. Nothing runs. (Roadmap 8.1.)
func TestInv_AskProposesOnlyWhatChecks(t *testing.T) {
	good := reply(`"summary": "I'll say hi every week.", "program": "(echo \"hi\")", "every": "168h", "name": "weekly-hi"`)
	for name, bad := range map[string]string{
		"not json":             "(echo \"hi\")",
		"no summary":           reply(`"program": "(echo \"hi\")"`),
		"neither":              reply(`"summary": "s"`),
		"both":                 reply(`"summary": "s", "program": "(echo)", "skill": "a skill"`),
		"doesn't compile":      reply(`"summary": "s", "program": "(nope)"`),
		"unknown field":        reply(`"summary": "s", "program": "(echo)", "run": true`),
		"bad every":            reply(`"summary": "s", "program": "(echo)", "every": "weekly", "name": "w"`),
		"too often":            reply(`"summary": "s", "program": "(echo)", "every": "10s", "name": "w"`),
		"every and cron":       reply(`"summary": "s", "program": "(echo)", "every": "24h", "cron": "0 8 * * 1", "name": "w"`),
		"bad cron":             reply(`"summary": "s", "program": "(echo)", "cron": "every monday", "name": "w"`),
		"bad zone":             reply(`"summary": "s", "program": "(echo)", "cron": "0 8 * * 1", "timeZone": "Mars/Base", "name": "w"`),
		"no name":              reply(`"summary": "s", "program": "(echo)", "every": "24h"`),
		"bad name":             reply(`"summary": "s", "program": "(echo)", "every": "24h", "name": "Weekly Hi"`),
		"name, no schedule":    reply(`"summary": "s", "program": "(echo)", "name": "w"`),
		"schedule, no program": reply(`"summary": "s", "skill": "a skill", "every": "24h", "name": "w"`),
		"long summary":         reply(`"summary": "` + strings.Repeat("s", loom.MaxSummary+1) + `", "program": "(echo)"`),
	} {
		a := &scripted{replies: []string{bad, good}}
		p, err := writer(a).Ask(ctx, loom.AskRequest{Request: "Say hi every week", Today: "2026-11-04", TimeZone: "Europe/Lisbon"})
		if err != nil || p.Attempts != 2 || p.Program != "(echo \"hi\")\n" || p.Plan == nil || len(p.Plan.Nodes) != 1 ||
			p.Schedule == nil || p.Schedule.Every != 168*time.Hour || p.Name != "weekly-hi" {
			t.Errorf("%s: %+v %v", name, p, err)
			continue
		}
		if second := a.asked[1].Sources; second[len(second)-1].ID != "previous-attempt" || !strings.Contains(second[len(second)-1].Text, "can't be used") {
			t.Errorf("%s: the second ask doesn't say what was wrong: %+v", name, second)
		}
	}
	a := &scripted{replies: []string{good}}
	earlier := []loom.Turn{{Request: "say hi", Summary: "I'll say hi.", Program: "(echo \"hi\")"}}
	if _, err := writer(a).Ask(ctx, loom.AskRequest{Request: "every week, please", Earlier: earlier, Today: "2026-11-04", TimeZone: "Europe/Lisbon"}); err != nil {
		t.Fatal(err)
	}
	q := a.asked[0]
	if !strings.Contains(q.Instruction, "every week, please") || !strings.Contains(q.Instruction, "2026-11-04") || !strings.Contains(q.Instruction, "Europe/Lisbon") ||
		len(q.Sources) != 3 || !strings.Contains(q.Sources[1].Text, "Triages bugs (a skill).") || q.Sources[2].ID != "conversation" || !strings.Contains(q.Sources[2].Text, "I'll say hi.") {
		t.Errorf("question %+v", q)
	}
	cron := &scripted{replies: []string{reply(`"summary": "s", "program": "(echo)", "cron": "0 8 * * 1", "timeZone": "Europe/Lisbon", "name": "mondays"`)}}
	if p, err := writer(cron).Ask(ctx, loom.AskRequest{Request: "x"}); err != nil || p.Schedule == nil || p.Schedule.Cron != "0 8 * * 1" || p.Schedule.TimeZone != "Europe/Lisbon" {
		t.Errorf("cron: %+v %v", p, err)
	}
	skill := &scripted{replies: []string{reply(`"summary": "Nothing here books flights; a skill could.", "skill": "Books a flight with a travel connector."`)}}
	if p, err := writer(skill).Ask(ctx, loom.AskRequest{Request: "book me a flight"}); err != nil || p.Program != "" || p.Plan != nil || p.Skill == "" {
		t.Errorf("skill: %+v %v", p, err)
	}
	never := &scripted{replies: []string{"no"}}
	if _, err := writer(never).Ask(ctx, loom.AskRequest{Request: "x"}); !errors.Is(err, loom.ErrNoProgram) || len(never.asked) != loom.MaxAskAttempts {
		t.Errorf("never usable: %v after %d", err, len(never.asked))
	}
}

// TestInv_AskRefusesBadRequests: an empty or overlong request, too many
// earlier turns, or a day or time zone that isn't one is refused before
// Claude is asked. (Roadmap 8.1.)
func TestInv_AskRefusesBadRequests(t *testing.T) {
	many := make([]loom.Turn, loom.MaxTurns+1)
	for _, bad := range []loom.AskRequest{
		{}, {Request: "  "}, {Request: strings.Repeat("x", loom.MaxRequest+1)},
		{Request: "x", Earlier: many},
		{Request: "x", Earlier: []loom.Turn{{Request: strings.Repeat("x", loom.MaxRequest+1)}}},
		{Request: "x", Today: "4 Nov 2026"}, {Request: "x", Today: "2026-02-30"},
		{Request: "x", TimeZone: "Local"}, {Request: "x", TimeZone: "Nowhere/Town"},
	} {
		none := &scripted{replies: []string{reply(`"summary": "s", "program": "(echo)"`)}}
		if _, err := writer(none).Ask(ctx, bad); !errors.Is(err, pirn.ErrInvalid) || len(none.asked) != 0 {
			t.Errorf("%+v: %v (asked %d)", bad, err, len(none.asked))
		}
	}
}

// TestInv_AskingIsAWorkflowThatRunsNothing: AskWorkflow proposes on a
// worker and returns the proposal with its plan; it never runs it; a
// request it can't propose for fails once, never retried. (Roadmap 8.1.)
func TestInv_AskingIsAWorkflowThatRunsNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.RegisterAsk(env, writer(&scripted{replies: []string{reply(`"summary": "Hi.", "program": "(echo \"hi\")"`)}}))
	env.ExecuteWorkflow(loom.AskWorkflowName, loom.AskRequest{Request: "Say hi."})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var p loom.Proposal
	env.GetWorkflowResult(&p)
	if p.Summary != "Hi." || p.Plan == nil || len(p.Plan.Nodes) != 1 {
		t.Errorf("proposal %+v", p)
	}
	never := &scripted{replies: []string{"no"}}
	env = suite.NewTestWorkflowEnvironment()
	loom.RegisterAsk(env, writer(never))
	env.ExecuteWorkflow(loom.AskWorkflowName, loom.AskRequest{Request: "x"})
	if env.GetWorkflowError() == nil || len(never.asked) != loom.MaxAskAttempts {
		t.Errorf("no proposal: %v, asked %d", env.GetWorkflowError(), len(never.asked))
	}
}

// TestInv_NeedsAreWhatStepsAsk: a plan needs the connectors its skills
// declare (at the versions it pins) and its agent steps grant, together,
// and Claude when any step asks it; a skill version the library doesn't
// have is an error, never no needs. (Roadmap 8.2.)
func TestInv_NeedsAreWhatStepsAsk(t *testing.T) {
	lib := &treadle.Library{Store: newStore(t)}
	sk, err := lib.Add(ctx, map[string][]byte{"SKILL.md": []byte("---\nname: trip\ndescription: Plans trips.\nmetadata:\n  connectors: \"search maps/directions\"\n---\nPlan it.\n")}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := func(id, act string, args ...string) plan.Node {
		return plan.Node{ID: plan.NodeID(id), Activity: act, Args: args}
	}
	p := plan.Plan{Nodes: []plan.Node{
		node("n0", "Echo", "x"),
		node("n1", treadle.ActivityName, "trip", string(sk.Version), "Lisbon"),
		node("n2", pirn.AgentActivityName, "tidy it", "maps/search_places", "mail/send"),
	}}
	g, claude, err := loom.Needs(ctx, p, lib)
	if err != nil || !claude || g.String() != "mail/send maps/directions maps/search_places search" {
		t.Errorf("needs %q %v %v", g, claude, err)
	}
	if g, claude, err := loom.Needs(ctx, plan.Plan{Nodes: []plan.Node{node("n0", "Echo")}}, lib); err != nil || claude || len(g) != 0 {
		t.Errorf("echo only: %q %v %v", g, claude, err)
	}
	if g, claude, err := loom.Needs(ctx, plan.Plan{Nodes: []plan.Node{node("n0", pirn.ActivityName, "q")}}, nil); err != nil || !claude || len(g) != 0 {
		t.Errorf("ask only: %q %v %v", g, claude, err)
	}
	missing := plan.Plan{Nodes: []plan.Node{node("n0", treadle.ActivityName, "trip", strings.Repeat("0", 40))}}
	if _, _, err := loom.Needs(ctx, missing, lib); err == nil {
		t.Error("a version the library doesn't have")
	}
}

// TestInv_ProposalsSayIfTheyCanRun: a proposal with a program says
// whether this worker can run it now, from its steps' needs (CheckReady):
// each connector ready or why not, and the Anthropic key when a step asks
// Claude; a check that fails says why, never "ready". (Roadmap 8.2.)
func TestInv_ProposalsSayIfTheyCanRun(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	lib := &treadle.Library{Store: newStore(t)}
	sk, err := lib.Add(ctx, map[string][]byte{"SKILL.md": []byte("---\nname: trip\ndescription: Plans trips.\nmetadata:\n  connectors: \"search maps\"\n---\nPlan it.\n")}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p := plan.Plan{Nodes: []plan.Node{{ID: "n0", Activity: treadle.ActivityName, Args: []string{"trip", string(sk.Version), "Lisbon"}}}}
	cs := pirn.Connectors{{Name: "search", URL: "https://search.example/mcp", TokenEnv: "LOOM_TEST_SEARCH_KEY"}}
	keys := memVault{"LOOM_TEST_SEARCH_KEY": "s"}
	r, err := loom.CheckReady(ctx, p, lib, cs, keys)
	if err != nil || r.Ready || r.Claude != "key" || len(r.Needs) != 2 || r.Needs[0].Connector != "maps" || r.Needs[0].Problem != "absent" || r.Needs[1].Problem != "" {
		t.Errorf("no maps, no Claude key: %+v %v", r, err)
	}
	cs = append(cs, pirn.Connector{Name: "maps", URL: "https://maps.example/mcp"})
	keys["ANTHROPIC_API_KEY"] = "k"
	if r, err := loom.CheckReady(ctx, p, lib, cs, keys); err != nil || !r.Ready || r.Claude != "" {
		t.Errorf("all there: %+v %v", r, err)
	}
	w := writer(&scripted{replies: []string{reply(`"summary": "Hi.", "program": "(echo \"hi\")"`)}})
	w.Ready = func(context.Context, plan.Plan) (loom.Readiness, error) {
		return loom.Readiness{}, errors.New("connectors file: bad")
	}
	if prop, err := w.Ask(ctx, loom.AskRequest{Request: "hi"}); err != nil || prop.Ready == nil || prop.Ready.Ready || prop.Ready.Error != "connectors file: bad" {
		t.Errorf("a check that fails: %+v %v", prop.Ready, err)
	}
	w.Ready = func(context.Context, plan.Plan) (loom.Readiness, error) { return loom.Readiness{Ready: true}, nil }
	if prop, _ := w.Ask(ctx, loom.AskRequest{Request: "hi"}); prop.Ready == nil || !prop.Ready.Ready {
		t.Errorf("ready: %+v", prop.Ready)
	}
}

type memVault map[string]string

func (m memVault) Lookup(_ context.Context, n string) (string, bool, error) {
	v, ok := m[n]
	return v, ok, nil
}

package loom_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/reed"
)

// lawRegistry has echo, ask and agent: every kind of step whose needs
// differ, with no skill library.
func lawRegistry(t *testing.T) *registry.Registry {
	reg := draft.DefaultRegistry()
	for _, s := range []registry.BuiltinSpec{pirn.BuiltinSpec(), pirn.AgentBuiltinSpec(), reed.BuiltinSpec()} {
		if err := reg.Register(s); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

// randomProgram is a random term of draft's syntax: leaves that need
// nothing (echo), Claude (ask) or Claude, a container and connectors
// (agent with grants), under pipe (;) and par (⊗), up to depth levels.
func randomProgram(r *rand.Rand, depth int) string {
	grants := []string{`"maps"`, `"maps/directions"`, `"maps/search_places"`, `"search"`, `"mail/send"`, `"mail/read"`}
	if depth == 0 || r.Intn(3) == 0 {
		switch r.Intn(4) {
		case 0:
			return fmt.Sprintf(`(echo "e%d")`, r.Intn(9))
		case 3:
			return fmt.Sprintf(`(browse "site%d.example.org")`, r.Intn(9))
		case 1:
			return fmt.Sprintf(`(ask "q%d")`, r.Intn(9))
		default:
			var gs []string
			for i := r.Intn(3); i > 0; i-- {
				gs = append(gs, grants[r.Intn(len(grants))])
			}
			return fmt.Sprintf(`(agent "a%d" %s)`, r.Intn(9), strings.Join(gs, " "))
		}
	}
	op := []string{"pipe", "par"}[r.Intn(2)]
	parts := []string{randomProgram(r, depth-1), randomProgram(r, depth-1)}
	if r.Intn(2) == 0 {
		parts = append(parts, randomProgram(r, depth-1))
	}
	return "(" + op + " " + strings.Join(parts, " ") + ")"
}

// union is what two needs need together.
func needsUnion(a, b loom.Requirements) loom.Requirements {
	g := pirn.Grant{}
	for _, h := range []pirn.Grant{a.Grant, b.Grant} {
		for c, t := range h {
			old, ok := g[c]
			switch {
			case old.All || t.All:
				g[c] = pirn.Tools{All: true}
			default:
				names := map[string]bool{}
				if ok {
					for n := range old.Names {
						names[n] = true
					}
				}
				for n := range t.Names {
					names[n] = true
				}
				g[c] = pirn.Tools{Names: names}
			}
		}
	}
	return loom.Requirements{Grant: g, Claude: a.Claude || b.Claude, Agent: a.Agent || b.Agent, Browser: a.Browser || b.Browser}
}

func needsString(r loom.Requirements) string {
	return fmt.Sprintf("%s claude=%v agent=%v browser=%v", r.Grant, r.Claude, r.Agent, r.Browser)
}

// TestLaw_NeedsIsAHomomorphism: what a program needs is a homomorphism
// from programs to needs: needs(f ; g) = needs(f) ∪ needs(g) =
// needs(f ⊗ g), and a step that needs nothing (echo) is the unit. Needs
// form a join-semilattice under ∪ (commutative, associative,
// idempotent), so a program's needs don't depend on how it's bracketed
// or ordered, only on its steps. Checked on 300 random programs.
// (Soundness law 16; roadmap 8.2.)
func TestLaw_NeedsIsAHomomorphism(t *testing.T) {
	reg := lawRegistry(t)
	r := rand.New(rand.NewSource(16))
	needs := func(src string) loom.Requirements {
		t.Helper()
		p, err := loom.Compile(ctx, reg, src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		n, err := loom.Needs(ctx, p, nil)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		return n
	}
	unit := needs(`(echo "unit")`)
	if len(unit.Grant) != 0 || unit.Claude || unit.Agent || unit.Browser {
		t.Fatalf("echo needs %s", needsString(unit))
	}
	for i := 0; i < 300; i++ {
		f, g, h := randomProgram(r, 3), randomProgram(r, 3), randomProgram(r, 2)
		nf, ng, nh := needs(f), needs(g), needs(h)
		want := needsString(needsUnion(nf, ng))
		for _, src := range []string{"(pipe " + f + " " + g + ")", "(par " + f + " " + g + ")"} {
			if got := needsString(needs(src)); got != want {
				t.Fatalf("needs(%s) = %s, want needs(f) ∪ needs(g) = %s", src, got, want)
			}
		}
		if got := needsString(needs("(pipe " + f + ` (echo "u"))`)); got != needsString(nf) {
			t.Fatalf("unit: needs(f ; echo) = %s, needs(f) = %s", got, needsString(nf))
		}
		if needsString(needsUnion(nf, ng)) != needsString(needsUnion(ng, nf)) ||
			needsString(needsUnion(needsUnion(nf, ng), nh)) != needsString(needsUnion(nf, needsUnion(ng, nh))) ||
			needsString(needsUnion(nf, nf)) != needsString(nf) {
			t.Fatalf("∪ isn't a semilattice on %s, %s, %s", f, g, h)
		}
		// Bracketing and order don't matter.
		a := needsString(needs("(pipe (pipe " + f + " " + g + ") " + h + ")"))
		b := needsString(needs("(par " + h + " (par " + g + " " + f + "))"))
		if a != b {
			t.Fatalf("(f;g);h needs %s but h⊗(g⊗f) needs %s", a, b)
		}
	}
}

// TestLaw_ReadyIsAMeet: a program can run exactly when every part of it
// can: ready(f ; g) = ready(f) ∧ ready(g) = ready(f ⊗ g), with the
// worker fixed. So the front door's verdict on a whole proposal is the
// conjunction of the verdicts on its steps, and adding a step never makes
// a plan readier. Checked on 300 random programs against three workers.
// (Soundness law 17; roadmap 8.2.)
func TestLaw_ReadyIsAMeet(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	reg := lawRegistry(t)
	r := rand.New(rand.NewSource(17))
	all := pirn.Connectors{
		{Name: "maps", URL: "https://maps.example/mcp", Tools: []string{"directions", "search_places"}},
		{Name: "search", URL: "https://search.example/mcp", TokenEnv: "LAW_SEARCH_KEY"},
		{Name: "mail", URL: "https://mail.example/mcp", Writes: []string{"send"}, Approve: true},
	}
	workers := []loom.Worker{
		{Connectors: all, Keys: memVault{"ANTHROPIC_API_KEY": "k", "LAW_SEARCH_KEY": "s"}, Agents: true, Browsers: true},
		{Connectors: all[:2], Keys: memVault{"ANTHROPIC_API_KEY": "k"}, Agents: true}, // no mail, no search key
		{Connectors: all, Keys: memVault{"LAW_SEARCH_KEY": "s"}, Agents: false},       // no Claude, no agent image
	}
	ready := func(src string, w loom.Worker) bool {
		t.Helper()
		p, err := loom.Compile(ctx, reg, src)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		rd, err := loom.CheckReady(ctx, p, nil, w)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		return rd.Ready
	}
	for i := 0; i < 300; i++ {
		f, g := randomProgram(r, 3), randomProgram(r, 3)
		for wi, w := range workers {
			want := ready(f, w) && ready(g, w)
			for _, src := range []string{"(pipe " + f + " " + g + ")", "(par " + f + " " + g + ")"} {
				if got := ready(src, w); got != want {
					t.Fatalf("worker %d: ready(%s) = %v, want ready(f) ∧ ready(g) = %v", wi, src, got, want)
				}
			}
		}
	}
}

// TestLaw_ProposalsCompileAsRunsDo: a proposal's plan is exactly what
// its program compiles to (byte for byte), so what the card draws and
// what Run submits (the program, compiled again by the run) are one
// plan: Ask adds nothing to compilation. (Soundness law 18; roadmap 8.1.)
func TestLaw_ProposalsCompileAsRunsDo(t *testing.T) {
	reg := lawRegistry(t)
	r := rand.New(rand.NewSource(18))
	for i := 0; i < 50; i++ {
		src := randomProgram(r, 3)
		answer := reply(fmt.Sprintf(`"summary": "s", "program": %q`, src))
		w := loom.Writer{Asker: &scripted{replies: []string{answer}}, Commands: func(context.Context) (*registry.Registry, []loom.Command, error) {
			return reg, loom.Commands(reg, loom.BuiltinCommands), nil
		}}
		p, err := w.Ask(ctx, loom.AskRequest{Request: "x"})
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		again, err := loom.Compile(ctx, reg, p.Program)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := loom.PlanJSON(*p.Plan)
		b, _ := loom.PlanJSON(again)
		if !bytes.Equal(a, b) {
			t.Fatalf("%s: the proposal's plan isn't its program's", src)
		}
	}
}

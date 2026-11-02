package loom_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
)

// scripted is a stand-in Claude that answers with each of its replies in
// order, and keeps every question.
type scripted struct {
	replies []string
	asked   []pirn.Question
}

func (s *scripted) Ask(_ context.Context, q pirn.Question) (pirn.Answer, error) {
	s.asked = append(s.asked, q)
	r := s.replies[min(len(s.asked), len(s.replies))-1]
	return pirn.Answer{Pirn: 1, Model: "m", Text: r}, nil
}

func writer(a pirn.Asker) loom.Writer {
	return loom.Writer{Asker: a, Commands: func(context.Context) (*registry.Registry, []loom.Command, error) {
		reg := draft.DefaultRegistry()
		cmds := loom.Commands(reg, loom.BuiltinCommands)
		return reg, append(cmds, loom.Command{Name: "triage", Args: `(triage "arg" ...)`, About: "Triages bugs (a skill)."}), nil
	}}
}

// TestInv_ProgramsAreWrittenAndCompiled: the writer asks Claude with the
// task, the language and every command (skills with their descriptions),
// and returns a program only once it compiles, with its plan; a program
// that doesn't compile goes back with the compiler's error, up to
// MaxWriteAttempts; an empty or overlong description is refused before
// Claude is asked; nothing runs.
func TestInv_ProgramsAreWrittenAndCompiled(t *testing.T) {
	a := &scripted{replies: []string{"(pipe (echo \"a\") (nope))", "Here:\n```lisp\n(pipe (echo \"a\") echo)\n```\n"}}
	w, err := writer(a).Write(ctx, "Say a, then pass it on.")
	if err != nil || w.Program != "(pipe (echo \"a\") echo)\n" || w.Attempts != 2 || len(w.Plan.Nodes) != 2 {
		t.Fatalf("written %+v %v", w, err)
	}
	first, second := a.asked[0], a.asked[1]
	if !strings.Contains(first.Instruction, "Say a, then pass it on.") || len(first.Sources) != 2 ||
		!strings.Contains(first.Sources[0].Text, "(pipe a b c)") || !strings.Contains(first.Sources[1].Text, "Triages bugs (a skill).") ||
		!strings.Contains(first.Sources[1].Text, `(echo "text" ...)`) {
		t.Errorf("first question %+v", first)
	}
	if len(second.Sources) != 3 || second.Sources[2].ID != "previous-attempt" || !strings.Contains(second.Sources[2].Text, "(nope)") ||
		!strings.Contains(second.Sources[2].Text, "doesn't compile") {
		t.Errorf("second question %+v", second.Sources)
	}
	never := &scripted{replies: []string{"(nope)"}}
	if _, err := writer(never).Write(ctx, "x"); !errors.Is(err, loom.ErrNoProgram) || len(never.asked) != loom.MaxWriteAttempts {
		t.Errorf("never compiles: %v after %d", err, len(never.asked))
	}
	for _, bad := range []string{"", "  ", strings.Repeat("x", loom.MaxDescription+1)} {
		none := &scripted{replies: []string{"(echo)"}}
		if _, err := writer(none).Write(ctx, bad); !errors.Is(err, pirn.ErrInvalid) || len(none.asked) != 0 {
			t.Errorf("description %d bytes: %v", len(bad), err)
		}
	}
}

// TestInv_WritingIsAWorkflowThatRunsNothing: WriteProgramWorkflow writes
// on a worker and returns the program and its plan; it never runs it; a
// task Claude can't write a program for fails once, never retried.
func TestInv_WritingIsAWorkflowThatRunsNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.RegisterWriter(env, writer(&scripted{replies: []string{"(echo \"hi\")"}}))
	env.ExecuteWorkflow(loom.WriteProgramWorkflowName, loom.WriteRequest{Description: "Say hi."})
	var w loom.Written
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	env.GetWorkflowResult(&w)
	if w.Program != "(echo \"hi\")\n" || len(w.Plan.Nodes) != 1 {
		t.Errorf("written %+v", w)
	}
	never := &scripted{replies: []string{"(nope)"}}
	env = suite.NewTestWorkflowEnvironment()
	loom.RegisterWriter(env, writer(never))
	env.ExecuteWorkflow(loom.WriteProgramWorkflowName, loom.WriteRequest{Description: "x"})
	if env.GetWorkflowError() == nil || len(never.asked) != loom.MaxWriteAttempts {
		t.Errorf("a task with no program: %v, asked %d", env.GetWorkflowError(), len(never.asked))
	}
	// Review 20: a failure after some asks isn't retried from the start,
	// so Claude is never asked more than MaxWriteAttempts times.
	flaky := &failing{after: 2}
	env = suite.NewTestWorkflowEnvironment()
	loom.RegisterWriter(env, writer(flaky))
	env.ExecuteWorkflow(loom.WriteProgramWorkflowName, loom.WriteRequest{Description: "x"})
	if env.GetWorkflowError() == nil || flaky.asked > loom.MaxWriteAttempts {
		t.Errorf("a flaky Claude: %v, asked %d", env.GetWorkflowError(), flaky.asked)
	}
}

// failing answers with a program that doesn't compile, then fails with a
// retryable error.
type failing struct{ after, asked int }

func (f *failing) Ask(context.Context, pirn.Question) (pirn.Answer, error) {
	f.asked++
	if f.asked > f.after {
		return pirn.Answer{}, errors.New("connection reset")
	}
	return pirn.Answer{Pirn: 1, Model: "m", Text: "(nope)"}, nil
}

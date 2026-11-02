package loom_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/treadle"
)

// changeSkills runs SkillsWorkflow in Temporal's test environment with a
// worker changing lib, as loom worker does.
func changeSkills(t *testing.T, lib *treadle.Library, in loom.SkillsChange) (treadle.Skill, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.RegisterSkills(env, lib)
	env.ExecuteWorkflow(loom.SkillsWorkflowName, in)
	if err := env.GetWorkflowError(); err != nil {
		return treadle.Skill{}, err
	}
	var s treadle.Skill
	err := env.GetWorkflowResult(&s)
	return s, err
}

func refused(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && app.Type() == loom.SkillsRefused
}

// TestInv_SkillsChangeThroughAWorkflow: a library changes from outside
// the worker only through SkillsWorkflow: an import (trust rules apply),
// an approval or rejection of exactly the version reviewed, or a folder
// added by hand, at most MaxSkillsChangeBytes; a refusal is final
// (SkillsRefused) and says why. (That no command runs it is
// cmd/loom's TestWorkerCommandsAreTheRegistry.)
func TestInv_SkillsChangeThroughAWorkflow(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(dir+"/SKILL.md", []byte("---\nname: pdf\ndescription: Reads PDFs.\n---\nx\n"), 0o644)
	for _, a := range [][]string{{"init", "-q", "-b", "main"}, {"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "x"}} {
		if out, err := exec.Command("git", append([]string{"-C", dir}, a...)...).CombinedOutput(); err != nil {
			t.Fatalf("%v %s", err, out)
		}
	}
	lib := &treadle.Library{Store: newStore(t)}
	p, err := changeSkills(t, lib, loom.SkillsChange{Op: loom.SkillsImport, Source: &treadle.Source{Repo: "file://" + dir}})
	if err != nil || !p.Pending {
		t.Fatalf("import: %+v %v", p, err)
	}
	if _, err := changeSkills(t, lib, loom.SkillsChange{Op: loom.SkillsApprove, Name: "pdf", Version: "0000000000000000000000000000000000000000"}); !refused(err) {
		t.Errorf("approving another version: %v", err)
	}
	a, err := changeSkills(t, lib, loom.SkillsChange{Op: loom.SkillsApprove, Name: "pdf", Version: p.Version})
	if err != nil || a.Version != p.Version || a.Pending {
		t.Fatalf("approve: %+v %v", a, err)
	}
	if vs := must(lib.Versions(ctx, "pdf")); len(vs) != 1 {
		t.Errorf("versions %+v", vs)
	}
	if _, err := changeSkills(t, lib, loom.SkillsChange{Op: loom.SkillsReject, Name: "pdf", Version: p.Version}); !refused(err) || !strings.Contains(err.Error(), "no import") {
		t.Errorf("rejecting what's no longer waiting: %v", err)
	}
	added, err := changeSkills(t, lib, loom.SkillsChange{Op: loom.SkillsAdd, Files: map[string][]byte{"SKILL.md": []byte("---\nname: brief\ndescription: Writes briefs.\n---\nBe short.\n")}})
	if err != nil || added.Name != "brief" || added.Pending {
		t.Errorf("add: %+v %v", added, err)
	}
	for _, bad := range []loom.SkillsChange{
		{Op: loom.SkillsAdd, Files: map[string][]byte{"SKILL.md": []byte("no front matter")}},
		{Op: loom.SkillsAdd, Files: map[string][]byte{"SKILL.md": []byte("---\nname: big\ndescription: d\n---\n"), "x": make([]byte, loom.MaxSkillsChangeBytes)}},
		{Op: loom.SkillsImport},
		{Op: loom.SkillsImport, Source: &treadle.Source{Repo: "--upload-pack=x"}},
		{Op: "trust", Name: "https://github.com/acme"},
	} {
		if _, err := changeSkills(t, lib, bad); !refused(err) {
			t.Errorf("%s: %v", bad.Op, err)
		}
	}
}

// TestInv_SkillsChangesEndOrNeverHappen: a change no worker finishes
// within SkillsTimeout is dropped (the workflow has that timeout), never
// made later; an import is tried SkillsImportAttempts times and every
// other change once; and a refusal's message is the library's reason,
// without Temporal's wrapping. (Review 18: a change queued with no
// worker was made days later, and a refusal repeated its reason three
// times.)
func TestInv_SkillsChangesEndOrNeverHappen(t *testing.T) {
	c := &mocks.Client{}
	var got client.StartWorkflowOptions
	run := &mocks.WorkflowRun{}
	run.On("Get", mock.Anything, mock.Anything).Return(temporal.NewNonRetryableApplicationError(
		"treadle: warp: invalid argument: no import of \"pdf\" is waiting for review", loom.SkillsRefused, nil))
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, loom.SkillsWorkflowName, mock.Anything).
		Run(func(args mock.Arguments) { got = args.Get(1).(client.StartWorkflowOptions) }).Return(run, nil)
	_, err := loom.ChangeSkills(ctx, c, "loom", loom.SkillsChange{Op: loom.SkillsReject, Name: "pdf", Version: "x"})
	if got.WorkflowExecutionTimeout != loom.SkillsTimeout {
		t.Errorf("workflow timeout %s, want %s", got.WorkflowExecutionTimeout, loom.SkillsTimeout)
	}
	if !errors.Is(err, treadle.ErrInvalid) || strings.Count(err.Error(), "waiting for review") != 1 || strings.Contains(err.Error(), "SkillsRefused") {
		t.Errorf("refusal: %v", err)
	}
	for op, want := range map[string]int{loom.SkillsImport: loom.SkillsImportAttempts, loom.SkillsApprove: 1, loom.SkillsReject: 1, loom.SkillsAdd: 1} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		loom.RegisterSkills(env, &treadle.Library{Store: newStore(t)})
		calls := 0
		env.OnActivity(loom.SkillsChangeName, mock.Anything, mock.Anything).Return(func(context.Context, loom.SkillsChange) (treadle.Skill, error) {
			calls++
			return treadle.Skill{}, errors.New("git: connection reset")
		})
		env.ExecuteWorkflow(loom.SkillsWorkflowName, loom.SkillsChange{Op: op, Source: &treadle.Source{Repo: "https://example.org/r"}})
		if env.GetWorkflowError() == nil || calls != want {
			t.Errorf("%s: tried %d times, want %d", op, calls, want)
		}
	}
}

package loom_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/treadle"
)

// skillsRepo is a git repository of skill folders: name to SKILL.md.
func skillsRepo(t *testing.T, skills map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, md := range skills {
		os.MkdirAll(filepath.Join(dir, "skills", name), 0o755)
		os.WriteFile(filepath.Join(dir, "skills", name, "SKILL.md"), []byte(md), 0o644)
	}
	for _, a := range [][]string{{"init", "-q", "-b", "main"}, {"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "x"}} {
		if out, err := exec.Command("git", append([]string{"-C", dir}, a...)...).CombinedOutput(); err != nil {
			t.Fatalf("%v %s", err, out)
		}
	}
	return "file://" + dir
}

func listCatalog(t *testing.T, src treadle.Source) (loom.Catalog, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.RegisterSkills(env, loom.Skills{Library: &treadle.Library{Store: newStore(t)}})
	env.ExecuteWorkflow(loom.CatalogWorkflowName, loom.CatalogRequest{Source: src})
	if err := env.GetWorkflowError(); err != nil {
		return loom.Catalog{}, err
	}
	var c loom.Catalog
	err := env.GetWorkflowResult(&c)
	return c, err
}

// TestInv_CatalogsAreListedOnAWorker: a worker lists a repository's
// skills (CatalogWorkflow), each with its name, description and source,
// one treadle refuses with its problem; at most MaxCatalogEntries, the
// rest counted; a folder's link is refused, finally; listing changes
// nothing.
func TestInv_CatalogsAreListedOnAWorker(t *testing.T) {
	md := func(name, desc string) string {
		return "---\nname: " + name + "\ndescription: " + desc + "\n---\nGo.\n"
	}
	repo := skillsRepo(t, map[string]string{"pdf": md("pdf", "Reads PDFs."), "brand": md("brand", "Writes in a brand's voice."), "oddly": md("other", "Misnamed.")})
	c, err := listCatalog(t, treadle.Source{Repo: repo})
	if err != nil || len(c.Entries) != 3 || c.Omitted != 0 {
		t.Fatalf("catalog %+v %v", c, err)
	}
	byName := map[string]treadle.CatalogEntry{}
	for _, e := range c.Entries {
		byName[e.Source.Path] = e
	}
	if e := byName["skills/pdf"]; e.Name != "pdf" || e.Description != "Reads PDFs." || e.Source.Repo != repo || e.Source.Commit == "" || e.Problem != "" {
		t.Errorf("pdf %+v", e)
	}
	if e := byName["skills/oddly"]; e.Problem == "" {
		t.Errorf("a misnamed skill has no problem: %+v", e)
	}
	if _, err := listCatalog(t, treadle.Source{Repo: repo, Path: "skills/pdf"}); !refused(err) {
		t.Errorf("a folder's link: %v", err)
	}

	many := map[string]string{}
	for i := 0; i < loom.MaxCatalogEntries+3; i++ {
		n := fmt.Sprintf("s%03d", i)
		many[n] = md(n, "One of many.")
	}
	c, err = listCatalog(t, treadle.Source{Repo: skillsRepo(t, many)})
	if err != nil || len(c.Entries) != loom.MaxCatalogEntries || c.Omitted != 3 {
		t.Errorf("a big catalog: %d entries, %d omitted, %v", len(c.Entries), c.Omitted, err)
	}
}

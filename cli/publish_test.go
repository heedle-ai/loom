package cli

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestInv_SkillsPublishCommand: heedle skills publish NAME -repo DIR
// commits the skill into your clone, says so, and prints the push for
// you to run; flags go before publish or after NAME; anything else is
// refused. (Roadmap 10.6.)
func TestInv_SkillsPublishCommand(t *testing.T) {
	ctx := context.Background()
	store := filepath.Join(t.TempDir(), "store.git")
	folder := t.TempDir()
	os.WriteFile(filepath.Join(folder, "SKILL.md"), []byte("---\nname: weather\ndescription: Says the weather for a city.\n---\nSay it.\n"), 0o644)
	var out bytes.Buffer
	if err := run(ctx, []string{"skills", "-store", store, "-add", folder}, &out); err != nil {
		t.Fatal(err)
	}
	repo := t.TempDir()
	for _, c := range [][]string{{"init", "-q", "-b", "main"}, {"config", "user.name", "Ada"}, {"config", "user.email", "ada@example.org"}} {
		if b, err := exec.Command("git", append([]string{"-C", repo}, c...)...).CombinedOutput(); err != nil {
			t.Fatalf("%v %s", err, b)
		}
	}
	out.Reset()
	if err := run(ctx, []string{"skills", "publish", "weather", "-repo", repo, "-store", store}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "published weather at ") || !strings.Contains(out.String(), "push it yourself: git -C "+repo+" push") {
		t.Errorf("%s", out.String())
	}
	if _, err := os.Stat(filepath.Join(repo, "skills", "weather", "SKILL.md")); err != nil {
		t.Error(err)
	}
	out.Reset()
	if err := run(ctx, []string{"skills", "-store", store, "publish", "weather", "-repo", repo}, &out); err != nil || !strings.Contains(out.String(), "nothing to commit") {
		t.Errorf("again, flags first: %v %s", err, out.String())
	}
	for _, bad := range [][]string{
		{"skills", "publish"},
		{"skills", "publish", "weather", "-store", store},
		{"skills", "publish", "weather", "-repo", repo},
		{"skills", "publish", "weather", "-repo", repo, "-store", store, "-trusted"},
		{"skills", "publish", "weather", "extra", "-repo", repo, "-store", store},
		{"skills", "publish", "-repo", repo, "-store", store},
	} {
		if err := run(ctx, bad, &out); err == nil {
			t.Errorf("%v: no error", bad)
		}
	}
}

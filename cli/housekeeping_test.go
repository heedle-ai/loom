package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// podmanOnPath puts a fake podman first on the PATH: it logs its calls and
// answers ps and images with the given JSON.
func podmanOnPath(t *testing.T, ps, images string) string {
	dir := t.TempDir()
	log := filepath.Join(dir, "log")
	sh := "#!/bin/sh\necho \"$@\" >> " + log + "\ncase \"$1\" in\n ps) cat <<'EOF'\n" + ps + "\nEOF\n;;\n images) cat <<'EOF'\n" + images + "\nEOF\n;;\n stats) echo '[]';;\nesac\n"
	if err := os.WriteFile(filepath.Join(dir, "podman"), []byte(sh), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return log
}

// TestInv_HousekeepingCommands: loom containers lists heedle's
// containers even with Temporal down (their runs unknown); loom images
// lists heedle's images, marking those kept, and -prune removes only old
// ones and refuses to run without -keep; cancel, containers and images
// refuse arguments they don't take before podman or Temporal is asked
// (LOOM-504).
func TestInv_HousekeepingCommands(t *testing.T) {
	id := strings.Repeat("a", 64)
	log := podmanOnPath(t,
		`[{"Id":"`+id+`","Names":["bobbin-session-1"],"Image":"ghcr.io/heedle-ai/pirn-agent@sha256:x","State":"running","Created":1,"Labels":{"heedle.kind":"agent","heedle.workflow":"loom-a/plan","heedle.run":"p1","heedle.step":"n0"}}]`,
		`[{"Id":"1","Names":["ghcr.io/heedle-ai/pirn-agent@sha256:new"],"Size":300000000,"Containers":1,"Created":1793876600},
		  {"Id":"2","Names":["ghcr.io/heedle-ai/pirn-agent@sha256:old"],"Size":290000000,"Containers":0,"Created":1793000000},
		  {"Id":"3","Names":["localhost/mine:1"],"Size":5,"Containers":0}]`)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, args := range [][]string{{"cancel"}, {"cancel", "a", "b", "c"}, {"containers", "-stop", "loom-a", "-clean"}, {"containers", "x"}, {"images", "x"}, {"images", "-prune"}} {
		var out bytes.Buffer
		if err := run(ctx, args, &out); err == nil {
			t.Errorf("%q succeeded", args)
		}
	}
	if b, _ := os.ReadFile(log); len(b) != 0 {
		t.Fatalf("podman ran for commands that should have been refused:\n%s", b)
	}

	var out bytes.Buffer
	start := time.Now()
	if err := run(ctx, []string{"containers", "-address", "127.0.0.1:1"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "agent") || !strings.Contains(out.String(), "loom-a/plan") || !strings.Contains(out.String(), "unknown") || !strings.Contains(out.String(), "n0") {
		t.Errorf("containers with Temporal down:\n%s", out.String())
	}
	if d := time.Since(start); d > 20*time.Second {
		t.Errorf("listing with Temporal down took %v", d)
	}

	out.Reset()
	if err := run(ctx, []string{"images", "-keep", "ghcr.io/heedle-ai/pirn-agent@sha256:new"}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(out.String(), "\n")
	if len(lines) < 3 || !strings.Contains(lines[1], "sha256:new") || !strings.HasSuffix(strings.TrimSpace(lines[1]), "yes") ||
		!strings.Contains(lines[2], "sha256:old") || !strings.HasSuffix(strings.TrimSpace(lines[2]), "no") || strings.Contains(out.String(), "mine") {
		t.Errorf("images:\n%s", out.String())
	}
	os.Remove(log)
	out.Reset()
	if err := run(ctx, []string{"images", "-keep", "ghcr.io/heedle-ai/pirn-agent@sha256:new", "-prune"}, &out); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(log)
	if !strings.Contains(string(b), "image rm ghcr.io/heedle-ai/pirn-agent@sha256:old\n") || strings.Count(string(b), "image rm") != 1 {
		t.Errorf("a prune ran:\n%s", b)
	}
}

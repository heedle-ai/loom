package loom_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/loom/examples/hello/shipment"
)

// TestInv_OnlyWarpRunsGit: no package here runs programs itself; all Git
// work goes through warp, the single writer.
func TestInv_OnlyWarpRunsGit(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{.ImportPath}}: {{join .Imports \" \"}}", "./...").Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		pkg, imports, _ := strings.Cut(line, ": ")
		for _, imp := range strings.Fields(imports) {
			if imp == "os/exec" {
				t.Errorf("%s imports os/exec; run git only through warp", pkg)
			}
		}
	}
}

// TestInv_WikiDiagramsMatchPlans: every ```lisp block in the wiki that is
// immediately followed by a ```mermaid block must compile (with the hello
// world's commands), and the diagram must be what draft.Mermaid draws.
func TestInv_WikiDiagramsMatchPlans(t *testing.T) {
	block := regexp.MustCompile("(?s)```lisp\n(.*?)```\\s*```mermaid\n(.*?)```")
	pages, _ := filepath.Glob("docs/wiki/*.md")
	checked := 0
	for _, page := range pages {
		b, err := os.ReadFile(page)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range block.FindAllStringSubmatch(string(b), -1) {
			p, err := loom.Compile(context.Background(), shipment.Registry(), m[1])
			if err != nil {
				t.Errorf("%s: program doesn't compile: %v", page, err)
				continue
			}
			if want := draft.Mermaid(p); m[2] != want {
				t.Errorf("%s: diagram disagrees with the compiled plan\nhave:\n%s\nwant:\n%s", page, m[2], want)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no lisp+mermaid pairs found in docs/wiki; the check would be vacuous")
	}
}

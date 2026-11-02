package loom_test

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
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

// TestInv_OnlyWarpRunsGit: no package here starts processes itself; all
// Git work goes through warp, the single writer. Every way the standard
// library offers to start a process is banned: os/exec, syscall,
// golang.org/x/sys, and os.StartProcess.
func TestInv_OnlyWarpRunsGit(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{.Dir}}|{{.ImportPath}}|{{join .Imports \" \"}}|{{join .GoFiles \" \"}}", "./...").Output()
	if err != nil {
		t.Fatal(err)
	}
	banned := map[string]bool{"os/exec": true, "syscall": true, "golang.org/x/sys/unix": true, "golang.org/x/sys/execabs": true}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Split(line, "|")
		dir, pkg, imports, files := f[0], f[1], strings.Fields(f[2]), strings.Fields(f[3])
		for _, imp := range imports {
			if banned[imp] || strings.HasPrefix(imp, "golang.org/x/sys") {
				t.Errorf("%s imports %s; run git only through warp", pkg, imp)
			}
		}
		for _, name := range files {
			b, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if regexp.MustCompile(`\bos\.StartProcess\b`).Match(b) {
				t.Errorf("%s/%s calls os.StartProcess; run git only through warp", pkg, name)
			}
		}
	}
}

// TestInv_WikiDiagramsMatchPlans: every ```lisp block in the wiki that is
// immediately followed by a ```mermaid block must compile (with the hello
// world's commands), and the diagram must be what draft.Mermaid draws.
func TestInv_WikiDiagramsMatchPlans(t *testing.T) {
	fence := regexp.MustCompile("(?m)^```mermaid$") // a fenced block, not an inline mention
	block := regexp.MustCompile("```lisp\n([^`]*)```\\s*```mermaid\n([^`]*)```")
	pages, _ := filepath.Glob("docs/wiki/*.md")
	checked := 0
	for _, page := range pages {
		b, err := os.ReadFile(page)
		if err != nil {
			t.Fatal(err)
		}
		pairs := block.FindAllStringSubmatch(string(b), -1)
		if n := len(fence.FindAllIndex(b, -1)); n != len(pairs) {
			t.Errorf("%s: %d mermaid blocks but only %d follow a lisp program; every diagram must be generated from its program", page, n, len(pairs))
		}
		for _, m := range pairs {
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

// TestInv_WorkflowsDoNoIO: workflow functions (those taking a
// workflow.Context) only use workflow APIs, Temporal types and pure helpers.
// Every package-qualified name they use must be on this list, so a call to
// os, net, warp's Store or time.Now can't slip in; I/O belongs in
// activities. workflowcheck covers non-determinism; this covers I/O.
func TestInv_WorkflowsDoNoIO(t *testing.T) {
	allowed := map[string]func(string) bool{
		"go.temporal.io/sdk/workflow":       func(string) bool { return true },
		"go.temporal.io/sdk/temporal":       func(string) bool { return true },
		"github.com/heedle-ai/shuttle/plan": func(string) bool { return true }, // types and names only: plan does no I/O (shuttle SHUT-3)
		"errors":                            func(string) bool { return true },
		"fmt":                               func(n string) bool { return n == "Errorf" || n == "Sprintf" },
		"time": func(n string) bool {
			return n == "Millisecond" || n == "Second" || n == "Minute" || n == "Hour" || n == "Duration"
		},
		"github.com/heedle-ai/warp": func(n string) bool { return n == "ID" || n == "ValidName" },
		// pirn's pure parsing and names, for a run's queued writes.
		"github.com/heedle-ai/pirn": func(n string) bool {
			return n == "ParseAgent" || n == "ParseGrant" || n == "AgentActivityName" || n == "WriteActivityName" || n == "Write" || n == "WriteResult"
		},
		"encoding/json": func(n string) bool { return n == "MarshalIndent" || n == "Marshal" || n == "Unmarshal" },
		// A type and a constant: SkillsWorkflow's result and its timeout.
		"github.com/heedle-ai/treadle": func(n string) bool { return n == "Skill" || n == "FetchTimeout" },
	}
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			imports := map[string]string{} // local name -> path
			for _, im := range file.Imports {
				path := strings.Trim(im.Path.Value, `"`)
				name := path[strings.LastIndex(path, "/")+1:]
				if im.Name != nil {
					name = im.Name.Name
				}
				imports[name] = path
			}
			for _, d := range file.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil || len(fn.Type.Params.List) == 0 {
					continue
				}
				if sel, ok := fn.Type.Params.List[0].Type.(*ast.SelectorExpr); !ok || sel.Sel.Name != "Context" || imports[fmt.Sprint(sel.X)] != "go.temporal.io/sdk/workflow" {
					continue
				}
				found++
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.SelectorExpr:
						id, ok := n.X.(*ast.Ident)
						if !ok || imports[id.Name] == "" || id.Obj != nil {
							return true
						}
						if ok := allowed[imports[id.Name]]; ok == nil || !ok(n.Sel.Name) {
							t.Errorf("%s: workflow %s uses %s.%s; do I/O in an activity", fset.Position(n.Pos()), fn.Name.Name, imports[id.Name], n.Sel.Name)
						}
					case *ast.GoStmt:
						t.Errorf("%s: workflow %s starts a goroutine; use workflow.Go", fset.Position(n.Pos()), fn.Name.Name)
					}
					return true
				})
			}
		}
	}
	if found == 0 {
		t.Fatal("no workflow functions found; the check would be vacuous")
	}
}

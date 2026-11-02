package loom_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// agentOutput is an agent step's output, encoded by pirn itself.
func agentOutput(t *testing.T, files ...pirn.File) string {
	t.Helper()
	b, err := json.Marshal(pirn.AgentResult{Answer: pirn.Answer{Model: "m", Text: "done"}, Agent: 1, Files: files})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func recordOutputs(t *testing.T, s *warp.Store, runID string, outs map[string]string) warp.ID {
	t.Helper()
	src := `(par (echo "a") (echo "b"))`
	return must((&loom.Recorder{Store: s}).Record(ctx, loom.RecordInput{
		Program: src, Plan: must(loom.Compile(ctx, draft.DefaultRegistry(), src)), Result: plan.Result{Outputs: outs},
		Ref: loom.DefaultRef, WorkflowID: "wf", RunID: runID, At: time.Unix(1700000000, 0),
	}))
}

// file reads path (under tree) from the store.
func file(t *testing.T, s *warp.Store, tree warp.ID, path string) (string, bool) {
	t.Helper()
	segs := strings.Split(path, "/")
	for i, seg := range segs {
		var found *warp.Entry
		for _, e := range must(s.Tree(ctx, tree)) {
			if e.Name == seg {
				e := e
				found = &e
			}
		}
		if found == nil {
			return "", false
		}
		if i == len(segs)-1 {
			return string(must(s.Blob(ctx, found.ID))), found.Kind == warp.KindBlob
		}
		tree = found.ID
	}
	return "", false
}

// TestInv_StepFilesAreInTheRecord: the files an agent step left in
// /work/out are real files in its run's commit, under files/<node>/, with
// their exact contents (text or binary, any name Git can hold, in
// subdirectories), so plain Git can show and diff them. A run whose steps
// carry no files has no files/ directory, so its record is as before.
func TestInv_StepFilesAreInTheRecord(t *testing.T) {
	s := newStore(t)
	bin := []byte{0, 1, 2, 0xff}
	c := recordOutputs(t, s, "run-1", map[string]string{
		"n0": agentOutput(t,
			pirn.File{Path: "report.txt", Size: 6, Text: "hello\n"},
			pirn.File{Path: "dir/my report.txt", Size: 1, Text: "a"},
			pirn.File{Path: "two\nlines", Size: 1, Text: "b"},
			pirn.File{Path: "empty", Size: 0},
			pirn.File{Path: "data.bin", Size: 4, Base64: base64.StdEncoding.EncodeToString(bin)}),
		"n1": "plain text output",
	})
	tree, _ := must2(s.Parents(ctx, c))
	files := entry(t, s, tree, loom.FilesDir).ID
	for path, want := range map[string]string{"n0/report.txt": "hello\n", "n0/dir/my report.txt": "a", "n0/two\nlines": "b", "n0/empty": "", "n0/data.bin": string(bin)} {
		if got, ok := file(t, s, files, path); !ok || got != want {
			t.Errorf("files/%s = %q (found %v), want %q", path, got, ok, want)
		}
	}
	if _, ok := file(t, s, files, "n1"); ok {
		t.Error("a plain output has files")
	}
	// The output itself is recorded unchanged.
	if got := string(must(s.Blob(ctx, entry(t, s, entry(t, s, tree, "outputs").ID, "n1").ID))); got != "plain text output" {
		t.Errorf("outputs/n1 = %q", got)
	}

	plain := newStore(t)
	tree2, _ := must2(plain.Parents(ctx, must(record(plain, "run-1", time.Unix(1700000000, 0)))))
	for _, e := range must(plain.Tree(ctx, tree2)) {
		if e.Name == loom.FilesDir {
			t.Error("a run without step files has a files/ directory")
		}
	}
}

// TestInv_StepFilesDiffAcrossRuns: when a later run's agent changes a file,
// Git's diff between the two records shows that file's change.
func TestInv_StepFilesDiffAcrossRuns(t *testing.T) {
	s := newStore(t)
	a := recordOutputs(t, s, "run-1", map[string]string{"n0": agentOutput(t, pirn.File{Path: "report.txt", Text: "rain on Monday\n"})})
	b := recordOutputs(t, s, "run-2", map[string]string{"n0": agentOutput(t, pirn.File{Path: "report.txt", Text: "sun on Monday\n"})})
	d := string(must(s.Diff(ctx, a, b)))
	if !strings.Contains(d, "files/n0/report.txt") || !strings.Contains(d, "-rain on Monday") || !strings.Contains(d, "+sun on Monday") {
		t.Errorf("diff:\n%s", d)
	}
}

// TestInv_StepFilesNeverFailARecord: a file whose path Git refuses or
// treats specially, a repeated path, a path that clashes with another
// ignoring case, or a malformed entry, is left out of files/ (it stays in
// the output) without dropping the step's other files, in the same way
// whatever order the output lists them in; the record always succeeds and
// the store passes git fsck --strict, so it can still be pushed. (Review
// 8: such paths made fsck fail, and one mistyped entry dropped every
// file.)
func TestInv_StepFilesNeverFailARecord(t *testing.T) {
	text := func(p string) map[string]any { return map[string]any{"path": p, "text": "x"} }
	evil := "[submodule \"../../evil\"]\n\tpath = x\n\turl = --upload-pack=touch /tmp/pwned\n"
	list := []any{
		text("ok.txt"), text("../escape"), text("/abs"), text("a//b"), text(""), text("."), text(".git/config"), text("d/.GIT"),
		text("f"), text("f/under-a-file"), text("g/h"), text("g"), text("twice"), text("twice"),
		map[string]any{"path": "both", "text": "x", "base64": "eA=="}, map[string]any{"path": "bad64", "base64": "!!"}, text("nul\x00name"),
		text(".g\u200cit/config"), text(".git /config"), text(".git./config"), text("git~1/config"), text(".gitmodules/x"),
		map[string]any{"path": ".gitmodules", "text": evil}, map[string]any{"path": ".gitm\u200codules", "text": evil},
		map[string]any{"path": "gi7eba~1", "text": evil}, text(".gitattributes"), text("dot."), text("space "),
		text("README"), text("readme"), text("Dir/a"), text("dir/b"), text("F/x"),
		map[string]any{"path": "typed", "text": 5}, map[string]any{"path": 7, "text": "x"}, "not an object",
		map[string]any{"PATH": "loud", "TEXT": "x"}, map[string]any{"path": "sub/ok", "text": "y"},
	}
	// In sorted order "F/x" comes before "f", so the directory wins and the
	// file f is left out (ignoring case); "g" comes before "g/h", so the
	// file wins. "Dir/a" and "dir/b" can share one directory on macOS.
	x := []byte("x")
	want := map[string][]byte{"ok.txt": x, "F/x": x, "f/under-a-file": x, "g": x, "README": x, "Dir/a": x, "dir/b": x, "sub/ok": []byte("y")}
	for i := 0; i < 2; i++ {
		b, _ := json.Marshal(map[string]any{"files": list, "FILES": []any{text("shadow")}})
		got, skipped := loom.StepFiles(string(b))
		if !reflect.DeepEqual(got, want) || len(skipped) != len(list)-len(want) {
			t.Errorf("order %d: files %q, skipped %q", i, got, skipped)
		}
		s := newStore(t)
		recordOutputs(t, s, "run-1", map[string]string{"n0": string(b)})
		if out, err := exec.Command("git", "--git-dir", s.Dir(), "fsck", "--strict").CombinedOutput(); err != nil {
			t.Errorf("git fsck --strict: %v\n%s", err, out)
		}
		for l, r := 0, len(list)-1; l < r; l, r = l+1, r-1 {
			list[l], list[r] = list[r], list[l]
		}
	}
	for _, out := range []string{"", "null", "[]", `{"files":"x"}`, `{"files":[1]}`, `{"files":[{"path":1}]}`, "{", "  {\"files\":null}", `{"Files":[{"path":"a"}]}`} {
		if got, _ := loom.StepFiles(out); len(got) != 0 {
			t.Errorf("StepFiles(%q) = %q", out, got)
		}
	}
}

// TestInv_StepFilesAreBounded: a record holds at most MaxRecordedFiles
// step files in all (the first in node, then path, order), so it finishes
// well inside Record's timeout. (Review 8: 20,000 files took 56 s, and
// past the timeout every attempt failed and the run was lost.)
func TestInv_StepFilesAreBounded(t *testing.T) {
	var fs []map[string]string
	for i := 0; i < 3000; i++ {
		fs = append(fs, map[string]string{"path": fmt.Sprintf("f%05d", i)})
	}
	b, _ := json.Marshal(map[string]any{"files": fs})
	s := newStore(t)
	start := time.Now()
	c := recordOutputs(t, s, "run-1", map[string]string{"n0": string(b), "n1": filesOutput1("late.txt")})
	took := time.Since(start)
	tree, _ := must2(s.Parents(ctx, c))
	files := entry(t, s, tree, loom.FilesDir).ID
	n0 := must(s.Tree(ctx, entry(t, s, files, "n0").ID))
	if len(n0) != loom.MaxRecordedFiles || n0[0].Name != "f00000" || n0[len(n0)-1].Name != fmt.Sprintf("f%05d", loom.MaxRecordedFiles-1) {
		t.Errorf("%d files recorded, first %s", len(n0), n0[0].Name)
	}
	if _, ok := file(t, s, files, "n1/late.txt"); ok {
		t.Error("a later node's files were recorded past the limit")
	}
	t.Logf("recorded %d files in %v", loom.MaxRecordedFiles, took)
}

func filesOutput1(path string) string {
	b, _ := json.Marshal(map[string]any{"files": []map[string]string{{"path": path, "text": "x"}}})
	return string(b)
}

// FuzzStepFiles: whatever a step outputs, the files it keeps are ones a
// Git tree can hold: every path segment passes warp.ValidName, and no path
// is, or runs through, another file's path. (Those are exactly what
// putDir's trees need; TestInv_StepFilesNeverFailARecord records them.)
func FuzzStepFiles(f *testing.F) {
	f.Add(`{"files":[{"path":"a/b","text":"x"},{"path":"a","text":"y"}]}`)
	f.Add(`{"files":[{"path":"a b/c\nd","base64":"AAE="},{"path":"a b","text":""}]}`)
	f.Add(`{"files":[{"path":"x/./y"},{"path":"x/../y"},{"path":"X/.Git/y"}]}`)
	f.Fuzz(func(t *testing.T, out string) {
		files, skipped := loom.StepFiles(out)
		for path := range files {
			segs := strings.Split(path, "/")
			for i, seg := range segs {
				if warp.ValidName(seg) != nil {
					t.Fatalf("kept %q: segment %q", path, seg)
				}
				if _, isFile := files[strings.Join(segs[:i], "/")]; i > 0 && isFile {
					t.Fatalf("kept %q under the file %q", path, strings.Join(segs[:i], "/"))
				}
			}
		}
		again, skipped2 := loom.StepFiles(out)
		if !reflect.DeepEqual(files, again) || !reflect.DeepEqual(skipped, skipped2) {
			t.Fatal("not a function of the output")
		}
	})
}

// TestInv_RegistryFilesArePointers: a step file kept in an OCI registry
// (pirn's large files) is recorded as a pointer naming the repository,
// digest and size, never as an empty file; a malformed one is left out.
func TestInv_RegistryFilesArePointers(t *testing.T) {
	sum := strings.Repeat("ab", 32)
	s := newStore(t)
	c := recordOutputs(t, s, "run-1", map[string]string{"n0": agentOutput(t,
		pirn.File{Path: "big.bin", Size: 3000000, OCI: "ghcr.io/acme/files", SHA256: sum},
		pirn.File{Path: "small.txt", Size: 2, Text: "hi"})})
	tree, _ := must2(s.Parents(ctx, c))
	files := entry(t, s, tree, loom.FilesDir).ID
	if got, _ := file(t, s, files, "n0/big.bin"); got != "oci ghcr.io/acme/files@sha256:"+sum+"\nsize 3000000\n" {
		t.Errorf("pointer %q", got)
	}
	for _, bad := range []map[string]any{
		{"path": "a", "oci": "ghcr.io/acme/files", "sha256": sum},
		{"path": "a", "oci": "ghcr.io/acme/files", "size": 1},
		{"path": "a", "oci": "ghcr.io/acme/files", "sha256": "AB", "size": 1},
		{"path": "a", "oci": "ghcr.io/acme/files", "sha256": sum, "size": -1},
		{"path": "a", "oci": "ghcr.io/acme/files", "sha256": sum, "size": 1.5},
		{"path": "a", "oci": "ghcr.io/acme/files\nsize 0", "sha256": sum, "size": 1},
		{"path": "a", "oci": "ghcr.io/acme/files:v1", "sha256": sum, "size": 1},
		{"path": "a", "oci": "ghcr.io/acme/files", "sha256": sum, "size": 1, "text": "x"},
		{"path": "a", "oci": 5, "sha256": sum, "size": 1},
		// Review 10: a null size was recorded as 0, and a null oci as an empty file.
		{"path": "a", "oci": "ghcr.io/acme/files", "sha256": sum, "size": nil},
		{"path": "a", "oci": nil, "sha256": sum, "size": 1},
		{"path": "a", "text": nil},
	} {
		b, _ := json.Marshal(map[string]any{"files": []any{bad}})
		if got, _ := loom.StepFiles(string(b)); len(got) != 0 {
			t.Errorf("%v recorded as %q", bad, got)
		}
	}
}

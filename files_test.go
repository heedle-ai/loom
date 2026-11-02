package loom_test

import (
	"encoding/base64"
	"encoding/json"
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

// TestInv_StepFilesNeverFailARecord: a file whose path Git can't hold, or
// that is malformed, is left out of files/ (it stays in the output), in
// the same way whatever order the output lists files in; the record
// always succeeds. (A record that fails non-retryably loses the run.)
func TestInv_StepFilesNeverFailARecord(t *testing.T) {
	text := func(p string) map[string]any { return map[string]any{"path": p, "text": "x"} }
	list := []map[string]any{
		text("ok.txt"), text("../escape"), text("/abs"), text("a//b"), text(""), text("."), text(".git/config"), text("d/.GIT"),
		text("f"), text("f/under-a-file"), text("g/h"), text("g"), text("ok.txt"),
		{"path": "both", "text": "x", "base64": "eA=="}, {"path": "bad64", "base64": "!!"}, text("nul\x00name"),
	}
	// In sorted order "g" comes before "g/h", so the file g wins.
	want := map[string][]byte{"ok.txt": []byte("x"), "f": []byte("x"), "g": []byte("x")}
	for i := 0; i < 2; i++ {
		b, _ := json.Marshal(map[string]any{"files": list})
		got, skipped := loom.StepFiles(string(b))
		if !reflect.DeepEqual(got, want) || len(skipped) != len(list)-len(want) {
			t.Errorf("order %d: files %q, skipped %q", i, got, skipped)
		}
		s := newStore(t)
		recordOutputs(t, s, "run-1", map[string]string{"n0": string(b)})
		for l, r := 0, len(list)-1; l < r; l, r = l+1, r-1 {
			list[l], list[r] = list[r], list[l]
		}
	}
	for _, out := range []string{"", "null", "[]", `{"files":"x"}`, `{"files":[1]}`, `{"files":[{"path":1}]}`, "{", "  {\"files\":null}"} {
		if got, _ := loom.StepFiles(out); len(got) != 0 {
			t.Errorf("StepFiles(%q) = %q", out, got)
		}
	}
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

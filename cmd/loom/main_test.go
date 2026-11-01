package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

func TestCompileCommand(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"compile", `(pipe (echo "a") echo)`}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"requires": [`) {
		t.Errorf("unexpected plan:\n%s", out.String())
	}
	if err := run(context.Background(), []string{"compile", `(pipe (echo "a") shout)`}, &out); err == nil ||
		!strings.Contains(err.Error(), `unknown builtin "shout"`) {
		t.Errorf("err = %v, want unknown builtin", err)
	}
	// bobbin's container command is available to programs.
	img := strings.Repeat("ab", 32)
	if err := run(context.Background(), []string{"compile", `(container "` + img + `" "a")`}, &out); err != nil ||
		!strings.Contains(out.String(), `"activity": "Container"`) {
		t.Errorf("container program: %v\n%s", err, out.String())
	}
	for _, bad := range [][]string{nil, {"bogus"}, {"compile"}, {"worker"}} {
		if err := run(context.Background(), bad, &out); err == nil {
			t.Errorf("run(%v) succeeded, want usage error", bad)
		}
	}
}

func TestBeliefsCommand(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir() + "/store.git"
	s, err := warp.Init(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	rec := &loom.Recorder{Store: s}
	for i, q := range []string{"a", "b", "a"} {
		src := `(echo "` + q + `")`
		p, err := loom.Compile(ctx, draft.DefaultRegistry(), src)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rec.Record(ctx, loom.RecordInput{Program: src, Plan: p, Ref: loom.DefaultRef,
			Result:     plan.Result{Outputs: map[string]string{"n0": q + " v" + string(rune('1'+i))}},
			WorkflowID: "wf", RunID: "run-" + string(rune('1'+i)), At: time.Unix(1700000000+int64(i), 0)}); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := run(ctx, []string{"beliefs", "-store", dir}, &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	// Two questions; a's belief is its latest run (v3), not the first.
	if strings.Count(got, "(echo ") != 2 || !strings.Contains(got, "n0     a v3") || strings.Contains(got, "a v1") || !strings.Contains(got, "n0     b v2") {
		t.Errorf("beliefs output:\n%s", got)
	}
	for _, bad := range [][]string{{"beliefs"}, {"beliefs", "-store", dir, "-ref", "refs/loom/none"}, {"merge", "extra"}} {
		if err := run(ctx, bad, &out); err == nil {
			t.Errorf("run(%v) succeeded, want an error", bad)
		}
	}
}

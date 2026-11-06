package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// TestInv_CostsCommand: heedle costs prints the month's runs by schedule
// and model, with their recorded tokens and an estimate for each that
// has a price ("no price" for a model with none, never a dollar figure),
// the price table's date, and a total that says when some steps have no
// estimate; a malformed prices file is said and the built-in prices
// used. Runs of other months, and in another time zone's month, aren't
// counted. (Roadmap 10.4.)
func TestInv_CostsCommand(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "store.git")
	s, err := warp.Init(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	reg := draft.DefaultRegistry()
	reg.MustRegister(pirn.BuiltinSpec())
	src := `(ask "news")`
	p, err := loom.Compile(ctx, reg, src)
	if err != nil {
		t.Fatal(err)
	}
	rec := &loom.Recorder{Store: s}
	for i, r := range []struct {
		schedule, model string
		in, out         int64
		at              time.Time
	}{
		{"news", "claude-sonnet-5-5", 99, 99, time.Date(2026, 10, 31, 23, 0, 0, 0, time.UTC)},
		{"news", "claude-sonnet-5-5", 10000, 1000, time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)},
		{"news", "claude-sonnet-5-5", 10000, 1000, time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)},
		{"", "claude-mythos-5-1", 7, 3, time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)},
	} {
		b, _ := json.Marshal(pirn.Answer{Pirn: 1, Model: r.model, InputTokens: r.in, OutputTokens: r.out})
		if _, err := rec.Record(ctx, loom.RecordInput{Program: src, Plan: p, Result: plan.Result{Outputs: map[string]string{"n0": string(b)}},
			Ref: loom.DefaultRef, WorkflowID: "wf", RunID: "run-" + string(rune('a'+i)), At: r.at, Schedule: r.schedule}); err != nil {
			t.Fatal(err)
		}
	}
	// The ledger's calls no record holds (roadmap 10.4.1): a front-door
	// ask, and a failed attempt of a run news started.
	novAt := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	for _, e := range []loom.LedgerEntry{
		{Kind: loom.LedgerFrontDoor, At: novAt, Workflow: "ask-1", Run: "a1", Activity: "1", Calls: []pirn.Call{{Model: "claude-sonnet-5-5", Input: 500, Output: 50}}},
		{Kind: loom.LedgerRun, At: novAt, Workflow: "wf-failed", Run: "r9", Schedule: "news"},
		{Kind: loom.LedgerAttempt, At: novAt, Workflow: "wf-failed/plan", Run: "p9", Activity: "1", Node: "n0", Step: "ask", Attempt: 1, Failed: true, Schedule: "news", Calls: []pirn.Call{{Model: "claude-sonnet-5-5", Input: 300, Output: 30}}},
	} {
		if err := loom.WriteLedger(ctx, s, e); err != nil {
			t.Fatal(err)
		}
	}
	prices := filepath.Join(t.TempDir(), "prices.json")
	var out bytes.Buffer
	if err := run(ctx, []string{"costs", "-store", dir, "-month", "2026-11", "-prices", prices}, &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"Claude usage in November 2026 (UTC), every call: as recorded, and from the usage ledger; dollars are estimates (est.) with prices as of 2026-11-05",
		"(not scheduled)  recorded     claude-mythos-5-1  1     7      3       no price",
		"(not scheduled)  front door   claude-sonnet-5-5  1     500    50      <$0.01",
		"news             recorded     claude-sonnet-5-5  2     20000  2000    $0.06",
		"news             failed runs  claude-sonnet-5-5  1     300    30      <$0.01",
		"total: $0.06 est. for the steps with a price; 1 steps have no estimate (tokens only)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("no %q in\n%s", want, got)
		}
	}
	// Recorded first, then the ledger's kinds in the page's order
	// (review 56: "retries" sorted before "recorded").
	if i, j := strings.Index(got, "news             recorded"), strings.Index(got, "news             failed runs"); i < 0 || j < i || !strings.Contains(got, "SCHEDULE         KIND") {
		t.Errorf("order or header:\n%s", got)
	}
	if strings.Contains(got, "$0.00") || strings.Contains(got, "99") {
		t.Errorf("a dollar figure without a price, or October's run:\n%s", got)
	}
	// In Tokyo, October's last run (23:00 UTC on the 31st) is November's.
	out.Reset()
	run(ctx, []string{"costs", "-store", dir, "-month", "2026-11", "-tz", "Asia/Tokyo", "-prices", prices}, &out)
	if !strings.Contains(out.String(), "news             recorded     claude-sonnet-5-5  3     20099  2099") {
		t.Errorf("Tokyo:\n%s", out.String())
	}
	os.WriteFile(prices, []byte(`{"models":{"claude-sonnet-5-5":{"input":"two"}}}`), 0o600)
	out.Reset()
	run(ctx, []string{"costs", "-store", dir, "-month", "2026-11", "-prices", prices}, &out)
	if !strings.Contains(out.String(), "loom: prices file "+prices) || !strings.Contains(out.String(), "using the built-in prices") || !strings.Contains(out.String(), "$0.06") {
		t.Errorf("malformed prices:\n%s", out.String())
	}
	// A month whose only usage has no price: no dollar figure at all.
	out.Reset()
	if err := run(ctx, []string{"costs", "-store", dir, "-month", "2026-11", "-tz", "UTC", "-prices", prices}, &out); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(t.TempDir(), "s.git")
	es, _ := warp.Init(ctx, empty)
	b, _ := json.Marshal(pirn.Answer{Pirn: 1, Model: "claude-mythos-5-1", InputTokens: 7, OutputTokens: 3})
	(&loom.Recorder{Store: es}).Record(ctx, loom.RecordInput{Program: src, Plan: p, Result: plan.Result{Outputs: map[string]string{"n0": string(b)}},
		Ref: loom.DefaultRef, WorkflowID: "wf", RunID: "only", At: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)})
	out.Reset()
	run(ctx, []string{"costs", "-store", empty, "-month", "2026-11", "-prices", prices}, &out)
	if strings.Contains(out.String(), "$") || !strings.Contains(out.String(), "total: no estimate; 1 steps have none (tokens only)") {
		t.Errorf("unpriced month:\n%s", out.String())
	}
	out.Reset()
	run(ctx, []string{"costs", "-store", empty, "-month", "2025-01", "-prices", prices}, &out)
	if !strings.Contains(out.String(), "no runs asked Claude in January 2025") {
		t.Errorf("an empty month:\n%s", out.String())
	}
	for _, bad := range [][]string{
		{"costs"},
		{"costs", "-store", dir, "-month", "November"},
		{"costs", "-store", dir, "-tz", "Mars/Olympus"},
		{"schedule", "-name", "x", "-every", "1h", "-budget", "five", `(echo "a")`},
		{"schedule", "-name", "x", "-every", "1h", "-budget", "0.001", `(echo "a")`},
		{"schedule", "-name", "x", "-every", "1h", "-resume-next-month", `(echo "a")`},
		{"schedules", "-budget", "5"},
	} {
		if err := run(ctx, bad, &out); err == nil {
			t.Errorf("%v: no error", bad)
		}
	}
}

// TestInv_WorkerResumesBudgets: the worker itself looks for schedules a
// new month resumes, so they resume whether or not heedle up's watcher
// runs. (Review 48: only the watcher did.)
func TestInv_WorkerResumesBudgets(t *testing.T) {
	src, err := os.ReadFile("cli.go")
	if err != nil || !strings.Contains(string(src), "go loom.ResumeBudgetsEvery(ctx, c, *queue,") {
		t.Errorf("loom worker doesn't resume schedules in a new month (%v)", err)
	}
}

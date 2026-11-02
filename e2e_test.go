package loom_test

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/treadle"
)

// TestE2E_ARealServer runs a worker against a real Temporal dev server
// (the Temporal CLI, which StartDevServer downloads) and, through a
// client, the whole of phases 3 and 4: a hello world run recorded in
// warp; a run whose agent step queues a write that waits for approval,
// queried and approved over the wire, then run once; a program written
// from a description; and a skill created from one, left pending. CI's
// e2e job runs it with LOOM_E2E=1; elsewhere it is skipped.
func TestE2E_ARealServer(t *testing.T) {
	if os.Getenv("LOOM_E2E") != "1" {
		t.Skip("set LOOM_E2E=1 to run against a real Temporal server")
	}
	c0, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	srv, err := testsuite.StartDevServer(c0, testsuite.DevServerOptions{LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	c := srv.Client()
	const queue = "e2e"

	s := newStore(t)
	reg := draft.DefaultRegistry()
	reg.MustRegister(pirn.AgentBuiltinSpec())
	w := worker.New(c, queue, worker.Options{})
	loom.Register(w, s, reg)
	var mu sync.Mutex
	var sent []pirn.Write
	w.RegisterActivityWithOptions(func(context.Context, any) (string, error) {
		return agentOut(pirn.Write{ID: "w1", Connector: "mail", Tool: "send", Input: json.RawMessage(`{"to":"boss"}`), Approve: true}), nil
	}, activity.RegisterOptions{Name: pirn.AgentActivityName})
	w.RegisterActivityWithOptions(func(_ context.Context, wr pirn.Write) (pirn.WriteResult, error) {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, wr)
		return pirn.WriteResult{Output: "sent"}, nil
	}, activity.RegisterOptions{Name: pirn.WriteActivityName})
	loom.RegisterWriter(w, writer(&scripted{replies: []string{"```\n(pipe (echo \"a\") (triage \"x\"))\n```"}}))
	lib := &treadle.Library{Store: newStore(t)}
	loom.RegisterSkills(w, loom.Skills{Library: lib, Create: func(ctx context.Context, desc string) (map[string][]byte, error) {
		return map[string][]byte{"SKILL.md": []byte("---\nname: triage\ndescription: Triages bugs.\n---\nTriage.\n")}, nil
	}})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	run := func(src string) client.WorkflowRun {
		r, err := loom.Submit(c0, c, queue, loom.RunInput{Program: src, Plan: must(loom.Compile(c0, reg, src))})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	var hello loom.RunResult
	if err := run(`(pipe (echo "a") echo)`).Get(c0, &hello); err != nil || hello.Commit == "" {
		t.Fatalf("hello world: %+v %v", hello, err)
	}

	r := run(`(agent "mail" "mail/send")`)
	var waiting []loom.WriteOutcome
	for i := 0; i < 100 && (len(waiting) == 0 || waiting[0].Status != "waiting"); i++ {
		time.Sleep(100 * time.Millisecond)
		waiting, _ = loom.Writes(c0, c, r.GetID())
	}
	if len(waiting) != 1 || waiting[0].Status != "waiting" {
		t.Fatalf("waiting writes %+v", waiting)
	}
	if err := loom.DecideWrite(c0, c, r.GetID(), loom.WriteDecision{Node: "n0", ID: "w1", Approve: true}); err != nil {
		t.Fatal(err)
	}
	var res loom.RunResult
	if err := r.Get(c0, &res); err != nil || statuses(res.Writes) != "n0/w1=done " || len(sent) != 1 || !sent[0].Approved {
		t.Fatalf("approved write: %+v %v, sent %+v", res.Writes, err, sent)
	}

	wr, err := loom.WriteProgram(c0, c, queue, "Triage the bugs.")
	if err != nil || wr.Attempts != 1 || len(wr.Plan.Nodes) != 2 {
		t.Fatalf("written: %+v %v", wr, err)
	}

	sk, err := loom.ChangeSkills(c0, c, queue, loom.SkillsChange{Op: loom.SkillsCreate, Description: "Triages bugs."})
	if err != nil || !sk.Pending || sk.Name != "triage" {
		t.Fatalf("created: %+v %v", sk, err)
	}
}

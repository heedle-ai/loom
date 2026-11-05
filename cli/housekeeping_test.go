package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/heedle-ai/bobbin"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/shuttle/plan"
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
	for _, args := range [][]string{{"cancel"}, {"cancel", "a", "b", "c"}, {"containers", "-stop", "loom-a", "-clean"}, {"containers", "-run", "r1"}, {"containers", "x"}, {"images", "x"}, {"images", "-prune"}} {
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
	// Review 55: only an explicit -store names a home; without one,
	// nothing is limited to the current directory's (LOOM-523).
	if strings.Contains(out.String(), "This home is") || strings.Contains(out.String(), "(this)") {
		t.Errorf("containers without -store took a home:\n%s", out.String())
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

// stopTemporal is a fake Temporal for -stop: it describes the executions
// it was given ("workflow run"; run empty for the latest) and records
// cancellations.
type stopTemporal struct {
	client.Client
	runs      map[string]*workflowservice.DescribeWorkflowExecutionResponse
	cancelled []string
}

func (f *stopTemporal) DescribeWorkflowExecution(_ context.Context, wf, run string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if d, ok := f.runs[wf+" "+run]; ok {
		return d, nil
	}
	return nil, serviceerror.NewNotFound("no such workflow")
}

func (f *stopTemporal) CancelWorkflow(_ context.Context, wf, run string) error {
	f.cancelled = append(f.cancelled, wf+" "+run)
	return nil
}

func described(wf, run, typ string, status enums.WorkflowExecutionStatus, parent *commonpb.WorkflowExecution) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: wf, RunId: run}, Type: &commonpb.WorkflowType{Name: typ}, Status: status, ParentExecution: parent}}
}

// TestInv_StopStopsTheRunItNames: containers -stop WORKFLOW cancels the
// run its containers belong to, or the one -run names, never Temporal's
// latest run of that workflow ID; with no container of the workflow, or
// containers of several of its runs, it asks for -run and cancels
// nothing; the listing shows each container's home, marking this one's
// (LOOM-521, roadmap 9.2.1).
func TestInv_StopStopsTheRunItNames(t *testing.T) {
	home := bobbin.HomeOf(filepath.Join(t.TempDir(), "store.git"))
	other := "0123456789ab"
	ctr := func(id, wf, run, h string) string {
		l := `"heedle.kind":"agent","heedle.workflow":"` + wf + `","heedle.run":"` + run + `","heedle.step":"n0"`
		if h != "" {
			l += `,"heedle.home":"` + h + `"`
		}
		return `{"Id":"` + strings.Repeat(id, 64) + `","Names":["c` + id + `"],"State":"running","Created":1,"Labels":{` + l + `}}`
	}
	podmanOnPath(t, "["+strings.Join([]string{ctr("1", "loom-a/plan", "p1", home), ctr("2", "loom-c/plan", "p5", home), ctr("3", "loom-d/plan", "p6", home), ctr("4", "loom-d/plan", "p7", ""), ctr("5", "loom-e/plan", "p8", other)}, ",")+"]", "[]")
	parent := func(wf, run string) *commonpb.WorkflowExecution {
		return &commonpb.WorkflowExecution{WorkflowId: wf, RunId: run}
	}
	running, failed := enums.WORKFLOW_EXECUTION_STATUS_RUNNING, enums.WORKFLOW_EXECUTION_STATUS_FAILED
	f := &stopTemporal{runs: map[string]*workflowservice.DescribeWorkflowExecutionResponse{
		// loom-a's run r1, whose step has the container, failed; a newer
		// run r3 of the same workflow ID is the latest, and running.
		"loom-a/plan p1": described("loom-a/plan", "p1", plan.WorkflowName, failed, parent("loom-a", "r1")),
		"loom-a r1":      described("loom-a", "r1", loom.RunWorkflowName, failed, nil),
		"loom-a r3":      described("loom-a", "r3", loom.RunWorkflowName, running, nil),
		"loom-a ":        described("loom-a", "r3", loom.RunWorkflowName, running, nil),
		"loom-c/plan p5": described("loom-c/plan", "p5", plan.WorkflowName, running, parent("loom-c", "r5")),
		"loom-c r5":      described("loom-c", "r5", loom.RunWorkflowName, running, nil),
		"loom-d/plan p6": described("loom-d/plan", "p6", plan.WorkflowName, running, parent("loom-d", "r6")),
		"loom-d r6":      described("loom-d", "r6", loom.RunWorkflowName, failed, nil),
		"loom-d/plan p7": described("loom-d/plan", "p7", plan.WorkflowName, running, parent("loom-d", "r7")),
		"loom-d r7":      described("loom-d", "r7", loom.RunWorkflowName, running, nil),
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out bytes.Buffer
	// The run its containers belong to has ended: nothing is cancelled,
	// and certainly not the newer r3.
	err := containersCommand(ctx, &out, f, containersArgs{stop: "loom-a"})
	if err == nil || !strings.Contains(err.Error(), "already ended (failed)") || len(f.cancelled) != 0 {
		t.Errorf("-stop loom-a: %v; cancelled %q", err, f.cancelled)
	}
	// The run named with -run is the one cancelled.
	if err := containersCommand(ctx, &out, f, containersArgs{stop: "loom-a", run: "r3"}); err != nil || strings.Join(f.cancelled, ",") != "loom-a r3" {
		t.Errorf("-stop loom-a -run r3: %v; cancelled %q", err, f.cancelled)
	}
	f.cancelled = nil
	if err := containersCommand(ctx, &out, f, containersArgs{stop: "loom-c"}); err != nil || strings.Join(f.cancelled, ",") != "loom-c r5" {
		t.Errorf("-stop loom-c: %v; cancelled %q", err, f.cancelled)
	}
	f.cancelled = nil
	for _, stop := range []string{"loom-d", "loom-z"} {
		if err := containersCommand(ctx, &out, f, containersArgs{stop: stop}); err == nil || !strings.Contains(err.Error(), "-run RUN") {
			t.Errorf("-stop %s: %v, want it to ask for -run", stop, err)
		}
	}
	if len(f.cancelled) != 0 {
		t.Errorf("cancelled %q", f.cancelled)
	}
	out.Reset()
	homeStore := filepath.Join(t.TempDir(), "store.git")
	h := bobbin.HomeOf(homeStore)
	podmanOnPath(t, "["+ctr("1", "loom-a/plan", "p1", h)+","+ctr("5", "loom-e/plan", "p8", other)+","+ctr("4", "loom-d/plan", "p7", "")+"]", "[]")
	if err := containersCommand(ctx, &out, f, containersArgs{store: homeStore}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "This home is "+h) || !strings.Contains(out.String(), h+" (this)") || !strings.Contains(out.String(), other) ||
		!strings.Contains(out.String(), "HOME") || strings.Contains(out.String(), other+" (this)") || !strings.Contains(out.String(), "-stop WORKFLOW -run RUN") {
		t.Errorf("the listing:\n%s", out.String())
	}
}

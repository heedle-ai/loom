package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// Replay without Claude (roadmap 9.9). An act step's record keeps its
// actions as a script; the next run of the same program finds it here
// and replays it (pirn's Act.Script). The script is the last successful
// run's: the newest record on the run's ref whose Loom-Program is this
// run's program and whose step at that node is an act step that finished
// with a script. A script from another program is never used.

// MaxScriptWalk bounds how many records back on a ref a step looks for
// its program's last successful run.
const MaxScriptWalk = 1000

// LastScript is the script the last successful run of program (its
// WorkflowID) on ref kept for its act step at node: the newest record
// naming the program (TrailerProgram) whose plan has an act step at node
// and whose output there is an act result that finished with a script.
// ok is false if there is none within MaxScriptWalk records.
func LastScript(ctx context.Context, s Records, ref, program, node string) (pirn.ActScript, bool, error) {
	if program == "" || warp.ValidName(node) != nil {
		return pirn.ActScript{}, false, fmt.Errorf("%w: no program or node to find a script for", warp.ErrInvalid)
	}
	head, ok, err := s.Resolve(ctx, ref)
	if err != nil || !ok {
		return pirn.ActScript{}, false, err
	}
	log, err := s.Log(ctx, head, MaxScriptWalk)
	if err != nil {
		return pirn.ActScript{}, false, err
	}
	for _, c := range log {
		if trailer(c.Trailers, TrailerProgram) != program || len(c.Parents) > 1 {
			continue // another program's run, or a merge (its files are its first parent's run)
		}
		r, ok, err := scriptIn(ctx, s, c, node)
		if err != nil {
			return pirn.ActScript{}, false, err
		}
		if ok {
			return r, true, nil
		}
	}
	return pirn.ActScript{}, false, nil
}

// scriptIn is node's act result in the record c, if its plan has an act
// step there that finished with a script.
func scriptIn(ctx context.Context, s Records, c warp.CommitInfo, node string) (pirn.ActScript, bool, error) {
	es, err := s.Tree(ctx, c.Tree)
	if err != nil {
		return pirn.ActScript{}, false, err
	}
	var planID, outsID warp.ID
	for _, e := range es {
		switch {
		case e.Name == "plan.json" && e.Kind == warp.KindBlob:
			planID = e.ID
		case e.Name == "outputs" && e.Kind == warp.KindTree:
			outsID = e.ID
		}
	}
	if planID == "" || outsID == "" {
		return pirn.ActScript{}, false, nil
	}
	b, err := s.Blob(ctx, planID)
	if err != nil {
		return pirn.ActScript{}, false, err
	}
	var p plan.Plan
	if json.Unmarshal(b, &p) != nil {
		return pirn.ActScript{}, false, nil
	}
	act := false
	for _, n := range p.Nodes {
		act = act || string(n.ID) == node && n.Activity == pirn.ActActivityName
	}
	if !act {
		return pirn.ActScript{}, false, nil
	}
	outs, err := s.Tree(ctx, outsID)
	if err != nil {
		return pirn.ActScript{}, false, err
	}
	for _, e := range outs {
		if e.Name != node || e.Kind != warp.KindBlob {
			continue
		}
		out, err := s.Blob(ctx, e.ID)
		if err != nil {
			return pirn.ActScript{}, false, err
		}
		r, ok := pirn.ParseAct(string(out))
		if !ok || r.Ended != pirn.EndedFinished || len(r.Script) == 0 {
			return pirn.ActScript{}, false, nil
		}
		return pirn.ActScript{Run: trailer(c.Trailers, TrailerRun), Record: string(c.ID), Result: r}, true, nil
	}
	return pirn.ActScript{}, false, nil
}

// Scripts is pirn's Act.Script for a worker: inside an act activity it
// reads the step's run's input from Temporal (the first event of its
// history, "<run>/plan"'s parent), so it knows the run's program and ref
// exactly, and finds that program's last script for the step there
// (LastScript). A step that isn't in a loom run has none. It reads the
// input with Temporal's default data converter, as loom's workers use: a
// worker with a codec of its own would find no script, and its steps would
// say why in their record (replay.why) and ask Claude.
func Scripts(c client.Client, s Records) func(context.Context, string) (pirn.ActScript, bool, error) {
	return func(ctx context.Context, node string) (pirn.ActScript, bool, error) {
		if !activity.IsActivity(ctx) {
			return pirn.ActScript{}, false, nil
		}
		run, ok := strings.CutSuffix(activity.GetInfo(ctx).WorkflowExecution.ID, "/plan")
		if !ok {
			return pirn.ActScript{}, false, nil
		}
		in, err := runInput(ctx, c, run)
		if err != nil {
			return pirn.ActScript{}, false, err
		}
		program, err := WorkflowID(ctx, in)
		if err != nil {
			return pirn.ActScript{}, false, err
		}
		return LastScript(ctx, s, in.Ref, program, node)
	}
}

// runInput is the input the run workflowID was started with, from the
// first event of its history; its ref defaulted as RunWorkflow defaults it.
func runInput(ctx context.Context, c client.Client, workflowID string) (RunInput, error) {
	it := c.GetWorkflowHistory(ctx, workflowID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !it.HasNext() {
		return RunInput{}, errors.New("loom: the run's history is empty")
	}
	ev, err := it.Next()
	if err != nil {
		return RunInput{}, err
	}
	a := ev.GetWorkflowExecutionStartedEventAttributes()
	if a == nil || a.GetWorkflowType().GetName() != RunWorkflowName {
		return RunInput{}, errors.New("loom: the step's workflow isn't a loom run")
	}
	var in RunInput
	if err := converter.GetDefaultDataConverter().FromPayloads(a.GetInput(), &in); err != nil {
		return RunInput{}, fmt.Errorf("loom: reading the run's input: %w", err)
	}
	if in.Ref == "" {
		in.Ref = DefaultRef
	}
	return in, nil
}

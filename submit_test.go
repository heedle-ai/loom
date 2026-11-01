package loom_test

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
)

// TestSubmit_TagsTheRef: Submit stores the run's ref in the workflow memo,
// so a UI can list each ref's runs while they're still running.
func TestSubmit_TagsTheRef(t *testing.T) {
	c := &mocks.Client{}
	var got client.StartWorkflowOptions
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, loom.RunWorkflowName, mock.Anything).
		Run(func(args mock.Arguments) { got = args.Get(1).(client.StartWorkflowOptions) }).
		Return(&mocks.WorkflowRun{}, nil)
	src := `(echo "a")`
	in := loom.RunInput{Program: src, Plan: must(loom.Compile(ctx, draft.DefaultRegistry(), src)), Ref: "refs/loom/what-if"}
	if _, err := loom.Submit(ctx, c, "loom", in); err != nil {
		t.Fatal(err)
	}
	if got.Memo[loom.MemoRef] != "refs/loom/what-if" || got.ID != must(loom.WorkflowID(ctx, in)) {
		t.Errorf("start options = %+v", got)
	}
}

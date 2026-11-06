package loom

import (
	"context"
	"time"

	"github.com/heedle-ai/shuttle/plan"

	"github.com/heedle-ai/warp"
)

// CheckUnion exposes the merge's postcondition to tests.
var CheckUnion = checkUnion

// RerunOptions exposes the options re-runs start with.
var RerunOptions = rerunOptions

// ReadIndex exposes a record's beliefs index: question -> run ID.
func ReadIndex(ctx context.Context, s History, commit warp.ID) (map[string]string, error) {
	idx, err := readIndex(ctx, s, commit)
	return idx.runs, err
}

// SetHashBlob replaces how Counted hashes an output, until the test ends.
func SetHashBlob(f func(context.Context, []byte) (warp.ID, error)) func() {
	old := hashBlob
	hashBlob = f
	return func() { hashBlob = old }
}

// SetNoteRecorded replaces how Record notes a record, until the test ends.
func SetNoteRecorded(f func(context.Context, *warp.Store, string, string, string, time.Time, plan.Plan, map[string]string) error) func() {
	old := noteRecorded
	noteRecorded = f
	return func() { noteRecorded = old }
}

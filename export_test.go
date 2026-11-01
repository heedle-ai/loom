package loom

import (
	"context"

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

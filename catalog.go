package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/treadle"
	"github.com/heedle-ai/warp"
)

// The skills catalog: what a repository of Agent Skills holds, listed on
// a worker (which fetches it, as it does for an import), so drawdown can
// browse a repository and import the skills it chooses. Listing changes
// nothing.
const (
	CatalogWorkflowName = "LoomSkillsCatalog"
	CatalogActivityName = "LoomSkillsCatalogList"
	// MaxCatalogEntries and MaxCatalogBytes (of JSON) bound a listing, well
	// under Temporal's 2 MiB payload limit; the rest are counted, not sent
	// (review 24).
	MaxCatalogEntries = 500
	MaxCatalogBytes   = 1 << 20
	// CatalogTimeout bounds listing one repository, retries included.
	CatalogTimeout = treadle.FetchTimeout + 2*time.Minute
)

// CatalogRequest names a repository (Source.Repo, and Source.Ref for a
// branch or tag); a folder in it (Source.Path) is refused.
type CatalogRequest struct {
	Source treadle.Source `json:"source"`
}

// Catalog is a repository's skills, at most MaxCatalogEntries of them in
// at most MaxCatalogBytes of JSON; Omitted counts the rest.
type Catalog struct {
	Entries []treadle.CatalogEntry `json:"entries"`
	Omitted int                    `json:"omitted,omitempty"`
}

// CatalogWorkflow lists a repository's skills on a worker. A listing
// that fails to fetch is tried again once; a refusal (SkillsRefused) is
// final.
func CatalogWorkflow(ctx workflow.Context, in CatalogRequest) (Catalog, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    treadle.FetchTimeout + time.Minute,
		ScheduleToCloseTimeout: CatalogTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: SkillsImportAttempts, NonRetryableErrorTypes: []string{SkillsRefused}},
	})
	var out Catalog
	err := workflow.ExecuteActivity(ctx, CatalogActivityName, in).Get(ctx, &out)
	return out, err
}

// ListCatalog is the activity behind CatalogWorkflow.
func (s Skills) ListCatalog(ctx context.Context, in CatalogRequest) (Catalog, error) {
	if in.Source.Path != "" {
		return Catalog{}, nonRetryable(fmt.Errorf("%w: a catalog lists a whole repository: give the repository's link, not a folder's", treadle.ErrInvalid), SkillsRefused)
	}
	entries, err := treadle.Catalog(ctx, in.Source.Repo, in.Source.Ref)
	if errors.Is(err, warp.ErrInvalid) || errors.Is(err, treadle.ErrInvalid) {
		return Catalog{}, nonRetryable(err, SkillsRefused)
	}
	if err != nil {
		return Catalog{}, err
	}
	out := Catalog{Entries: []treadle.CatalogEntry{}}
	size := 0
	for i, e := range entries {
		e.Problem = refusal(e.Problem) // the reason, without the error's wrapping
		b, err := json.Marshal(e)
		if err != nil {
			return Catalog{}, err
		}
		if i >= MaxCatalogEntries || size+len(b)+1 > MaxCatalogBytes {
			out.Omitted = len(entries) - i
			break
		}
		size += len(b) + 1
		out.Entries = append(out.Entries, e)
	}
	return out, nil
}

// RegisterCatalog registers CatalogWorkflow and its activity.
func RegisterCatalog(w plan.Registry, s Skills) {
	w.RegisterWorkflowWithOptions(CatalogWorkflow, workflow.RegisterOptions{Name: CatalogWorkflowName})
	w.RegisterActivityWithOptions(s.ListCatalog, activity.RegisterOptions{Name: CatalogActivityName})
}

// ListSkills runs CatalogWorkflow and waits for it. A refusal's error is
// treadle.ErrInvalid with the reason.
func ListSkills(ctx context.Context, c client.Client, taskQueue string, src treadle.Source) (Catalog, error) {
	run, err := submitJSONWithin(ctx, c, taskQueue, "loom-catalog-", CatalogWorkflowName, CatalogRequest{Source: src}, CatalogTimeout+time.Minute)
	if err != nil {
		return Catalog{}, err
	}
	var out Catalog
	err = run.Get(ctx, &out)
	var app *temporal.ApplicationError
	if errors.As(err, &app) && app.Type() == SkillsRefused {
		return Catalog{}, fmt.Errorf("%w: %s", treadle.ErrInvalid, refusal(app.Message()))
	}
	return out, err
}

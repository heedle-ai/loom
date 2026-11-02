package loom

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/treadle"
	"github.com/heedle-ai/warp"
)

// Changing a store's skills from somewhere other than the worker (such as
// drawdown, which never writes) is a workflow: SkillsWorkflow runs one
// SkillsChangeName activity on a worker, which changes the worker's own
// library. It is not a draft command, so no program, and no agent, can
// add or approve a skill.
const (
	SkillsWorkflowName = "LoomSkills"
	SkillsChangeName   = "LoomSkillsChange"
	// SkillsRefused is the error type of a change the library refused:
	// an invalid skill or source, or nothing waiting for review.
	SkillsRefused = "SkillsRefused"
	// MaxSkillsChangeBytes bounds the files a SkillsAdd carries, well
	// under Temporal's limit on a workflow's input (2 MiB by default).
	// Bigger folders are added with loom skills -add, on the worker.
	MaxSkillsChangeBytes = 1 << 20
	// SkillsTimeout bounds a whole change, from being asked for to done:
	// a change no worker picks up in time is dropped, never made later,
	// when the one who asked has been told it failed (review 18).
	SkillsTimeout = treadle.FetchTimeout + 2*time.Minute
	// SkillsImportAttempts is how often an import is tried; a decision
	// or an add is tried once.
	SkillsImportAttempts = 2
)

// The changes SkillsWorkflow makes, one per SkillsChange.Op.
const (
	SkillsImport  = "import"  // Source: Library.Import (trust rules apply)
	SkillsAdd     = "add"     // Files: Library.Add, a folder someone gave by hand
	SkillsApprove = "approve" // Name, Version, Replaces: Library.Approve
	SkillsReject  = "reject"  // Name, Version: Library.Reject
	// SkillsCreate has Claude write a skill, and its test, from
	// Description, in an agent step on the worker (treadle.Create); it
	// waits for review like an untrusted import.
	SkillsCreate = "create"
)

// SkillsCreateTimeout bounds creating a skill, as long as an agent step.
const SkillsCreateTimeout = 22 * time.Minute

// SkillsCreateHeartbeat is how long creating a skill may go without a
// heartbeat: an agent step's (pirn.AgentOptions.Heartbeat).
const SkillsCreateHeartbeat = 5 * time.Minute

// ChangeTimeout is how long a change may take, from being asked for to
// done: SkillsCreateTimeout to create a skill, SkillsTimeout for the rest.
func ChangeTimeout(op string) time.Duration {
	if op == SkillsCreate {
		return SkillsCreateTimeout
	}
	return SkillsTimeout
}

// SkillsChange is one change to a library.
type SkillsChange struct {
	Op       string            `json:"op"`
	Source   *treadle.Source   `json:"source,omitempty"`
	Files    map[string][]byte `json:"files,omitempty"`
	Name     string            `json:"name,omitempty"`
	Version  warp.ID           `json:"version,omitempty"`
	Replaces warp.ID           `json:"replaces,omitempty"`
	// Description is what to create a skill from (SkillsCreate).
	Description string `json:"description,omitempty"`
}

// size is the number of bytes c's files hold.
func (c SkillsChange) size() int {
	n := 0
	for p, b := range c.Files {
		n += len(p) + len(b)
	}
	return n
}

// SkillsWorkflow makes one change to the worker's library and returns the
// skill it concerns (for a rejection, just its name and version). Only an
// import is retried, briefly: fetching is idempotent, and a decision
// isn't, so a decision that fails is reported, never made twice.
func SkillsWorkflow(ctx workflow.Context, in SkillsChange) (treadle.Skill, error) {
	attempts := int32(1)
	if in.Op == SkillsImport {
		attempts = SkillsImportAttempts
	}
	start := treadle.FetchTimeout + time.Minute
	if in.Op == SkillsCreate {
		start = SkillsCreateTimeout - time.Minute
	}
	var heartbeat time.Duration
	if in.Op == SkillsCreate {
		// Creating is an agent step: it heartbeats as one, so a worker
		// that dies is noticed in minutes, not at the timeout (review 20).
		heartbeat = SkillsCreateHeartbeat
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: start,
		HeartbeatTimeout:    heartbeat,
		// All attempts end within the workflow's own timeout, so nothing
		// is still trying when the one who asked has been told it failed.
		ScheduleToCloseTimeout: ChangeTimeout(in.Op),
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: attempts, NonRetryableErrorTypes: []string{SkillsRefused}},
	})
	var s treadle.Skill
	err := workflow.ExecuteActivity(ctx, SkillsChangeName, in).Get(ctx, &s)
	return s, err
}

// Skills changes a library: it is the activity behind SkillsWorkflow.
type Skills struct {
	Library *treadle.Library
	// Create, if set, writes a skill folder from a description (with an
	// agent step: treadle.Create); without it, creating is refused.
	Create func(ctx context.Context, description string) (map[string][]byte, error)
}

// Change makes one change. A refusal (treadle.ErrInvalid, or
// warp.ErrNotFound for nothing waiting) is a non-retryable SkillsRefused
// error whose message says why.
func (s Skills) Change(ctx context.Context, in SkillsChange) (treadle.Skill, error) {
	sk, err := s.change(ctx, in)
	if errors.Is(err, warp.ErrInvalid) || errors.Is(err, warp.ErrNotFound) {
		return treadle.Skill{}, nonRetryable(err, SkillsRefused)
	}
	return sk, err
}

func (s Skills) change(ctx context.Context, in SkillsChange) (treadle.Skill, error) {
	at := time.Now()
	if in.size() > MaxSkillsChangeBytes {
		return treadle.Skill{}, fmt.Errorf("%w: a change carries at most %d bytes of files; add bigger folders with loom skills -add", treadle.ErrInvalid, MaxSkillsChangeBytes)
	}
	switch in.Op {
	case SkillsImport:
		if in.Source == nil {
			return treadle.Skill{}, fmt.Errorf("%w: an import needs a source", treadle.ErrInvalid)
		}
		return s.Library.Import(ctx, *in.Source, at)
	case SkillsAdd:
		return s.Library.Add(ctx, in.Files, at)
	case SkillsApprove:
		return s.Library.Approve(ctx, in.Name, in.Version, in.Replaces, at)
	case SkillsCreate:
		if s.Create == nil {
			return treadle.Skill{}, fmt.Errorf("%w: this worker can't create skills: it has no agent image", treadle.ErrInvalid)
		}
		files, err := s.Create(ctx, in.Description)
		if err != nil {
			return treadle.Skill{}, err
		}
		return s.Library.Propose(ctx, files, in.Description, at)
	case SkillsReject:
		if err := s.Library.Reject(ctx, in.Name, in.Version, at); err != nil {
			return treadle.Skill{}, err
		}
		return treadle.Skill{Name: in.Name, Version: in.Version}, nil
	}
	return treadle.Skill{}, fmt.Errorf("%w: unknown change %q", treadle.ErrInvalid, in.Op)
}

// RegisterSkills registers SkillsWorkflow and its activity, changing lib.
func RegisterSkills(w plan.Registry, s Skills) {
	w.RegisterWorkflowWithOptions(SkillsWorkflow, workflow.RegisterOptions{Name: SkillsWorkflowName})
	w.RegisterActivityWithOptions(s.Change, activity.RegisterOptions{Name: SkillsChangeName})
}

// refusal is a library's refusal without the error wrapping it came in.
func refusal(msg string) string {
	for _, p := range []string{"treadle: ", "warp: invalid argument: ", "warp: not found: "} {
		msg = strings.TrimPrefix(msg, p)
	}
	return msg
}

// ChangeSkills runs SkillsWorkflow and waits for it. An identical change
// in progress is joined. A refusal's error is the library's message.
func ChangeSkills(ctx context.Context, c client.Client, taskQueue string, in SkillsChange) (treadle.Skill, error) {
	if in.size() > MaxSkillsChangeBytes {
		return treadle.Skill{}, fmt.Errorf("%w: a change carries at most %d bytes of files; add bigger folders with loom skills -add", treadle.ErrInvalid, MaxSkillsChangeBytes)
	}
	run, err := submitJSONWithin(ctx, c, taskQueue, "loom-skills-", SkillsWorkflowName, in, ChangeTimeout(in.Op))
	if err != nil {
		return treadle.Skill{}, err
	}
	var s treadle.Skill
	err = run.Get(ctx, &s)
	var app *temporal.ApplicationError
	if errors.As(err, &app) && app.Type() == SkillsRefused {
		return treadle.Skill{}, fmt.Errorf("%w: %s", treadle.ErrInvalid, refusal(app.Message()))
	}
	return s, err
}

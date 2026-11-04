package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/treadle"
	"github.com/heedle-ai/warp"
)

// The front door: ask for something in plain words, and Claude proposes
// how heedle would do it: a summary, a program that compiles from the
// commands and skills the worker has, and, if the request says when, a
// schedule. If nothing the worker has can do it, the proposal says which
// skill would, for you to create. Claude answers in a fixed JSON shape,
// which loom checks and compiles, sending what's wrong back up to
// MaxAskAttempts times. Nothing runs: running or scheduling a proposal is
// an ordinary submit or schedule.
const (
	AskWorkflowName = "LoomAsk"
	AskActivityName = "LoomAskDraft"
	// MaxAskAttempts bounds how often Claude is asked for one proposal.
	MaxAskAttempts = 3
	// MaxRequest bounds a request; MaxTurns how many earlier turns of the
	// conversation are sent with it, newest kept.
	MaxRequest = 4 << 10
	MaxTurns   = 10
	// MaxSummary bounds a proposal's summary, MaxSkillIdea its skill.
	MaxSummary   = 1000
	MaxSkillIdea = 2000
	// MaxTurnProgram bounds the program of an earlier turn.
	MaxTurnProgram = 64 << 10
	// AskTimeout bounds one proposal, all attempts together.
	AskTimeout = 5 * time.Minute
)

// Turn is an earlier exchange in the conversation: what was asked, and
// what was proposed.
type Turn struct {
	Request string `json:"request"`
	Summary string `json:"summary,omitempty"`
	Program string `json:"program,omitempty"`
}

// AskRequest is a request, with the conversation before it, and the day
// and time zone of whoever asks (for "next Friday" and "every Monday at
// 8").
type AskRequest struct {
	Request  string `json:"request"`
	Earlier  []Turn `json:"earlier,omitempty"`
	Today    string `json:"today,omitempty"`    // YYYY-MM-DD; today in UTC if empty
	TimeZone string `json:"timeZone,omitempty"` // an IANA name; UTC if empty
}

// Proposal is what heedle would do for a request: a program (compiled,
// with its plan) and, if asked for, a schedule with a name; or, when
// nothing the worker has can do it, Skill: a skill to create.
type Proposal struct {
	Summary  string        `json:"summary"`
	Program  string        `json:"program,omitempty"`
	Plan     *plan.Plan    `json:"plan,omitempty"`
	Schedule *ScheduleSpec `json:"schedule,omitempty"`
	Name     string        `json:"name,omitempty"` // the schedule's name
	Skill    string        `json:"skill,omitempty"`
	Attempts int           `json:"attempts"`
	// Ready is whether the worker can run the program now, as it was when
	// proposed (CheckReady); nil without a program, or a worker that
	// doesn't say.
	Ready *Readiness `json:"ready,omitempty"`
}

// Readiness is whether a plan can run now: each connector its steps ask
// for (pirn's Ready), and the Anthropic key if any step asks Claude
// (Claude: "", "key" or "unreadable", with why in ClaudeDetail). Error
// says why it couldn't be told, such as a connectors file that fails its
// check. Ready is all of them fine.
type Readiness struct {
	Needs        []pirn.Need `json:"needs"`
	Claude       string      `json:"claude,omitempty"`
	ClaudeDetail string      `json:"claudeDetail,omitempty"`
	Error        string      `json:"error,omitempty"`
	Ready        bool        `json:"ready"`
}

// CheckReady is whether p can run now on a worker with lib, cs and keys
// from the environment, then v: as its steps would decide when they start.
func CheckReady(ctx context.Context, p plan.Plan, lib SkillFiles, cs pirn.Connectors, v pirn.Vault) (Readiness, error) {
	g, claude, err := Needs(ctx, p, lib)
	if err != nil {
		return Readiness{}, err
	}
	r := Readiness{Needs: cs.Ready(ctx, g, v)}
	if claude {
		r.Claude, r.ClaudeDetail = pirn.KeyReady(ctx, v, "ANTHROPIC_API_KEY")
	}
	r.Ready = r.Claude == ""
	for _, n := range r.Needs {
		r.Ready = r.Ready && n.Problem == ""
	}
	return r, nil
}

// askAnswer is Claude's reply, as it's asked to give it.
type askAnswer struct {
	Summary  string `json:"summary"`
	Program  string `json:"program"`
	Every    string `json:"every"`
	Cron     string `json:"cron"`
	TimeZone string `json:"timeZone"`
	Name     string `json:"name"`
	Skill    string `json:"skill"`
}

// AskFormat is the answer Claude is asked for.
const AskFormat = `Reply with only one JSON object, in a ` + "```json" + ` block, with these fields:
- "summary": one or two plain sentences, to the person, saying what you'll do (or, if nothing listed can, what skill would). No jargon.
- "program": the draft program that does it, using only the listed commands; "" if none of them can.
- "every": how often to run it, as a Go duration such as "24h" or "168h", only if the request asks for it to repeat; else "".
- "cron": instead of "every", five-field cron times such as "0 8 * * 1" (Mondays at 8:00), when the request names times; else "".
- "timeZone": the IANA time zone for "cron" (the person's own, unless they name another); else "".
- "name": when it repeats, a short name for the schedule: lower-case letters, digits and hyphens, such as "lisbon-weekly"; else "".
- "skill": only when "program" is "": what a new skill would do, its arguments, and the connectors it would need, in a few sentences; else "".
Prefer a skill that fits over general commands. Fill a skill's or command's arguments from the request; write dates as YYYY-MM-DD, worked out from today.`

var jsonFence = regexp.MustCompile("(?s)```(?:json)?\\s*\\n(.*?)```")

// parseAnswer reads Claude's reply: the JSON in its fenced block, or all of it.
func parseAnswer(text string) (askAnswer, error) {
	body := strings.TrimSpace(text)
	if m := jsonFence.FindStringSubmatch(text); m != nil {
		body = strings.TrimSpace(m[1])
	}
	var a askAnswer
	dec := json.NewDecoder(strings.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&a); err != nil {
		return a, fmt.Errorf("it isn't the JSON object asked for: %v", err)
	}
	return a, nil
}

// propose checks a against reg: a summary; exactly one of a program that
// compiles and a skill to create; and a schedule only with a program,
// given one way, sensibly, with a name.
func propose(ctx context.Context, reg *registry.Registry, a askAnswer) (Proposal, error) {
	p := Proposal{Summary: strings.TrimSpace(a.Summary), Program: strings.TrimSpace(a.Program), Skill: strings.TrimSpace(a.Skill)}
	switch {
	case p.Summary == "" || len(p.Summary) > MaxSummary:
		return Proposal{}, fmt.Errorf("the summary is 1 to %d bytes", MaxSummary)
	case (p.Program == "") == (p.Skill == ""):
		return Proposal{}, errors.New(`give exactly one of "program" and "skill"`)
	case len(p.Skill) > MaxSkillIdea:
		return Proposal{}, fmt.Errorf("the skill is at most %d bytes", MaxSkillIdea)
	}
	repeats := a.Every != "" || a.Cron != ""
	if p.Skill != "" {
		if repeats || a.Name != "" {
			return Proposal{}, errors.New("a schedule goes with a program, not a skill to create")
		}
		return p, nil
	}
	pl, err := Compile(ctx, reg, p.Program)
	if err != nil {
		return Proposal{}, fmt.Errorf("the program doesn't compile: %v", err)
	}
	p.Plan, p.Program = &pl, p.Program+"\n"
	if !repeats {
		if a.Name != "" || a.TimeZone != "" {
			return Proposal{}, errors.New(`"name" and "timeZone" go with "every" or "cron"`)
		}
		return p, nil
	}
	spec := ScheduleSpec{Cron: a.Cron, TimeZone: a.TimeZone}
	if a.Every != "" {
		if spec.Every, err = time.ParseDuration(a.Every); err != nil {
			return Proposal{}, fmt.Errorf(`"every" %q isn't a duration such as "168h"`, a.Every)
		}
	}
	if err := spec.Check(); err != nil {
		return Proposal{}, err
	}
	if !scheduleName.MatchString(a.Name) {
		return Proposal{}, fmt.Errorf(`"name" %q: lower-case letters, digits and hyphens, at most 40`, a.Name)
	}
	p.Schedule, p.Name = &spec, a.Name
	return p, nil
}

var isoDay = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// checkAsk refuses a request heedle can't take: empty or too long, more
// than MaxTurns earlier turns, or a day or time zone that isn't one.
func checkAsk(in AskRequest) error {
	if r := strings.TrimSpace(in.Request); r == "" || len(in.Request) > MaxRequest {
		return fmt.Errorf("%w: a request is 1 to %d bytes", pirn.ErrInvalid, MaxRequest)
	}
	if len(in.Earlier) > MaxTurns {
		return fmt.Errorf("%w: at most %d earlier turns", pirn.ErrInvalid, MaxTurns)
	}
	for _, t := range in.Earlier {
		if len(t.Request) > MaxRequest || len(t.Summary) > MaxSummary || len(t.Program) > MaxTurnProgram {
			return fmt.Errorf("%w: an earlier turn is too long", pirn.ErrInvalid)
		}
	}
	if in.Today != "" {
		if _, err := time.Parse("2006-01-02", in.Today); err != nil || !isoDay.MatchString(in.Today) {
			return fmt.Errorf("%w: today %q isn't YYYY-MM-DD", pirn.ErrInvalid, in.Today)
		}
	}
	if in.TimeZone != "" {
		if _, err := time.LoadLocation(in.TimeZone); err != nil || in.TimeZone == "Local" || strings.ContainsAny(in.TimeZone, " \t") {
			return fmt.Errorf("%w: time zone %q isn't an IANA name", pirn.ErrInvalid, in.TimeZone)
		}
	}
	return nil
}

// Ask proposes what heedle would do for in. Each attempt asks Claude with
// the language, the commands, the conversation so far and, after a
// failure, what it answered and what was wrong, as sources.
func (w Writer) Ask(ctx context.Context, in AskRequest) (Proposal, error) {
	if err := checkAsk(in); err != nil {
		return Proposal{}, err
	}
	today, zone := in.Today, in.TimeZone
	if today == "" {
		today = time.Now().UTC().Format("2006-01-02")
	}
	if zone == "" {
		zone = "UTC"
	}
	reg, cmds, err := w.Commands(ctx)
	if err != nil {
		return Proposal{}, err
	}
	sources := []pirn.Source{{ID: "language", Text: Language}, {ID: "commands", Text: listing(cmds)}}
	if len(in.Earlier) > 0 {
		var conv strings.Builder
		for i, t := range in.Earlier {
			fmt.Fprintf(&conv, "Turn %d. Asked: %s\nProposed: %s\n%s\n", i+1, t.Request, t.Summary, t.Program)
		}
		sources = append(sources, pirn.Source{ID: "conversation", Text: conv.String()})
	}
	base := len(sources)
	instruction := "You are heedle's front door: a person asks for something, and you propose how heedle will do it with the listed commands and skills" +
		" (a conversation so far, if any, is a source: this request may refine it).\n\nToday is " + today + ", in the time zone " + zone +
		".\n\nThe request:\n\n" + strings.TrimSpace(in.Request) + "\n\n" + AskFormat
	var last string
	for attempt := 1; attempt <= MaxAskAttempts; attempt++ {
		a, err := w.Asker.Ask(ctx, pirn.Question{Instruction: instruction, Sources: sources})
		if err != nil {
			return Proposal{}, err
		}
		ans, err := parseAnswer(a.Text)
		if err == nil {
			var p Proposal
			if p, err = propose(ctx, reg, ans); err == nil {
				p.Attempts = attempt
				if p.Plan != nil && w.Ready != nil {
					r, err := w.Ready(ctx, *p.Plan)
					if err != nil {
						r = Readiness{Error: err.Error()}
					}
					p.Ready = &r
				}
				return p, nil
			}
		}
		last = fmt.Sprintf("%s\n\nThat askAnswer can't be used: %v", strings.TrimSpace(a.Text), err)
		sources = append(sources[:base:base], pirn.Source{ID: "previous-attempt", Text: last})
	}
	return Proposal{}, fmt.Errorf("%w after %d attempts; the last:\n%s", ErrNoProgram, MaxAskAttempts, last)
}

// listing is the commands as Claude is shown them.
func listing(cmds []Command) string {
	var b strings.Builder
	for _, c := range cmds {
		fmt.Fprintf(&b, "%s\n  %s\n", c.Args, c.About)
	}
	return b.String()
}

// AskWorkflow proposes on a worker (whose Claude key, commands and skills
// it uses) and returns the proposal. It runs nothing else.
func AskWorkflow(ctx workflow.Context, in AskRequest) (Proposal, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    AskTimeout,
		ScheduleToCloseTimeout: AskTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	var out Proposal
	err := workflow.ExecuteActivity(ctx, AskActivityName, in).Get(ctx, &out)
	return out, err
}

// AskActivity is the activity behind AskWorkflow.
func (w Writer) AskActivity(ctx context.Context, in AskRequest) (Proposal, error) {
	out, err := w.Ask(ctx, in)
	switch {
	case errors.Is(err, pirn.ErrInvalid):
		return out, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidInput", nil)
	case errors.Is(err, ErrNoProgram):
		return out, temporal.NewNonRetryableApplicationError(err.Error(), "NoProgram", nil)
	}
	return out, err
}

// RegisterAsk registers AskWorkflow and its activity.
func RegisterAsk(r plan.Registry, w Writer) {
	r.RegisterWorkflowWithOptions(AskWorkflow, workflow.RegisterOptions{Name: AskWorkflowName})
	r.RegisterActivityWithOptions(w.AskActivity, activity.RegisterOptions{Name: AskActivityName})
}

// Ask runs AskWorkflow and waits for it. A request it can't propose for
// is an error saying why.
func Ask(ctx context.Context, c client.Client, taskQueue string, in AskRequest) (Proposal, error) {
	run, err := submitJSONWithin(ctx, c, taskQueue, "loom-ask-", AskWorkflowName, in, AskTimeout+time.Minute)
	if err != nil {
		return Proposal{}, err
	}
	var out Proposal
	err = run.Get(ctx, &out)
	var app *temporal.ApplicationError
	if errors.As(err, &app) && (app.Type() == "InvalidInput" || app.Type() == "NoProgram") {
		return Proposal{}, fmt.Errorf("%w: %s", pirn.ErrInvalid, app.Message())
	}
	return out, err
}

// SkillFiles is where a skill's files are found by version: a
// *treadle.Library.
type SkillFiles interface {
	Files(ctx context.Context, version warp.ID) (map[string][]byte, error)
}

// Needs is what p's steps need: the connectors they ask for, all
// together (each skill's metadata.connectors, at the version p pins, and
// each agent step's grants), and whether any asks Claude.
func Needs(ctx context.Context, p plan.Plan, lib SkillFiles) (pirn.Grant, bool, error) {
	g := pirn.Grant{}
	claude := false
	add := func(h pirn.Grant) {
		for conn, t := range h {
			old, ok := g[conn]
			switch {
			case !ok:
				g[conn] = t
			case old.All || t.All:
				g[conn] = pirn.Tools{All: true}
			default:
				for n := range t.Names {
					old.Names[n] = true
				}
			}
		}
	}
	for _, n := range p.Nodes {
		switch n.Activity {
		case pirn.ActivityName:
			claude = true
		case pirn.AgentActivityName:
			claude = true
			if len(n.Args) > 1 {
				h, err := pirn.ParseGrant(n.Args[1:])
				if err != nil {
					return nil, false, err
				}
				add(h)
			}
		case treadle.ActivityName:
			claude = true
			if lib == nil || len(n.Args) < 2 {
				return nil, false, fmt.Errorf("loom: skill step %s: no library or no pinned version", n.ID)
			}
			files, err := lib.Files(ctx, warp.ID(n.Args[1]))
			if err != nil {
				return nil, false, err
			}
			h, err := treadle.Connectors(files[treadle.SkillFile])
			if err != nil {
				return nil, false, err
			}
			add(h)
		}
	}
	return g, claude, nil
}

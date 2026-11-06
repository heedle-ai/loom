package loom

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/shuttle/plan"
)

// The program writer: describe a task in words, and Claude writes the
// draft program for it from the commands the worker has. The program is
// compiled, so it is shown with its plan before anything runs; running it
// is a separate, ordinary submit. A program that doesn't compile goes
// back to Claude with the compiler's error, up to MaxWriteAttempts times.
const (
	WriteProgramWorkflowName = "LoomWriteProgram"
	WriteProgramActivityName = "LoomWriteProgramDraft"
	// MaxWriteAttempts bounds how often Claude is asked for one program.
	MaxWriteAttempts = 3
	// MaxDescription bounds a task's description.
	MaxDescription = 4 << 10
	// WriteTimeout bounds writing one program, all attempts together.
	WriteTimeout = 5 * time.Minute
)

// Language is how draft programs are written, as Claude is told it.
const Language = `A draft program is one s-expression.
- (cmd "arg" ...) runs one command with literal string arguments.
- (pipe a b c) runs a, then b, then c: each stage gets the previous stage's output as its input.
- (par a b c) runs a, b and c side by side on the same input; a stage after a par gets all their outputs.
- Strings are double-quoted, with \n and \" escapes. Comments start with ;.
- Only the commands listed are available, each with the arguments its entry says.
For example, three sources side by side, then an assessment of all three:
(pipe
  (par (source "weather") (source "carrier") (source "factory"))
  (assess "the shipment will be late"))`

// Command is one command a program may use, as the writer describes it.
type Command struct {
	Name  string
	Args  string // how to call it, such as (browse "HOST" ...)
	About string
}

// BuiltinCommands describes the commands heedle's workers have, besides
// skills, which describe themselves.
var BuiltinCommands = map[string]Command{
	"echo":      {Args: `(echo "text" ...)`, About: "Outputs its arguments, then its inputs, joined with \" | \". Good for passing fixed text along."},
	"browse":    {Args: `(browse "https://example.org/page" ...)`, About: "Opens each page in a fresh browser that may reach only those hosts, and outputs its URL, title and text."},
	"act":       {Args: `(act "instruction" "https://example.org/form" ["host" ...])`, About: "Claude fills in a form in a fresh browser that may reach only the start page's host and the hosts listed, and stops before each send to ask the person, recording every action with a screenshot; its output is what it did and its final reply. It can't sign in, upload or download by itself: if the form needs a sign-in, a code or a CAPTCHA, it pauses and asks the person to take the wheel of its browser, so say so in the summary. Never add :send-without-asking (which lets it send without asking) unless the request explicitly says to send without asking. To fill in the person's own details, add :uses with the ones the form needs, from name email phone address birthdate, such as :uses \"name email\"; never write the details themselves in the program. A form it went through once is replayed next time without Claude (from the place the page differs, Claude carries on); add :replay \"off\" only if the request says to have Claude do it afresh every time. To start signed in as the person on a site where they kept their sign-in, add :signed-in with that site's host, such as :signed-in \"forms.example.org\", only if the request is to act as them there; never with :send-without-asking."},
	"ask":       {Args: `(ask "instruction")`, About: "Claude answers the instruction using only the step's inputs. No tools."},
	"agent":     {Args: `(agent "instruction" ["connector" or "connector/tool" ...])`, About: "Claude works in a fresh container with a shell: inputs are files in /work/in, results go in /work/out. Extra arguments grant it connectors the worker offers."},
	"container": {Args: `(container "IMAGE-ID" "arg" ...)`, About: "Runs a pinned container image with no network; its output is the container's standard output."},
	"image":     {Args: `(image "CONTAINERFILE" ["push"])`, About: "Builds an image from a Containerfile written in the program, with earlier outputs as in/<node>.txt."},
	"source":    {Args: `(source "name")`, About: "The hello world's evidence: reads sources/NAME.txt and outputs \"NAME: delay|on-time: summary\"."},
	"assess":    {Args: `(assess "claim")`, About: "The hello world's verdict: SUPPORTED if any input source signals delay, naming which are for and against."},
}

// Commands lists what reg offers, described from known (by name) and,
// failing that, as a generic command.
func Commands(reg *registry.Registry, known map[string]Command) []Command {
	var out []Command
	for _, name := range reg.Names() {
		c, ok := known[name]
		if !ok {
			c = Command{Args: "(" + name + ` "arg" ...)`, About: "A command this worker has."}
		}
		c.Name = name
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// WriteRequest is a task to write a program for.
type WriteRequest struct {
	Description string `json:"description"`
}

// Written is a program the writer wrote, compiled.
type Written struct {
	Program  string    `json:"program"`
	Plan     plan.Plan `json:"plan"`
	Attempts int       `json:"attempts"`
}

// Writer writes programs with Claude (Asker) from the commands a
// registry has (Commands gives both, as they are when asked).
type Writer struct {
	Asker    pirn.Asker
	Commands func(ctx context.Context) (*registry.Registry, []Command, error)
	// Ready, if set, tells Ask whether a proposal's plan can run now on
	// this worker (CheckReady with its library, connectors and keys).
	Ready func(ctx context.Context, p plan.Plan) (Readiness, error)
	// MemoryFile, if set, is what this worker's front door remembers about
	// the person (ReadMemory), sent to Claude with every request.
	MemoryFile string
}

// ErrNoProgram is a description Claude couldn't write a compiling program
// for, within MaxWriteAttempts.
var ErrNoProgram = errors.New("loom: no program compiled")

var fence = regexp.MustCompile("(?s)```[a-z]*\\n(.*?)```")

// program is the program in an answer: its fenced block, or all of it.
func program(answer string) string {
	if m := fence.FindStringSubmatch(answer); m != nil {
		return strings.TrimSpace(m[1])
	}
	return strings.TrimSpace(answer)
}

// Write writes and compiles a program for the description. Each attempt
// asks Claude with the language, the commands and, after a failure, the
// program it wrote and the compiler's error, as sources.
func (w Writer) Write(ctx context.Context, description string) (Written, error) {
	description = strings.TrimSpace(description)
	if description == "" || len(description) > MaxDescription {
		return Written{}, fmt.Errorf("%w: a description is 1 to %d bytes", pirn.ErrInvalid, MaxDescription)
	}
	reg, cmds, err := w.Commands(ctx)
	if err != nil {
		return Written{}, err
	}
	sources := []pirn.Source{{ID: "language", Text: Language}, {ID: "commands", Text: listing(cmds)}}
	instruction := "Write one draft program that does this task, using only the listed commands:\n\n" + description +
		"\n\nReply with only the program, in a ``` block."
	var last string
	for attempt := 1; attempt <= MaxWriteAttempts; attempt++ {
		q := pirn.Question{Instruction: instruction, Sources: sources}
		a, err := w.Asker.Ask(ctx, q)
		if err != nil {
			return Written{}, err
		}
		src := program(a.Text)
		p, err := Compile(ctx, reg, src)
		if err == nil {
			return Written{Program: src + "\n", Plan: p, Attempts: attempt}, nil
		}
		last = fmt.Sprintf("%s\n\nIt doesn't compile: %v", src, err)
		sources = append(sources[:2:2], pirn.Source{ID: "previous-attempt", Text: last})
	}
	return Written{}, fmt.Errorf("%w after %d attempts; the last:\n%s", ErrNoProgram, MaxWriteAttempts, last)
}

// WriteProgramWorkflow writes one program on a worker (whose Claude key
// and commands it uses) and returns it. It runs nothing else.
func WriteProgramWorkflow(ctx workflow.Context, in WriteRequest) (Written, error) {
	// One activity attempt, ending within WriteTimeout: Claude is asked at
	// most MaxWriteAttempts times, and nothing is still asking when the
	// one who asked has been told it failed (review 20).
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    WriteTimeout,
		ScheduleToCloseTimeout: WriteTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	var out Written
	err := workflow.ExecuteActivity(ctx, WriteProgramActivityName, in).Get(ctx, &out)
	return out, err
}

// WriteActivity is the activity behind WriteProgramWorkflow.
func (w Writer) WriteActivity(ctx context.Context, in WriteRequest) (Written, error) {
	out, err := w.Write(ctx, in.Description)
	switch {
	case errors.Is(err, pirn.ErrInvalid):
		return out, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidInput", nil)
	case errors.Is(err, ErrNoProgram):
		return out, temporal.NewNonRetryableApplicationError(err.Error(), "NoProgram", nil)
	}
	return out, err
}

// RegisterWriter registers WriteProgramWorkflow and its activity.
func RegisterWriter(r plan.Registry, w Writer) {
	r.RegisterWorkflowWithOptions(WriteProgramWorkflow, workflow.RegisterOptions{Name: WriteProgramWorkflowName})
	r.RegisterActivityWithOptions(w.WriteActivity, activity.RegisterOptions{Name: WriteProgramActivityName})
}

// WriteProgram runs WriteProgramWorkflow and waits for it. A description
// it can't write a program for is an error saying why, with the last
// attempt.
func WriteProgram(ctx context.Context, c client.Client, taskQueue string, description string) (Written, error) {
	run, err := submitJSONWithin(ctx, c, taskQueue, "loom-write-", WriteProgramWorkflowName, WriteRequest{Description: description}, WriteTimeout+time.Minute)
	if err != nil {
		return Written{}, err
	}
	var out Written
	err = run.Get(ctx, &out)
	var app *temporal.ApplicationError
	if errors.As(err, &app) && (app.Type() == "InvalidInput" || app.Type() == "NoProgram") {
		return Written{}, fmt.Errorf("%w: %s", pirn.ErrInvalid, app.Message())
	}
	return out, err
}

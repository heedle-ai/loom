// Package cli is the loom command-line tool as a library (Run), so
// heedle can offer the same commands. loom runs draft programs on
// Temporal and records them in warp.
//
//	loom worker  -store DIR                    start a worker
//	loom run     [-ref REF] PROGRAM | -f FILE  compile, run and record a program
//	loom compile PROGRAM | -f FILE             print the compiled plan; runs nothing
//	loom branch  -from REF -to REF             start a new history at REF's head
//	loom merge   -into REF -from REF [-resolve ours|theirs|rerun]
//	                                           merge one history into another
//	loom beliefs -store DIR [-ref REF]         print the latest answer to every question
//
// Programs may use echo, container (a fresh Podman container per step, from
// heedle-ai/bobbin) and browse (a fresh browser per step, from
// heedle-ai/reed). worker, run, branch and merge take -address (default localhost:7233) and
// -queue (default "loom").
package cli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/heedle-ai/bobbin"
	"github.com/heedle-ai/draft"
	draftregistry "github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/reed"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/treadle"
	"github.com/heedle-ai/warp"
)

const usage = `usage:
  loom worker  -store DIR [-address HOST:PORT] [-queue NAME] [-browser-image ID] [-browser-agent-image ID] [-browser-network NAME] [-ask-model MODEL] [-agent-image ID] [-agent-registry REPO] [-image-registry REPO] [-connectors FILE] [-memory FILE]
  loom worker  -store DIR [-address HOST:PORT] [-queue NAME] [-browser-image ID] [-browser-agent-image ID] [-browser-network NAME] [-ask-model MODEL] [-agent-image ID] [-agent-registry REPO] [-image-registry REPO] [-connectors FILE] [-prices FILE]
  loom run     [-address HOST:PORT] [-queue NAME] [-ref REF] [-store DIR] [-allow-act] (PROGRAM | -f FILE)
  loom compile [-store DIR] (PROGRAM | -f FILE)
  loom skills  -store DIR [-add FOLDER | -add - | -import LINK [-path P] [-branch B] | -catalog LINK [-branch B]]
  loom skills  -store DIR -create "DESCRIPTION" -agent-image ID
  loom skills  publish NAME -repo DIR -store DIR
  loom skills  -store DIR (-review NAME | -approve NAME@VERSION [-replacing VERSION] | -reject NAME@VERSION | -trust PREFIX | -untrust PREFIX | -trusted)
  loom branch  [-address HOST:PORT] [-queue NAME] -from REF -to REF
  loom merge   [-address HOST:PORT] [-queue NAME] -into REF -from REF [-resolve ours|theirs|rerun] [-allow-act]
  loom beliefs -store DIR [-ref REF]
  loom writes  [-address HOST:PORT] -workflow ID [-approve NODE/ID | -decline NODE/ID]
  loom forms   [-address HOST:PORT] -workflow ID [-send ID | -cancel ID]
  loom write   [-store DIR] "DESCRIPTION OF A TASK"
  loom ask     [-address HOST:PORT] [-queue NAME] [-ref REF] [-then run|schedule] [-allow-act] "WHAT YOU WANT DONE"
  loom secret  (set NAME | delete NAME | delete --all)
  loom memory  [-memory FILE] [-add TEXT | -forget ID | -forget-all]
  loom schedule  -name NAME (-every DURATION | -cron "M H DOM MON DOW" [-tz ZONE]) [-address HOST:PORT] [-queue NAME] [-ref REF] [-store DIR] [-allow-act] (PROGRAM | -f FILE)
  loom schedules [-address HOST:PORT] [-pause NAME | -resume NAME | -delete NAME | -run NAME]
  loom cancel     [-address HOST:PORT] WORKFLOW [RUN]
  loom containers [-address HOST:PORT] [-store DIR] [-stop WORKFLOW [-run RUN] | -clean]
  loom images     [-keep REF ...] [-prune]
  loom schedule  -name NAME (-every DURATION | -cron "M H DOM MON DOW" [-tz ZONE]) [-budget USD [-resume-next-month]] [-address HOST:PORT] [-queue NAME] [-ref REF] [-store DIR] [-allow-act] (PROGRAM | -f FILE)
  loom schedules [-address HOST:PORT] [-pause NAME | -resume NAME | -delete NAME | -run NAME | -name NAME -budget USD [-resume-next-month]]
  loom costs   -store DIR [-month YYYY-MM] [-tz ZONE] [-prices FILE]
`

// Run runs one loom command: args are the command's name and its
// arguments, as typed after "loom" (or "heedle").
func Run(ctx context.Context, args []string, out io.Writer) error { return run(ctx, args, out) }

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("missing command\n" + usage)
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	address := fs.String("address", client.DefaultHostPort, "Temporal frontend address")
	queue := fs.String("queue", "loom", "Temporal task queue")
	store := fs.String("store", "", "warp store directory (a bare Git repository; created if missing)")
	ref := fs.String("ref", loom.DefaultRef, "warp ref to record runs on")
	file := fs.String("f", "", "read the program from FILE")
	from := fs.String("from", "", "branch, merge: the ref to start from or merge in")
	to := fs.String("to", "", "branch: the new ref")
	into := fs.String("into", "", "merge: the ref to merge into")
	resolve := fs.String("resolve", "", "merge: settle conflicting questions with ours, theirs or rerun")
	browserImage := fs.String("browser-image", "", "worker: reed browser image ID for the browse command")
	browserAgentImage := fs.String("browser-agent-image", "", "worker: reed browser-agent image ID for the act command (Playwright MCP and Chromium)")
	browserNetwork := fs.String("browser-network", "", "worker: Podman network browse and act containers join")
	askModel := fs.String("ask-model", pirn.DefaultModel, "worker: the Claude model ask and agent use (key from ANTHROPIC_API_KEY, or the keychain: loom secret set ANTHROPIC_API_KEY)")
	agentImage := fs.String("agent-image", "", "worker: content-pinned image ID agent steps run in (needs /bin/sh)")
	imageRegistry := fs.String("image-registry", "", "worker: OCI repository (such as ghcr.io/acme/heedle-images) that (image … \"push\") pushes to, with the worker's podman login")
	add := fs.String("add", "", "skills: a skill's folder (with SKILL.md) to add to the store, or - for a SKILL.md on standard input; memory: something for heedle to remember about you")
	importSkill := fs.String("import", "", "skills: a GitHub link to a skill's folder, or an https:// or file:// repository (with -path and -branch), to import")
	catalog := fs.String("catalog", "", "skills: a GitHub repository link, or an https:// or file:// repository, whose skills to list")
	skillPath := fs.String("path", "", "skills: with -import of a repository URL, the skill's folder in it")
	branch := fs.String("branch", "", "skills: with -import or -catalog of a repository URL, the branch or tag (default: its default branch)")
	review := fs.String("review", "", "skills: show the import of a skill waiting for review: its files and its diff from the skill's newest version")
	createSkill := fs.String("create", "", "skills: a description of a skill for Claude to write, with its test, in an agent step (needs ANTHROPIC_API_KEY and -agent-image); it waits for review")
	replacing := fs.String("replacing", "", "skills: with -approve, the skill's newest version the review showed the diff from (none for a new skill)")
	reject := fs.String("reject", "", "skills: NAME@VERSION, the reviewed import to turn down")
	trust := fs.String("trust", "", "skills: a repository URL prefix (such as https://github.com/your-org) whose imports need no review")
	untrust := fs.String("untrust", "", "skills: a trusted prefix to remove; later imports from it wait for review")
	trusted := fs.Bool("trusted", false, "skills: list the trusted prefixes")
	connectorsFile := fs.String("connectors", "", "worker: a JSON file of the MCP servers agent steps may be granted (tokens by name: an environment variable, or a keychain secret set with `loom secret set NAME`)")
	workflowID := fs.String("workflow", "", "writes: the run's workflow ID, as loom run prints it")
	approveWrite := fs.String("approve", "", "skills: NAME@VERSION, the reviewed import to add as the skill's newest version; writes: NODE/ID, a write to approve")
	declineWrite := fs.String("decline", "", "writes: NODE/ID, a write to decline")
	scheduleNameFlag := fs.String("name", "", "schedule: the schedule's name (lower-case letters, digits and hyphens)")
	every := fs.Duration("every", 0, "schedule: run every so often, such as 1h or 24h (at least 1m)")
	cron := fs.String("cron", "", `schedule: run at cron times instead, five fields, such as "0 9 * * 1-5"`)
	tz := fs.String("tz", "", "schedule: the time zone cron times are in, such as Europe/Lisbon (default UTC); costs: the time zone months are in (default UTC)")
	budget := fs.String("budget", "", "schedule, schedules -name: the schedule's monthly budget in USD, such as 5 (0: none); at or over it, estimated, a run starts nothing and pauses the schedule")
	resumeNextMonth := fs.Bool("resume-next-month", false, "schedule, schedules -name with -budget: a new month resumes the schedule if its budget paused it")
	pricesFile := fs.String("prices", defaultPrices(), "worker, costs: your prices file, overriding the built-in prices model by model (USD per million tokens)")
	month := fs.String("month", "", "costs: the month to add up, such as 2026-11 (default: this month)")
	pauseFlag := fs.String("pause", "", "schedules: the schedule to pause")
	resumeFlag := fs.String("resume", "", "schedules: the schedule to resume")
	deleteFlag := fs.String("delete", "", "schedules: the schedule to delete (its runs' records stay)")
	runNow := fs.String("run", "", "schedules: the schedule to run once now; containers -stop: the run ID to stop (default: the one run of WORKFLOW with containers here)")
	then := fs.String("then", "", "ask: run the proposal now (run), or schedule it as proposed (schedule), if the worker can run it")
	sendForm := fs.String("send", "", "forms: the ID of a form waiting to be sent, to send it")
	cancelForm := fs.String("cancel", "", "forms: the ID of a form waiting to be sent, to cancel it (nothing is sent)")
	allowAct := fs.Bool("allow-act", false, "run, schedule, ask -then, merge -resolve rerun: let the program's act steps click and type on the hosts they name, stopping before each send to ask")
	allowFreely := fs.Bool("allow-send-without-asking", false, "with -allow-act: also let act steps whose program says :send-without-asking submit forms without asking (roadmap 9.7)")
	repoDir := fs.String("repo", "", "skills publish: a clone of your repository to publish the skill into, at skills/NAME (it commits; you push)")
	agentRegistry := fs.String("agent-registry", "", "worker: OCI repository (such as ghcr.io/acme/heedle-files) for agent files too big for the record, pushed with oras")
	stopRun := fs.String("stop", "", "containers: the run (its workflow ID, and -run its run ID) to stop, through Temporal")
	clean := fs.Bool("clean", false, "containers: remove heedle's containers whose run has ended (with -store, only that home's)")
	prune := fs.Bool("prune", false, "images: remove heedle's images that aren't kept (-keep) and that no container uses")
	var keep refs
	fs.Var(&keep, "keep", "images: an image to keep (repo@sha256:…), as many times as needed")
	memoryFile := fs.String("memory", "", "worker, memory: the file of what heedle remembers about you, which goes to Claude with every request to the front door (memory: default ~/.heedle/memory.json, heedle up's)")
	forget := fs.String("forget", "", "memory: the ID of a memory to forget, as loom memory lists it")
	forgetAll := fs.Bool("forget-all", false, "memory: forget everything heedle remembers")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	switch args[0] {
	case "secret":
		if fs.NArg() != 2 || (fs.Arg(0) != "set" && fs.Arg(0) != "delete") {
			return errors.New("secret takes set NAME, delete NAME or delete --all\n" + usage)
		}
		if fs.Arg(0) == "delete" && (fs.Arg(1) == "--all" || fs.Arg(1) == "-all") {
			return deleteAllSecrets(ctx, out)
		}
		return secretCommand(ctx, out, fs.Arg(0), fs.Arg(1))

	case "memory":
		set := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
		if set["add"] && strings.TrimSpace(*add) == "" || set["forget"] && *forget == "" {
			return errors.New("memory: -add takes the text to remember, and -forget the ID of a memory (heedle memory lists them)")
		}
		return memoryCommand(out, *memoryFile, *add, *forget, *forgetAll, fs.NArg())

	case "worker":
		if *store == "" {
			return errors.New("worker needs -store DIR")
		}
		s, err := warp.Init(ctx, *store)
		if err != nil {
			return err
		}
		c, err := client.Dial(client.Options{HostPort: *address})
		if err != nil {
			return fmt.Errorf("connect to Temporal at %s: %w (is `temporal server start-dev` running?)", *address, err)
		}
		defer c.Close()
		// Every container a step starts says this store's home (roadmap
		// 9.2.1, LOOM-520).
		w := worker.New(c, *queue, reed.WorkerOptions(worker.Options{BackgroundActivityContext: loom.WorkerContext(*store)}))
		// The key and the connectors are read again for every step, so a key
		// stored or a connector added while the worker runs needs no restart.
		if _, err := pirn.LookupSecret(ctx, vault, "ANTHROPIC_API_KEY"); err != nil {
			// The worker still runs every step that doesn't ask Claude.
			fmt.Fprintf(out, "ANTHROPIC_API_KEY: %v; ask and agent steps will fail until the worker can read it\n", err)
		}
		// Each keeps what it last read if a read fails: a keychain locked
		// for a moment, or a connectors file mid-save (review 27).
		key := pirn.LastGood(func() (string, error) {
			return pirn.LookupSecret(context.Background(), vault, "ANTHROPIC_API_KEY")
		})
		loadConnectors := pirn.LastGood(func() (pirn.Connectors, error) { return readConnectors(*connectorsFile) })
		claude := pirn.Claude{Model: *askModel, KeyFrom: func(context.Context) (string, error) { return key() }}
		connectors, err := loadConnectors()
		if err != nil {
			return err
		}
		if _, err := os.Stat(*connectorsFile); *connectorsFile != "" && err != nil {
			fmt.Fprintf(out, "connectors: %s isn't there yet, so no connectors until it is\n", *connectorsFile)
		}
		agent := pirn.Agent{Claude: claude, Runner: bobbin.Runner{}, Image: *agentImage, Registry: registryFor(*agentRegistry), Connectors: connectors, Vault: vault,
			LoadConnectors: loadConnectors}
		act := pirn.Act{Claude: claude, Browser: pirn.ReedBrowser{Browser: reed.Browser{Image: *browserAgentImage, Network: *browserNetwork}}, Image: *browserAgentImage,
			Confirm: loom.Confirmer(c),  // a person confirms each send (roadmap 9.7)
			Script:  loom.Scripts(c, s)} // the program's last script, replayed without Claude (roadmap 9.9)
		reg, err := workerCommands(w, claude, agent, act, reed.Browser{Image: *browserImage, Network: *browserNetwork}, imageRegistryFor(*imageRegistry))
		if err != nil {
			return err
		}
		// Skills are the store's, at their newest when the worker starts;
		// each run is checked against, and records, the versions it pins.
		lib := &treadle.Library{Store: s}
		skipped, err := treadle.Register(ctx, reg, loom.Labelled(w), agent, lib)
		if err != nil {
			return err
		}
		warnSkipped(skipped)
		loom.RegisterWith(w, &loom.Checker{Registry: reg, RegistryFor: func(ctx context.Context, p plan.Plan) (*draftregistry.Registry, error) {
			return lib.Pinned(ctx, registry(), p)
		}}, &loom.Recorder{Store: s, Uses: lib.Uses, Schedules: loom.TemporalSchedules{Client: c}, PricesFile: *pricesFile})
		if _, err := loom.ReadPrices(*pricesFile); err != nil {
			fmt.Fprintln(out, err)
		}
		// A new month resumes the schedules their budget paused that are
		// to resume (roadmap 10.4).
		go loom.ResumeBudgetsEvery(ctx, c, *queue, loom.ResumeBudgetsLook, func(f string, a ...any) { fmt.Fprintf(out, f+"\n", a...) })
		// Skills change only through a workflow on this worker (drawdown never
		// writes), never through a program.
		loom.RegisterSkills(w, loom.Skills{Library: lib, Create: creator(agent)})
		// The program writer, for drawdown: Claude with this worker's key,
		// and its commands and newest skills.
		// and the front door (ask), which also says whether a proposal can
		// run now, from this worker's skills, connectors and keys.
		writer := loom.Writer{Asker: claude, Commands: func(ctx context.Context) (*draftregistry.Registry, []loom.Command, error) {
			return commandsFor(ctx, *store, registry())
		}, Ready: func(ctx context.Context, p plan.Plan) (loom.Readiness, error) {
			cs, err := loadConnectors()
			if err != nil {
				return loom.Readiness{}, err
			}
			return loom.CheckReady(ctx, p, lib, loom.Worker{Connectors: cs, Keys: agent.Vault, Agents: agent.Image != "", Browsers: *browserImage != "", BrowserAgents: *browserAgentImage != ""})
		}, MemoryFile: *memoryFile}
		loom.RegisterWriter(w, writer)
		loom.RegisterAsk(w, writer)
		fmt.Fprintf(out, "worker on queue %q, recording to %s\n", *queue, s.Dir())
		return w.Run(worker.InterruptCh())

	case "compile":
		src, err := program(fs, *file)
		if err != nil {
			return err
		}
		reg, err := compileRegistry(ctx, *store)
		if err != nil {
			return err
		}
		p, err := loom.Compile(ctx, reg, src)
		if err != nil {
			return err
		}
		b, err := loom.PlanJSON(p)
		if err != nil {
			return err
		}
		_, err = out.Write(b)
		return err

	case "run":
		src, err := program(fs, *file)
		if err != nil {
			return err
		}
		reg, err := compileRegistry(ctx, *store)
		if err != nil {
			return err
		}
		p, err := loom.Compile(ctx, reg, src)
		if err != nil {
			return err
		}
		if err := actAllowed(p, *allowAct, *allowFreely); err != nil {
			return err
		}
		noteUses(out, p)
		c, err := client.Dial(client.Options{HostPort: *address})
		if err != nil {
			return fmt.Errorf("connect to Temporal at %s: %w (is `temporal server start-dev` running?)", *address, err)
		}
		defer c.Close()
		wr, err := loom.Submit(ctx, c, *queue, loom.RunInput{Program: src, Plan: p, Ref: *ref})
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "workflow %s run %s\n", wr.GetID(), wr.GetRunID())
		var res loom.RunResult
		if err := wr.Get(ctx, &res); err != nil {
			return err
		}
		fmt.Fprintf(out, "commit   %s on %s\n", res.Commit, *ref)
		printWrites(out, res.Writes)
		ids := make([]string, 0, len(res.Outputs))
		for id := range res.Outputs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			fmt.Fprintf(out, "%-8s %s\n", id, res.Outputs[id])
		}
		return nil

	case "schedule":
		if *scheduleNameFlag == "" {
			return errors.New("schedule needs -name, and -every or -cron\n" + usage)
		}
		src, err := program(fs, *file)
		if err != nil {
			return err
		}
		reg, err := compileRegistry(ctx, *store)
		if err != nil {
			return err
		}
		p, err := loom.Compile(ctx, reg, src)
		if err != nil {
			return err
		}
		if err := actAllowed(p, *allowAct, *allowFreely); err != nil {
			return err
		}
		noteUses(out, p)
		spec := loom.ScheduleSpec{Every: *every, Cron: *cron, TimeZone: *tz}
		in := loom.RunInput{Program: src, Plan: p, Ref: *ref}
		b, err := budgetFlags(*budget, *resumeNextMonth)
		if err != nil {
			return err
		}
		if _, err := loom.ScheduleOptions(ctx, *queue, *scheduleNameFlag, in, spec); err != nil {
			return err // refused before Temporal is asked
		}
		c, err := client.Dial(client.Options{HostPort: *address})
		if err != nil {
			return fmt.Errorf("connect to Temporal at %s: %w (is `temporal server start-dev` running?)", *address, err)
		}
		defer c.Close()
		if err := loom.CreateScheduleWithBudget(ctx, c, *queue, *scheduleNameFlag, in, spec, b); err != nil {
			return err
		}
		fmt.Fprintf(out, "schedule %s: %s%s\n", *scheduleNameFlag, describeSpec(spec), describeBudget(&b))
		return nil

	case "schedules":
		actions := map[string]string{"pause": *pauseFlag, "resume": *resumeFlag, "delete": *deleteFlag, "run": *runNow}
		var action, name string
		if *budget != "" || *resumeNextMonth {
			action, name = "budget", *scheduleNameFlag
			if name == "" {
				return errors.New("schedules -budget needs -name, the schedule's\n" + usage)
			}
		}
		for a, n := range actions {
			if n != "" {
				if action != "" {
					return errors.New("schedules takes at most one of -pause, -resume, -delete, -run or -budget\n" + usage)
				}
				action, name = a, n
			}
		}
		c, err := client.Dial(client.Options{HostPort: *address})
		if err != nil {
			return fmt.Errorf("connect to Temporal at %s: %w (is `temporal server start-dev` running?)", *address, err)
		}
		defer c.Close()
		switch action {
		case "":
		case "budget":
			b, err := budgetFlags(*budget, *resumeNextMonth)
			if err != nil {
				return err
			}
			if err := loom.SetBudget(ctx, c, name, b); err != nil {
				return err
			}
		default:
			if err := loom.ScheduleAction(ctx, c, name, action); err != nil {
				return err
			}
		}
		ss, err := loom.Schedules(ctx, c, *queue)
		if err != nil {
			return err
		}
		printSchedules(out, ss)
		return nil

	case "branch", "merge":
		if fs.NArg() != 0 {
			return errors.New("unexpected arguments\n" + usage)
		}
		c, err := client.Dial(client.Options{HostPort: *address})
		if err != nil {
			return fmt.Errorf("connect to Temporal at %s: %w (is `temporal server start-dev` running?)", *address, err)
		}
		defer c.Close()
		if args[0] == "branch" {
			wr, err := loom.SubmitBranch(ctx, c, *queue, loom.BranchInput{From: *from, To: *to})
			if err != nil {
				return err
			}
			var head warp.ID
			if err := wr.Get(ctx, &head); err != nil {
				return err
			}
			fmt.Fprintf(out, "%s now at %s (from %s)\n", *to, head, *from)
			return nil
		}
		wr, err := loom.SubmitMerge(ctx, c, *queue, loom.MergeInput{Into: *into, From: *from, Resolve: *resolve, AllowAct: *allowAct, AllowSendFreely: *allowFreely})
		if err != nil {
			return err
		}
		var res loom.MergeResult
		if err := wr.Get(ctx, &res); err != nil {
			return err
		}
		fmt.Fprintf(out, "%s: %s now at %s\n", res.Kind, *into, res.Commit)
		for _, q := range sortedKeys(res.Resolved) {
			fmt.Fprintf(out, "resolved %s: %s", q, res.Resolved[q])
			if c, ok := res.RerunCommits[q]; ok {
				fmt.Fprintf(out, " (new run %s)", c)
			}
			fmt.Fprintln(out)
		}
		return nil

	case "ask":
		if fs.NArg() != 1 || *then != "" && *then != "run" && *then != "schedule" {
			return errors.New("ask takes one request, in quotes, and -then run or -then schedule\n" + usage)
		}
		c, err := client.Dial(client.Options{HostPort: *address})
		if err != nil {
			return fmt.Errorf("connect to Temporal at %s: %w (is heedle up running?)", *address, err)
		}
		defer c.Close()
		p, err := loom.Ask(ctx, c, *queue, loom.AskRequest{Request: fs.Arg(0), Today: time.Now().Format("2006-01-02"), TimeZone: localZone()})
		if err != nil {
			return err
		}
		printProposal(out, p)
		return askThen(ctx, out, c, *queue, *ref, *then, *allowAct, *allowFreely, p)

	case "write":
		if fs.NArg() != 1 {
			return errors.New("write takes one description, in quotes\n" + usage)
		}
		key, err := pirn.LookupSecret(ctx, vault, "ANTHROPIC_API_KEY")
		if err != nil {
			return err
		}
		if key == "" {
			return errors.New("write asks Claude: run `loom secret set ANTHROPIC_API_KEY`, or set ANTHROPIC_API_KEY")
		}
		w := loom.Writer{Asker: pirn.Claude{APIKey: key, Model: *askModel}, Commands: func(ctx context.Context) (*draftregistry.Registry, []loom.Command, error) {
			return commandsFor(ctx, *store, registry())
		}}
		written, err := w.Write(ctx, fs.Arg(0))
		if err != nil {
			return err
		}
		fmt.Fprint(out, written.Program)
		fmt.Fprintf(out, "; %d steps, compiled (attempt %d); run it with: loom run -store DIR -f FILE\n", len(written.Plan.Nodes), written.Attempts)
		return nil

	case "cancel":
		if fs.NArg() < 1 || fs.NArg() > 2 {
			return errors.New("cancel needs a run's workflow ID (and, optionally, its run ID)\n" + usage)
		}
		c, err := client.Dial(client.Options{HostPort: *address})
		if err != nil {
			return fmt.Errorf("connect to Temporal at %s: %w (is `temporal server start-dev` running?)", *address, err)
		}
		defer c.Close()
		return cancelCommand(ctx, out, c, fs.Arg(0), fs.Arg(1))

	case "containers":
		if fs.NArg() != 0 || *stopRun != "" && *clean || *runNow != "" && *stopRun == "" {
			return errors.New("containers takes at most one of -stop WORKFLOW [-run RUN] or -clean\n" + usage)
		}
		// The listing works without Temporal (runs show as unknown);
		// stopping and cleaning need it.
		c, err := client.NewLazyClient(client.Options{HostPort: *address})
		if err != nil {
			return err
		}
		defer c.Close()
		return containersCommand(ctx, out, c, containersArgs{stop: *stopRun, run: *runNow, store: *store, clean: *clean})

	case "images":
		if fs.NArg() != 0 {
			return errors.New("images takes only -keep and -prune\n" + usage)
		}
		return imagesCommand(ctx, out, keep, *prune)

	case "forms":
		if *workflowID == "" || fs.NArg() != 0 || *sendForm != "" && *cancelForm != "" {
			return errors.New("forms needs -workflow ID, and at most one of -send or -cancel\n" + usage)
		}
		c, err := client.Dial(client.Options{HostPort: *address})
		if err != nil {
			return fmt.Errorf("connect to Temporal at %s: %w (is `temporal server start-dev` running?)", *address, err)
		}
		defer c.Close()
		return formsCommand(ctx, out, c, *workflowID, *sendForm, *cancelForm)

	case "writes":
		if *workflowID == "" || fs.NArg() != 0 || *approveWrite != "" && *declineWrite != "" {
			return errors.New("writes needs -workflow ID, and at most one of -approve or -decline\n" + usage)
		}
		c, err := client.Dial(client.Options{HostPort: *address})
		if err != nil {
			return fmt.Errorf("connect to Temporal at %s: %w (is `temporal server start-dev` running?)", *address, err)
		}
		defer c.Close()
		return writesCommand(ctx, out, c, *workflowID, *approveWrite, *declineWrite)

	case "costs":
		if *store == "" || fs.NArg() != 0 {
			return errors.New("costs needs -store DIR\n" + usage)
		}
		return costs(ctx, out, *store, *month, *tz, *pricesFile, time.Now())

	case "beliefs":
		if *store == "" || fs.NArg() != 0 {
			return errors.New("beliefs needs -store DIR\n" + usage)
		}
		s, err := warp.Open(ctx, *store)
		if err != nil {
			return err
		}
		head, ok, err := s.Resolve(ctx, *ref)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("ref %s doesn't exist", *ref)
		}
		bs, err := loom.Beliefs(ctx, s, head)
		if err != nil {
			return err
		}
		for _, b := range bs {
			line, _, _ := strings.Cut(strings.TrimSpace(b.Program), "\n")
			fmt.Fprintf(out, "%.14s  %s\n", b.Question, line)
			for _, id := range sortedKeys(b.Outputs) {
				fmt.Fprintf(out, "  %-6s %s\n", id, strings.ReplaceAll(strings.TrimRight(b.Outputs[id], "\n"), "\n", "\n         "))
			}
		}
		return nil

	case "skills":
		if fs.NArg() > 0 && fs.Arg(0) == "publish" {
			return publishSkill(ctx, out, fs, *store, repoDir)
		}
		set := 0
		for _, f := range []string{*add, *importSkill, *catalog, *review, *approveWrite, *reject, *trust, *untrust, *createSkill} {
			if f != "" {
				set++
			}
		}
		if *trusted {
			set++
		}
		if set > 1 || (*skillPath != "" && *importSkill == "") || (*branch != "" && *importSkill == "" && *catalog == "") || (*replacing != "" && *approveWrite == "") {
			return errors.New("skills takes one of -add, -import, -catalog, -review, -approve, -reject, -trust, -untrust or -trusted; -path only with -import, -branch with -import or -catalog, -replacing only with -approve\n" + usage)
		}
		if *catalog != "" {
			src, err := treadle.SourceFor(*catalog, "", *branch)
			if err != nil {
				return err
			}
			if src.Path != "" {
				return errors.New("-catalog lists a whole repository: give the repository's link, not a folder's")
			}
			entries, err := treadle.Catalog(ctx, src.Repo, src.Ref)
			if err != nil {
				return err
			}
			for _, e := range entries {
				if e.Problem != "" {
					fmt.Fprintf(out, "%-28s can't be imported: %s\n", printable(e.Source.Path), printable(e.Problem))
					continue
				}
				fmt.Fprintf(out, "%-28s %s: %s\n", printable(e.Source.Path), e.Name, printable(e.Description))
			}
			return nil
		}
		if *store == "" || fs.NArg() != 0 {
			return errors.New("skills needs -store DIR\n" + usage)
		}
		s, err := warp.Init(ctx, *store)
		if err != nil {
			return err
		}
		lib := &treadle.Library{Store: s}
		cmd := "loom skills -store " + shellWord(*store)
		if *createSkill != "" {
			key, err := pirn.LookupSecret(ctx, vault, "ANTHROPIC_API_KEY")
			if err != nil {
				return err
			}
			if key == "" || *agentImage == "" {
				return errors.New("-create runs an agent step: store ANTHROPIC_API_KEY (loom secret set ANTHROPIC_API_KEY, or the variable) and give -agent-image")
			}
			agent := pirn.Agent{Claude: pirn.Claude{APIKey: key, Model: *askModel}, Runner: bobbin.Runner{}, Image: *agentImage}
			files, res, err := treadle.Create(ctx, agent.Run, *createSkill)
			if err != nil {
				return err
			}
			sk, err := lib.Propose(ctx, files, *createSkill, time.Now())
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "%s\n%s at %s waits for review: %s -review %s\n", printable(strings.TrimSpace(res.Text)), sk.Name, sk.Version, cmd, sk.Name)
			return nil
		}
		if done, err := reviewCommand(ctx, out, lib, cmd, *review, *approveWrite, *replacing, *reject, *trust, *untrust, *trusted); done || err != nil {
			return err
		}
		if *add != "" || *importSkill != "" {
			var sk treadle.Skill
			switch {
			case *importSkill != "":
				src, err := treadle.SourceFor(*importSkill, *skillPath, *branch)
				if err != nil {
					return err
				}
				if sk, err = lib.Import(ctx, src, time.Now()); err != nil {
					return err
				}
			case *add == "-":
				md, err := io.ReadAll(io.LimitReader(stdin, treadle.MaxSkillMD+1))
				if err != nil {
					return err
				}
				if len(md) > treadle.MaxSkillMD {
					return fmt.Errorf("a pasted SKILL.md is at most %d bytes", treadle.MaxSkillMD)
				}
				if sk, err = lib.Add(ctx, map[string][]byte{treadle.SkillFile: md}, time.Now()); err != nil {
					return err
				}
			default:
				files, err := treadle.ReadFolder(*add)
				if err != nil {
					return err
				}
				if sk, err = lib.Add(ctx, files, time.Now()); err != nil {
					return err
				}
			}
			if sk.Pending {
				fmt.Fprintf(out, "%s at %s waits for review: %s -review %s\n", sk.Name, sk.Version, cmd, sk.Name)
				return nil
			}
			fmt.Fprintf(out, "added %s at %s\n", sk.Name, sk.Version)
			return nil
		}
		skills, bad, err := lib.Skills(ctx)
		if err != nil {
			return err
		}
		warnSkipped(bad)
		for _, sk := range skills {
			fmt.Fprintf(out, "%-20s %.12s  %s\n", sk.Name, sk.Version, printable(sk.Description))
		}
		pending, bad, err := lib.Pending(ctx)
		if err != nil {
			return err
		}
		warnSkipped(bad)
		if len(pending) > 0 {
			fmt.Fprintf(out, "waiting for review (%s -review NAME):\n", cmd)
			for _, sk := range pending {
				fmt.Fprintf(out, "%-20s %.12s  %s\n", sk.Name, sk.Version, printable(sk.Description))
			}
		}
		return nil

	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}

// reviewCommand runs loom skills -review, -approve, -reject, -trust,
// -untrust or -trusted, if one is given, and reports whether it did.
func reviewCommand(ctx context.Context, out io.Writer, lib *treadle.Library, cmd, review, approve, replacing, reject, trust, untrust string, trusted bool) (bool, error) {
	nameAt := func(flag, v string) (string, warp.ID, error) {
		name, version, ok := strings.Cut(v, "@")
		if !ok || name == "" || version == "" {
			return "", "", fmt.Errorf("-%s takes NAME@VERSION, the version -review showed", flag)
		}
		return name, warp.ID(version), nil
	}
	switch {
	case review != "":
		r, err := lib.Review(ctx, review)
		if err != nil {
			return true, err
		}
		p := r.Pending
		fmt.Fprintf(out, "%s at %s, waiting for review\n", p.Name, p.Version)
		if src := p.Source; src != nil {
			fmt.Fprintf(out, "  from %s", printable(src.Repo))
			if src.Path != "" {
				fmt.Fprintf(out, " folder %s", printable(src.Path))
			}
			fmt.Fprintf(out, " at commit %s\n", printable(src.Commit))
		}
		approveCmd := fmt.Sprintf("%s -approve %s@%s", cmd, p.Name, p.Version)
		if r.Replaces != "" {
			fmt.Fprintf(out, "  replaces %s\n", r.Replaces)
			approveCmd += " -replacing " + string(r.Replaces)
		} else {
			fmt.Fprintln(out, "  a new skill")
		}
		fmt.Fprintln(out, "files:")
		for _, f := range r.Files {
			fmt.Fprintf(out, "  %10d  %s\n", f.Size, printable(f.Path))
		}
		fmt.Fprintf(out, "%s, in full:\n%s", treadle.SkillFile, printable(string(r.SkillMD)))
		if !strings.HasSuffix(string(r.SkillMD), "\n") {
			fmt.Fprintln(out)
		}
		fmt.Fprintln(out, "diff:")
		fmt.Fprint(out, printable(string(r.Diff)))
		if len(r.Binary) > 0 {
			fmt.Fprintln(out, "not in the diff, because git takes them for binary; read them yourself before approving:")
			for _, b := range r.Binary {
				fmt.Fprintf(out, "  %s\n", printable(b))
			}
		}
		fmt.Fprintf(out, "approve: %s\nreject:  %s -reject %s@%s\n", approveCmd, cmd, p.Name, p.Version)
	case approve != "":
		name, version, err := nameAt("approve", approve)
		if err != nil {
			return true, err
		}
		sk, err := lib.Approve(ctx, name, version, warp.ID(replacing), time.Now())
		if err != nil {
			return true, err
		}
		fmt.Fprintf(out, "approved %s at %s\n", sk.Name, sk.Version)
	case reject != "":
		name, version, err := nameAt("reject", reject)
		if err != nil {
			return true, err
		}
		if err := lib.Reject(ctx, name, version, time.Now()); err != nil {
			return true, err
		}
		fmt.Fprintf(out, "rejected %s at %s\n", name, version)
	case trust != "":
		if err := lib.Trust(ctx, trust, time.Now()); err != nil {
			return true, err
		}
		fmt.Fprintf(out, "imports from %s need no review\n", trust)
	case untrust != "":
		if err := lib.Untrust(ctx, untrust, time.Now()); err != nil {
			return true, err
		}
		fmt.Fprintf(out, "imports from %s wait for review\n", printable(untrust))
	case trusted:
		list, err := lib.Trusted(ctx)
		if err != nil {
			return true, err
		}
		for _, p := range list {
			fmt.Fprintln(out, printable(p))
		}
	default:
		return false, nil
	}
	return true, nil
}

// shellWord is path as one shell word: as it is if that's safe, else
// single-quoted.
func shellWord(path string) string {
	if path != "" && strings.Trim(path, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._/~-") == "" && !strings.HasPrefix(path, "-") && !strings.HasPrefix(path, "~") {
		return path
	}
	return "'" + strings.ReplaceAll(printable(path), "'", `'\''`) + "'"
}

// printable is text from a skill or its source, safe to print on a
// terminal: control and format characters (escapes, bidirectional
// overrides) are shown as \u escapes, never sent as they are; newlines and
// tabs stay.
func printable(s string) string {
	var b strings.Builder
	for _, r := range strings.ToValidUTF8(s, "\uFFFD") {
		if r == '\n' || r == '\t' || !(unicode.IsControl(r) || unicode.Is(unicode.Cf, r)) {
			b.WriteRune(r)
		} else {
			fmt.Fprintf(&b, "\\u%04x", r)
		}
	}
	return b.String()
}

// registry is the commands programs may use: draft's echo, bobbin's
// container (a step in a fresh Podman container) and reed's browse (a page
// in a fresh browser that may reach only the hosts the program names) and
// pirn's ask (Claude, answering from the step's inputs), agent (Claude
// working in a fresh container) and act (Claude filling in a form in a
// fresh browser; roadmap 9.5).
func registry() *draftregistry.Registry {
	reg := draft.DefaultRegistry()
	reg.MustRegister(bobbin.BuiltinSpec())
	reg.MustRegister(bobbin.ImageBuiltinSpec())
	reg.MustRegister(reed.BuiltinSpec())
	reg.MustRegister(pirn.BuiltinSpec())
	reg.MustRegister(pirn.AgentBuiltinSpec())
	reg.MustRegister(pirn.ActBuiltinSpec())
	return reg
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// program returns the source from -f FILE or the single positional argument.
func program(fs *flag.FlagSet, file string) (string, error) {
	switch {
	case file != "" && fs.NArg() == 0:
		b, err := os.ReadFile(file)
		return string(b), err
	case file == "" && fs.NArg() == 1:
		return fs.Arg(0), nil
	default:
		return "", errors.New("give exactly one of PROGRAM or -f FILE\n" + usage)
	}
}

// registryFor is the registry agent files too big for the record go to,
// or nil (they're skipped) without one.
func registryFor(repo string) *pirn.Registry {
	if repo == "" {
		return nil
	}
	return &pirn.Registry{Repo: repo}
}

// imageRegistryFor is where (image … "push") pushes, or nil: then such
// steps fail with ImageSetup.
func imageRegistryFor(repo string) *bobbin.ImageRegistry {
	if repo == "" {
		return nil
	}
	return &bobbin.ImageRegistry{Repo: repo}
}

// workerCommands registers every command but skills with a worker, and
// returns their registry: the same commands registry() has, which
// TestWorkerCommandsAreTheRegistry checks, since a run is checked
// against registry() with the plan's skill pins.
func workerCommands(w worker.Registry, claude pirn.Claude, agent pirn.Agent, act pirn.Act, browser reed.Browser, images *bobbin.ImageRegistry) (*draftregistry.Registry, error) {
	reg := draft.DefaultRegistry()
	// Every step's containers are labelled with its run and step (LOOM-500).
	steps := loom.Labelled(w)
	if err := bobbin.Register(reg, steps, bobbin.Runner{}); err != nil {
		return nil, err
	}
	if err := bobbin.RegisterImage(reg, steps, bobbin.Runner{}, images); err != nil {
		return nil, err
	}
	if err := reed.Register(reg, steps, browser); err != nil {
		return nil, err
	}
	if err := pirn.Register(reg, steps, claude); err != nil {
		return nil, err
	}
	if err := pirn.RegisterAgent(reg, steps, agent); err != nil {
		return nil, err
	}
	if err := pirn.RegisterAct(reg, steps, act); err != nil {
		return nil, err
	}
	return reg, nil
}

// compileRegistry is the commands a program compiled here may use:
// registry(), and with a store the newest version of each of its skills,
// as the worker on that store has them.
func compileRegistry(ctx context.Context, store string) (*draftregistry.Registry, error) {
	reg := registry()
	if store == "" {
		return reg, nil
	}
	s, err := warp.Open(ctx, store)
	if err != nil {
		return nil, err
	}
	skipped, err := (&treadle.Library{Store: s}).AddTo(ctx, reg)
	warnSkipped(skipped)
	return reg, err
}

// warnSkipped says which skills were left out, and why, on stderr.
func warnSkipped(skipped []error) {
	for _, err := range skipped {
		fmt.Fprintln(os.Stderr, "loom: skill left out:", err)
	}
}

// creator creates skills with the worker's agent, if it has an image.
func creator(a pirn.Agent) func(context.Context, string) (map[string][]byte, error) {
	if a.Image == "" {
		return nil
	}
	return func(ctx context.Context, description string) (map[string][]byte, error) {
		a := a
		a.OnProgress = func(r pirn.AgentResult) { activity.RecordHeartbeat(ctx, len(r.Commands)) }
		files, _, err := treadle.Create(ctx, a.Run, description)
		return files, err
	}
}

// commandsFor is base with the store's newest skills, and what a program
// writer is told about each: built-in commands from loom.BuiltinCommands,
// skills from their own descriptions.
func commandsFor(ctx context.Context, store string, base *draftregistry.Registry) (*draftregistry.Registry, []loom.Command, error) {
	known := map[string]loom.Command{}
	for k, v := range loom.BuiltinCommands {
		known[k] = v
	}
	if store != "" {
		s, err := warp.Open(ctx, store)
		if err != nil {
			return nil, nil, err
		}
		lib := &treadle.Library{Store: s}
		if _, err := lib.AddTo(ctx, base); err != nil {
			return nil, nil, err
		}
		skills, _, err := lib.Skills(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, sk := range skills {
			known[sk.Name] = loom.Command{Args: "(" + sk.Name + ` "arg" ...)`, About: "A skill: " + sk.Description}
		}
	}
	return base, loom.Commands(base, known), nil
}

// readConnectors reads a worker's connectors file, if it has one: a
// file that isn't there (yet) is no connectors.
func readConnectors(path string) (pirn.Connectors, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil // not made yet: no connectors until it is
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return pirn.ReadConnectors(f)
}

// writesCommand lists a run's writes, or approves or declines one
// ("NODE/ID") and then lists them.
func writesCommand(ctx context.Context, out io.Writer, c client.Client, wf, approve, decline string) error {
	if d := approve + decline; d != "" {
		node, id, ok := strings.Cut(d, "/")
		if !ok || node == "" || id == "" {
			return fmt.Errorf("-approve and -decline take NODE/ID, such as n0/w1")
		}
		if err := loom.DecideWrite(ctx, c, wf, loom.WriteDecision{Node: node, ID: id, Approve: approve != ""}); err != nil {
			return err
		}
	}
	ws, err := loom.Writes(ctx, c, wf)
	if err != nil {
		return err
	}
	if len(ws) == 0 {
		fmt.Fprintln(out, "no writes (yet: a run's writes are known once its steps are done)")
	}
	printWrites(out, ws)
	return nil
}

// printWrites lists writes, one per line, then what came of each.
func printWrites(out io.Writer, ws []loom.WriteOutcome) {
	for _, w := range ws {
		fmt.Fprintf(out, "write    %s/%s %s %s/%s %s\n", w.Node, w.ID, w.Status, w.Connector, w.Tool, printable(string(w.Input)))
		switch {
		case w.Error != "":
			fmt.Fprintf(out, "         %s\n", printable(w.Error))
		case w.Output != "":
			fmt.Fprintf(out, "         %s\n", printable(strings.ReplaceAll(strings.TrimSpace(w.Output), "\n", "\n         ")))
		}
	}
}

// stdin is where -add - reads a SKILL.md; tests replace it.
var stdin io.Reader = os.Stdin

// vault is where workers and commands find secrets the environment
// doesn't hold: the operating system's keychain. A variable for tests.
var vault interface {
	pirn.Vault
	Store(ctx context.Context, name, value string) error
	Delete(ctx context.Context, name string) error
	DeleteAll(ctx context.Context) (int, error)
} = pirn.Keychain{}

// deleteAllSecrets deletes every key heedle keeps in the keychain (its
// service, pirn.KeychainService), as when uninstalling. At a terminal it
// asks first; a script (as an uninstaller is) isn't asked.
func deleteAllSecrets(ctx context.Context, out io.Writer) error {
	if isTerminal() {
		fmt.Fprintf(out, "Delete every key heedle keeps in your keychain (service %q), and forget your saved sign-ins? This can't be undone. Type yes: ", pirn.KeychainService)
		line, _ := bufio.NewReader(io.LimitReader(stdin, 64)).ReadString('\n')
		if strings.TrimSpace(line) != "yes" {
			fmt.Fprintln(out, "Nothing deleted.")
			return nil
		}
	}
	// Saved sign-ins go first (roadmap 9.11): each file and its key, and
	// nothing else in their folder (review 53). If that fails, every other
	// key is deleted all the same, and the command fails naming both
	// (review 53, round 2).
	forgot := (pirn.Sessions{Dir: sessionsDir(), Vault: vault}).ForgetAll(ctx)
	if forgot != nil {
		forgot = fmt.Errorf("forgetting your saved sign-ins: %w", forgot)
	}
	n, err := vault.DeleteAll(ctx)
	if err != nil || forgot != nil {
		return errors.Join(forgot, err)
	}
	if n < 0 {
		fmt.Fprintln(out, "Deleted every heedle key from your keychain, and your saved sign-ins.")
	} else {
		fmt.Fprintf(out, "Deleted %d heedle key%s from your keychain, and your saved sign-ins.\n", n, map[bool]string{true: "", false: "s"}[n == 1])
	}
	return nil
}

// sessionsDir is where heedle up keeps saved sign-ins: ~/.heedle/sessions,
// or HEEDLE_SESSIONS. A variable for tests.
var sessionsDir = func() string {
	if d := os.Getenv("HEEDLE_SESSIONS"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".heedle", "sessions")
}

// isTerminal reports whether standard input is a terminal; a variable for
// tests.
var isTerminal = func() bool {
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// secretCommand stores a secret read from standard input in the keychain,
// or deletes one. At a terminal the value isn't echoed (pirn.NoEcho), and
// it is never an argument, so neither the screen, the shell's history nor
// a process listing shows it.
func secretCommand(ctx context.Context, out io.Writer, op, name string) error {
	if pirn.IsProfileName(name) {
		return fmt.Errorf("%s is one of your details: change it under You on the Connectors page (heedle up), or forget every secret with delete --all", name)
	}
	if pirn.IsSessionName(name) {
		return fmt.Errorf("%s is the key of a saved sign-in: forget it under You on the Connectors page (heedle up), or forget every secret with delete --all", name)
	}
	if op == "delete" {
		if err := vault.Delete(ctx, name); err != nil {
			return err
		}
		fmt.Fprintf(out, "Deleted %s from your keychain.\n", name)
		return nil
	}
	if err := pirn.CheckSecret(name, "x"); err != nil {
		return err
	}
	if isTerminal() {
		// Never show what was promised hidden (review 26).
		restore, err := noEcho(ctx)
		if err != nil {
			return fmt.Errorf("can't hide what you type (stty: %v), so nothing was read", err)
		}
		fmt.Fprintf(out, "Paste %s and press Enter (it won't be shown): ", name)
		defer func() { restore(); fmt.Fprintln(out) }()
	}
	// Read beside ctx, so Ctrl-C (which cancels it) ends the prompt and
	// the deferred restore turns echo back on (review 26).
	type read struct {
		line string
		err  error
	}
	got := make(chan read, 1)
	in := stdin
	go func() {
		line, err := bufio.NewReader(io.LimitReader(in, pirn.MaxSecret+2)).ReadString('\n')
		got <- read{line, err}
	}()
	var r read
	select {
	case <-ctx.Done():
		return fmt.Errorf("nothing stored: %w", ctx.Err())
	case r = <-got:
	}
	if r.err != nil && !errors.Is(r.err, io.EOF) {
		return r.err
	}
	value := strings.TrimRight(r.line, "\r\n")
	if err := vault.Store(ctx, name, value); err != nil {
		return err
	}
	if name == "ANTHROPIC_API_KEY" {
		// Workers read this key for every request (pirn.Claude.KeyFrom).
		fmt.Fprintf(out, "Stored %s in your keychain (service %q). Workers use it from their next step; no restart needed.\n", name, pirn.KeychainService)
		return nil
	}
	fmt.Fprintf(out, "Stored %s in your keychain (service %q). Connectors that name %s use it from their next step; no restart needed.\n", name, pirn.KeychainService, name)
	return nil
}

// noEcho is pirn.NoEcho; a variable for tests.
var noEcho = pirn.NoEcho

// describeSpec says when a schedule runs, for people.
func describeSpec(s loom.ScheduleSpec) string {
	if s.Every != 0 {
		return "every " + s.Every.String()
	}
	tz := s.TimeZone
	if tz == "" {
		tz = "UTC"
	}
	return fmt.Sprintf("at %q (%s)", s.Cron, tz)
}

// printSchedules lists schedules: name, when, state, next run, program.
func printSchedules(out io.Writer, ss []loom.Schedule) {
	if len(ss) == 0 {
		fmt.Fprintln(out, "no schedules")
		return
	}
	for _, s := range ss {
		state, next := "on", "-"
		if s.Paused {
			state = "paused"
			if s.Note != "" {
				state = "paused (" + s.Note + ")"
			}
		}
		if len(s.Next) > 0 {
			next = s.Next[0].Local().Format(time.RFC3339)
		}
		prog := strings.Join(strings.Fields(s.Program), " ")
		if len(prog) > 60 {
			prog = prog[:60] + "…"
		}
		fmt.Fprintf(out, "%-20s %-24s %-7s next %-26s %d recent runs  %s%s\n", s.Name, describeSpec(s.Spec), state, next, len(s.Recent), prog, describeBudget(s.Budget))
	}
}

// actAllowed refuses to run or schedule a plan with act steps unless the
// person said -allow-act, saying what they would do: the card isn't the
// only way in (review 39). Steps that send without asking need
// -allow-send-without-asking too, so a habit of -allow-act never lets
// one through unseen (review 41).
func actAllowed(p plan.Plan, ok, freely bool) error {
	if len(loom.ActHosts(p)) > 0 && !ok {
		return fmt.Errorf("its act steps %s: give -allow-act to let them", strings.Join(actDisclosures(p), "; "))
	}
	if f := loom.ActSendsFreely(p); len(f) > 0 && !freely {
		return fmt.Errorf("its act steps %s: give -allow-send-without-asking as well to let them", ActDisclosure(f, true))
	}
	return nil
}

// actDisclosures say what a plan's act steps may do, on which hosts:
// fill forms and stop before sending to ask (roadmap 9.7), or, where the
// program said :send-without-asking, send without asking again.
func actDisclosures(p plan.Plan) []string {
	freely := loom.ActSendsFreely(p)
	var asks []string
	for _, h := range loom.ActHosts(p) {
		if !slices.Contains(freely, h) {
			asks = append(asks, h)
		}
	}
	var out []string
	if len(asks) > 0 {
		out = append(out, ActDisclosure(asks, false))
	}
	if len(freely) > 0 {
		out = append(out, ActDisclosure(freely, true))
	}
	if u := loom.ActUses(p); len(u) > 0 {
		out = append(out, UsesDisclosure(u))
	}
	if s := loom.ActSignedIn(p); len(s) > 0 {
		out = append(out, SignedInDisclosure(s))
	}
	return out
}

// SignedInDisclosure says where act steps start signed in as the person
// (roadmap 9.11).
func SignedInDisclosure(hosts []string) string {
	return "start signed in as you on " + strings.Join(hosts, ", ") + ", with the sign-ins you kept, and always ask before sending"
}

// noteUses says, before a run or schedule starts, which of the person's
// details its act steps may use: -allow-act lets them, so the person
// still sees which (review 42).
func noteUses(out io.Writer, p plan.Plan) {
	if u := loom.ActUses(p); len(u) > 0 {
		fmt.Fprintf(out, "act steps %s\n", UsesDisclosure(u))
	}
	if s := loom.ActSignedIn(p); len(s) > 0 {
		fmt.Fprintf(out, "act steps %s\n", SignedInDisclosure(s))
	}
}

// UsesDisclosure says which of the person's details act steps may use
// (roadmap 9.8): "may use your name, email address and date of birth".
func UsesDisclosure(fields []string) string {
	var words []string
	for _, f := range fields {
		words = append(words, pirn.ProfileLabels[f])
	}
	list := words[0]
	if n := len(words); n > 1 {
		list = strings.Join(words[:n-1], ", ") + " and " + words[n-1]
	}
	return "may use your " + list
}

// ActDisclosure says what act steps may do on hosts: fill in forms and
// stop before sending to ask you (roadmap 9.7), or, freely, send without
// asking again (roadmap 9.5's disclosure): shown before a proposal runs.
func ActDisclosure(hosts []string, freely bool) string {
	if freely {
		return "can click, type and submit forms on " + strings.Join(hosts, ", ") + " without asking again"
	}
	return "can click and type on " + strings.Join(hosts, ", ") + ", and stop before sending and ask you"
}

// printProposal shows a proposal: what heedle would do, the program, when
// it would run, and whether it can run now and, if not, what to set up.
func printProposal(out io.Writer, p loom.Proposal) {
	fmt.Fprintln(out, p.Summary)
	// What heedle remembers went with the request; what it could remember
	// is only suggested (roadmap 10.1; review 47: the terminal said
	// neither).
	if n := len(p.Memory); n > 0 {
		fmt.Fprintf(out, "\nUsed %d thing%s heedle remembers (heedle memory lists them)\n", n, map[bool]string{true: "", false: "s"}[n == 1])
	}
	for _, r := range p.Remember {
		fmt.Fprintf(out, "\nheedle could remember: %s\n  nothing is remembered until you say so: heedle memory -add %s\n", printable(r), shellWord(r))
	}
	if p.Program == "" && p.Skill == "" {
		return // only something to remember
	}
	defer printProposalUsage(out, p.Usage)
	if p.Skill != "" {
		fmt.Fprintf(out, "\nNo command or skill here can do that. A skill that could:\n  %s\nCreate it with: heedle skills -store DIR -create %q -agent-image ID (it waits for your review)\n", p.Skill, p.Skill)
		return
	}
	fmt.Fprintf(out, "\n%s", p.Program)
	if p.Schedule != nil {
		fmt.Fprintf(out, "\nschedule %s: %s\n", p.Name, describeSpec(*p.Schedule))
	}
	if p.Plan != nil {
		if d := actDisclosures(*p.Plan); len(d) > 0 {
			fmt.Fprintln(out)
			for _, line := range d {
				// Sending without asking stands out, on its own line
				// (review 41).
				mark := ""
				if strings.HasSuffix(line, "without asking again") {
					mark = "! "
				}
				fmt.Fprintf(out, "%sact steps %s\n", mark, line)
			}
		}
	}
	if r := p.Ready; r != nil {
		fmt.Fprintln(out)
		for _, line := range readinessLines(*r) {
			fmt.Fprintln(out, line)
		}
	}
}

// readinessLines says, a line each, what a proposal needs and whether
// it's there.
func readinessLines(r loom.Readiness) []string {
	if r.Error != "" {
		return []string{"can't tell if it can run: " + r.Error}
	}
	var out []string
	if r.Agent != "" {
		out = append(out, "missing  agent steps: "+r.Agent)
	}
	if r.Browser != "" {
		out = append(out, "missing  browse steps: "+r.Browser)
	}
	if r.BrowserAgent != "" {
		out = append(out, "missing  act steps: "+r.BrowserAgent)
	}
	switch r.Claude {
	case "key":
		out = append(out, "missing  Claude: no ANTHROPIC_API_KEY; set it with: heedle secret set ANTHROPIC_API_KEY")
	case "unreadable":
		out = append(out, "missing  Claude: can't read ANTHROPIC_API_KEY now: "+r.ClaudeDetail)
	}
	for _, n := range r.Needs {
		name := n.Connector
		if len(n.Tools) > 0 {
			name += " (" + strings.Join(n.Tools, ", ") + ")"
		}
		if len(n.Writes) > 0 {
			name += ", which can change things: " + strings.Join(n.Writes, ", ") + map[bool]string{true: " (each waits for your approval)", false: " (without asking you)"}[n.Approve]
		}
		switch n.Problem {
		case "":
			out = append(out, "ready    "+name)
		case "absent":
			out = append(out, "missing  "+name+": no connector of that name; add one on the Connectors page, named "+n.Connector)
		case "tools":
			out = append(out, "missing  "+name+": the connector doesn't allow those tools; add them to its tools in the connectors file")
		case "key":
			out = append(out, "missing  "+name+": its key "+n.Key+" isn't set; set it with: heedle secret set "+n.Key)
		default:
			out = append(out, "missing  "+name+": can't read its key "+n.Key+" now: "+n.Detail)
		}
	}
	if r.Ready {
		out = append(out, "It can run now.")
	}
	return out
}

// askThen does what -then says with a proposal: run it, or schedule it as
// proposed; each only if the worker can run it now.
func askThen(ctx context.Context, out io.Writer, c client.Client, queue, ref, then string, allowAct, allowFreely bool, p loom.Proposal) error {
	if then == "" {
		if p.Plan != nil {
			fmt.Fprintln(out, "\nRun it with -then run"+map[bool]string{true: ", or schedule it with -then schedule", false: ""}[p.Schedule != nil]+".")
		}
		return nil
	}
	switch {
	case p.Plan == nil:
		return errors.New("there's no program to " + then)
	case p.Ready == nil:
		return errors.New("the worker didn't say whether it can run this: ask again")
	case !p.Ready.Ready:
		return errors.New("it can't run yet: set up what's missing above, then ask again")
	case then == "schedule" && p.Schedule == nil:
		return errors.New("the request doesn't say when to run it: ask again saying how often, or use -then run")
	}
	if err := actAllowed(*p.Plan, allowAct, allowFreely); err != nil {
		return err
	}
	noteUses(out, *p.Plan)
	in := loom.RunInput{Program: p.Program, Plan: *p.Plan, Ref: ref}
	if then == "schedule" {
		if err := loom.CreateSchedule(ctx, c, queue, p.Name, in, *p.Schedule); err != nil {
			return err
		}
		fmt.Fprintf(out, "scheduled %s: %s\n", p.Name, describeSpec(*p.Schedule))
		return nil
	}
	wr, err := loom.Submit(ctx, c, queue, in)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "workflow %s run %s\n", wr.GetID(), wr.GetRunID())
	var res loom.RunResult
	if err := wr.Get(ctx, &res); err != nil {
		return err
	}
	fmt.Fprintf(out, "commit   %s on %s\n", res.Commit, ref)
	printWrites(out, res.Writes)
	return nil
}

// localZone is this machine's IANA time zone: TZ if it names one, else
// what /etc/localtime links to (macOS and Linux both link it into a
// zoneinfo directory); "" if neither says.
func localZone() string {
	ok := func(z string) bool { _, err := time.LoadLocation(z); return z != "" && z != "Local" && err == nil }
	if z := strings.TrimPrefix(os.Getenv("TZ"), ":"); ok(z) {
		return z
	}
	if target, err := os.Readlink("/etc/localtime"); err == nil {
		if _, z, found := strings.Cut(target, "zoneinfo/"); found && ok(z) {
			return z
		}
	}
	return ""
}

// memoryCommand lists what heedle remembers (in file, or heedle up's),
// or adds, forgets or forgets all of it (roadmap 10.1). What you add from
// the terminal is your own yes; what Claude suggests is added only when
// you press Remember.
func memoryCommand(out io.Writer, file, add, forget string, forgetAll bool, extra int) error {
	ops := 0
	for _, on := range []bool{add != "", forget != "", forgetAll} {
		if on {
			ops++
		}
	}
	if ops > 1 || extra > 0 {
		return errors.New("memory takes one of -add TEXT, -forget ID and -forget-all, or nothing to list\n" + usage)
	}
	if file == "" {
		// heedle up's, unless it was started with -home DIR: then give
		// -memory DIR/memory.json (every line says which file it is).
		home, err := os.UserHomeDir()
		if err != nil {
			return errors.New("no home directory: give -memory FILE")
		}
		file = filepath.Join(home, ".heedle", "memory.json")
	}
	switch {
	case add != "":
		m, err := loom.AddMemory(file, "", add, time.Now())
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "remembered %s: %s\n", m.ID, printable(m.Text))
		return nil
	case forget != "":
		if _, err := loom.ForgetMemory(file, "", forget); err != nil {
			return err
		}
		fmt.Fprintf(out, "forgot %s; it's gone from %s\n", forget, file)
		return nil
	case forgetAll:
		if _, err := loom.ForgetAllMemory(file, ""); err != nil {
			return err
		}
		fmt.Fprintf(out, "forgot everything; %s is empty\n", file)
		return nil
	}
	// As it is, so a memory edited by hand into one heedle won't send can
	// be seen, with why, and forgotten (review 47).
	ms, err := loom.ReadMemoryAsIs(file)
	if err != nil {
		return fmt.Errorf("%w (heedle memory -forget-all empties it)", err)
	}
	if len(ms) == 0 {
		fmt.Fprintf(out, "heedle remembers nothing about you (%s)\n", file)
		return nil
	}
	problems := ms.Problems()
	fmt.Fprintf(out, "heedle remembers, and sends to Claude with every request (%s):\n", file)
	for _, m := range ms {
		used := "never used"
		if !m.Used.IsZero() {
			used = "used " + m.Used.Local().Format("2006-01-02")
		}
		fmt.Fprintf(out, "  %s  added %s, %s  %s\n", m.ID, m.Added.Local().Format("2006-01-02"), used, printable(m.Text))
		if why := problems[m.ID]; why != "" {
			fmt.Fprintf(out, "      can't go to Claude: %s; forget it with heedle memory -forget %s\n", why, m.ID)
		}
	}
	if why := problems[""]; why != "" {
		fmt.Fprintf(out, "  can't go to Claude: %s\n", why)
	}
	return nil
}

// formsCommand lists the forms run workflowID's act steps wait to send,
// or sends or cancels one (roadmap 9.7).
func formsCommand(ctx context.Context, out io.Writer, c client.Client, workflowID, send, cancel string) error {
	if id := send + cancel; id != "" {
		if err := loom.DecideForm(ctx, c, workflowID, loom.FormDecision{ID: id, Send: send != ""}); err != nil {
			return err
		}
		fmt.Fprintf(out, "%s %s\n", map[bool]string{true: "sending", false: "cancelled"}[send != ""], id)
		return nil
	}
	forms, err := loom.PendingForms(ctx, c, workflowID)
	if err != nil {
		return err
	}
	if len(forms) == 0 {
		fmt.Fprintln(out, "no form waits to be sent")
		return nil
	}
	for _, f := range forms {
		fmt.Fprintf(out, "%s  step %s  %s\n  %s\n", f.ID, f.Node, f.URL, f.Summary)
		// The filled form, to look at before deciding (review 41).
		if shot, err := os.CreateTemp("", "loom-form-"+f.ID+"-*.jpg"); err == nil {
			_, werr := shot.Write(f.JPEG)
			if shot.Close() == nil && werr == nil {
				fmt.Fprintf(out, "  the filled form: %s\n", shot.Name())
			}
		}
		fmt.Fprintf(out, "  send it: loom forms -workflow %s -send %s\n  cancel it: loom forms -workflow %s -cancel %s\n", workflowID, f.ID, workflowID, f.ID)
	}
	return nil
}

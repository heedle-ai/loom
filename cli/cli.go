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
  loom worker  -store DIR [-address HOST:PORT] [-queue NAME] [-browser-image ID] [-browser-network NAME] [-ask-model MODEL] [-agent-image ID] [-agent-registry REPO] [-image-registry REPO] [-connectors FILE]
  loom run     [-address HOST:PORT] [-queue NAME] [-ref REF] [-store DIR] (PROGRAM | -f FILE)
  loom compile [-store DIR] (PROGRAM | -f FILE)
  loom skills  -store DIR [-add FOLDER | -add - | -import LINK [-path P] [-branch B] | -catalog LINK [-branch B]]
  loom skills  -store DIR -create "DESCRIPTION" -agent-image ID
  loom skills  -store DIR (-review NAME | -approve NAME@VERSION [-replacing VERSION] | -reject NAME@VERSION | -trust PREFIX | -untrust PREFIX | -trusted)
  loom branch  [-address HOST:PORT] [-queue NAME] -from REF -to REF
  loom merge   [-address HOST:PORT] [-queue NAME] -into REF -from REF [-resolve ours|theirs|rerun]
  loom beliefs -store DIR [-ref REF]
  loom writes  [-address HOST:PORT] -workflow ID [-approve NODE/ID | -decline NODE/ID]
  loom write   [-store DIR] "DESCRIPTION OF A TASK"
  loom secret  (set NAME | delete NAME)
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
	browserNetwork := fs.String("browser-network", "", "worker: Podman network browse containers join")
	askModel := fs.String("ask-model", pirn.DefaultModel, "worker: the Claude model ask and agent use (key from ANTHROPIC_API_KEY, or the keychain: loom secret set ANTHROPIC_API_KEY)")
	agentImage := fs.String("agent-image", "", "worker: content-pinned image ID agent steps run in (needs /bin/sh)")
	imageRegistry := fs.String("image-registry", "", "worker: OCI repository (such as ghcr.io/acme/heedle-images) that (image … \"push\") pushes to, with the worker's podman login")
	add := fs.String("add", "", "skills: a skill's folder (with SKILL.md) to add to the store, or - for a SKILL.md on standard input")
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
	agentRegistry := fs.String("agent-registry", "", "worker: OCI repository (such as ghcr.io/acme/heedle-files) for agent files too big for the record, pushed with oras")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	switch args[0] {
	case "secret":
		if fs.NArg() != 2 || (fs.Arg(0) != "set" && fs.Arg(0) != "delete") {
			return errors.New("secret takes set NAME or delete NAME\n" + usage)
		}
		return secretCommand(ctx, out, fs.Arg(0), fs.Arg(1))

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
		w := worker.New(c, *queue, reed.WorkerOptions(worker.Options{}))
		// The key and the connectors are read again for every step, so a key
		// stored or a connector added while the worker runs needs no restart.
		if _, err := pirn.LookupSecret(ctx, vault, "ANTHROPIC_API_KEY"); err != nil {
			// The worker still runs every step that doesn't ask Claude.
			fmt.Fprintf(out, "ANTHROPIC_API_KEY: %v; ask and agent steps will fail until the worker can read it\n", err)
		}
		claude := pirn.Claude{Model: *askModel, KeyFrom: func(ctx context.Context) (string, error) {
			return pirn.LookupSecret(ctx, vault, "ANTHROPIC_API_KEY")
		}}
		connectors, err := readConnectors(*connectorsFile)
		if err != nil {
			return err
		}
		agent := pirn.Agent{Claude: claude, Runner: bobbin.Runner{}, Image: *agentImage, Registry: registryFor(*agentRegistry), Connectors: connectors, Vault: vault,
			LoadConnectors: func() (pirn.Connectors, error) { return readConnectors(*connectorsFile) }}
		reg, err := workerCommands(w, claude, agent, reed.Browser{Image: *browserImage, Network: *browserNetwork}, imageRegistryFor(*imageRegistry))
		if err != nil {
			return err
		}
		// Skills are the store's, at their newest when the worker starts;
		// each run is checked against, and records, the versions it pins.
		lib := &treadle.Library{Store: s}
		skipped, err := treadle.Register(ctx, reg, w, agent, lib)
		if err != nil {
			return err
		}
		warnSkipped(skipped)
		loom.RegisterWith(w, &loom.Checker{Registry: reg, RegistryFor: func(ctx context.Context, p plan.Plan) (*draftregistry.Registry, error) {
			return lib.Pinned(ctx, registry(), p)
		}}, &loom.Recorder{Store: s, Uses: lib.Uses})
		// Skills change only through a workflow on this worker (drawdown never
		// writes), never through a program.
		loom.RegisterSkills(w, loom.Skills{Library: lib, Create: creator(agent)})
		// The program writer, for drawdown: Claude with this worker's key,
		// and its commands and newest skills.
		loom.RegisterWriter(w, loom.Writer{Asker: claude, Commands: func(ctx context.Context) (*draftregistry.Registry, []loom.Command, error) {
			return commandsFor(ctx, *store, registry())
		}})
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
		wr, err := loom.SubmitMerge(ctx, c, *queue, loom.MergeInput{Into: *into, From: *from, Resolve: *resolve})
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
// pirn's ask (Claude, answering from the step's inputs) and agent (Claude
// working in a fresh container).
func registry() *draftregistry.Registry {
	reg := draft.DefaultRegistry()
	reg.MustRegister(bobbin.BuiltinSpec())
	reg.MustRegister(bobbin.ImageBuiltinSpec())
	reg.MustRegister(reed.BuiltinSpec())
	reg.MustRegister(pirn.BuiltinSpec())
	reg.MustRegister(pirn.AgentBuiltinSpec())
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
func workerCommands(w worker.Registry, claude pirn.Claude, agent pirn.Agent, browser reed.Browser, images *bobbin.ImageRegistry) (*draftregistry.Registry, error) {
	reg := draft.DefaultRegistry()
	if err := bobbin.Register(reg, w, bobbin.Runner{}); err != nil {
		return nil, err
	}
	if err := bobbin.RegisterImage(reg, w, bobbin.Runner{}, images); err != nil {
		return nil, err
	}
	if err := reed.Register(reg, w, browser); err != nil {
		return nil, err
	}
	if err := pirn.Register(reg, w, claude); err != nil {
		return nil, err
	}
	if err := pirn.RegisterAgent(reg, w, agent); err != nil {
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
} = pirn.Keychain{}

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
		// Workers read this key when they start.
		fmt.Fprintf(out, "Stored %s in your keychain (service %q). Restart heedle (or the worker) to use it.\n", name, pirn.KeychainService)
		return nil
	}
	fmt.Fprintf(out, "Stored %s in your keychain (service %q). Connectors that name %s use it from their next step; no restart needed.\n", name, pirn.KeychainService, name)
	return nil
}

// noEcho is pirn.NoEcho; a variable for tests.
var noEcho = pirn.NoEcho

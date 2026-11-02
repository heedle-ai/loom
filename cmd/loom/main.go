// Command loom runs draft programs on Temporal and records them in warp.
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
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"

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
  loom worker  -store DIR [-address HOST:PORT] [-queue NAME] [-browser-image ID] [-browser-network NAME] [-ask-model MODEL] [-agent-image ID] [-agent-registry REPO] [-image-registry REPO]
  loom run     [-address HOST:PORT] [-queue NAME] [-ref REF] [-store DIR] (PROGRAM | -f FILE)
  loom compile [-store DIR] (PROGRAM | -f FILE)
  loom skills  -store DIR [-add FOLDER | -add - | -import LINK [-path P] [-branch B] | -catalog LINK [-branch B]]
  loom skills  -store DIR (-review NAME | -approve NAME@VERSION [-replacing VERSION] | -reject NAME@VERSION | -trust PREFIX | -untrust PREFIX | -trusted)
  loom branch  [-address HOST:PORT] [-queue NAME] -from REF -to REF
  loom merge   [-address HOST:PORT] [-queue NAME] -into REF -from REF [-resolve ours|theirs|rerun]
  loom beliefs -store DIR [-ref REF]
`

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "loom:", err)
		os.Exit(1)
	}
}

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
	askModel := fs.String("ask-model", pirn.DefaultModel, "worker: the Claude model ask and agent use (key from ANTHROPIC_API_KEY)")
	agentImage := fs.String("agent-image", "", "worker: content-pinned image ID agent steps run in (needs /bin/sh)")
	imageRegistry := fs.String("image-registry", "", "worker: OCI repository (such as ghcr.io/acme/heedle-images) that (image … \"push\") pushes to, with the worker's podman login")
	add := fs.String("add", "", "skills: a skill's folder (with SKILL.md) to add to the store, or - for a SKILL.md on standard input")
	importSkill := fs.String("import", "", "skills: a GitHub link to a skill's folder, or an https:// or file:// repository (with -path and -branch), to import")
	catalog := fs.String("catalog", "", "skills: a GitHub repository link, or an https:// or file:// repository, whose skills to list")
	skillPath := fs.String("path", "", "skills: with -import of a repository URL, the skill's folder in it")
	branch := fs.String("branch", "", "skills: with -import or -catalog of a repository URL, the branch or tag (default: its default branch)")
	review := fs.String("review", "", "skills: show the import of a skill waiting for review: its files and its diff from the skill's newest version")
	approve := fs.String("approve", "", "skills: NAME@VERSION, the reviewed import to add as the skill's newest version")
	replacing := fs.String("replacing", "", "skills: with -approve, the skill's newest version the review showed the diff from (none for a new skill)")
	reject := fs.String("reject", "", "skills: NAME@VERSION, the reviewed import to turn down")
	trust := fs.String("trust", "", "skills: a repository URL prefix (such as https://github.com/your-org) whose imports need no review")
	untrust := fs.String("untrust", "", "skills: a trusted prefix to remove; later imports from it wait for review")
	trusted := fs.Bool("trusted", false, "skills: list the trusted prefixes")
	agentRegistry := fs.String("agent-registry", "", "worker: OCI repository (such as ghcr.io/acme/heedle-files) for agent files too big for the record, pushed with oras")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	switch args[0] {
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
		claude := pirn.Claude{APIKey: os.Getenv("ANTHROPIC_API_KEY"), Model: *askModel}
		agent := pirn.Agent{Claude: claude, Runner: bobbin.Runner{}, Image: *agentImage, Registry: registryFor(*agentRegistry)}
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
		loom.RegisterSkills(w, lib)
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
		for _, f := range []string{*add, *importSkill, *catalog, *review, *approve, *reject, *trust, *untrust} {
			if f != "" {
				set++
			}
		}
		if *trusted {
			set++
		}
		if set > 1 || (*skillPath != "" && *importSkill == "") || (*branch != "" && *importSkill == "" && *catalog == "") || (*replacing != "" && *approve == "") {
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
		if done, err := reviewCommand(ctx, out, lib, cmd, *review, *approve, *replacing, *reject, *trust, *untrust, *trusted); done || err != nil {
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

// stdin is where -add - reads a SKILL.md; tests replace it.
var stdin io.Reader = os.Stdin

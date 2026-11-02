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
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

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
  loom skills  -store DIR [-add FOLDER]
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
	add := fs.String("add", "", "skills: a skill's folder (with SKILL.md) to add to the store")
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
		if *store == "" || fs.NArg() != 0 {
			return errors.New("skills needs -store DIR, and optionally -add FOLDER\n" + usage)
		}
		s, err := warp.Init(ctx, *store)
		if err != nil {
			return err
		}
		lib := &treadle.Library{Store: s}
		if *add != "" {
			files, err := readFolder(*add)
			if err != nil {
				return err
			}
			sk, err := lib.Add(ctx, files, time.Now())
			if err != nil {
				return err
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
			fmt.Fprintf(out, "%-20s %.12s  %s\n", sk.Name, sk.Version, sk.Description)
		}
		return nil

	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
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

// readFolder reads every regular file under dir, by "/"-separated path.
// Hidden files and folders (.git, .DS_Store) aren't part of a skill and
// are left out; symbolic links and other special files are refused: a
// skill is its files' bytes.
func readFolder(dir string) (map[string][]byte, error) {
	files := map[string][]byte{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path != dir && strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("%s isn't a regular file", path)
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(path)
		files[filepath.ToSlash(rel)] = b
		return err
	})
	return files, err
}

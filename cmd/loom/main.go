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

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/heedle-ai/bobbin"
	"github.com/heedle-ai/draft"
	draftregistry "github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/reed"
	"github.com/heedle-ai/warp"
)

const usage = `usage:
  loom worker  -store DIR [-address HOST:PORT] [-queue NAME] [-browser-image ID] [-browser-network NAME]
  loom run     [-address HOST:PORT] [-queue NAME] [-ref REF] (PROGRAM | -f FILE)
  loom compile (PROGRAM | -f FILE)
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
		reg := draft.DefaultRegistry()
		if err := bobbin.Register(reg, w, bobbin.Runner{}); err != nil {
			return err
		}
		if err := reed.Register(reg, w, reed.Browser{Image: *browserImage, Network: *browserNetwork}); err != nil {
			return err
		}
		loom.Register(w, s, reg)
		fmt.Fprintf(out, "worker on queue %q, recording to %s\n", *queue, s.Dir())
		return w.Run(worker.InterruptCh())

	case "compile":
		src, err := program(fs, *file)
		if err != nil {
			return err
		}
		p, err := loom.Compile(ctx, registry(), src)
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
		p, err := loom.Compile(ctx, registry(), src)
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

	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}

// registry is the commands programs may use: draft's echo, bobbin's
// container (a step in a fresh Podman container) and reed's browse (a page
// in a fresh browser that may reach only the hosts the program names).
func registry() *draftregistry.Registry {
	reg := draft.DefaultRegistry()
	reg.MustRegister(bobbin.BuiltinSpec())
	reg.MustRegister(reed.BuiltinSpec())
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

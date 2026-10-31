// Command loom runs draft programs on Temporal and records them in warp.
//
//	loom worker  -store DIR                   start a worker
//	loom run     [-ref REF] PROGRAM | -f FILE  compile, run and record a program
//	loom compile PROGRAM | -f FILE             print the compiled plan; runs nothing
//
// worker and run take -address (default localhost:7233) and -queue
// (default "loom").
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/warp"
)

const usage = `usage:
  loom worker  -store DIR [-address HOST:PORT] [-queue NAME]
  loom run     [-address HOST:PORT] [-queue NAME] [-ref REF] (PROGRAM | -f FILE)
  loom compile (PROGRAM | -f FILE)
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
		w := worker.New(c, *queue, worker.Options{})
		loom.Register(w, s)
		fmt.Fprintf(out, "worker on queue %q, recording to %s\n", *queue, s.Dir())
		return w.Run(worker.InterruptCh())

	case "compile":
		src, err := program(fs, *file)
		if err != nil {
			return err
		}
		p, err := loom.Compile(ctx, draft.DefaultRegistry(), src)
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
		p, err := loom.Compile(ctx, draft.DefaultRegistry(), src)
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

	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
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

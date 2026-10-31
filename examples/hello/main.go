// Command hello is heedle-ai's hello world: is the shipment late?
//
// Three sources are read in parallel and an assessor combines them into a
// conclusion naming the sources that support it. The run executes durably on
// Temporal and is recorded in a warp store as a Git commit. Edit a source,
// run again, and git diff shows which evidence changed and how the
// conclusion moved.
//
//	temporal server start-dev                       # in another terminal
//	go run ./examples/hello -store ~/hello-store.git
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/loom/examples/hello/shipment"
	"github.com/heedle-ai/warp"
)

const queue = "loom-hello"

func main() {
	store := flag.String("store", "", "warp store directory (created if missing)")
	sources := flag.String("sources", shipment.SourcesDir(), "directory of source files")
	address := flag.String("address", client.DefaultHostPort, "Temporal frontend address")
	flag.Parse()
	if *store == "" {
		log.Fatal("give -store DIR, e.g. -store ~/hello-store.git")
	}
	if fi, err := os.Stat(*sources); err != nil || !fi.IsDir() {
		log.Fatalf("sources directory %q not found; pass -sources DIR", *sources)
	}
	ctx := context.Background()

	s, err := warp.Init(ctx, *store)
	if err != nil {
		log.Fatal(err)
	}
	c, err := client.Dial(client.Options{HostPort: *address})
	if err != nil {
		log.Fatalf("connect to Temporal at %s: %v (is `temporal server start-dev` running?)", *address, err)
	}
	defer c.Close()

	w := worker.New(c, queue, worker.Options{})
	loom.Register(w, s)
	shipment.RegisterCommands(w, *sources)
	if err := w.Start(); err != nil {
		log.Fatal(err)
	}
	defer w.Stop()

	p, err := loom.Compile(ctx, shipment.Registry(), shipment.Program)
	if err != nil {
		log.Fatal(err)
	}
	run, err := loom.Submit(ctx, c, queue, loom.RunInput{Program: shipment.Program, Plan: p})
	if err != nil {
		log.Fatal(err)
	}
	var res loom.RunResult
	if err := run.Get(ctx, &res); err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(os.Stderr, "workflow %s run %s\n", run.GetID(), run.GetRunID())
	ids := make([]string, 0, len(res.Outputs))
	for id := range res.Outputs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		fmt.Printf("%s  %s", id, res.Outputs[id])
	}
	fmt.Printf("\nrecorded as %s on %s\n", res.Commit, loom.DefaultRef)
	fmt.Printf("  git --git-dir %s log --oneline %s\n", *store, loom.DefaultRef)
}

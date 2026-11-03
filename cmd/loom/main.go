// Command loom compiles and runs draft programs on Temporal, recording
// every run in a warp store. It is cli.Run; heedle offers the same
// commands.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/heedle-ai/loom/cli"
)

func main() {
	// Ctrl-C cancels the command, so it can clean up (such as turning a
	// terminal's echo back on after a secret's prompt).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := cli.Run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "loom:", err)
		os.Exit(1)
	}
}

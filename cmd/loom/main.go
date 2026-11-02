// Command loom compiles and runs draft programs on Temporal, recording
// every run in a warp store. It is cli.Run; heedle offers the same
// commands.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/heedle-ai/loom/cli"
)

func main() {
	if err := cli.Run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "loom:", err)
		os.Exit(1)
	}
}

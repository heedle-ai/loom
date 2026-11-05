package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/heedle-ai/bobbin"
	"github.com/heedle-ai/loom"
)

// Housekeeping (roadmap 9.2): heedle's containers and images, and
// stopping a run. Nothing here stops or removes anything that isn't
// heedle's: containers are bobbin's labelled listing, images are named
// under bobbin.HeedleRepo, and a run is stopped only through Temporal.

// refs is a flag that may be given more than once.
type refs []string

func (r *refs) String() string     { return strings.Join(*r, ",") }
func (r *refs) Set(v string) error { *r = append(*r, v); return nil }

// cancelCommand cancels a run through Temporal (loom.Cancel).
func cancelCommand(ctx context.Context, out io.Writer, c client.Client, workflowID, runID string) error {
	run, err := loom.Cancel(ctx, c, workflowID, runID)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Cancelled run %s (%s): its steps end, and Podman removes their containers.\n", workflowID, run)
	return nil
}

// containersArgs are the containers command's flags.
type containersArgs struct {
	stop, run string // -stop WORKFLOW and -run RUN
	store     string // -store: the home whose containers -clean may remove
	clean     bool
}

// containersCommand lists heedle's containers with their runs and homes,
// or stops one run (-stop, with -run) through Temporal, or removes the
// containers of runs that have ended (-clean; with -store, only that
// home's).
func containersCommand(ctx context.Context, out io.Writer, c client.Client, a containersArgs) error {
	r := bobbin.Runner{}
	home := ""
	if a.store != "" {
		home = bobbin.HomeOf(a.store)
	}
	switch {
	case a.stop != "":
		run, err := stopRun(ctx, c, r, home, a.stop, a.run)
		if err != nil {
			return err
		}
		return cancelCommand(ctx, out, c, a.stop, run)
	case a.clean:
		gone, err := loom.Clean(ctx, c, r, home)
		if err != nil {
			return err
		}
		if len(gone) == 0 {
			fmt.Fprintln(out, "Nothing to clean: no heedle container belongs to a run that has ended.")
			return nil
		}
		fmt.Fprintf(out, "Removed %d container%s of runs that have ended:\n", len(gone), plural(len(gone)))
		for _, ct := range gone {
			fmt.Fprintf(out, "  %s  %s, step %s of run %s %s (%s)\n", ct.Name, ct.Labels.Kind, orDash(ct.Labels.Step), ct.Workflow, ct.Run, ct.Status)
		}
		return nil
	}
	cs, err := loom.Containers(ctx, c, r)
	if err != nil {
		return err
	}
	if home != "" {
		fmt.Fprintf(out, "This home is %s (%s).\n", home, a.store)
	}
	if len(cs) == 0 {
		fmt.Fprintln(out, "No heedle containers.")
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KIND\tHOME\tWORKFLOW\tRUN\tSTATUS\tSTEP\tAGE\tIMAGE\tCPU\tMEMORY")
	for _, ct := range cs {
		h := orDash(ct.Labels.Home)
		if home != "" && ct.Labels.Home == home {
			h += " (this)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", ct.Labels.Kind, h, orDash(ct.Workflow), orDash(ct.Run), orDash(ct.Status), orDash(ct.Labels.Step),
			age(ct.Created), short(ct.Image, 48), orDash(ct.CPU), orDash(ct.Memory))
	}
	tw.Flush()
	fmt.Fprintln(out, "Stop a run with: heedle containers -stop WORKFLOW -run RUN (it's cancelled through Temporal); remove the containers of ended runs with -clean.")
	return nil
}

// stopRun is the run ID -stop WORKFLOW cancels: -run's, else the one run
// of workflow that heedle's containers here belong to (of home's, with a
// home), never Temporal's latest run of that workflow (roadmap 9.2.1,
// LOOM-521). With no such container, or several runs, it asks for -run.
func stopRun(ctx context.Context, c client.Client, r bobbin.Runner, home, workflow, run string) (string, error) {
	if run != "" {
		return run, nil
	}
	cs, err := loom.Containers(ctx, c, r)
	if err != nil {
		return "", err
	}
	var runs []string
	for _, ct := range cs {
		if ct.Workflow == workflow && ct.Run != "" && loom.OfHome(ct.Labels, home) && !slices.Contains(runs, ct.Run) {
			runs = append(runs, ct.Run)
		}
	}
	switch len(runs) {
	case 1:
		return runs[0], nil
	case 0:
		return "", fmt.Errorf("no heedle container here belongs to a run of %s; give its run ID with -run RUN (heedle containers lists them)", workflow)
	}
	return "", fmt.Errorf("heedle's containers belong to %d runs of %s (%s); say which with -run RUN", len(runs), workflow, strings.Join(runs, ", "))
}

// imagesCommand lists heedle's images, marking those kept, or removes the
// old ones (-prune): names under bobbin.HeedleRepo of images neither kept
// nor used by a container.
func imagesCommand(ctx context.Context, out io.Writer, keep []string, prune bool) error {
	r := bobbin.Runner{}
	if prune {
		if len(keep) == 0 {
			return errors.New("images -prune needs -keep REF for each image to keep (heedle images gives its own release's)")
		}
		removed, err := r.PruneImages(ctx, bobbin.HeedleRepo, keep)
		if len(removed) == 0 && err == nil {
			fmt.Fprintln(out, "No old images: every heedle image is kept or in use.")
		}
		for _, n := range removed {
			fmt.Fprintf(out, "Removed %s\n", n)
		}
		return err
	}
	ims, err := r.Images(ctx, bobbin.HeedleRepo, keep)
	if err != nil {
		return err
	}
	if len(ims) == 0 {
		fmt.Fprintln(out, "No heedle images.")
		return nil
	}
	var total, old int64
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "IMAGE\tSIZE\tCREATED\tUSED BY\tKEPT")
	for _, im := range ims {
		kept := "no"
		if im.Pinned {
			kept = "yes"
		} else if im.Containers == 0 {
			old += im.Size
		}
		total += im.Size
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n", strings.Join(im.Names, ", "), size(im.Size), im.Created.Format("2006-01-02"), im.Containers, kept)
	}
	tw.Flush()
	fmt.Fprintf(out, "%s in all; -prune removes the %s that isn't kept or used.\n", size(total), size(old))
	return nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func short(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func age(t time.Time) string {
	d := time.Since(t).Round(time.Second)
	switch {
	case d < time.Minute:
		return d.String()
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// size is n bytes for people: 1.2 GB, 340 MB, 12 kB.
func size(n int64) string {
	switch {
	case n >= 1e9:
		return fmt.Sprintf("%.1f GB", float64(n)/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.0f MB", float64(n)/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.0f kB", float64(n)/1e3)
	}
	return fmt.Sprintf("%d B", n)
}

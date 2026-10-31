package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/shuttle"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/weft"
)

// The two commands this example adds to draft. Each is a weft arrow from a
// plan node's input to a string, registered as a Temporal activity.
//
//	(source "weather")   read sources/weather.txt and report its signal
//	(assess "claim")     combine every upstream source into a conclusion
//
// A source file is two lines: a signal ("delay" or "on-time"), then a
// one-line summary.

// Registry is draft's default registry plus source and assess.
func Registry() *registry.Registry {
	reg := draft.DefaultRegistry()
	oneString := registry.ArgSchema{Params: []registry.ParamSpec{{Name: "name", Type: registry.StringT}}}
	temporalOnly := []registry.Backend{registry.BackendTemporal}
	reg.MustRegister(registry.BuiltinSpec{Name: "source", AgentID: "Source", ArgSchema: oneString, Backends: temporalOnly})
	reg.MustRegister(registry.BuiltinSpec{Name: "assess", AgentID: "Assess", ArgSchema: oneString, Backends: temporalOnly})
	return reg
}

// Activities is the part of a Temporal worker the commands need.
type Activities interface {
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// RegisterCommands registers source (reading from dir) and assess.
func RegisterCommands(w Activities, dir string) {
	w.RegisterActivityWithOptions(shuttle.ArrowAsActivity(Source(dir)), activity.RegisterOptions{Name: "Source"})
	w.RegisterActivityWithOptions(shuttle.ArrowAsActivity(Assess()), activity.RegisterOptions{Name: "Assess"})
}

var sourceName = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// Source reads one source file: read >>> format.
func Source(dir string) weft.Arrow[plan.Input, string] {
	read := weft.ArrowFunc(func(_ context.Context, in plan.Input) ([2]string, error) {
		name := in.Args[0]
		if !sourceName.MatchString(name) {
			return [2]string{}, temporal.NewNonRetryableApplicationError("bad source name "+name, "InvalidInput", nil)
		}
		b, err := os.ReadFile(filepath.Join(dir, name+".txt"))
		return [2]string{name, string(b)}, err
	})
	format := weft.Pure(func(f [2]string) string {
		signal, summary, _ := strings.Cut(strings.TrimSpace(f[1]), "\n")
		return fmt.Sprintf("%s: %s: %s\n", f[0], strings.TrimSpace(signal), strings.TrimSpace(summary))
	})
	return weft.Compose(read, format)
}

type evidence struct {
	claim       string
	for_, other []string
}

// Assess judges the claim from its upstream sources: collect >>> render.
// The claim is supported if at least one source signals "delay", and the
// conclusion names exactly which sources did. Names are sorted, so the
// output doesn't depend on node order.
func Assess() weft.Arrow[plan.Input, string] {
	collect := weft.Pure(func(in plan.Input) evidence {
		e := evidence{claim: in.Args[0]}
		for _, out := range in.Upstream {
			parts := strings.SplitN(strings.TrimSpace(out), ": ", 3)
			if len(parts) < 2 {
				continue
			}
			if parts[1] == "delay" {
				e.for_ = append(e.for_, parts[0])
			} else {
				e.other = append(e.other, parts[0])
			}
		}
		sort.Strings(e.for_)
		sort.Strings(e.other)
		return e
	})
	render := weft.Pure(func(e evidence) string {
		verdict := "NOT SUPPORTED"
		if len(e.for_) > 0 {
			verdict = "SUPPORTED"
		}
		return fmt.Sprintf("%s: %s\n  for:     %s\n  against: %s\n",
			e.claim, verdict, list(e.for_), list(e.other))
	})
	return weft.Compose(collect, render)
}

func list(xs []string) string {
	if len(xs) == 0 {
		return "(none)"
	}
	return strings.Join(xs, ", ")
}

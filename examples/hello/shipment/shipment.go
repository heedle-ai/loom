// Package shipment holds the hello world's two commands, source and
// assess, and its program. It is importable so other tools (such as the
// drawdown UI) can offer the example.
package shipment

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
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

// Program is the hello world program: is the shipment late?
const Program = `(pipe
  (par (source "weather") (source "carrier") (source "factory"))
  (assess "the shipment will be late"))
`

// The two commands this example adds to draft. Each is a weft arrow from a
// plan node's input to a string, registered as a Temporal activity.
//
//	(source "weather")   read sources/weather.txt and report its signal
//	(assess "claim")     combine every upstream source into a conclusion
//
// A source file is two lines: a signal ("delay" or "on-time"), then a
// one-line summary.

// SourcesDir is the example's sources folder (examples/hello/sources),
// found next to this file so it works from any directory.
func SourcesDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "sources"
	}
	return filepath.Join(filepath.Dir(file), "..", "sources")
}

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

// Source reads one source file: read >>> format. Its output is
// "<name>: <signal>" plus ": <summary>" when there is one. Anything that can
// never succeed (a bad name, a missing or non-regular file, an unknown
// signal) fails with the non-retryable error type "InvalidInput".
func Source(dir string) weft.Arrow[plan.Input, string] {
	read := weft.ArrowFunc(func(_ context.Context, in plan.Input) ([3]string, error) {
		if len(in.Args) != 1 || !sourceName.MatchString(in.Args[0]) {
			return [3]string{}, invalid("source takes one name of lowercase letters, digits and hyphens, got %q", in.Args)
		}
		name := in.Args[0]
		path := filepath.Join(dir, name+".txt")
		// Lstat, not Stat: a symlink could point anywhere on the machine.
		if fi, err := os.Lstat(path); err != nil || !fi.Mode().IsRegular() {
			return [3]string{}, invalid("source %q: %s is missing or not a regular file", name, path)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return [3]string{}, err
		}
		signal, summary, _ := strings.Cut(strings.TrimSpace(string(b)), "\n")
		signal = strings.TrimSpace(signal)
		if signal != signalDelay && signal != signalOnTime {
			return [3]string{}, invalid("source %q: first line must be %q or %q, got %q", name, signalDelay, signalOnTime, signal)
		}
		return [3]string{name, signal, strings.TrimSpace(summary)}, nil
	})
	format := weft.Pure(func(f [3]string) string {
		if f[2] == "" {
			return f[0] + ": " + f[1] + "\n"
		}
		return f[0] + ": " + f[1] + ": " + f[2] + "\n"
	})
	return weft.Compose(read, format)
}

const (
	signalDelay  = "delay"
	signalOnTime = "on-time"
)

func invalid(format string, args ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, args...), "InvalidInput", nil)
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
	collect := weft.ArrowFunc(func(_ context.Context, in plan.Input) (evidence, error) {
		if len(in.Args) != 1 {
			return evidence{}, invalid("assess takes one claim, got %q", in.Args)
		}
		e := evidence{claim: in.Args[0]}
		for node, out := range in.Upstream {
			name, rest, ok := strings.Cut(strings.TrimSpace(out), ": ")
			signal, _, _ := strings.Cut(rest, ": ")
			switch {
			case !ok:
				return evidence{}, invalid("assess: input from %s isn't a source report: %q", node, out)
			case signal == signalDelay:
				e.for_ = append(e.for_, name)
			case signal == signalOnTime:
				e.other = append(e.other, name)
			default:
				return evidence{}, invalid("assess: input from %s has unknown signal %q", node, signal)
			}
		}
		sort.Strings(e.for_)
		sort.Strings(e.other)
		return e, nil
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

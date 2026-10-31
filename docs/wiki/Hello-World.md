**Is the shipment late?** This one example uses every piece of heedle-ai. Three sources of evidence are read in parallel, an assessor combines them into a conclusion that names its sources, and every run is recorded as a Git commit. When the evidence changes, `git diff` shows exactly which source changed and how the conclusion moved.

## The program

```lisp
(pipe
  (par (source "weather") (source "carrier") (source "factory"))
  (assess "the shipment will be late"))
```

| Piece | What it does here |
|---|---|
| [draft](https://github.com/heedle-ai/draft/wiki) | Compiles the program: `par` gives three independent nodes, and `pipe` makes `assess` wait for all three |
| [weft](https://github.com/heedle-ai/weft/wiki) | `source` and `assess` are arrows, each built with `Compose` (`read >>> format`, `collect >>> render`) |
| [shuttle](https://github.com/heedle-ai/shuttle/wiki) | Runs the three sources in parallel on Temporal, then the assessor |
| [warp](https://github.com/heedle-ai/warp/wiki) | Stores the program, the plan and every output as Git objects in one commit |
| loom | Composes it all: `RunWorkflow = PlanWorkflow >>> Record` |
| [drawdown](https://github.com/heedle-ai/drawdown/wiki) | The web UI: run it from the browser and see the diff there; or use `git log`, `git show` and `git diff` |

The sources are files in `examples/hello/sources/`. Each is a signal on the first line, exactly `delay` or `on-time`, then an optional one-line summary. Anything else (an unknown signal, a missing file, a symlink, a name outside `[a-z0-9-]`) fails the run at once rather than being guessed at.

```text
weather.txt   delay     Storm closes the port of Long Beach for three days.
carrier.txt   on-time   Carrier confirms the container is loaded and sailing on schedule.
factory.txt   delay     Factory line 2 is down for maintenance until Friday.
```

The claim is **supported** if at least one source signals a delay, and the conclusion lists exactly which sources are for and against it.

## Run it

You need the sibling repos cloned and the Temporal CLI installed (see [Getting Started](Getting-Started)).

```bash
temporal server start-dev                          # terminal 1
```

```bash
cd ~/src/heedle-ai/loom                            # terminal 2
go run ./examples/hello -store ~/hello-store.git
```

```text
workflow loom-41b0aaac09ebfaccdd28ef3db9aff2bf149bbdff run <run ID>
n0  weather: delay: Storm closes the port of Long Beach for three days.
n1  carrier: on-time: Carrier confirms the container is loaded and sailing on schedule.
n2  factory: delay: Factory line 2 is down for maintenance until Friday.
n3  the shipment will be late: SUPPORTED
  for:     factory, weather
  against: carrier

recorded as <commit ID> on refs/loom/runs
```

The program starts its own worker, so one terminal plus the dev server is enough. Watch the run at http://localhost:8233: the three `Source` activities start together, and `Assess` starts when all three are done.

Prefer a browser? [drawdown](https://github.com/heedle-ai/drawdown/wiki/Getting-Started) runs this same example with a plan graph, live status and a visual diff.

## Change the evidence, run again

The storm passes:

```bash
printf 'on-time\nStorm has passed; the port reopened this morning.\n' > examples/hello/sources/weather.txt
go run ./examples/hello -store ~/hello-store.git
```

```text
n3  the shipment will be late: SUPPORTED
  for:     factory
  against: carrier, weather
```

Then the factory is repaired:

```bash
printf 'on-time\nLine 2 is repaired; production resumed.\n' > examples/hello/sources/factory.txt
go run ./examples/hello -store ~/hello-store.git
```

```text
n3  the shipment will be late: NOT SUPPORTED
  for:     (none)
  against: carrier, factory, weather
```

Restore the sources with `git checkout examples/hello/sources`.

## Ask Git why

Every run is a commit, each on top of the last (your commit IDs will differ, since they include when each run happened):

```bash
git --git-dir ~/hello-store.git log --format='%h %s' refs/loom/runs
```

```text
9331f40 run loom-41b0aaac09ebfaccdd28ef3db9aff2bf149bbdff
f740fa2 run loom-41b0aaac09ebfaccdd28ef3db9aff2bf149bbdff
9b104ca run loom-41b0aaac09ebfaccdd28ef3db9aff2bf149bbdff
```

All three runs share one workflow ID, because it's the Git ID of the run's input (the program, the plan and the ref), and that didn't change. The commit IDs differ, because the evidence did.

**What changed between the first and second runs?** Exactly the weather source, and the conclusion:

```bash
git --git-dir ~/hello-store.git diff refs/loom/runs~2 refs/loom/runs~1 -- outputs
```

```diff
--- a/outputs/n0
+++ b/outputs/n0
@@ -1 +1 @@
-weather: delay: Storm closes the port of Long Beach for three days.
+weather: on-time: Storm has passed; the port reopened this morning.
--- a/outputs/n3
+++ b/outputs/n3
@@ -1,3 +1,3 @@
 the shipment will be late: SUPPORTED
-  for:     factory, weather
-  against: carrier
+  for:     factory
+  against: carrier, weather
```

**When did the factory evidence change?**

```bash
git --git-dir ~/hello-store.git log --format='%h %s' refs/loom/runs -- outputs/n2
```

lists the first run (when it was recorded) and the third (when it changed).

**What did we conclude at any point?**

```bash
git --git-dir ~/hello-store.git show refs/loom/runs~2:outputs/n3
```

## Checked by tests

`examples/hello/hello_test.go` runs this whole story against a real Git store:

- `TestHello_EvidenceChangesAndGitShowsWhy` runs it three times with changing sources. It checks each output, that history is three commits each on top of the last, that `git diff --name-only` between consecutive runs lists exactly the changed source and the conclusion, that the plan blob is reused, and that `git fsck --strict` passes.
- `TestHello_RunsThroughLoom` runs it through the full `RunWorkflow`.
- `TestHello_BadSourceNameIsRejected` and `TestHello_SourcesAreStrict` check that sources can't escape their directory, that summary-less sources are read correctly, and that malformed input fails without retrying.

To keep the test's store and explore it yourself:

```bash
HELLO_STORE=/tmp/hello-store.git go test -run TestHello_EvidenceChanges ./examples/hello
git --git-dir /tmp/hello-store.git log --format='%h %s' refs/loom/runs
```

## What it shows, and what it doesn't

This is the core of heedle-ai in miniature: a conclusion that names its evidence, and a permanent, diffable history of how it changed. It's deliberately small. The "logic" is one rule (any delay signal supports the claim), sources are local files, and the provenance is the `for` and `against` lists, not yet a structured, queryable graph. Real commands (an LLM call, an API lookup) slot in the same way: a weft arrow registered as a command.

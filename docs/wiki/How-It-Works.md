## The workflow

[[/images/run.svg|alt=String diagram of a run]]

```go
func RunWorkflow(ctx workflow.Context, in RunInput) (RunResult, error)
```

1. Check the input before anything runs. Every node ID must be a valid Git tree entry name (`warp.ValidName`, no I/O). Then the **`Check`** local activity verifies that the ref passes `git check-ref-format` and that the program, compiled with the worker's registry, gives exactly `in.Plan` (compared as `PlanJSON`). Bad names or refs fail at once with the non-retryable error type `InvalidInput`; a plan the program doesn't compile to fails with `ProgramPlanMismatch`. A client can't record a plan its program didn't produce. An empty ref means `refs/loom/runs`.
2. Run `in.Plan` as shuttle's `PlanWorkflow`, as a **child workflow** with ID `<run workflow ID>/plan`. loom doesn't wrap or change it.
3. Run the **`Record`** activity with the program, plan, results, ref, the Temporal workflow and run IDs, and `workflow.Now`.
4. Return the outputs and the commit ID.

`RunWorkflow` uses only Temporal workflow APIs, so it replays deterministically. All Git I/O happens inside `Check` and `Record`; `TestInv_WorkflowsDoNoIO` keeps it that way.

## The record

Each run is one commit on the ref (default `refs/loom/runs`):

```text
program.draft      the source, byte for byte
plan.json          the compiled plan, canonical JSON
outputs/<node ID>  each node's output
files/<node ID>/   the files a node's output carries, as real files (only when some node's does)
beliefs/q-<ID>     the beliefs index: for every question asked on this history, the run ID of its latest run
```

### Step files

An agent step's output lists the files it left in `/work/out` (pirn's agent result). Recording copies each one to `files/<node ID>/<path>` with its exact bytes, so plain Git can show it, list its history and diff it between runs. For a store `$S` whose last two runs had an agent step `n0` that wrote `report.txt`:

```bash
git --git-dir $S show refs/loom/runs:files/n0/report.txt
git --git-dir $S log --format='%h %s' refs/loom/runs -- files/n0/report.txt
git --git-dir $S diff refs/loom/runs~1 refs/loom/runs -- files/
```

Any output that is a JSON object whose key `files` (exactly) is an array of `{"path", "text" or "base64"}` objects is read the same way (`loom.StepFiles`); an entry with neither is an empty file. The rest of what changed in the container, the scratch, is only listed in the output (`changes`), never stored.

Each entry is checked on its own; one that fails stays in the output but not in `files/`, and the step's other files are still recorded:

| Left out | Why |
|---|---|
| Not an object with a string `path`, and a string `text` or `base64`; both of them; bad base64 | Malformed |
| A path segment that is empty, `.` or `..`; starts with `.git` or `git~` in any case; has `~` and a digit; ends in `.` or a space; or holds a character macOS ignores in names | Git refuses these, or treats them as `.git` or `.gitmodules` on some systems, and a store holding one fails `git fsck --strict` and can't be pushed |
| A path listed more than once | Ambiguous: all of them are left out |
| A path that, ignoring case, matches or runs through an earlier file, or matches an earlier directory (in sorted order) | It couldn't be checked out on macOS's case-insensitive disks |

A record holds at most 500 step files in all (`MaxRecordedFiles`): each costs a git process, and recording must finish well inside its timeout. Past that, files are taken in node order, then path order. Which files are recorded depends only on the set of entries, never their order, so recording never fails because of a step's files, and `files/` is derived from the outputs alone: re-running the plan reproduces the outputs, and they reproduce `files/`. Two names differing only in Unicode normalization (`café` written two ways) are both kept; macOS shows them as one file.

These rules decide what a record holds, so a `Record` retried across an upgrade that changes them, after the first attempt reached Git, fails with `RecordMismatch` rather than writing a different record of the same run.

The beliefs index is what makes [Branching and Merging](Branching-and-Merging) precise.

The commit message is `run <workflow ID>`, with Git trailers:

```text
Shuttle-Workflow: <Temporal workflow ID>
Shuttle-Run: <Temporal run ID>
```

The commit time is the workflow's clock at the moment of recording, never the wall clock, so recording the same run twice produces the same commit.

## Naming

| Thing | ID | Why |
|---|---|---|
| Workflow | `loom-` + Git blob ID of the run input: canonical JSON of the program, plan and ref | Same request, same workflow ID. Submitting exactly the same request while it's running joins that run (Temporal's `USE_EXISTING` policy). Any difference, even a comment in the program or another ref, is a different workflow |
| Run | Temporal's run ID | Tells separate executions of the same request apart |
| Record | Git commit ID | Content-addressed; the commit's trailers name the workflow and run |

## Concurrency and retries

Several runs can finish at once and all append to the same ref. `Record` reads the head, builds its commit on top, and moves the ref with warp's compare-and-swap. If another run moved the ref first, the swap fails with `warp.ErrConflict`. The activity fails, and **Temporal's retry policy** runs it again on the new head, backing off from 100 ms to 5 s for up to 10 minutes. Retries are bounded by time, not by a number of attempts, so heavy contention can't exhaust them. loom has no retry loop of its own.

## Idempotency

A Temporal activity can run more than once, and attempts can even overlap: a retry after a timeout can start while the first attempt is still finishing. `Record` reads the ref's head **once**, then looks for a commit with this run's `Shuttle-Run` trailer reachable from that head:

- **found, same tree:** return it. Nothing new is written and the ref doesn't move.
- **found, different tree:** fail with the non-retryable error type `RecordMismatch`. A run ID can never point to two different records.
- **not found:** commit on top of that head and advance from it.

Because the check and the compare-and-swap use the same head, if another attempt records this run in between, the swap fails with a conflict, and the retry then finds that commit. One run is recorded at most once; `TestRecord_ConcurrentAttemptsOfOneRunRecordItOnce` races six attempts of one run, 15 times over.

Input that can never be recorded (an invalid ref or node name) fails with the non-retryable error type `InvalidInput`, so Temporal doesn't retry it.

## Limits

- **Outputs must be small.** Each output is stored in Temporal's history three times: in the child workflow's result, in `Record`'s input, and in `RunWorkflow`'s result. Temporal limits payloads to a few megabytes. A command that produces something large should store it elsewhere and return a reference.
- **Joins see outputs keyed by node ID.** A node after a `par` receives `Upstream`, a map from node ID to output. The built-in `echo` joins them in node-ID **string** order (`n10` sorts before `n2`) and skips empty outputs.

## Using loom as a library

```go
p, err := loom.Compile(ctx, draft.DefaultRegistry(), src)
run, err := loom.Submit(ctx, temporalClient, "loom", loom.RunInput{Program: src, Plan: p})
var res loom.RunResult
err = run.Get(ctx, &res)
```

On the worker:

```go
store, err := warp.Init(ctx, "/path/to/store.git")
w := worker.New(temporalClient, "loom", worker.Options{})
loom.Register(w, store, draft.DefaultRegistry()) // PlanWorkflow, Echo, RunWorkflow, Check, Record
```

To add commands, register them with draft and the worker as described in [draft → Adding Commands](https://github.com/heedle-ai/draft/wiki/Adding-Commands), passing the same registry to `loom.Compile` and `loom.Register`: the worker recompiles every program with it to check the plan. The `loom` command-line tool uses `draft.DefaultRegistry()` (`echo`) plus [bobbin](https://github.com/heedle-ai/bobbin/wiki)'s `container`, which runs a step in a fresh Podman container, and [reed](https://github.com/heedle-ai/reed/wiki)'s `browse`, which loads a page in a fresh browser that may reach only the hosts the program names. Its worker registers all three; give `loom worker` a reed browser image with `-browser-image ID` for `browse` steps to run.

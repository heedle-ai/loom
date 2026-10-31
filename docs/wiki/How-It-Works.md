## The workflow

```go
func RunWorkflow(ctx workflow.Context, in RunInput) (RunResult, error)
```

1. Check that every node ID can be a Git tree entry name (`warp.ValidName`, no I/O). If not, fail at once with the non-retryable error type `InvalidInput`, before anything runs. An empty ref means `refs/loom/runs`.
2. Run `in.Plan` as shuttle's `PlanWorkflow`, as a **child workflow** with ID `<run workflow ID>/plan`. loom doesn't wrap or change it.
3. Run the **`Record`** activity with the program, plan, results, ref, the Temporal workflow and run IDs, and `workflow.Now`.
4. Return the outputs and the commit ID.

`RunWorkflow` uses only Temporal workflow APIs, so it replays deterministically. All Git I/O happens inside `Record`.

## The record

Each run is one commit on the ref (default `refs/loom/runs`):

```text
program.draft      the source, byte for byte
plan.json          the compiled plan, canonical JSON
outputs/<node ID>  each node's output
```

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

Several runs can finish at once and all append to the same ref. `Record` reads the head, builds its commit on top, and moves the ref with warp's compare-and-swap. If another run moved the ref first, the swap fails with `warp.ErrConflict`. The activity fails, and **Temporal's retry policy** runs it again on the new head: up to 20 attempts, backing off from 100 ms to 5 s. loom has no retry loop of its own.

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
loom.Register(w, store)   // PlanWorkflow, Echo, RunWorkflow, Record
```

To add commands, register them with draft and the worker as described in [draft → Adding Commands](https://github.com/heedle-ai/draft/wiki/Adding-Commands), passing your registry to `loom.Compile`. The `loom` command-line tool uses `draft.DefaultRegistry()`, which has `echo` only.

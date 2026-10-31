## The workflow

```go
func RunWorkflow(ctx workflow.Context, in RunInput) (RunResult, error)
```

1. Run `in.Plan` as shuttle's `PlanWorkflow`, as a **child workflow** with ID `<run workflow ID>/plan`. loom doesn't wrap or change it.
2. Run the **`Record`** activity with the program, plan, results, ref, the Temporal workflow and run IDs, and `workflow.Now`.
3. Return the outputs and the commit ID.

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
| Workflow | `loom-` + Git blob ID of `plan.json` | Same plan, same workflow ID. Submitting a plan that's already running joins that run (Temporal's `USE_EXISTING` policy) |
| Run | Temporal's run ID | Tells separate executions of the same plan apart |
| Record | Git commit ID | Content-addressed; points to the plan blob whose ID names the workflow |

## Concurrency and retries

Several runs can finish at once and all append to the same ref. `Record` reads the head, builds its commit on top, and moves the ref with warp's compare-and-swap. If another run moved the ref first, the swap fails with `warp.ErrConflict`. The activity fails, and **Temporal's retry policy** runs it again on the new head: up to 20 attempts, backing off from 100 ms to 5 s. loom has no retry loop of its own.

## Idempotency

A Temporal activity can run more than once: a retry after a timeout might follow a success that wasn't acknowledged. Before committing, `Record` looks for a commit with this run's `Shuttle-Run` trailer:

- **found, same tree:** return it. Nothing new is written and the ref doesn't move.
- **found, different tree:** fail with the non-retryable error type `RecordMismatch`. A run ID can never point to two different records.
- **not found:** commit and advance.

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

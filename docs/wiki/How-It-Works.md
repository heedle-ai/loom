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
uses/<name>        the trees the plan's steps use, such as each skill version it pins (only when it uses any)
beliefs/q-<ID>     the beliefs index: for every question asked on this history, the run ID of its latest run
```

### Step files

An agent step's output lists the files it left in `/work/out` (pirn's agent result). Recording copies each one to `files/<node ID>/<path>` with its exact bytes, so plain Git can show it, list its history and diff it between runs. For a store `$S` whose last two runs had an agent step `n0` that wrote `report.txt`:

```bash
git --git-dir $S show refs/loom/runs:files/n0/report.txt
git --git-dir $S log --format='%h %s' refs/loom/runs -- files/n0/report.txt
git --git-dir $S diff refs/loom/runs~1 refs/loom/runs -- files/
```

Any output that is a JSON object whose key `files` (exactly) is an array of `{"path", "text" or "base64"}` objects is read the same way (`loom.StepFiles`); an entry with neither is an empty file. An entry with `oci`, `sha256` and `size` instead is a file the agent kept in an OCI registry because it was too big for the record ([pirn → Large files](https://github.com/heedle-ai/pirn/wiki/How-It-Works#large-files)): its path holds a small pointer, never an empty file:

```text
oci ghcr.io/acme/heedle-files@sha256:<64 hex digits>
size 5662310
```

`oras blob fetch --output FILE <that reference>` gets the file back. A pointer is an ordinary small text file in Git: what tells it from a step's own text file is the step's output, which marks it with `oci`. The rest of what changed in the container, the scratch, is only listed in the output (`changes`), never stored.

Each entry is checked on its own; one that fails stays in the output but not in `files/`, and the step's other files are still recorded:

| Left out | Why |
|---|---|
| Not an object with a string `path`, and a string `text` or `base64`; both of them; bad base64; `oci` with `text` or `base64`, a repository that isn't `host/path` in lower case, a `sha256` that isn't 64 lower-case hex digits, or no whole non-negative `size`; any of these fields present but `null` | Malformed |
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

## Writes

An agent step granted a connector's write tool (such as sending an email) never runs it: it queues it ([pirn → Connectors](https://github.com/heedle-ai/pirn/wiki/How-It-Works#connectors)). After the plan's steps, `RunWorkflow` takes the queued writes from the agent steps' outputs, in plan order (`QueuedWrites`, run as a local activity, so workflow code decodes no JSON): only from steps whose activity is pirn's `Agent`, and only writes the step's own arguments granted; any other is `refused`, never run.

| Status | Meaning |
|---|---|
| `waiting` | Its connector says `approve`: it waits for a person, up to 7 days (`WriteApprovalTimeout`) |
| `approved` | Approved, or needing no approval: it runs next |
| `declined` | A person said no: never run |
| `expired` | Nobody decided in time: never run |
| `done` | Ran once; `output` is what the tool said |
| `failed` | Its one attempt failed (or its outcome is unknown): never tried again |
| `refused` | Its step wasn't granted it: never run |

A decision is a signal to the run (`WriteSignal`, a `WriteDecision`); `loom writes -workflow ID -approve n0/w1` (or `-decline`) sends one, as drawdown's Approve and Decline buttons do, and `loom writes -workflow ID` lists a run's writes so far (`WritesQuery`). Each approved write runs as pirn's `PirnWrite` activity with one attempt: Temporal never retries it, and a worker that dies mid-write leaves it `failed`, so a write runs at most once (Soundness law 15). Then the record keeps every write and what came of it in `writes.json`; a run with no writes has none. `loom run` prints them after the outputs. A later step in the same run never sees a write's result: writes come after all the steps. A decision is final: the first one for a write stands. A decision sent before the run's writes were known is dropped, so a person decides only a write they could see, and a signal that isn't a decision is ignored. A write runs at most once **per run**: a new run of the same program, or a merge that re-runs a question (`-resolve rerun`), runs its agent steps again and queues their writes again, each waiting for its own approval if its connector says so.

## Changing skills

A worker's skills ([treadle](https://github.com/heedle-ai/treadle/wiki)) change in two ways: `loom skills` on the worker's store, or `SkillsWorkflow` (`loom.ChangeSkills`), which runs one `LoomSkillsChange` activity on a worker. drawdown uses the workflow, since it never writes to the store itself. A change is one of:

| `SkillsChange.Op` | What the worker does |
|---|---|
| `import` | `Library.Import(Source)`: from a repository that isn't trusted, the skill waits for review |
| `add` | `Library.Add(Files)`: a folder given by hand, at most `MaxSkillsChangeBytes` (1 MiB), since it travels as the workflow's input |
| `approve` | `Library.Approve(Name, Version, Replaces)`: exactly the version reviewed, against the version its diff was from |
| `reject` | `Library.Reject(Name, Version)` |
| `create` | `treadle.Create(Description)` with the worker's agent, then `Library.Propose`: Claude writes a skill and its test, and it waits for review. A worker without an agent image refuses. It has `SkillsCreateTimeout` (22 minutes), as long as an agent step |

A refusal (an invalid skill or source, a repository or branch that isn't there, or nothing waiting for review) fails the workflow at once with a `SkillsRefused` error saying why. Only an import is retried (`SkillsImportAttempts`, twice), since fetching again is harmless; a decision that fails is reported, never made twice. A change has `SkillsTimeout` (7 minutes) from being asked for to done: one that no worker finishes by then is dropped, never made later, after the one who asked has been told it failed. It isn't a draft command, so no program, and no agent, can add or approve a skill. A skill added this way runs at once: each run is checked against the library as it is when the run starts.

### Browsing a repository's skills

`CatalogWorkflow` (`loom.ListSkills`) lists the skills a repository holds, on a worker, the same way `loom skills -catalog LINK` does on the command line: `treadle.Catalog` fetches only the `SKILL.md` files, up to three folders down, and gives each skill's name, description and source, or the problem that keeps it from being imported. drawdown's **Browse a repository** uses it, then imports the skills you pick one by one with `import`, so each waits for review like any other import from a repository that isn't trusted. A listing has at most `MaxCatalogEntries` (500) skills in at most `MaxCatalogBytes` (1 MiB) of JSON, well under Temporal's 2 MiB payload limit, and counts the rest (`Omitted`); a link to a folder rather than a repository is refused; a listing that fails to fetch is tried once more, within `CatalogTimeout`. Listing changes nothing.

## Writing programs

`loom write -store DIR "Read the three shipment sources and tell me if the shipment will be late"` (or **Describe a task** in drawdown's New run) has Claude write the program: a `pirn` ask, its sources the draft language (`Language`) and every command the worker has, each with how to call it and what it does (`BuiltinCommands` for built-in ones, a skill's own description for skills). The answer's program is compiled; one that doesn't compile goes back to Claude with the compiler's error, up to three times (`MaxWriteAttempts`) in all, within five minutes (`WriteTimeout`): a failure isn't retried from the start, so Claude is never asked more. The result is a program and its plan, shown before anything runs: running it is an ordinary `loom run`, or drawdown's Run button. drawdown asks a worker, through `WriteProgramWorkflow`, so Claude's key stays on the worker.

It prints the program, then a line saying how many steps it has and on which attempt it compiled. Save it to a file to run it.

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

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
Loom-Program: <the program's own workflow ID>
```

`Loom-Program` is `WorkflowID` of the run's program, plan and ref: the same for every run of it, including a scheduled run, whose workflow ID also has the time Temporal adds. It's how a run finds the previous run of its program (`TestInv_RunsNameTheirProgram`); records made before it was added are matched by `Shuttle-Workflow`.

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

## Schedules

A schedule runs a program again and again: a [Temporal Schedule](https://docs.temporal.io/schedule) (`CreateSchedule`) whose action starts `RunWorkflow` with exactly the input `Submit` would give it, compiled once when the schedule is made, so every run is recorded like any other and its writes wait for approval like any other. Its Temporal ID is `loom-schedule-<name>` (a name is lower-case letters, digits and hyphens, at most 40); each run's workflow ID is the program's (`WorkflowID`) with the time Temporal adds, and its record names the program (`Loom-Program`), so it finds the previous run of the program. It runs every so often (`-every`, whole minutes, at least one), counted from when it was made, so "every 24h" made at 14:25 runs at 14:25; or at the times a five-field cron expression names (`-cron`: `*`, numbers, ranges `N-M`, lists and `/STEP` within each field's range, weekday 0 or 7 Sunday; no names or `@daily`), in `-tz` (an IANA name; UTC by default; only with cron). One run at a time: a run still going when the next is due skips it, while `-run` (run now) waits for it and then starts. A run missed by under 10 minutes (`ScheduleCatchup`; the machine asleep, Temporal down) still starts; older ones are skipped. A run that fails pauses the schedule, with Temporal's note saying so, rather than failing again and again; resume it once fixed (a skill its plan pinned may have gone, say). `ScheduleOptions` checks everything `Submit` does, and the name and times, before Temporal is asked (`TestInv_SchedulesStartRunsLikeSubmit`). `Schedules` lists only schedules on the worker's task queue, as the runs list shows only its runs.

```bash
go run ./cmd/loom schedule -name weekday-digest -cron "0 9 * * 1-5" -tz Europe/Lisbon '(pipe (echo "weekly digest") echo)'
go run ./cmd/loom schedules
go run ./cmd/loom schedules -run weekday-digest
go run ./cmd/loom schedules -pause weekday-digest
go run ./cmd/loom schedules -delete weekday-digest
```

`schedules` lists each with when it runs, whether it's paused and why, its next run and how many recent runs Temporal keeps for it; `-run` starts one now, and `-delete` removes the schedule (its runs' records stay). Temporal's test environment has no schedules, so heedle's end-to-end test makes, runs and deletes one on a real Temporal server.

## Housekeeping: containers, images and stopping a run

Every container a run starts carries heedle's labels (roadmap 9.2, [bobbin → heedle's labels](https://github.com/heedle-ai/bobbin/wiki/How-It-Works#heedles-labels-and-tidying-up)). The worker registers every command through `Labelled`, so each step's activity runs with its run labels: the workflow that scheduled it and that workflow's run, from Temporal's activity info, and the step's node ID. A skill's step, which treadle runs, is labelled the same way. Commands name the kind of container they start.

```bash
go run ./cmd/loom containers
go run ./cmd/loom images
```

`containers` lists heedle's containers: their kind, the run each belongs to and its status, the step, age, image, CPU and memory (`Containers`). A step's labels name the plan workflow that ran it (`<run>/plan`). Temporal says which run started that plan, so the listing shows the run you'd cancel. A container is `Stoppable` only when that run is a run (`LoomRun`) that is still running: a skill's creation, say, is never offered as a run to stop. Each execution is asked about once. Without Temporal the list still shows, with each run's status `unknown`.

- `containers -stop WORKFLOW`, or `cancel WORKFLOW [RUN]`, stops a run through Temporal (`Cancel`). Temporal tells its running steps through their heartbeats, so they end, and Podman removes their containers (`--rm`). The run ends **canceled**, with no record, as a run that fails has none. Only a run is cancelled. Anything else, such as a merge or a plan's own workflow, is refused, and a run that has already ended is left as it is: `cancel` says how it ended. A container is never killed directly.
- `containers -clean` removes the containers of runs that have ended (`Clean`): those whose workflow Temporal says has closed. It leaves a running run's container, one Temporal doesn't know (another Temporal's, perhaps) and one with no run label. Without Temporal it removes nothing.

`images` lists the images named under `ghcr.io/heedle-ai/`, with their sizes, how many containers use each, and which are kept. `images -keep REF ... -prune` removes the old ones: the names of images that aren't kept and that no container uses (`bobbin.PruneImages`). It refuses to prune without `-keep`, so it can't remove the release's own images. `heedle images` gives it the images the current heedle release pins. Your own images, and your own names on heedle's, are never touched.

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

## The front door: ask

`loom ask "Plan me three days in Lisbon from 12 December; I like food and tiles"` (or `heedle ask`, or **Ask heedle** at the top of drawdown's home page) proposes what heedle would do, and runs nothing. A worker asks Claude, through `AskWorkflow`, with the draft language, every command and approved skill it has (as the writer does), the conversation so far, if any (`Earlier`: up to ten turns, `MaxTurns`, so "make it four days" refines the last proposal), and today's date and your time zone (so "next Friday" and "every Monday at 8" mean what you meant). Claude answers in a fixed JSON shape (`AskFormat`): a summary in plain words, and either a program or, when nothing the worker has can do it, a description of a skill that could (for you to create, as drawdown's **Teach heedle** does). If the request says when, the answer also has a schedule: `every` or `cron` with a time zone, and a name. loom checks the answer: one JSON object of the shape asked for, each field once and nothing after it, a summary, exactly one of a program that compiles and a skill, and a schedule only with a program, given one way, sensibly (`ScheduleSpec.Check`) and named, cron times in your time zone unless the answer names another; a program at most 64 KiB, so it can go back with the next turn; anything else goes back to Claude with what's wrong, up to three times (`MaxAskAttempts`) within five minutes (`AskTimeout`), never retried from the start (`TestInv_AskProposesOnlyWhatChecks`, `TestInv_AskTakesOneAnswerAsMeant`, `TestInv_AskingIsAWorkflowThatRunsNothing`). A request that's empty or over 4 KiB, or a day or time zone that isn't one, is refused before Claude is asked (`TestInv_AskRefusesBadRequests`).

**A proposal says whether it can run now.** `Needs` reads, from the compiled plan and not from Claude, what its steps ask for: each skill step's connectors as it would be granted at the version the plan pins (treadle's `StepGrant`, which refuses what the step refuses: a version never added as that skill, or connectors nobody reviewed), each agent step's grants, Claude for any ask, agent or skill step, an agent image for agent and skill steps, which run in a container, a browser image for browse steps, and Claude and a browser-agent image for act steps (`TestInv_NeedsAreWhatStepsAsk`, `TestInv_ActNeedsClaudeAndABrowserAgent`). `CheckReady` checks those against the worker (`Worker`: its connectors, the vault its steps read keys from, and whether it has an agent image, a browser image and a browser-agent image) with [pirn's `Ready`](https://github.com/heedle-ai/pirn/wiki/How-It-Works#connectors), which decides as a starting step would; the worker puts the result in the proposal (`Proposal.Ready`): each connector ready or why not (no connector of that name, tools it doesn't allow, a key not set, a key that can't be read now), with the write tools it would allow and whether they wait for approval; the Anthropic key; an agent image; and a skill version the step would refuse (`TestInv_ProposalsSayIfTheyCanRun`). `loom ask` prints it a line each, with how to set up what's missing; `-then run` runs the proposal and `-then schedule` schedules it as proposed, each only if the worker said it can run now, and a schedule only if the request said when (`TestInv_AskSaysWhatsMissing`). The run or schedule is an ordinary one, recorded like any other; the worker checks the plan against the skill versions it pins, as for every run.

**Act steps are disclosed first.** An `(act "instruction" "https://start" "host"…)` step (roadmap 9.5) has Claude fill in and submit a form in a fresh browser that may reach only the hosts it names ([pirn → Act steps](https://github.com/heedle-ai/pirn/wiki/How-It-Works#act-steps)). It acts without asking again, so before a proposal with act steps runs, `loom ask` says so, naming every host (`ActHosts`: each act step's start host and further hosts, sorted, once each): "can click, type and submit forms on cdn.example.org, forms.example.org without asking again" (`TestInv_ActHostsAreDisclosed`, `TestInv_AskDisclosesActHosts`); drawdown's card shows the same in red. The writer and the front door are told act can't sign in, upload or download, and the front door says so in its summary when a request needs a sign-in (`TestInv_WritersKnowAct`). The card isn't the only way in, so loom asks too: `loom run`, `loom schedule` and `loom ask -then` refuse a program with act steps unless `-allow-act` is given, saying what they'd do on which hosts (`TestInv_ActNeedsAllowAct`), and a merge that resolves by re-running (`-resolve rerun`) won't re-run a question with act steps, which would submit its form again, without `-allow-act` (`MergeInput.AllowAct`, `TestInv_MergeRerunsActOnlyWhenAllowed`). Give the worker `-browser-agent-image ID` for act steps to run; without one, or without the Anthropic key, `loom ask` says what's missing (`TestInv_AskSaysWhatsMissing`).

[[/images/act.svg|alt=An act step: the instruction, start page and hosts become a question; Claude's loop in pirn, bounded, offered only the form tools; each call goes through podman exec to Playwright MCP and Chromium in a fresh reed container that reaches only allowed hosts; a screenshot after every action is heartbeated to the run page; the result is strict JSON recorded in Git]]

## Notices

A person who isn't watching should still hear when something needs them. `Watcher.Check` finds, on a task queue, each run a loom schedule started (Temporal's `TemporalScheduledById` names `loom-schedule-…`) that ended since it began watching, finished or not (a run that fails or times out pauses its schedule, and the notice says so; a canceled one doesn't), and each write waiting for approval in a run going now (`WritesQuery`, asked of at most 25 runs a look, `MaxRunsAsked`, each within 3 seconds). It reads every page of Temporal's list (up to 1,000 runs) and tells each thing once, however often it looks; it forgets runs that ended well before it last looked and writes no longer waiting, so what it remembers stays small. A run you started yourself isn't news (`TestInv_NoticesAreToldOnce`). `heedle up` shows them as the system's notifications.

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

To add commands, register them with draft and the worker as described in [draft → Adding Commands](https://github.com/heedle-ai/draft/wiki/Adding-Commands), passing the same registry to `loom.Compile` and `loom.Register`: the worker recompiles every program with it to check the plan. The `loom` command-line tool uses `draft.DefaultRegistry()` (`echo`) plus [bobbin](https://github.com/heedle-ai/bobbin/wiki)'s `container`, which runs a step in a fresh Podman container, and [reed](https://github.com/heedle-ai/reed/wiki)'s `browse`, which loads a page in a fresh browser that may reach only the hosts the program names. Its worker registers all three, and pirn's `ask`, `agent` and `act`; give `loom worker` a reed browser image with `-browser-image ID` for `browse` steps to run, and a browser-agent image with `-browser-agent-image ID` for `act` steps.

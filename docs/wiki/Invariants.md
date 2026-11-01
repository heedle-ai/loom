Every invariant below is guarded by a test or a CI step, and CI checks this page against the code on every push with `bash .github/invariants.sh`. Only the **Guarded by** column counts. Each test named there must exist in exactly one package and is run, and the gate fails if it fails or skips; each `ci:NAME` must be a step in `.github/workflows/ci.yml`. Every `TestLaw_*`, `TestInv_*` and `Fuzz*` test must be listed, and no test file may have build tags, which would hide tests from CI. A mistake isn't fixed until it has a row on this page. For the categorical view of the same guarantees, see [Soundness](Soundness).

[[/images/invariant-loop.svg|alt=From a mistake to a codified invariant: mistake, named invariant, guard, CI gate]]

[[/images/run.svg|alt=String diagram of a run: source copied, compiled, executed by shuttle, recorded in warp, viewed by drawdown]]

| ID | Invariant | Guarded by | Came from |
|---|---|---|---|
| LOOM-1 | Compiling is a function: the same source gives byte-identical plans | `TestLaw_CompileIsAFunction` | Records must be reproducible |
| LOOM-2 | loom adds nothing to execution: `run`'s outputs equal running the plan with shuttle alone | `TestLaw_LoomAgreesWithShuttle` | Composition only, never mutation |
| LOOM-3 | Records are self-verifying: recompiling `program.draft` gives `plan.json`, and rerunning gives the outputs | `TestLaw_RecordsAreReproducible` | Design |
| LOOM-4 | History only grows: each run's commit sits on the previous head | `TestLaw_RefOnlyMovesForward`, `TestRecord_ConcurrentRunsFormOneLinearHistory` | Design |
| LOOM-5 | A run is recorded at most once, even when retries overlap | `TestRecord_ConcurrentAttemptsOfOneRunRecordItOnce`, `TestRecord_Idempotent` | Review 1, finding 1: a double-record race (3 of 30 trials) |
| LOOM-6 | A run ID can never record two different contents | `TestRecord_SameRunDifferentContentIsRejected` | The Getting Started demo silently returned another run's commit |
| LOOM-7 | A workflow's ID is the Git ID of its whole input (program, plan, ref), so different requests never merge | `TestWorkflowID_IsGitIDOfTheRunInput` | Review 1, finding 10 |
| LOOM-8 | Input that can never be recorded fails before anything runs, and is never retried | `TestRun_InvalidNodeNamesFailBeforeRunning`, `TestRun_InvalidRefFailsBeforeRunning`, `TestRecord_InvalidInputIsNotRetryable` | Reviews 1 and 2: hopeless input was retried 20 times, after the plan had run |
| LOOM-9 | Workflow code is deterministic and does no I/O: workflow functions use only workflow APIs, Temporal types and pure helpers (an allowlist of package-level names) | `ci:workflowcheck`, `TestInv_WorkflowsDoNoIO` | Applied to every workflow after it caught shuttle; review 3 noted workflowcheck doesn't look for I/O |
| LOOM-10 | Only warp runs `git`; nothing here starts a process (no `os/exec`, `syscall`, `golang.org/x/sys` or `os.StartProcess`) | `TestInv_OnlyWarpRunsGit` | Reuse rule: one writer, through one door; review 3 found `syscall` and `os.StartProcess` unbanned |
| LOOM-11 | Every program diagram on this wiki is generated: each fenced Mermaid block follows a program and is exactly what it compiles to | `TestInv_WikiDiagramsMatchPlans` | Docs must never disagree with the code; review 3: an unpaired diagram was never checked |
| LOOM-12 | End to end, a run is compiled, executed, recorded and readable with plain `git` | `TestRun_EndToEnd`, `TestCompileCommand` | Design |
| LOOM-13 | Between runs, `git diff` shows exactly the evidence that changed and how the conclusion moved | `TestHello_EvidenceChangesAndGitShowsWhy`, `TestHello_RunsThroughLoom` | The hello world's promise |
| LOOM-14 | Example commands are strict: no path escapes, no symlinks, exact signals, non-retryable bad input | `TestHello_BadSourceNameIsRejected`, `TestHello_SourcesAreStrict` | Review 2: the hello example's input handling |
| LOOM-15 | A record never pairs a program with a plan it didn't produce: before anything runs, the worker recompiles the program and refuses a different plan (`ProgramPlanMismatch`, non-retryable) | `TestRun_ForgedPlanIsRejected` | Review 3: a client could submit any plan with any program and loom recorded the pair as if compiled |
| LOOM-16 | This page is checked strictly against the code: every listed guard exists once and runs without skipping, every `TestLaw_`, `TestInv_` and `Fuzz` test is listed, and no test file has build tags | `ci:invariants` | Review 3: a reviewer satisfied the old gate with a guard named only in prose, a skipped test and a build-tagged test |

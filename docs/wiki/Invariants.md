Every invariant below is guarded by a test or a CI step, and CI checks this page against the code on every push: each test named here must exist, and every `TestLaw_*`, `TestInv_*` and `Fuzz*` test must be listed here. A mistake isn't fixed until it has a row on this page. For the categorical view of the same guarantees, see [Soundness](Soundness).

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
| LOOM-9 | Workflow code is deterministic | `ci:workflowcheck` (Temporal's own analyzer) | Applied to every workflow after it caught shuttle |
| LOOM-10 | Only warp runs `git`; nothing here runs programs | `TestInv_OnlyWarpRunsGit` | Reuse rule: one writer, through one door |
| LOOM-11 | Program diagrams on this wiki are exactly what their programs compile to | `TestInv_WikiDiagramsMatchPlans` | Docs must never disagree with the code |
| LOOM-12 | End to end, a run is compiled, executed, recorded and readable with plain `git` | `TestRun_EndToEnd`, `TestCompileCommand` | Design |
| LOOM-13 | Between runs, `git diff` shows exactly the evidence that changed and how the conclusion moved | `TestHello_EvidenceChangesAndGitShowsWhy`, `TestHello_RunsThroughLoom` | The hello world's promise |
| LOOM-14 | Example commands are strict: no path escapes, no symlinks, exact signals, non-retryable bad input | `TestHello_BadSourceNameIsRejected`, `TestHello_SourcesAreStrict` | Review 2: the hello example's input handling |

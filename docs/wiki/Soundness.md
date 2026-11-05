This page states what heedle-ai guarantees, in categorical terms, and names the test behind each claim. [Invariants](Invariants) lists every guard, and CI checks it against the code.

[[/images/run.svg|alt=String diagram of a run: run = Δ ; (id ⊗ (compile ; Δ ; (id ⊗ (⟦–⟧ ; writes)))) ; record]]

[[/images/shipment.svg|alt=String diagram of the shipment program: Δ₃ ; (weather ⊗ carrier ⊗ factory) ; assess]]

Notation: `f ; g` is f then g (`pipe`, `Compose`); `f ⊗ g` is f and g side by side (`par`, `Par`); `Δ` copies a value onto several wires. "Holds" means the test exists and passes on every push, on Linux amd64, Linux arm64 and Apple Silicon macOS.

## The structure

| Concept | Where it lives | Meaning |
|---|---|---|
| Morphisms | weft `Arrow[A, B]` | Effectful steps; composition and identity obey the category laws |
| Syntax | draft programs | Terms built with `pipe` (composition) and `par` (product) |
| Compilation | `draft.Compile` | A function from syntax to plans |
| Interpretation | shuttle `PlanWorkflow` | Runs a plan's DAG; independent nodes run in parallel |
| Record | warp commit | Immutable, content-addressed evidence of one run |
| History | warp ref | A DAG of commits; the ref moves only forward |
| Branch | another warp ref | A history started from another's head |
| Merge | a commit with two parents | The union of two histories; beliefs merged per question by Git's three-way merge |

## Laws that hold

| # | Law | Test |
|---|---|---|
| 1 | **Category laws** for arrows: left and right identity, associativity, the `Map` functor laws, `Pipe` = nested `Compose`, and the product and coproduct laws (`Par(f, g) ; Fst = f`, `Left ; Sum(f, g) = f`) | weft `laws_test.go` (property-based, 200 random inputs each) |
| 2 | **`pipe` is sequential composition, `par` is a labelled product:** nested programs compile to exactly the expected dependency graph, including fan-out, fan-in and nesting. A joining node receives each branch's output under that branch's node ID | draft `TestPar_*` in `run_test.go` |
| 3 | **Compiling is a function:** the same source always gives byte-identical plans | loom `TestLaw_CompileIsAFunction` |
| 4 | **loom adds nothing to execution:** `RunWorkflow = Check ; PlanWorkflow ; Writes ; Record`, where `Check` only refuses input and never changes it, `Writes` runs only the writes agent steps queued (none, for a program with no granted writes) and changes no output, and its outputs equal running the plan with shuttle alone, for every program shape tested | loom `TestLaw_LoomAgreesWithShuttle` |
| 5 | **Records are self-verifying:** recompiling a commit's `program.draft`, with the command versions its `plan.json` pins (such as skills, whose trees the record keeps under `uses/`), reproduces that plan byte for byte, and re-running that plan reproduces its outputs. A run is checked against its own pins, never against what is newest when it runs | loom `TestLaw_RecordsAreReproducible`, `TestLaw_PinnedRunsCheckAgainstTheirPins` |
| 6 | **History only grows:** each run's commit has the previous head as its parent, so the ref moves only forward | loom `TestLaw_RefOnlyMovesForward`, `TestRecord_ConcurrentRunsFormOneLinearHistory` |
| 7 | **Identity is content:** equal content has equal IDs; a workflow's ID is the Git ID of its input (program, plan, ref), so any difference in the request is a different workflow | warp `TestBlob_ContentAddressed`, loom `TestWorkflowID_IsGitIDOfTheRunInput` |
| 8 | **Refs are linearizable and only move forward:** of N writers racing from the same head, exactly one wins; a ref can only move to a commit that descends from its current one | warp `TestAdvance_ExactlyOneConcurrentWinner`, `TestAdvance_OnlyFastForwardToCommits` |
| 9 | **Recording is idempotent and unambiguous:** re-recording a run returns the same commit; a run ID can't record two different contents; overlapping attempts of one run record it exactly once | loom `TestRecord_Idempotent`, `TestRecord_SameRunDifferentContentIsRejected`, `TestRecord_ConcurrentAttemptsOfOneRunRecordItOnce` |
| 10 | **Evidence is traceable with Git:** between two runs of the same program, `git diff` lists exactly the outputs whose evidence changed | loom `TestHello_EvidenceChangesAndGitShowsWhy` |
| 11 | **Records are honest:** a record's plan is always what its program compiles to, with the command versions that plan pins. The diagram's `compile` wire holds even when a client sends the plan, because the worker recompiles and refuses a mismatch | loom `TestRun_ForgedPlanIsRejected` |
| 12 | **Merge is the union of histories:** every run of both sides stays in the history, and the merged beliefs are exactly both sides' questions, each answered by the three-way rule (one side changed: that side; both changed differently: the chosen resolution) | loom `TestLaw_MergeIsUnionOfHistories`, warp `TestLaw_MergeTreesIsThreeWayMerge` |
| 13 | **Merge is commutative, associative and idempotent** on beliefs: A + B = B + A (ours on one side is theirs on the other), (A + B) + C = A + (B + C) without conflicts, and merging what's already merged changes nothing | loom `TestLaw_MergeIsCommutative`, `TestLaw_MergeIsAssociative`, `TestLaw_MergeIsIdempotent` |
| 14 | **Every commit's files are a real run,** merges included: a merge commit's `program.draft`, `plan.json` and `outputs/` are its first parent's run | loom `TestLaw_MergeRecordsStaySelfVerifying` |
| 15 | **Writes run at most once per run:** a write an agent step queued, and was granted, runs once after the run's steps (once approved, if it must be), with one attempt, never retried, however often its step was retried; a write it wasn't granted, or one in an output that isn't an agent step's, never runs; the record keeps every write and what came of it | loom `TestLaw_WritesRunAtMostOnce`, `TestInv_WritesWaitForApproval`; pirn `TestLaw_GrantsOnlyNarrow` (a step gets only what both it and the worker grant) |
| 16 | **What a program needs is a homomorphism:** needs(f ; g) = needs(f) ∪ needs(g) = needs(f ⊗ g), and a step that needs nothing is the unit. Needs (connectors with their tools, Claude, an agent image, a browser image, a browser-agent image and the hosts act steps disclose) form a join-semilattice under ∪, so a program's needs depend only on its steps, never on bracketing or order | loom `TestLaw_NeedsIsAHomomorphism` (300 random programs) |
| 17 | **Ready is a meet:** for a fixed worker, ready(f ; g) = ready(f) ∧ ready(g) = ready(f ⊗ g). The front door's verdict on a whole proposal is the conjunction of its steps', and adding a step never makes a plan readier | loom `TestLaw_ReadyIsAMeet` (300 random programs, four workers) |
| 18 | **Ask adds nothing to compilation:** a proposal's plan is exactly what its program compiles to, so the graph the card draws, and what Run submits and the worker recompiles (law 11), are one plan | loom `TestLaw_ProposalsCompileAsRunsDo` |
| 19 | **A schedule's run is a run:** each run a schedule starts is `RunWorkflow` with the same input `Submit` would give it, so laws 3 to 15 hold for scheduled runs too. A schedule with a budget checks it first, and either lets the run go on unchanged or ends it having run and recorded nothing | loom `TestInv_SchedulesStartRunsLikeSubmit` |
| 21 | **Usage is a homomorphism:** usage(f ; g) = usage(f) + usage(g) = usage(f ⊗ g), and a step that asks Claude nothing is the unit. Usages (each Claude step's model, tokens and requests, or why it has none) form a monoid under +, and their tokens per model, missing counts and estimated cost are commutative, so what a run used depends only on its steps, never on bracketing or order. | loom `TestLaw_UsageIsAHomomorphism` (300 random programs) |

## The front door

[[/images/front-door.svg|alt=String diagram of the front door: propose = ask ; check ; compile ; Δ ; (id ⊗ (needs ; ready)); nothing runs]]

Asking heedle is outside the run: it proposes, and Run is the ordinary run of the proposed program (laws 18, 11). Claude's part is the only arrow that isn't a function of its input; everything after it is checked and computed by loom, and what the proposal needs is computed from the plan, never taken from Claude (laws 16, 17).

## Where the guarantees stop

- **Law 5 is about determinism, not truth.** `echo` is deterministic, so re-running reproduces outputs. A command that calls the outside world (weather, prices) won't reproduce, and the record is then the only evidence of what it returned. That's the record's purpose.
- **Effects are outside the category.** Retries can repeat a side effect of a command, so commands with effects must be idempotent. A connector's declared writes are the exception: a step only queues them, and loom runs each at most once (law 15). A write whose single attempt fails without saying whether it happened is recorded as failed, never tried again: at most once, not exactly once. And it is at most once per run: a new run (resubmitting the program, or a merge's `rerun`) queues its writes again.
- **`par`'s product is labelled by node ID.** Branches are told apart by the IDs draft assigns (`n0`, `n1`, …), not by position. Join order is whatever the joining command chooses; `echo` uses node-ID string order.
- **`par` is a product of plans, not of arrows in one process.** Each branch is a separate Temporal activity; the laws above are checked for the shapes in the tests, not proved for every program.
- **Merges are of whole runs.** A merge never combines two runs into one; a question both sides answered differently is a conflict you resolve. After a criss-cross merge, questions the common ancestors disagree on conflict unless both sides agree. See [Branching and Merging](Branching-and-Merging).
- **Identity uses SHA-1,** Git's default. That's sound for content heedle-ai writes, but not a defence against deliberately crafted collisions.
- **Laws 16 and 17 are about one moment.** Ready says what a step starting now would get; a key deleted or a sign-in that expires between the check and the run still fails the run, which then says what's missing (drawdown's **What went wrong**).
- **A proposal's summary is Claude's words.** It isn't checked against the program. The card therefore shows the program, its graph and what each connector could change (writes, and whether they wait for approval), and nothing runs until you press Run.
- **Law 21 counts what is recorded.** A step's tokens are Claude's own counts, exactly; a run that failed, or an attempt that failed and was retried, isn't recorded, so isn't counted, by the Costs page or by a budget. Dollars are estimates from a price table of a date, never a bill.

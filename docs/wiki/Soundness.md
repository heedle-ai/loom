This page states what heedle-ai guarantees, in categorical terms, and names the test behind each claim. "Holds" means the test exists and passes on every push, on Linux amd64, Linux arm64 and Apple Silicon macOS.

## The structure

| Concept | Where it lives | Meaning |
|---|---|---|
| Morphisms | weft `Arrow[A, B]` | Effectful steps; composition and identity obey the category laws |
| Syntax | draft programs | Terms built with `pipe` (composition) and `par` (product) |
| Compilation | `draft.Compile` | A function from syntax to plans |
| Interpretation | shuttle `PlanWorkflow` | Runs a plan's DAG; independent nodes run in parallel |
| Record | warp commit | Immutable, content-addressed evidence of one run |
| History | warp ref | A chain of commits; the ref moves only forward |

## Laws that hold

| # | Law | Test |
|---|---|---|
| 1 | **Category laws** for arrows: left and right identity, associativity, the `Map` functor laws, `Pipe` = nested `Compose` | weft `laws_test.go` (property-based, 200 random inputs each) |
| 2 | **`pipe` is sequential composition, `par` is the product:** nested programs compile to exactly the expected dependency graph, including fan-out, fan-in and nesting | draft `TestPar_*` in `run_test.go` |
| 3 | **Compiling is a function:** the same source always gives byte-identical plans | loom `TestLaw_CompileIsAFunction` |
| 4 | **loom adds nothing to execution:** `RunWorkflow = PlanWorkflow >>> Record`, and its outputs equal running the plan with shuttle alone, for every program shape tested | loom `TestLaw_LoomAgreesWithShuttle` |
| 5 | **Records are self-verifying:** recompiling a commit's `program.draft` reproduces its `plan.json` byte for byte, and re-running that plan reproduces its outputs | loom `TestLaw_RecordsAreReproducible` |
| 6 | **History only grows:** each run's commit has the previous head as its parent, so the ref moves only forward | loom `TestLaw_RefOnlyMovesForward`, `TestRecord_ConcurrentRunsFormOneLinearHistory` |
| 7 | **Identity is content:** equal content has equal IDs; a workflow's ID is the Git ID of its plan | warp `TestBlob_ContentAddressed`, loom `TestWorkflowID_IsGitIDOfPlan` |
| 8 | **Refs are linearizable:** of N writers racing from the same head, exactly one wins | warp `TestAdvance_ExactlyOneConcurrentWinner` |
| 9 | **Recording is idempotent and unambiguous:** re-recording a run returns the same commit; a run ID can't record two different contents | loom `TestRecord_Idempotent`, `TestRecord_SameRunDifferentContentIsRejected` |

## Where the guarantees stop

- **Law 5 is about determinism, not truth.** `echo` is deterministic, so re-running reproduces outputs. A command that calls the outside world (weather, prices) won't reproduce, and the record is then the only evidence of what it returned. That's the record's purpose.
- **Effects are outside the category.** Retries can repeat a side effect. Commands with effects must be idempotent.
- **`par` is a product of plans, not of arrows in one process.** Each branch is a separate Temporal activity; the laws above are checked for the shapes in the tests, not proved for every program.
- **No merge yet.** Every run appends to one linear history. Merging divergent histories needs a merge defined on knowledge, not Git's text merge, and isn't built.
- **Identity uses SHA-1,** Git's default. That's sound for content heedle-ai writes, but not a defence against deliberately crafted collisions.

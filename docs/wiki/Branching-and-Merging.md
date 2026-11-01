A **branch** is a separate line of history: a what-if explored without touching main. A **merge** brings it back. Both are ordinary Git: a branch is a ref, a merge is a commit with two parents. What loom adds is a precise meaning for merging *knowledge*, so a merge never produces a record no run ever made.

[[/images/merge.svg|alt=Branching and merging: a what-if branch, a merge commit joining both histories, and Git's three-way merge of the beliefs indexes, where a question changed on one side takes that side's whole run and a question changed on both is a conflict]]

## Questions and beliefs

Every run answers a **question**: its program and plan. The question's ID is `q-` plus the Git blob ID of the canonical JSON of both (`loom.QuestionID`), so the same program asks the same question on any branch.

Every record carries a **beliefs index**, `beliefs/`: one entry per question ever asked on that history, holding the **run ID** of its latest run on one line. The run ID resolves to that run's commit through its `Shuttle-Run` trailer, so a belief always points into the history itself: `git gc`, clones and pushes keep everything it needs. Recording a run updates its own question's entry and carries every other one over unchanged:

```text
program.draft      this run's source
plan.json          this run's plan
outputs/<node ID>  this run's outputs
beliefs/q-<ID>     for every question: the run ID of its latest run
```

`loom beliefs -store DIR [-ref REF]` prints what a history currently believes, and `loom.Beliefs` reads it in Go. Entries are only ever added or replaced, never deleted. Every reader checks the index: an entry that isn't a blob named `q-<ID>` holding one run ID, or that names a run not in the history, is refused (`InvalidInput`), never merged or followed.

## Branching

```bash
loom branch -from refs/loom/runs -to refs/loom/what-if
```

**Sub-agents** are branches named with a dot: `loom branch -from refs/loom/research -to refs/loom/research.web` makes `research.web` a sub-agent of `research`, and [drawdown](https://github.com/heedle-ai/drawdown/wiki/How-It-Works) shows the tree with one browser tab per agent. (Dots, because Git can't hold both `refs/loom/research` and `refs/loom/research/web`.) Merging a sub-agent back is an ordinary merge.

`BranchWorkflow` pins the source's head, then creates the new ref there (warp's create-only compare-and-swap). It never moves an existing ref: if the target already exists at another commit it fails with `BranchExists`. A retry that finds the branch at the pinned head succeeds, even if the source has moved on since. Run on the branch with `loom run -ref refs/loom/what-if …`.

## Merging

```bash
loom merge -into refs/loom/runs -from refs/loom/what-if [-resolve ours|theirs|rerun]
```

`MergeWorkflow` pins the source's head, then does one of three things:

| Case | Result |
|---|---|
| The source is already in the target's history | Nothing changes (`up-to-date`) |
| The target has nothing the source lacks | The target moves to the source's head (`fast-forward`), no new commit |
| Both have new runs | A **merge commit** with parents *target head*, *source head* |

A merge commit is the **union** of both histories: every run on either side stays reachable. Its own files (`program.draft`, `plan.json`, `outputs/`) are the target's latest run, unchanged, so every commit's files are still a real run. Its `beliefs/` index is **Git's own three-way merge** (`git merge-tree`, through warp's `MergeTrees`) of the two sides' indexes against their common ancestor's. Each path is a question and each value is one line, a run ID, so Git compares whole runs:

| Question | Merged belief |
|---|---|
| Asked, or answered again, on one side only | That side's run |
| Answered by the same run on both, or by two runs that recorded identical files | That answer |
| Answered again on both sides since they diverged, differently | A **conflict** |

Because values are run IDs, not contents, a side that ran a question again counts as having answered it, even if its new answer happens to equal the common ancestor's.

After a criss-cross (A merged B and B merged A independently) there are several best common ancestors. The base is then what they all agree on: a question they disagree on counts as new on both sides, and conflicts unless both sides now agree. Nothing is picked silently.

Conflicts stop the merge with the non-retryable error `MergeConflict`, which names each conflicting question with its program and both sides' answers. Nothing moves. Merge again with a resolution:

| `-resolve` | The conflicting questions |
|---|---|
| `ours` | Keep the target's run |
| `theirs` | Take the source's run |
| `rerun` | First run the question again on the target (a child `RunWorkflow`), then merge keeping that fresh run, so the belief is today's evidence. If a re-run fails, nothing is merged; submitting the merge again starts over |

Each resolution is recorded on the merge commit as a trailer, `Loom-Resolve: <question> <ours|theirs|rerun>`, next to `Loom-Merge: <source head>`. Because every belief is a whole run, a merge never pairs one run's evidence with another run's conclusion. After merging, loom checks rather than trusts the result: the merged index must hold every question from both sides, each answered by one side's run (`MergeInvariant` otherwise).

Writes are retried by Temporal like `Record`: if another writer moves the target first, the activity fails with a conflict and runs again on the new head. A retry that finds its own merge commit returns it. Re-runs are tagged with the target in their memo, so drawdown lists them on the right history while they run.

## A what-if, end to end

This is `TestHello_WhatIfBranchAndMerge`, on the [Hello World](Hello-World) sources. Every output below is checked by the test.

1. On main, the storm closes the port and the factory is down: the shipment will be late, supported by factory and weather.
2. Branch `refs/loom/what-if`. On it, suppose the factory is fixed, and also ask a second question, `(pipe (source "factory") (assess "production will slip"))`:

   ```text
   the shipment will be late: SUPPORTED
     for:     weather
     against: carrier, factory
   production will slip: NOT SUPPORTED
     for:     (none)
     against: factory
   ```

3. Meanwhile on main, the storm passes (the factory is still down): late, supported by factory alone.
4. Merge without a resolution. Both sides changed the shipment question, so it's refused, and main doesn't move:

   ```text
   1 question(s) answered differently on both sides; merge again with resolve ours, theirs or rerun:
   q-b82ea7b9a95e1f2dfaa0867ea61ed1d86d39741e (pipe (par (source "weather") (source "carrier") (source "factory")) (assess "the shipment will be late"))
     ours:   the shipment will be late: SUPPORTED for: factory against: carrier, weather
     theirs: the shipment will be late: SUPPORTED for: weather against: carrier, factory
   ```

   Both say SUPPORTED, but for different reasons, which is exactly why the merge doesn't pick one silently.

5. The factory really is fixed now. Merge with `rerun`: the shipment question runs again on main on today's evidence, then the merge keeps that run; the production question, asked only on the branch, joins main as it was:

   ```text
   the shipment will be late: NOT SUPPORTED
     for:     (none)
     against: carrier, factory, weather
   production will slip: NOT SUPPORTED
     for:     (none)
     against: factory
   ```

`git log --graph --format=%s refs/loom/runs` on the store then shows both histories joined, with the re-run on main just before the merge:

```text
*   merge <what-if head> into refs/loom/runs
|\
| * run <workflow ID>
| * run <workflow ID>
* | run <merge workflow ID>/rerun-0
* | run <workflow ID>
|/
* run <workflow ID>
```

## Laws

On the [Soundness](Soundness) page, each checked by a `TestLaw_*` test on random histories:

- **Union:** a merge keeps every run of both sides, and its beliefs are exactly the union of both sides' questions, each following the three-way rule.
- **Commutative:** merging B into A believes the same as merging A into B (resolving `ours` on one equals `theirs` on the other).
- **Associative:** with no conflicts, (A + B) + C believes the same as A + (B + C).
- **Idempotent:** merging something already merged changes nothing.
- **Records stay real runs:** a merge commit's own files are its first parent's run.

## Where it stops

- **Criss-cross merges are conservative.** With several best common ancestors, any question they disagree on conflicts unless both sides agree now, even if one side never touched it since.
- **A re-run reads today's evidence.** That's its purpose, but it means the new belief can differ from both sides.
- **Questions are compared by exact program and plan.** Two programs that ask the same thing in different words are different questions, and never conflict.
- **No deletions.** A belief can be replaced by a newer run but never removed. warp's `MergeTrees` relies on this: Git always follows renames, which only matters when a side deletes a path.

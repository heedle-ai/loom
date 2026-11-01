# loom

Runs draft programs on shuttle and records every run in warp. Part of heedle-ai:
weft (algebra) → draft (language) → shuttle (execution) → bobbin (runtime) →
warp (object store) → drawdown (UI); reed gives browsing
agents a browser; loom runs them together.

## The first rule: reuse, never rewrite

Build on battle-tested tools (Git, Temporal, the Go standard library and
other long-proven software) through their public interfaces. Add on top of
them; never fork, patch, mutate or reimplement them. Prefer the real tool to
a reimplementation of it (for example the `git` CLI over a Go port of Git).
If a task seems to need a rewrite, stop and ask the owner first, explaining
why the existing tool can't do it.

## Rules for every change

1. **Keep the wiki current.** Any change that affects how a developer
   installs, builds, tests, uses or understands this repo updates
   `docs/wiki/` in the same commit or PR. That folder is the source of truth;
   `.github/workflows/wiki.yml` mirrors it to
   https://github.com/heedle-ai/loom/wiki on every push to `main`. Never edit
   the wiki on GitHub directly: the next sync overwrites it. Code, README and
   wiki must never disagree.
2. **Wiki quality bar.** Every command is copy-pasteable and has been run.
   Every code snippet compiles and is checked by a test; prefer quoting a
   Go `Example` test with an `// Output:` comment. State required versions.
   No placeholders, TODOs or "coming soon" on published pages. Mark design
   that isn't built yet as **Planned**, plainly.
3. **Platforms: Linux (amd64, arm64) and macOS on Apple Silicon only.** Don't
   write Windows instructions, scripts, CI jobs or path handling.
4. **Bare minimum.** Add only what the task needs. No speculative packages,
   dependencies or abstractions.
5. **Tests pass before commit:** `go vet ./...` and `go test -race ./...`.
6. **Codify every mistake.** A mistake isn't fixed until it has a row in
   `docs/wiki/Invariants.md` (ID, invariant, guard, where it came from) and
   a guard that fails on the old code: a test, a fuzz seed, or a CI step
   (`ci:NAME`). `bash .github/invariants.sh` checks the page against the
   code in both directions and runs in CI. Name guard tests `TestLaw_*` or
   `TestInv_*`; every `Fuzz*` test is a guard. Keep inputs that ever broke a
   fuzz test in `testdata/fuzz/`.
7. **Show, don't tell.** Explain structure with string diagrams: SVGs in
   `docs/wiki/images/` that work in light and dark mode, using `;` for
   "then" and `⊗` for "side by side". Program diagrams are never drawn by
   hand: put a generated Mermaid block straight after the program, and the
   `TestInv_WikiDiagramsMatchPlans` test keeps them identical.
8. **Get an independent review before significant pushes.** A fresh
   reviewer that hasn't seen the work tries to break it; every finding
   becomes an invariant row.
9. **Confirm soundness after every change.** The laws on the wiki's
   Soundness page are tests (`TestLaw_*`). Run them, and report which hold.

## Repo-specific

- **Composition only.** loom composes draft, shuttle and warp. It must not
  change their behaviour: `TestLaw_LoomAgreesWithShuttle` checks this.
- **Workflow code is deterministic.** Workflows use only workflow APIs;
  all warp I/O happens in activities (`TestInv_WorkflowsDoNoIO`).
- **Conflicts are retried by Temporal,** not by loops in loom.
- `go.mod` replaces the heedle-ai modules with `../` checkouts until they are
  published; clone all repos as siblings.

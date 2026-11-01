**loom** is where the heedle-ai pieces run together. It compiles a [draft](https://github.com/heedle-ai/draft/wiki) program, runs it durably with [shuttle](https://github.com/heedle-ai/shuttle/wiki) on Temporal, and records the run in [warp](https://github.com/heedle-ai/warp/wiki) as a Git commit.

[[/images/run.svg|alt=String diagram of a run: the source is copied and compiled; the plan is copied and executed by shuttle on Temporal; program, plan and outputs are recorded in warp; drawdown reads the commit]]

Read it top to bottom: wires carry values, boxes are steps, a dot copies a wire (`Δ`), side by side is `⊗` and one after another is `;`. Every box is an existing, tested piece. loom's own code is the wiring, `RunWorkflow = PlanWorkflow ; Record`, plus a small command-line tool.

## What you get

- **Durable runs.** Temporal retries failed steps and survives worker crashes.
- **A permanent record.** Each run is a commit on `refs/loom/runs`, holding the exact program, the compiled plan and every node's output. Read it with ordinary `git`.
- **A two-way link.** Each commit's trailers name its Temporal workflow and run. Each workflow is named after the Git ID of its input.
- **Checked soundness.** The laws on [Soundness](Soundness) are tests that run on every push.

## The repos

| Repo | Role |
|---|---|
| [weft](https://github.com/heedle-ai/weft/wiki) | Composition algebra: what a step *is* |
| [draft](https://github.com/heedle-ai/draft/wiki) | Language: how you *write* a computation |
| [shuttle](https://github.com/heedle-ai/shuttle/wiki) | Durable execution: how the work *ran* |
| [bobbin](https://github.com/heedle-ai/bobbin/wiki) | A fresh Podman container per agent step |
| [warp](https://github.com/heedle-ai/warp/wiki) | Content-addressed store: what the system *knows* |
| [drawdown](https://github.com/heedle-ai/drawdown/wiki) | Web UI: write, run and browse programs and records |
| **loom** | Runs them together (this repo) |

## Pages

- [Hello World](Hello-World): is the shipment late? Every piece in one small story
- [Getting Started](Getting-Started): run a program end to end and inspect it
- [How It Works](How-It-Works): the workflow, the record format, retries and idempotency
- [Branching and Merging](Branching-and-Merging): what-ifs on branches, and merging knowledge without inventing records
- [Soundness](Soundness): the laws loom guarantees, and the test behind each
- [Invariants](Invariants): every guarantee, the test or check that guards it, and the mistake it came from
- [Contributing](Contributing)

## Requirements

- Go **1.24** or later, `git`, and the Temporal CLI
- Linux (amd64 or arm64), or macOS on Apple Silicon

# loom

Runs [draft](https://github.com/heedle-ai/draft) programs on [shuttle](https://github.com/heedle-ai/shuttle) and records every run in [warp](https://github.com/heedle-ai/warp). Part of [heedle-ai](https://github.com/heedle-ai).

```bash
temporal server start-dev                                  # terminal 1
go run ./cmd/loom worker -store ~/heedle-store.git         # terminal 2
go run ./cmd/loom run '(pipe (par (echo "a") (echo "b")) echo)'   # terminal 3
git --git-dir ~/heedle-store.git log refs/loom/runs        # every run, as a Git commit
```

A run is two existing pieces in sequence: shuttle's `PlanWorkflow`, then a `Record` activity that commits the program, plan and outputs to warp. loom adds that composition and nothing else.

Documentation: **[wiki](https://github.com/heedle-ai/loom/wiki)**

```bash
go test -race ./...   # clone weft, draft, shuttle and warp next to this repo first
```

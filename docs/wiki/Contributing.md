## Ground rules

- **Reuse, never rewrite.** loom composes draft, shuttle, warp, Git and Temporal through their public APIs. If a change seems to need modifying or reimplementing one of them, stop and ask first.
- **Composition only.** loom must not change the behaviour of the pieces it composes; `TestLaw_LoomAgreesWithShuttle` checks this.
- **A mistake isn't fixed until it has a row on [Invariants](Invariants)** and a guard that fails on the old code. CI checks the page against the tests in both directions.
- **Every change ends with a soundness check.** Run the tests and confirm each law on [Soundness](Soundness) still holds. A change that adds a guarantee adds its law and test there.
- **Update this wiki in the same change.** The pages live in `docs/wiki/` in this repo, and every push to `main` publishes them here.
- Supported platforms are Linux (amd64, arm64) and macOS on Apple Silicon.

## Before opening a pull request

```bash
gofmt -l .        # prints nothing
go vet ./...
go test -race ./...
go test -run 'Law' -v ./...   # the soundness laws, by name
bash .github/invariants.sh
go run go.temporal.io/sdk/contrib/tools/workflowcheck@v0.6.0 ./...
```

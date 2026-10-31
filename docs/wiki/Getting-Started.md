## 1. Install Go, Git and the Temporal CLI

**macOS (Apple Silicon)**

```bash
brew install go git temporal
```

**Linux (amd64 or arm64)**

```bash
sudo apt-get install -y git        # or: sudo dnf install -y git
GO_VERSION=1.24.7
ARCH=$(uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')
curl -fsSLO "https://go.dev/dl/go${GO_VERSION}.linux-${ARCH}.tar.gz"
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf "go${GO_VERSION}.linux-${ARCH}.tar.gz"
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile && source ~/.profile
```

For the Temporal CLI on Linux, use `brew install temporal` if you have Homebrew, or download it from Temporal's [CLI setup page](https://docs.temporal.io/cli/setup-cli).

## 2. Clone all the repos as siblings

The modules refer to each other by relative path until they're published:

```bash
mkdir -p ~/src/heedle-ai && cd ~/src/heedle-ai
for r in weft draft shuttle warp loom; do git clone https://github.com/heedle-ai/$r.git; done
cd loom
go test -race ./...
```

The tests need no Temporal server: they run the real workflows in Temporal's in-process test suite, against real Git stores in temporary directories.

## 3. Compile a program (runs nothing)

```bash
go run ./cmd/loom compile '(pipe (par (echo "a") (echo "b")) echo)'
```

prints the plan as JSON: `n0` and `n1` with no dependencies, and `n2` requiring both.

## 4. Run it

Terminal 1, the Temporal dev server (gRPC on `localhost:7233`, Web UI on http://localhost:8233):

```bash
temporal server start-dev
```

Terminal 2, a worker that records into a warp store (created if missing):

```bash
cd ~/src/heedle-ai/loom
go run ./cmd/loom worker -store ~/heedle-store.git
```

Terminal 3:

```bash
cd ~/src/heedle-ai/loom
go run ./cmd/loom run '(pipe (par (echo "a") (echo "b")) echo)'
```

```text
workflow loom-c9fb904cb1266678057634d4bdb6f45b303f5e99 run <run ID>
commit   <commit ID> on refs/loom/runs
n0       a
n1       b
n2       a b
```

The workflow ID is always the same for this program, because it's the Git ID of the compiled plan. The run ID and commit ID differ per run.

## 5. Inspect runs with Git

```bash
S=~/heedle-store.git
git --git-dir $S log --format='%h %s%n%(trailers:only,unfold)' refs/loom/runs
git --git-dir $S ls-tree -r --name-only refs/loom/runs
git --git-dir $S show refs/loom/runs:program.draft
git --git-dir $S show refs/loom/runs:outputs/n2
git --git-dir $S diff --stat refs/loom/runs~1 refs/loom/runs    # once you have two runs
git --git-dir $S fsck --strict
```

After two runs, the log looks like this:

```text
5102e44 run loom-11a9c0156802f8997c9bcc0770b2135aca3caeb5
Shuttle-Workflow: loom-11a9c0156802f8997c9bcc0770b2135aca3caeb5
Shuttle-Run: 0a1b2c3d-run-2

38731d5 run loom-c9fb904cb1266678057634d4bdb6f45b303f5e99
Shuttle-Workflow: loom-c9fb904cb1266678057634d4bdb6f45b303f5e99
Shuttle-Run: 0a1b2c3d-run-1
```

The `Shuttle-Workflow` value is what you search for in Temporal's Web UI to see the execution behind a commit: its timings, retries and full event history.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `connect to Temporal at localhost:7233` | The dev server isn't running |
| `run` waits forever | No worker on the `loom` queue; start one, or pass the same `-queue` to both |
| `unknown builtin "…"` | The command isn't registered; see [draft → Adding Commands](https://github.com/heedle-ai/draft/wiki/Adding-Commands) |
| `go: … ../warp: no such file` | A sibling repo isn't cloned |

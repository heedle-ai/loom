> **One binary:** [heedle](https://github.com/heedle-ai/heedle/wiki) offers every command below (`heedle run`, `heedle compile`, …). loom's command-line tool is also a library, `cli.Run`.

## 1. Install Go, Git, the GitHub CLI and the Temporal CLI

**macOS (Apple Silicon)**

```bash
brew install go git gh temporal
```

**Linux (amd64 or arm64)**

```bash
sudo apt-get install -y git gh     # or: sudo dnf install -y git gh
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
for r in weft draft shuttle warp bobbin reed pirn treadle loom; do git clone https://github.com/heedle-ai/$r.git; done
cd loom
go test -race ./...
```

The tests need no Temporal server: they run the real workflows in Temporal's in-process test suite, against real Git stores in temporary directories.

For a fuller example, see [Hello World](Hello-World).

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
workflow loom-807ad5ae49ffdcead915401c2e8633be94b798ae run <run ID>
commit   <commit ID> on refs/loom/runs
n0       a
n1       b
n2       a b
```

The workflow ID is always the same for this program and ref, because it's the Git ID of the run's input (program, plan and ref). The run ID and commit ID differ per run.

### Ask Claude

The worker also runs pirn's `ask` when it has your Anthropic API key. Restart it in terminal 2 with the key in its environment:

```bash
ANTHROPIC_API_KEY=sk-ant-... go run ./cmd/loom worker -store ~/heedle-store.git
```

Or keep the key in your keychain (the macOS Keychain, or the Linux secret service through `secret-tool`) once, and start the worker without it: the worker reads the environment first, then the keychain, for every request. `secret set` asks for the value without showing it.

```bash
go run ./cmd/loom secret set ANTHROPIC_API_KEY
go run ./cmd/loom worker -store ~/heedle-store.git
```

Connectors' keys work the same way: a connector that names `MAPS_KEY` finds it in the environment or the keychain from its next step. `loom secret delete NAME` removes one, and `loom secret delete --all` every key heedle keeps (asking first at a terminal).

Then in terminal 3:

```bash
go run ./cmd/loom run '(pipe (par (echo "The port is closed.") (echo "The ship sailed on time.")) (ask "Will the cargo be late? Cite the sources."))'
```

The last output is Claude's answer as JSON: its text, the model (`claude-sonnet-5-5` unless the worker sets `-ask-model`) and token counts. For `(agent "…")` steps, where Claude works in a fresh container, also give the worker `-agent-image <image ID>`, and `-agent-registry <repository>` to keep files too big for the record in an OCI registry. Give it `-connectors <file>` to offer agent steps MCP servers (it reads the file again before every step, so editing it needs no restart, and a file that isn't there yet is no connectors), which a step uses only as its arguments grant (`(agent "…" "linear/search_issues")`); see [pirn → Connectors](https://github.com/heedle-ai/pirn/wiki/How-It-Works#connectors). A run whose steps queue writes runs each once after its steps; `loom writes -workflow <ID>` lists them and approves or declines the ones that wait ([Writes](How-It-Works#writes)). `(image "<Containerfile>")` builds an image ([bobbin → Image builds](https://github.com/heedle-ai/bobbin/wiki/How-It-Works#image-builds)); give the worker `-image-registry <repository>` for `(image … "push")`; see [pirn → Agent steps](https://github.com/heedle-ai/pirn/wiki/How-It-Works#agent-steps). For `(act "…" "https://start" "host"…)` steps, which fill in a form, give it `-browser-agent-image <image ID>` ([reed → A browser-agent image](https://github.com/heedle-ai/reed/wiki/Getting-Started#6-a-browser-agent-image-for-act)). On Linux, give the worker `-browser-image` and the sources can be live pages; see [pirn](https://github.com/heedle-ai/pirn/wiki/Getting-Started).

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
5d5738e run loom-e9cc13255d1334447dd36e99e2a181a61b1aa4fd
Shuttle-Workflow: loom-e9cc13255d1334447dd36e99e2a181a61b1aa4fd
Shuttle-Run: 0a1b2c3d-run-2

72d3927 run loom-807ad5ae49ffdcead915401c2e8633be94b798ae
Shuttle-Workflow: loom-807ad5ae49ffdcead915401c2e8633be94b798ae
Shuttle-Run: 0a1b2c3d-run-1
```

The `Shuttle-Workflow` value is what you search for in Temporal's Web UI to see the execution behind a commit: its timings, retries and full event history.

## 6. What heedle remembers

The front door (`loom ask`, or **Ask heedle** in drawdown) sends Claude what you've asked heedle to remember about you with every request, and the proposal says which ([How It Works → Memory you can see](How-It-Works#the-front-door-ask)). A worker reads it from `-memory FILE`; heedle up's is `~/.heedle/memory.json`, which `loom memory` uses unless you give `-memory FILE`:

```bash
loom memory -add "Vegetarian."
loom memory -add "Home airport: IAD."
loom memory
```

```
remembered m78fbf5839f81: Vegetarian.
remembered mba723a3ad8f0: Home airport: IAD.
heedle remembers, and sends to Claude with every request (/home/you/.heedle/memory.json):
  m78fbf5839f81  added 2026-11-05, never used  Vegetarian.
  mba723a3ad8f0  added 2026-11-05, never used  Home airport: IAD.
```

It won't keep a secret:

```bash
loom memory -add "my password is hunter2"
```

```
loom: heedle can't remember that: that looks like a password, which heedle never remembers (keep keys in your keychain: heedle secret set NAME)
```

Forget one by its ID, or everything; either rewrites the file without them:

```bash
loom memory -forget m78fbf5839f81
loom memory -forget-all
```

```
forgot m78fbf5839f81; it's gone from /home/you/.heedle/memory.json
forgot everything; /home/you/.heedle/memory.json is empty
```

## Troubleshooting

| Symptom | Cause |
|---|---|
| `connect to Temporal at localhost:7233` | The dev server isn't running |
| `run` waits forever | No worker on the `loom` queue; start one, or pass the same `-queue` to both |
| `unknown builtin "…"` | The command isn't registered; see [draft → Adding Commands](https://github.com/heedle-ai/draft/wiki/Adding-Commands) |
| `go: … ../warp: no such file` | A sibling repo isn't cloned |

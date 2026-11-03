package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/testsuite"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/pirn"
	"github.com/heedle-ai/reed"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

func TestCompileCommand(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"compile", `(pipe (echo "a") echo)`}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"requires": [`) {
		t.Errorf("unexpected plan:\n%s", out.String())
	}
	if err := run(context.Background(), []string{"compile", `(pipe (echo "a") shout)`}, &out); err == nil ||
		!strings.Contains(err.Error(), `unknown builtin "shout"`) {
		t.Errorf("err = %v, want unknown builtin", err)
	}
	// bobbin's container command is available to programs.
	img := strings.Repeat("ab", 32)
	if err := run(context.Background(), []string{"compile", `(container "` + img + `" "a")`}, &out); err != nil ||
		!strings.Contains(out.String(), `"activity": "Container"`) {
		t.Errorf("container program: %v\n%s", err, out.String())
	}
	// reed's browse command is available too; its allowlist is in the plan.
	if err := run(context.Background(), []string{"compile", `(browse "https://a.example/" "cdn.example")`}, &out); err != nil ||
		!strings.Contains(out.String(), `"activity": "Browse"`) || !strings.Contains(out.String(), `"cdn.example"`) {
		t.Errorf("browse program: %v\n%s", err, out.String())
	}
	// pirn's ask command is available too, after browse in a pipeline.
	out.Reset()
	if err := run(context.Background(), []string{"compile", `(pipe (browse "https://a.example/") (ask "Summarize."))`}, &out); err != nil ||
		!strings.Contains(out.String(), `"activity": "Ask"`) || !strings.Contains(out.String(), `"Summarize."`) {
		t.Errorf("ask program: %v\n%s", err, out.String())
	}
	// and pirn's agent, with its 20-minute budget in the plan.
	out.Reset()
	if err := run(context.Background(), []string{"compile", `(agent "Tidy up.")`}, &out); err != nil ||
		!strings.Contains(out.String(), `"activity": "Agent"`) || !strings.Contains(out.String(), `"timeout": 1200000000000`) {
		t.Errorf("agent program: %v\n%s", err, out.String())
	}
	// and bobbin's image, with its 30-minute budget, over several lines.
	out.Reset()
	if err := run(context.Background(), []string{"compile", "(pipe (agent \"Write app.json.\") (image \"FROM docker.io/library/alpine@sha256:" + img + "\nCOPY in/n0.txt /app/agent-result.json\"))"}, &out); err != nil ||
		!strings.Contains(out.String(), `"activity": "Image"`) || !strings.Contains(out.String(), `"timeout": 1800000000000`) ||
		!strings.Contains(out.String(), `COPY in/n0.txt`) {
		t.Errorf("image program: %v\n%s", err, out.String())
	}
	for _, bad := range [][]string{nil, {"bogus"}, {"compile"}, {"worker"}} {
		if err := run(context.Background(), bad, &out); err == nil {
			t.Errorf("run(%v) succeeded, want usage error", bad)
		}
	}
}

func TestBeliefsCommand(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir() + "/store.git"
	s, err := warp.Init(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	rec := &loom.Recorder{Store: s}
	for i, q := range []string{"a", "b", "a"} {
		src := `(echo "` + q + `")`
		p, err := loom.Compile(ctx, draft.DefaultRegistry(), src)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rec.Record(ctx, loom.RecordInput{Program: src, Plan: p, Ref: loom.DefaultRef,
			Result:     plan.Result{Outputs: map[string]string{"n0": q + " v" + string(rune('1'+i))}},
			WorkflowID: "wf", RunID: "run-" + string(rune('1'+i)), At: time.Unix(1700000000+int64(i), 0)}); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := run(ctx, []string{"beliefs", "-store", dir}, &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	// Two questions; a's belief is its latest run (v3), not the first.
	if strings.Count(got, "(echo ") != 2 || !strings.Contains(got, "n0     a v3") || strings.Contains(got, "a v1") || !strings.Contains(got, "n0     b v2") {
		t.Errorf("beliefs output:\n%s", got)
	}
	for _, bad := range [][]string{{"beliefs"}, {"beliefs", "-store", dir, "-ref", "refs/loom/none"}, {"merge", "extra"}} {
		if err := run(ctx, bad, &out); err == nil {
			t.Errorf("run(%v) succeeded, want an error", bad)
		}
	}
}

// TestSkillsCommand: loom skills adds a skill's folder to a store and
// lists it; loom compile with that store turns the skill into a command
// pinned to its version.
func TestSkillsCommand(t *testing.T) {
	dir := t.TempDir()
	store, folder := dir+"/store.git", dir+"/weather"
	os.MkdirAll(folder+"/scripts", 0o755)
	os.WriteFile(folder+"/SKILL.md", []byte("---\nname: weather\ndescription: Says the weather for a city.\n---\nRun scripts/get.sh.\n"), 0o644)
	os.WriteFile(folder+"/scripts/get.sh", []byte("echo sunny\n"), 0o644)
	var out bytes.Buffer
	if err := run(context.Background(), []string{"skills", "-store", store, "-add", folder}, &out); err != nil || !strings.HasPrefix(out.String(), "added weather at ") {
		t.Fatalf("add: %v %s", err, out.String())
	}
	version := strings.TrimSpace(strings.TrimPrefix(out.String(), "added weather at "))
	out.Reset()
	if err := run(context.Background(), []string{"skills", "-store", store}, &out); err != nil || !strings.Contains(out.String(), "weather") || !strings.Contains(out.String(), version[:12]) {
		t.Errorf("list: %v %s", err, out.String())
	}
	out.Reset()
	if err := run(context.Background(), []string{"compile", "-store", store, `(weather "Boston")`}, &out); err != nil ||
		!strings.Contains(out.String(), `"activity": "Skill"`) || !strings.Contains(out.String(), version) {
		t.Errorf("compile: %v %s", err, out.String())
	}
	if err := run(context.Background(), []string{"compile", `(weather "Boston")`}, &out); err == nil {
		t.Error("compiled a skill without its store")
	}
	// Hidden files aren't part of a skill (review 13: a stray .DS_Store
	// refused the whole folder); and a skill named like a command is left
	// out of compiles, not fatal (review 13: it stopped every worker).
	os.WriteFile(folder+"/.DS_Store", []byte{0, 1}, 0o644)
	os.MkdirAll(folder+"/.git", 0o755)
	os.WriteFile(folder+"/.git/HEAD", []byte("x"), 0o644)
	out.Reset()
	if err := run(context.Background(), []string{"skills", "-store", store, "-add", folder}, &out); err != nil || !strings.Contains(out.String(), version) {
		t.Errorf("adding with hidden files: %v %s", err, out.String())
	}
	os.MkdirAll(dir+"/agent", 0o755)
	os.WriteFile(dir+"/agent/SKILL.md", []byte("---\nname: agent\ndescription: Shadows the agent command.\n---\nx\n"), 0o644)
	if err := run(context.Background(), []string{"skills", "-store", store, "-add", dir + "/agent"}, &out); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := run(context.Background(), []string{"compile", "-store", store, `(weather "Boston")`}, &out); err != nil || !strings.Contains(out.String(), version) {
		t.Errorf("compile beside a bad skill: %v %s", err, out.String())
	}
	// Review 13: run couldn't submit a program using a skill.
	err := run(context.Background(), []string{"run", "-store", store, "-address", "127.0.0.1:1", `(weather "Boston")`}, &out)
	if err == nil || !strings.Contains(err.Error(), "connect to Temporal") {
		t.Errorf("run -store: %v", err)
	}
	os.Symlink("/etc/passwd", folder+"/passwd")
	if err := run(context.Background(), []string{"skills", "-store", store, "-add", folder}, &out); err == nil {
		t.Error("added a folder with a symbolic link")
	}
}

// TestWorkerCommandsAreTheRegistry: a worker registers exactly the
// commands registry() has, so checking a run against registry() with its
// plan's skill pins checks it against the worker's own commands.
// (Review 13: nothing checked that the two stay the same.)
func TestWorkerCommandsAreTheRegistry(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	reg, err := workerCommands(env, pirn.Claude{}, pirn.Agent{}, reed.Browser{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(reg.Names(), " "), strings.Join(registry().Names(), " "); got != want {
		t.Errorf("worker commands %s, registry() %s", got, want)
	}
	// No program, so no agent, can change skills: no command runs
	// loom's skills workflow or its activity.
	for _, name := range reg.Names() {
		if spec, _ := reg.Lookup(name); spec.AgentID == loom.SkillsChangeName || spec.AgentID == loom.SkillsWorkflowName {
			t.Errorf("the command %s changes skills", name)
		}
	}
}

// TestImportCommands: loom skills -catalog lists a repository's skills,
// -import keeps one with its source, and -add - keeps a pasted SKILL.md.
func TestImportCommands(t *testing.T) {
	dir := t.TempDir()
	repo := dir + "/repo"
	os.MkdirAll(repo+"/skills/pdf", 0o755)
	os.WriteFile(repo+"/skills/pdf/SKILL.md", []byte("---\nname: pdf\ndescription: Reads PDFs.\n---\nx\n"), 0o644)
	for _, a := range [][]string{{"init", "-q", "-b", "main"}, {"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "x"}} {
		if out, err := exec.Command("git", append([]string{"-C", repo}, a...)...).CombinedOutput(); err != nil {
			t.Fatalf("%v %s", err, out)
		}
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"skills", "-catalog", "file://" + repo}, &out); err != nil || !strings.Contains(out.String(), "skills/pdf") || !strings.Contains(out.String(), "pdf: Reads PDFs.") {
		t.Errorf("catalog: %v %s", err, out.String())
	}
	store := dir + "/store.git"
	out.Reset()
	if err := run(context.Background(), []string{"skills", "-store", store, "-trust", "file://" + dir}, &out); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := run(context.Background(), []string{"skills", "-store", store, "-import", "file://" + repo, "-path", "skills/pdf"}, &out); err != nil || !strings.HasPrefix(out.String(), "added pdf at ") {
		t.Errorf("import: %v %s", err, out.String())
	}
	stdin = strings.NewReader("---\nname: brief\ndescription: Writes a brief.\n---\nBe short.\n")
	defer func() { stdin = os.Stdin }()
	out.Reset()
	if err := run(context.Background(), []string{"skills", "-store", store, "-add", "-"}, &out); err != nil || !strings.HasPrefix(out.String(), "added brief at ") {
		t.Errorf("paste: %v %s", err, out.String())
	}
	out.Reset()
	if err := run(context.Background(), []string{"skills", "-store", store}, &out); err != nil || !strings.Contains(out.String(), "brief") || !strings.Contains(out.String(), "pdf") {
		t.Errorf("list: %v %s", err, out.String())
	}
	// Review 14: a long paste was cut silently, and conflicting or
	// stray flags were ignored.
	stdin = strings.NewReader("---\nname: long\ndescription: d\n---\n" + strings.Repeat("x", 1<<20))
	if err := run(context.Background(), []string{"skills", "-store", store, "-add", "-"}, &out); err == nil {
		t.Error("a pasted SKILL.md over the limit was kept")
	}
	for _, bad := range [][]string{
		{"skills", "-store", store, "-add", "x", "-import", "file://" + repo},
		{"skills", "-store", store, "-path", "skills/pdf"},
		{"skills", "-store", store, "-import", "https://github.com/a/b/tree/main/x", "-branch", "v1"},
		{"skills", "-catalog", "https://github.com/a/b/tree/main/skills"},
	} {
		if err := run(context.Background(), bad, &out); err == nil {
			t.Errorf("%v succeeded", bad)
		}
	}
}

// TestReviewCommands: an import from an untrusted repository waits; the
// skills list shows it waiting; -review shows its files and diff with
// terminal escapes made visible; -approve takes exactly the version shown
// and makes it a command; -reject closes a review; -trust and -untrust
// change which imports wait.
func TestReviewCommands(t *testing.T) {
	dir := t.TempDir()
	repo := dir + "/repo"
	os.MkdirAll(repo, 0o755)
	git := func(a ...string) {
		if out, err := exec.Command("git", append([]string{"-C", repo, "-c", "user.name=t", "-c", "user.email=t@x"}, a...)...).CombinedOutput(); err != nil {
			t.Fatalf("%v %s", err, out)
		}
	}
	os.WriteFile(repo+"/SKILL.md", []byte("---\nname: pdf\ndescription: Reads PDFs.\n---\nx\n"), 0o644)
	os.WriteFile(repo+"/run.sh", []byte("echo \x1b]0;owned\x07 \u202e\n"), 0o644)
	os.WriteFile(repo+"/notes.md", []byte("hidden\x00\n"), 0o644)
	git("init", "-q", "-b", "main")
	git("add", "-A")
	git("commit", "-q", "-m", "x")
	store := dir + "/store.git"
	var out bytes.Buffer
	do := func(args ...string) (string, error) {
		out.Reset()
		err := run(context.Background(), append([]string{"skills", "-store", store}, args...), &out)
		return out.String(), err
	}
	got, err := do("-import", "file://"+repo)
	if err != nil || !strings.Contains(got, "waits for review") {
		t.Fatalf("import: %v %s", err, got)
	}
	version := strings.Fields(got)[2]
	if got, _ := do(); !strings.Contains(got, "waiting for review") || !strings.Contains(got, "pdf") {
		t.Errorf("list: %s", got)
	}
	got, err = do("-review", "pdf")
	if err != nil || !strings.Contains(got, "a new skill") || !strings.Contains(got, "run.sh") || !strings.Contains(got, "+echo") ||
		strings.ContainsAny(got, "\x1b\x07\u202e\x00") || !strings.Contains(got, `\u001b`) || !strings.Contains(got, `\u202e`) ||
		!strings.Contains(got, "SKILL.md, in full:\n---\nname: pdf") || !strings.Contains(got, "read them yourself before approving:\n  notes.md") ||
		!strings.Contains(got, "approve: loom skills -store "+store+" -approve pdf@"+version+"\n") {
		t.Errorf("review: %v %q", err, got)
	}
	if _, err := do("-replacing", version); err == nil {
		t.Error("-replacing without -approve passed")
	}
	if _, err := do("-compile"); err == nil {
		t.Error("a stray flag passed")
	}
	if _, err := do("-approve", "pdf"); err == nil {
		t.Error("approved without a version")
	}
	if _, err := do("-approve", "pdf@"+strings.Repeat("0", 40)); err == nil {
		t.Error("approved a version that wasn't reviewed")
	}
	if got, err := do("-approve", "pdf@"+version); err != nil || !strings.HasPrefix(got, "approved pdf at "+version) {
		t.Errorf("approve: %v %s", err, got)
	}
	out.Reset()
	if err := run(context.Background(), []string{"compile", "-store", store, `(pdf "a.pdf")`}, &out); err != nil || !strings.Contains(out.String(), version) {
		t.Errorf("compile after approval: %v %s", err, out.String())
	}

	os.WriteFile(repo+"/run.sh", []byte("echo two\n"), 0o644)
	git("commit", "-q", "-am", "two")
	got, _ = do("-import", "file://"+repo)
	two := strings.Fields(got)[2]
	// Approving a change needs the version the review diffed it from.
	got, _ = do("-review", "pdf")
	if !strings.Contains(got, " -approve pdf@"+two+" -replacing "+version+"\n") {
		t.Errorf("review of a change: %s", got)
	}
	if _, err := do("-approve", "pdf@"+two); err == nil {
		t.Error("approved a change without -replacing")
	}
	if got, err := do("-reject", "pdf@"+two); err != nil || !strings.HasPrefix(got, "rejected pdf") {
		t.Errorf("reject: %v %s", err, got)
	}
	if got, _ := do(); strings.Contains(got, "waiting for review") {
		t.Errorf("still waiting after rejection: %s", got)
	}
	if _, err := do("-trust", "https://github.com"); err == nil {
		t.Error("trusted a whole host")
	}
	if _, err := do("-trust", "file://"+dir); err != nil {
		t.Fatal(err)
	}
	if got, _ := do("-trusted"); strings.TrimSpace(got) != "file://"+dir {
		t.Errorf("trusted: %q", got)
	}
	if got, err := do("-import", "file://"+repo); err != nil || !strings.HasPrefix(got, "added pdf at "+two) {
		t.Errorf("trusted import: %v %s", err, got)
	}
	if _, err := do("-untrust", "file://"+dir+"/"); err == nil {
		t.Error("untrusted a prefix that isn't trusted")
	}
	if _, err := do("-untrust", "file://"+dir); err != nil {
		t.Fatal(err)
	}
	if got, _ := do("-trusted"); got != "" {
		t.Errorf("after untrust: %q", got)
	}
	if _, err := do("-trust", "file://"+dir, "-review", "pdf"); err == nil {
		t.Error("two actions at once passed")
	}
}

// TestInv_WritesCommand: loom writes lists a run's writes from Temporal
// (escaping what a tool said), and -approve or -decline NODE/ID signals
// exactly that decision first; anything else is refused before Temporal
// is asked.
func TestInv_WritesCommand(t *testing.T) {
	c := &mocks.Client{}
	v := &mocks.Value{}
	v.On("Get", mock.Anything).Run(func(args mock.Arguments) {
		*(args.Get(0).(*[]loom.WriteOutcome)) = []loom.WriteOutcome{{Node: "n0", Status: loom.WriteDone, Output: "sent \x1b[2J",
			Write: pirn.Write{ID: "w1", Connector: "mail", Tool: "send", Input: json.RawMessage(`{"to":"a"}`)}}}
	}).Return(nil)
	c.On("QueryWorkflow", mock.Anything, "loom-x", "", loom.WritesQuery).Return(v, nil)
	var got loom.WriteDecision
	c.On("SignalWorkflow", mock.Anything, "loom-x", "", loom.WriteSignal, mock.Anything).
		Run(func(args mock.Arguments) { got = args.Get(4).(loom.WriteDecision) }).Return(nil)
	var out bytes.Buffer
	if err := writesCommand(context.Background(), &out, c, "loom-x", "", "n0/w2"); err != nil {
		t.Fatal(err)
	}
	if got != (loom.WriteDecision{Node: "n0", ID: "w2", Approve: false}) {
		t.Errorf("signalled %+v", got)
	}
	if want := "write    n0/w1 done mail/send {\"to\":\"a\"}\n         sent \\u001b[2J\n"; out.String() != want {
		t.Errorf("got %q, want %q", out.String(), want)
	}
	if err := writesCommand(context.Background(), &out, c, "loom-x", "w1", ""); err == nil {
		t.Error("a decision without NODE/ means nothing")
	}
	// A worker's -connectors file is read and checked before it starts.
	dir := t.TempDir()
	os.WriteFile(dir+"/ok.json", []byte(`{"connectors":[{"name":"mail","url":"https://mail.example.com/mcp","writes":["send"]}]}`), 0o644)
	os.WriteFile(dir+"/bad.json", []byte(`{"connectors":[{"name":"mail","url":"http://mail.example.com/mcp"}]}`), 0o644)
	if cs, err := readConnectors(dir + "/ok.json"); err != nil || len(cs) != 1 {
		t.Errorf("connectors %+v %v", cs, err)
	}
	if _, err := readConnectors(dir + "/bad.json"); err == nil {
		t.Error("an http:// connector was accepted")
	}
	if cs, err := readConnectors(""); err != nil || cs != nil {
		t.Errorf("no file: %v %v", cs, err)
	}
	// A file not made yet is no connectors: the worker reads it before
	// every step, so it may come later.
	if cs, err := readConnectors(dir + "/later.json"); err != nil || cs != nil {
		t.Errorf("a file not made yet: %v %v", cs, err)
	}
	for _, bad := range [][]string{{"writes"}, {"writes", "-workflow", "x", "-approve", "n0/w1", "-decline", "n0/w1"}} {
		if err := run(context.Background(), bad, &out); err == nil {
			t.Errorf("%v succeeded", bad)
		}
	}
}

// TestInv_WriterSeesEveryCommand: loom write and loom skills -create ask
// Claude, so they need a key (and -create an agent image) and say so; the
// writer is told about every command the worker has, built-in ones as
// loom.BuiltinCommands describes them and each skill by its own
// description.
func TestInv_WriterSeesEveryCommand(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	var out bytes.Buffer
	dir := t.TempDir()
	store := dir + "/store.git"
	if err := run(context.Background(), []string{"write", "Say hi."}, &out); err == nil || !strings.Contains(err.Error(), "ANTHROPIC_API_KEY") {
		t.Errorf("write without a key: %v", err)
	}
	if err := run(context.Background(), []string{"skills", "-store", store, "-create", "Sum a column."}, &out); err == nil || !strings.Contains(err.Error(), "-agent-image") {
		t.Errorf("create without a key or image: %v", err)
	}
	os.MkdirAll(dir+"/triage", 0o755)
	os.WriteFile(dir+"/triage/SKILL.md", []byte("---\nname: triage\ndescription: Triages bugs by severity.\n---\nGo.\n"), 0o644)
	if err := run(context.Background(), []string{"skills", "-store", store, "-add", dir + "/triage"}, &out); err != nil {
		t.Fatal(err)
	}
	reg, cmds, err := commandsFor(context.Background(), store, registry())
	if err != nil {
		t.Fatal(err)
	}
	about := map[string]string{}
	for _, c := range cmds {
		about[c.Name] = c.About
	}
	for _, name := range reg.Names() {
		if about[name] == "" || about[name] == "A command this worker has." {
			t.Errorf("the writer isn't told what %s does", name)
		}
	}
	if about["triage"] != "A skill: Triages bugs by severity." {
		t.Errorf("triage: %q", about["triage"])
	}
}

type fakeKeychain map[string]string

func (f fakeKeychain) Lookup(_ context.Context, name string) (string, bool, error) {
	v, ok := f[name]
	return v, ok, nil
}
func (f fakeKeychain) Store(_ context.Context, name, value string) error {
	if err := pirn.CheckSecret(name, value); err != nil {
		return err
	}
	f[name] = value
	return nil
}
func (f fakeKeychain) Delete(_ context.Context, name string) error { delete(f, name); return nil }
func (f fakeKeychain) DeleteAll(context.Context) (int, error) {
	n := len(f)
	clear(f)
	return n, nil
}

// TestInv_DeleteAllAsksAtATerminal: `loom secret delete --all` deletes
// every heedle key only after "yes" at a terminal, and at once from a
// script, as an uninstaller runs it. (Roadmap 7.1.)
func TestInv_DeleteAllAsksAtATerminal(t *testing.T) {
	ctx := context.Background()
	kc := fakeKeychain{"A": "1", "B": "2"}
	oldVault, oldTerm := vault, isTerminal
	t.Cleanup(func() { vault, isTerminal, stdin = oldVault, oldTerm, os.Stdin })
	vault = kc
	var out bytes.Buffer
	isTerminal = func() bool { return true }
	stdin = strings.NewReader("no\n")
	if err := Run(ctx, []string{"secret", "delete", "--all"}, &out); err != nil || len(kc) != 2 {
		t.Errorf("not yes: %v %v", err, kc)
	}
	stdin = strings.NewReader("yes\n")
	if err := Run(ctx, []string{"secret", "delete", "--all"}, &out); err != nil || len(kc) != 0 || !strings.Contains(out.String(), "Deleted 2 heedle keys") {
		t.Errorf("yes: %v %v %q", err, kc, out.String())
	}
	kc["C"] = "3"
	isTerminal = func() bool { return false }
	if err := Run(ctx, []string{"secret", "delete", "--all"}, &out); err != nil || len(kc) != 0 {
		t.Errorf("from a script: %v %v", err, kc)
	}
}

// TestInv_SecretSetNeverShowsTheValue: `loom secret set NAME` reads the
// value from standard input, never an argument; at a terminal it turns
// echo off first and back on after, even when storing fails or Ctrl-C
// ends the prompt, and reads nothing if it can't; nothing it prints holds
// the value; delete removes it. (Roadmap 7.1; review 26.)
func TestInv_SecretSetNeverShowsTheValue(t *testing.T) {
	ctx := context.Background()
	kc := fakeKeychain{}
	oldVault, oldTerm, oldNoEcho := vault, isTerminal, noEcho
	t.Cleanup(func() { vault, isTerminal, noEcho, stdin = oldVault, oldTerm, oldNoEcho, os.Stdin })
	vault = kc
	var calls []string
	noEcho = func(context.Context) (func(), error) {
		calls = append(calls, "-echo")
		return func() { calls = append(calls, "echo") }, nil
	}
	isTerminal = func() bool { return true }

	const value = "AIzaSy-maps-key.123"
	stdin = strings.NewReader(value + "\n")
	var out bytes.Buffer
	if err := Run(ctx, []string{"secret", "set", "MAPS_KEY"}, &out); err != nil {
		t.Fatal(err)
	}
	if kc["MAPS_KEY"] != value || strings.Contains(out.String(), value) || strings.Join(calls, ",") != "-echo,echo" {
		t.Errorf("stored %q, printed %q, stty %v", kc["MAPS_KEY"], out.String(), calls)
	}

	calls = nil
	stdin = strings.NewReader("has space\n")
	if err := Run(ctx, []string{"secret", "set", "MAPS_KEY"}, &out); err == nil || strings.Join(calls, ",") != "-echo,echo" {
		t.Errorf("a refused value: %v, stty %v (echo must come back)", err, calls)
	}
	for _, bad := range [][]string{{"secret", "set"}, {"secret", "get", "MAPS_KEY"}, {"secret", "set", "MAPS_KEY", value}, {"secret", "set", "bad-name"}} {
		if err := Run(ctx, bad, &out); err == nil {
			t.Errorf("%v: accepted", bad)
		}
	}
	if kc["MAPS_KEY"] != value {
		t.Error("a refused command changed the secret")
	}
	if err := Run(ctx, []string{"secret", "delete", "MAPS_KEY"}, &out); err != nil || len(kc) != 0 {
		t.Errorf("delete: %v %v", err, kc)
	}

	// Ctrl-C at the prompt ends it, stores nothing, and turns echo back
	// on (review 26).
	calls = nil
	pr, pw := io.Pipe()
	defer pw.Close()
	stdin = pr
	cctx, cancel := context.WithCancel(ctx)
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	if err := Run(cctx, []string{"secret", "set", "MAPS_KEY"}, &out); err == nil || len(kc) != 0 || strings.Join(calls, ",") != "-echo,echo" {
		t.Errorf("Ctrl-C: %v, stored %v, stty %v", err, kc, calls)
	}
	// A terminal whose echo can't be turned off is never read from.
	noEcho = func(context.Context) (func(), error) { return nil, errors.New("not a tty") }
	stdin = strings.NewReader(value + "\n")
	if err := Run(ctx, []string{"secret", "set", "MAPS_KEY"}, &out); err == nil || len(kc) != 0 {
		t.Errorf("echo on: %v, stored %v", err, kc)
	}
}

// TestMain keeps every test off the keychain of the machine running it.
func TestMain(m *testing.M) {
	vault = fakeKeychain{}
	os.Exit(m.Run())
}

// TestInv_SecretSetSaysNoRestart: storing the Anthropic key says it's used
// from the next step, as it is (workers read it per request), not to
// restart (it said so after workers stopped needing it: the first brew
// install).
func TestInv_SecretSetSaysNoRestart(t *testing.T) {
	kc := fakeKeychain{}
	oldVault, oldTerm := vault, isTerminal
	t.Cleanup(func() { vault, isTerminal, stdin = oldVault, oldTerm, os.Stdin })
	vault, isTerminal = kc, func() bool { return false }
	for _, name := range []string{"ANTHROPIC_API_KEY", "MAPS_KEY"} {
		stdin = strings.NewReader("k\n")
		var out bytes.Buffer
		if err := Run(context.Background(), []string{"secret", "set", name}, &out); err != nil {
			t.Fatal(err)
		}
		if s := out.String(); strings.Contains(strings.ToLower(s), "restart heedle") || !strings.Contains(s, "no restart needed") {
			t.Errorf("%s: %q", name, s)
		}
	}
}

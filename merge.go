package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

const (
	// BeliefsDir is the directory in every record that indexes the latest
	// run of each question asked on that history.
	BeliefsDir = "beliefs"

	// BranchWorkflowName and MergeWorkflowName are the registered names of
	// BranchWorkflow and MergeWorkflow.
	BranchWorkflowName = "LoomBranch"
	MergeWorkflowName  = "LoomMerge"
	// Activity names: PinName (a local activity) resolves a ref to its
	// head; BranchName creates a ref; MergePlanName works out a merge
	// without writing; MergeName writes it.
	PinName       = "LoomPinRef"
	BranchName    = "LoomBranchRef"
	MergePlanName = "LoomMergePlan"
	MergeName     = "LoomMergeRecord"

	// TrailerMerge marks a merge commit and names the commit merged in.
	TrailerMerge = "Loom-Merge"
	// TrailerResolve records how one conflicting question was resolved:
	// "<question> ours", "<question> theirs" or "<question> rerun".
	TrailerResolve = "Loom-Resolve"

	// Resolutions for MergeInput.Resolve.
	ResolveOurs   = "ours"
	ResolveTheirs = "theirs"
	ResolveRerun  = "rerun"
)

// QuestionID names what a run computes: "q-" plus the Git blob ID of the
// JSON of its program and plan. Runs of the same program and plan answer
// the same question, on any ref; a different program, even by a comment,
// is a different question.
func QuestionID(ctx context.Context, program string, p plan.Plan) (string, error) {
	b, err := json.MarshalIndent(struct {
		Program string    `json:"program"`
		Plan    plan.Plan `json:"plan"`
	}{program, p}, "", "  ")
	if err != nil {
		return "", fmt.Errorf("%w: %v", warp.ErrInvalid, err)
	}
	id, err := warp.HashBlob(ctx, append(b, '\n'))
	if err != nil {
		return "", err
	}
	return "q-" + string(id), nil
}

// === the beliefs index =====================================================

// History is the read-only part of a warp store that reading beliefs needs.
type History interface {
	Tree(ctx context.Context, id warp.ID) ([]warp.Entry, error)
	Blob(ctx context.Context, id warp.ID) ([]byte, error)
	Blobs(ctx context.Context, ids []warp.ID) ([][]byte, error)
	Parents(ctx context.Context, commit warp.ID) (tree warp.ID, parents []warp.ID, err error)
	FindByTrailer(ctx context.Context, from warp.ID, key, value string) (warp.ID, bool, error)
}

// index is a record's beliefs index: question -> run ID of its latest run.
type index struct {
	tree    warp.ID // "" when the record has none
	entries []warp.Entry
	runs    map[string]string
}

var questionName = regexp.MustCompile(`^q-([0-9a-f]{40}|[0-9a-f]{64})$`)

// readIndex reads and validates a record's beliefs index. Every entry must
// be a blob named q-<ID> holding one run ID on one line; anything else is
// warp.ErrInvalid, so a crafted index can't be merged or read as beliefs.
func readIndex(ctx context.Context, s History, commit warp.ID) (index, error) {
	idx := index{runs: map[string]string{}}
	root, _, err := s.Parents(ctx, commit)
	if err != nil {
		return idx, err
	}
	es, err := s.Tree(ctx, root)
	if err != nil {
		return idx, err
	}
	for _, e := range es {
		if e.Name == BeliefsDir {
			if e.Kind != warp.KindTree {
				return idx, fmt.Errorf("%w: %s in %s isn't a tree", warp.ErrInvalid, BeliefsDir, commit)
			}
			idx.tree = e.ID
		}
	}
	if idx.tree == "" {
		return idx, nil
	}
	if idx.entries, err = s.Tree(ctx, idx.tree); err != nil {
		return idx, err
	}
	ids := make([]warp.ID, len(idx.entries))
	for i, e := range idx.entries {
		if e.Kind != warp.KindBlob || !questionName.MatchString(e.Name) {
			return idx, fmt.Errorf("%w: beliefs entry %q in %s", warp.ErrInvalid, e.Name, commit)
		}
		ids[i] = e.ID
	}
	// One read for the whole index: every Record reads it, and it holds
	// every question ever asked on the history (LOOM-32).
	blobs, err := s.Blobs(ctx, ids)
	if err != nil {
		return idx, err
	}
	for i, e := range idx.entries {
		run, rest, _ := strings.Cut(string(blobs[i]), "\n")
		if run == "" || rest != "" || strings.TrimSpace(run) != run {
			return idx, fmt.Errorf("%w: beliefs entry %s in %s isn't one run ID", warp.ErrInvalid, e.Name, commit)
		}
		idx.runs[e.Name] = run
	}
	return idx, nil
}

// Belief is the latest answer to one question on a history: the latest run
// of it, found by its run ID.
type Belief struct {
	Question string            `json:"question"`
	RunID    string            `json:"runId"`
	Commit   warp.ID           `json:"commit"` // the run's record
	Program  string            `json:"program"`
	Plan     plan.Plan         `json:"plan"`
	Outputs  map[string]string `json:"outputs"`
	Leaves   []string          `json:"leaves"` // nodes nothing depends on: the answer
}

// Beliefs reads the beliefs index of a record: every question asked on
// its history and the latest run of each, sorted by question.
func Beliefs(ctx context.Context, s History, commit warp.ID) ([]Belief, error) {
	idx, err := readIndex(ctx, s, commit)
	if err != nil {
		return nil, err
	}
	var out []Belief
	for _, e := range idx.entries {
		b, err := belief(ctx, s, commit, e.Name, idx.runs[e.Name])
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// belief reads the run a belief names: the commit reachable from `from`
// whose Shuttle-Run trailer is run.
func belief(ctx context.Context, s History, from warp.ID, question, run string) (Belief, error) {
	b := Belief{Question: question, RunID: run, Outputs: map[string]string{}}
	c, ok, err := s.FindByTrailer(ctx, from, TrailerRun, run)
	if err != nil {
		return b, err
	}
	if !ok {
		return b, fmt.Errorf("%w: belief %s names run %s, which isn't in this history", warp.ErrInvalid, question, run)
	}
	b.Commit = c
	files, err := runFiles(ctx, s, c)
	if err != nil {
		return b, err
	}
	for _, f := range files {
		switch f.Name {
		case "program.draft":
			p, err := s.Blob(ctx, f.ID)
			if err != nil {
				return b, err
			}
			b.Program = string(p)
		case "plan.json":
			pj, err := s.Blob(ctx, f.ID)
			if err != nil {
				return b, err
			}
			if err := json.Unmarshal(pj, &b.Plan); err != nil {
				return b, fmt.Errorf("%w: run %s: plan.json: %v", warp.ErrInvalid, run, err)
			}
			for _, id := range b.Plan.Leaves() {
				b.Leaves = append(b.Leaves, string(id))
			}
		case "outputs":
			outs, err := s.Tree(ctx, f.ID)
			if err != nil {
				return b, err
			}
			for _, o := range outs {
				v, err := s.Blob(ctx, o.ID)
				if err != nil {
					return b, err
				}
				b.Outputs[o.Name] = string(v)
			}
		}
	}
	return b, nil
}

// runFiles returns a record's root entries other than the beliefs index:
// the files of the run it records.
func runFiles(ctx context.Context, s History, commit warp.ID) ([]warp.Entry, error) {
	root, _, err := s.Parents(ctx, commit)
	if err != nil {
		return nil, err
	}
	es, err := s.Tree(ctx, root)
	if err != nil {
		return nil, err
	}
	var run []warp.Entry
	for _, e := range es {
		if e.Name != BeliefsDir {
			run = append(run, e)
		}
	}
	return run, nil
}

// runTreeOf is the tree of the run a record holds, without its index.
func runTreeOf(ctx context.Context, s *warp.Store, commit warp.ID) (warp.ID, error) {
	run, err := runFiles(ctx, s, commit)
	if err != nil {
		return "", err
	}
	return s.PutTree(ctx, run)
}

// setEntry returns es with e added, or replacing the entry of that name.
func setEntry(es []warp.Entry, e warp.Entry) []warp.Entry {
	out := make([]warp.Entry, 0, len(es)+1)
	for _, x := range es {
		if x.Name != e.Name {
			out = append(out, x)
		}
	}
	return append(out, e)
}

// === branch ================================================================

// BranchInput asks for ref To to be created at ref From's head.
type BranchInput struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// BranchRecordInput is what the branch activity writes: ref To at commit At.
type BranchRecordInput struct {
	To string
	At warp.ID
}

// BranchWorkflow creates a branch: ref To, at From's head when the workflow
// started. It never moves an existing ref: if To exists at another commit
// it fails with the non-retryable error type "BranchExists".
func BranchWorkflow(ctx workflow.Context, in BranchInput) (warp.ID, error) {
	if err := checkRefShape(in.To); err != nil {
		return "", err
	}
	var head warp.ID
	if err := workflow.ExecuteLocalActivity(pinOptions(ctx), PinName, in.From).Get(ctx, &head); err != nil {
		return "", err
	}
	err := workflow.ExecuteActivity(writeOptions(ctx), BranchName, BranchRecordInput{To: in.To, At: head}).Get(ctx, nil)
	return head, err
}

// Branch is the activity behind BranchWorkflow. A retry that finds To
// already at At succeeds: it made the branch.
func (r *Recorder) Branch(ctx context.Context, in BranchRecordInput) error {
	s := r.Store
	err := s.Advance(ctx, in.To, "", in.At)
	if errors.Is(err, warp.ErrConflict) {
		if cur, ok, rerr := s.Resolve(ctx, in.To); rerr == nil && ok && cur == in.At {
			return nil
		}
		return temporal.NewNonRetryableApplicationError("ref "+in.To+" already exists", "BranchExists", err)
	}
	if errors.Is(err, warp.ErrInvalid) || errors.Is(err, warp.ErrNotFound) {
		return nonRetryable(err, "InvalidInput")
	}
	return err
}

// Pin is the local activity that resolves a ref to its head. The ref must
// be valid and exist.
func (r *Recorder) Pin(ctx context.Context, ref string) (warp.ID, error) {
	if err := warp.CheckRef(ctx, ref); err != nil {
		return "", nonRetryable(err, "InvalidInput")
	}
	head, ok, err := r.Store.Resolve(ctx, ref)
	if err != nil {
		return "", nonRetryable(err, "InvalidInput")
	}
	if !ok {
		return "", temporal.NewNonRetryableApplicationError("ref "+ref+" doesn't exist", "InvalidInput", nil)
	}
	return head, nil
}

// === merge =================================================================

// MergeInput asks for ref From to be merged into ref Into. Resolve says
// what to do with questions both sides answered differently since they
// diverged: "" (refuse the merge), "ours", "theirs" or "rerun" (run each
// such question again on Into first, then merge keeping that new run).
type MergeInput struct {
	Into    string `json:"into"`
	From    string `json:"from"`
	Resolve string `json:"resolve,omitempty"`
}

// MergeRecordInput is what the merge activities work on.
type MergeRecordInput struct {
	Into    string
	Theirs  warp.ID // From's head, pinned when the workflow started
	Resolve string
	// Rerun lists the questions this workflow ran again on Into; with
	// Resolve "rerun", only these conflicts may be settled.
	Rerun      []string
	WorkflowID string
	RunID      string
	At         time.Time
}

// Kinds of merge result.
const (
	MergeUpToDate    = "up-to-date"   // From was already part of Into
	MergeFastForward = "fast-forward" // Into had nothing new; it moved to From
	MergeCommitted   = "merge"        // a merge commit joins both histories
)

// Conflict is one question both sides answered differently since they
// diverged, and the run that would answer it again.
type Conflict struct {
	Question string
	Program  string // the program, on one line
	Ours     string // each side's answer (its leaf outputs), on one line
	Theirs   string
	Rerun    RunInput
}

// MergeResult is what MergeWorkflow returns.
type MergeResult struct {
	Commit   warp.ID           // Into's head after the merge
	Kind     string            // MergeUpToDate, MergeFastForward or MergeCommitted
	Resolved map[string]string // question -> how its conflict was resolved
	// RerunCommits are the records of the re-runs, by question.
	RerunCommits map[string]warp.ID
}

// MergeWorkflow merges From into Into: the union of both histories.
//
//	pin From ; plan the merge ; (re-run each conflict on Into, for "rerun") ; write the merge
//
// Re-runs happen before anything is merged, so if one fails nothing is
// merged and submitting the merge again starts over.
func MergeWorkflow(ctx workflow.Context, in MergeInput) (MergeResult, error) {
	switch in.Resolve {
	case "", ResolveOurs, ResolveTheirs, ResolveRerun:
	default:
		return MergeResult{}, temporal.NewNonRetryableApplicationError("resolve must be ours, theirs or rerun, not "+in.Resolve, "InvalidInput", nil)
	}
	if err := checkRefShape(in.Into); err != nil {
		return MergeResult{}, err
	}
	var theirs warp.ID
	if err := workflow.ExecuteLocalActivity(pinOptions(ctx), PinName, in.From).Get(ctx, &theirs); err != nil {
		return MergeResult{}, err
	}
	info := workflow.GetInfo(ctx)
	rec := MergeRecordInput{
		Into: in.Into, Theirs: theirs, Resolve: in.Resolve,
		WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID, At: workflow.Now(ctx),
	}
	var res MergeResult
	if in.Resolve == ResolveRerun {
		var conflicts []Conflict
		if err := workflow.ExecuteActivity(readOptions(ctx), MergePlanName, rec).Get(ctx, &conflicts); err != nil {
			return MergeResult{}, err
		}
		for i, c := range conflicts {
			child := workflow.WithChildOptions(ctx, rerunOptions(in.Into, fmt.Sprintf("%s/rerun-%d", info.WorkflowExecution.ID, i)))
			var run RunResult
			if err := workflow.ExecuteChildWorkflow(child, RunWorkflowName, c.Rerun).Get(ctx, &run); err != nil {
				return MergeResult{}, err
			}
			if res.RerunCommits == nil {
				res.RerunCommits = map[string]warp.ID{}
			}
			res.RerunCommits[c.Question] = run.Commit
			rec.Rerun = append(rec.Rerun, c.Question)
		}
	}
	reruns := res.RerunCommits
	if err := workflow.ExecuteActivity(writeOptions(ctx), MergeName, rec).Get(ctx, &res); err != nil {
		return MergeResult{}, err
	}
	res.RerunCommits = reruns
	return res, nil
}

// rerunOptions start a re-run: a child RunWorkflow whose memo names the
// ref it records on (MemoRef), like Submit's, so UIs list it there.
func rerunOptions(into, id string) workflow.ChildWorkflowOptions {
	return workflow.ChildWorkflowOptions{WorkflowID: id, Memo: map[string]any{MemoRef: into}}
}

// checkRefShape is the no-I/O part of ref validation, for workflow code.
func checkRefShape(ref string) error {
	if !strings.HasPrefix(ref, "refs/") {
		return temporal.NewNonRetryableApplicationError("ref "+ref+" must start with refs/", "InvalidInput", nil)
	}
	return nil
}

// plannedMerge is a merge worked out against Into's current head.
type plannedMerge struct {
	ours      warp.ID
	kind      string
	merged    []warp.Entry // the index, conflicts not yet resolved
	conflicts []Conflict
	oursIdx   index
	theirsIdx index
}

// MergePlan is the read-only activity that works out a merge's conflicts,
// with the run that would answer each again. It writes nothing that any
// ref points to.
func (r *Recorder) MergePlan(ctx context.Context, in MergeRecordInput) ([]Conflict, error) {
	pm, err := r.plan(ctx, in)
	if err != nil {
		return nil, classify(err)
	}
	return pm.conflicts, nil
}

// Merge is the activity behind MergeWorkflow. The merge is the union of
// both histories: the merge commit's parents are Into's head and Theirs,
// so every run on either side stays in the history. Its beliefs index is
// Git's three-way merge (warp.MergeTrees) of the two sides' indexes against
// their common ancestor's, where each path is a question and each value
// the run ID of its latest run:
//
//   - asked on one side only, or answered again on one side only: that
//     side's run;
//   - answered by the same run on both, or by runs with identical files:
//     that answer;
//   - answered again on both sides since they diverged, differently: a
//     conflict, which Resolve settles, or the merge fails with the
//     non-retryable error type "MergeConflict" naming every conflicting
//     question.
//
// Beliefs are whole runs, so a merge never pairs one run's evidence with
// another's conclusion. The merge commit's own files (program.draft,
// plan.json, outputs/, files/) are Into's latest run, unchanged.
func (r *Recorder) Merge(ctx context.Context, in MergeRecordInput) (MergeResult, error) {
	res, err := r.merge(ctx, in)
	return res, classify(err)
}

// classify makes errors that can never succeed non-retryable.
func classify(err error) error {
	if errors.Is(err, warp.ErrInvalid) {
		return nonRetryable(err, "InvalidInput")
	}
	return err
}

func (r *Recorder) plan(ctx context.Context, in MergeRecordInput) (plannedMerge, error) {
	s := r.Store
	var pm plannedMerge
	ours, ok, err := s.Resolve(ctx, in.Into)
	if err != nil {
		return pm, err
	}
	if !ok {
		return pm, temporal.NewNonRetryableApplicationError("ref "+in.Into+" doesn't exist; branch it first", "InvalidInput", nil)
	}
	pm.ours = ours
	if up, err := s.IsAncestor(ctx, in.Theirs, ours); err != nil {
		return pm, err
	} else if up {
		pm.kind = MergeUpToDate
		return pm, nil
	}
	if ff, err := s.IsAncestor(ctx, ours, in.Theirs); err != nil {
		return pm, err
	} else if ff {
		pm.kind = MergeFastForward
		return pm, nil
	}
	pm.kind = MergeCommitted
	if pm.oursIdx, err = readIndex(ctx, s, ours); err != nil {
		return pm, err
	}
	if pm.theirsIdx, err = readIndex(ctx, s, in.Theirs); err != nil {
		return pm, err
	}
	bases, err := s.MergeBases(ctx, ours, in.Theirs)
	if err != nil {
		return pm, err
	}
	base, err := r.virtualBase(ctx, bases)
	if err != nil {
		return pm, err
	}
	oursTree, err := treeOf(ctx, s, pm.oursIdx.entries)
	if err != nil {
		return pm, err
	}
	theirsTree, err := treeOf(ctx, s, pm.theirsIdx.entries)
	if err != nil {
		return pm, err
	}
	mergedTree, conflicted, err := s.MergeTrees(ctx, base, oursTree, theirsTree)
	if err != nil {
		return pm, err
	}
	all, err := s.Tree(ctx, mergedTree)
	if err != nil {
		return pm, err
	}
	isConflict := map[string]bool{}
	for _, q := range conflicted {
		isConflict[q] = true
	}
	for _, e := range all {
		if !isConflict[e.Name] {
			pm.merged = append(pm.merged, e)
		}
	}
	oursBy, theirsBy := byName(pm.oursIdx.entries), byName(pm.theirsIdx.entries)
	for _, q := range conflicted {
		o, ook := oursBy[q]
		_, tok := theirsBy[q]
		if !ook || !tok {
			return pm, fmt.Errorf("%w: conflicting path %s isn't a question on both sides", warp.ErrInvalid, q)
		}
		ob, err := belief(ctx, s, ours, q, pm.oursIdx.runs[q])
		if err != nil {
			return pm, err
		}
		tb, err := belief(ctx, s, in.Theirs, q, pm.theirsIdx.runs[q])
		if err != nil {
			return pm, err
		}
		// Two different runs that recorded identical files agree.
		same, err := sameRun(ctx, s, ob.Commit, tb.Commit)
		if err != nil {
			return pm, err
		}
		if same {
			pm.merged = append(pm.merged, o)
			continue
		}
		pm.conflicts = append(pm.conflicts, Conflict{
			Question: q, Program: oneLine(ob.Program, 120), Ours: answer(ob), Theirs: answer(tb),
			Rerun: RunInput{Program: ob.Program, Plan: ob.Plan, Ref: in.Into},
		})
	}
	sort.Slice(pm.conflicts, func(i, j int) bool { return pm.conflicts[i].Question < pm.conflicts[j].Question })
	return pm, nil
}

func (r *Recorder) merge(ctx context.Context, in MergeRecordInput) (MergeResult, error) {
	s := r.Store
	// Already merged by an earlier attempt of this workflow run?
	if ours, ok, err := s.Resolve(ctx, in.Into); err == nil && ok {
		if done, ok, err := s.FindByTrailer(ctx, ours, TrailerRun, in.RunID); err != nil {
			return MergeResult{}, err
		} else if ok {
			return mergeResultOf(ctx, s, done)
		}
	}
	pm, err := r.plan(ctx, in)
	if err != nil {
		return MergeResult{}, err
	}
	switch pm.kind {
	case MergeUpToDate:
		return MergeResult{Commit: pm.ours, Kind: MergeUpToDate}, nil
	case MergeFastForward:
		if err := s.Advance(ctx, in.Into, pm.ours, in.Theirs); err != nil {
			return MergeResult{}, err
		}
		return MergeResult{Commit: in.Theirs, Kind: MergeFastForward}, nil
	}

	rerun := map[string]bool{}
	for _, q := range in.Rerun {
		rerun[q] = true
	}
	var unresolved []string
	for _, c := range pm.conflicts {
		if in.Resolve == "" || (in.Resolve == ResolveRerun && !rerun[c.Question]) {
			unresolved = append(unresolved, fmt.Sprintf("%s %s\n  ours:   %s\n  theirs: %s", c.Question, c.Program, c.Ours, c.Theirs))
		}
	}
	if len(unresolved) > 0 {
		return MergeResult{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%d question(s) answered differently on both sides; merge again with resolve ours, theirs or rerun:\n%s",
				len(unresolved), strings.Join(unresolved, "\n")),
			"MergeConflict", nil)
	}
	final := append([]warp.Entry{}, pm.merged...)
	oursBy, theirsBy := byName(pm.oursIdx.entries), byName(pm.theirsIdx.entries)
	for _, c := range pm.conflicts {
		pick := oursBy[c.Question] // ours, or for rerun: the fresh run on Into
		if in.Resolve == ResolveTheirs {
			pick = theirsBy[c.Question]
		}
		final = append(final, pick)
	}
	// Postcondition, checked rather than trusted: the merged index has
	// every question from both sides, each answered by one side's run.
	if err := checkUnion(final, oursBy, theirsBy); err != nil {
		return MergeResult{}, temporal.NewNonRetryableApplicationError(err.Error(), "MergeInvariant", nil)
	}
	idxTree, err := treeOf(ctx, s, final)
	if err != nil {
		return MergeResult{}, err
	}
	run, err := runFiles(ctx, s, pm.ours)
	if err != nil {
		return MergeResult{}, err
	}
	tree, err := s.PutTree(ctx, append(run, warp.Entry{Name: BeliefsDir, Kind: warp.KindTree, ID: idxTree}))
	if err != nil {
		return MergeResult{}, err
	}
	trailers := []warp.Trailer{
		{Key: TrailerWorkflow, Value: in.WorkflowID},
		{Key: TrailerRun, Value: in.RunID},
		{Key: TrailerMerge, Value: string(in.Theirs)},
	}
	for _, c := range pm.conflicts {
		trailers = append(trailers, warp.Trailer{Key: TrailerResolve, Value: c.Question + " " + in.Resolve})
	}
	commit, err := s.Commit(ctx, warp.CommitSpec{
		Tree:     tree,
		Parents:  []warp.ID{pm.ours, in.Theirs},
		Message:  "merge " + string(in.Theirs) + " into " + in.Into,
		Trailers: trailers,
		Author:   warp.Signature{Name: "loom", Email: "loom@heedle.ai", When: in.At},
	})
	if err != nil {
		return MergeResult{}, err
	}
	// On warp.ErrConflict the activity fails and Temporal retries it.
	if err := s.Advance(ctx, in.Into, pm.ours, commit); err != nil {
		return MergeResult{}, err
	}
	return mergeResultOf(ctx, s, commit)
}

// virtualBase is the beliefs index the merge compares both sides against:
// the common ancestor's. After a criss-cross merge there are several best
// common ancestors; the base is then the entries on which they all agree,
// so a question they disagree on counts as new on both sides and conflicts
// unless both sides now agree. None (unrelated histories) is the empty index.
func (r *Recorder) virtualBase(ctx context.Context, bases []warp.ID) (warp.ID, error) {
	s := r.Store
	if len(bases) == 0 {
		return s.PutTree(ctx, nil)
	}
	var common map[string]warp.Entry
	for i, b := range bases {
		idx, err := readIndex(ctx, s, b)
		if err != nil {
			return "", err
		}
		by := byName(idx.entries)
		if i == 0 {
			common = by
			continue
		}
		for q, e := range common {
			if by[q] != e {
				delete(common, q)
			}
		}
	}
	var es []warp.Entry
	for _, e := range common {
		es = append(es, e)
	}
	return treeOf(ctx, s, es)
}

func treeOf(ctx context.Context, s *warp.Store, es []warp.Entry) (warp.ID, error) {
	return s.PutTree(ctx, es)
}

// sameRun reports whether two run records hold identical run files.
func sameRun(ctx context.Context, s *warp.Store, a, b warp.ID) (bool, error) {
	if a == b {
		return true, nil
	}
	fa, err := runFiles(ctx, s, a)
	if err != nil {
		return false, err
	}
	fb, err := runFiles(ctx, s, b)
	if err != nil {
		return false, err
	}
	if len(fa) != len(fb) {
		return false, nil
	}
	for i := range fa {
		if fa[i] != fb[i] {
			return false, nil
		}
	}
	return true, nil
}

// mergeResultOf rebuilds a merge's result from its commit, so a retry that
// finds the merge already written returns the same thing.
func mergeResultOf(ctx context.Context, s *warp.Store, commit warp.ID) (MergeResult, error) {
	ts, err := s.Trailers(ctx, commit)
	if err != nil {
		return MergeResult{}, err
	}
	res := MergeResult{Commit: commit, Kind: MergeCommitted, Resolved: map[string]string{}}
	for _, t := range ts {
		if t.Key == TrailerResolve {
			q, how, _ := strings.Cut(t.Value, " ")
			res.Resolved[q] = how
		}
	}
	return res, nil
}

// checkUnion is the merge's postcondition: final holds every question of
// both sides, each answered by one side's entry.
func checkUnion(final []warp.Entry, ours, theirs map[string]warp.Entry) error {
	got := map[string]warp.Entry{}
	for _, e := range final {
		if _, dup := got[e.Name]; dup {
			return fmt.Errorf("merge answered question %s twice", e.Name)
		}
		got[e.Name] = e
	}
	for _, side := range []map[string]warp.Entry{ours, theirs} {
		for q := range side {
			if _, ok := got[q]; !ok {
				return fmt.Errorf("merge lost question %s", q)
			}
		}
	}
	for q, e := range got {
		o, ok1 := ours[q]
		t, ok2 := theirs[q]
		if !(ok1 && e == o) && !(ok2 && e == t) {
			return fmt.Errorf("merge answered question %s with a run neither side has", q)
		}
	}
	return nil
}

func byName(es []warp.Entry) map[string]warp.Entry {
	m := make(map[string]warp.Entry, len(es))
	for _, e := range es {
		m[e.Name] = e
	}
	return m
}

// oneLine collapses whitespace and cuts s to at most n bytes.
func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		s = s[:n-1] + "…"
	}
	return s
}

// answer is a belief's leaf outputs on one line, for conflict messages.
func answer(b Belief) string {
	var leaves []string
	for _, id := range b.Leaves {
		leaves = append(leaves, oneLine(b.Outputs[id], 200))
	}
	return strings.Join(leaves, "; ")
}

func nonRetryable(err error, kind string) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), kind, err)
}

func pinOptions(ctx workflow.Context) workflow.Context {
	return workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: 10 * time.Second})
}

// readOptions are for activities that only read: retried briefly.
func readOptions(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    time.Minute,
		ScheduleToCloseTimeout: 5 * time.Minute,
	})
}

// writeOptions are the activity options for every activity that moves a
// ref: a conflicting writer fails the attempt and Temporal retries it on
// the new head, bounded by time rather than by a number of attempts.
func writeOptions(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    time.Minute,
		ScheduleToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Second,
		},
	})
}

// SubmitBranch starts BranchWorkflow; an identical request in progress is
// joined.
func SubmitBranch(ctx context.Context, c client.Client, taskQueue string, in BranchInput) (client.WorkflowRun, error) {
	return submitJSON(ctx, c, taskQueue, "loom-branch-", BranchWorkflowName, in)
}

// SubmitMerge starts MergeWorkflow; an identical request in progress is
// joined.
func SubmitMerge(ctx context.Context, c client.Client, taskQueue string, in MergeInput) (client.WorkflowRun, error) {
	return submitJSON(ctx, c, taskQueue, "loom-merge-", MergeWorkflowName, in)
}

func submitJSON(ctx context.Context, c client.Client, taskQueue, prefix, name string, in any) (client.WorkflowRun, error) {
	return submitJSONWithin(ctx, c, taskQueue, prefix, name, in, 0)
}

// submitJSONWithin is submitJSON with a timeout for the whole workflow (0:
// none).
func submitJSONWithin(ctx context.Context, c client.Client, taskQueue, prefix, name string, in any, timeout time.Duration) (client.WorkflowRun, error) {
	b, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		return nil, err
	}
	id, err := warp.HashBlob(ctx, append(b, '\n'))
	if err != nil {
		return nil, err
	}
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       prefix + string(id),
		TaskQueue:                taskQueue,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowExecutionTimeout: timeout,
	}, name, in)
}

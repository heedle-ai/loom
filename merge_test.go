package loom_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/heedle-ai/draft"
	"github.com/heedle-ai/draft/registry"
	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/shuttle/plan"
	"github.com/heedle-ai/warp"
)

// hist records runs and merges on a store without Temporal, calling the
// activities directly, each with its own run ID.
type hist struct {
	t   *testing.T
	s   *warp.Store
	rec *loom.Recorder
	n   int
}

func newHist(t *testing.T) *hist {
	s := newStore(t)
	return &hist{t: t, s: s, rec: &loom.Recorder{Store: s}}
}

func (h *hist) next() (string, time.Time) {
	h.n++
	return fmt.Sprintf("r%03d", h.n), time.Unix(1700000000+int64(h.n)*60, 0)
}

// ask records a run of question q (the program (echo "q")) on ref with
// output answer.
func (h *hist) ask(ref, q, answer string) warp.ID {
	h.t.Helper()
	src := fmt.Sprintf("(echo %q)", q)
	p := must(loom.Compile(ctx, draft.DefaultRegistry(), src))
	id, at := h.next()
	c, err := h.rec.Record(ctx, loom.RecordInput{Program: src, Plan: p,
		Result: plan.Result{Outputs: map[string]string{"n0": answer}}, Ref: ref, WorkflowID: "wf-" + id, RunID: id, At: at})
	if err != nil {
		h.t.Fatal(err)
	}
	return c
}

func (h *hist) branch(from, to string) {
	h.t.Helper()
	if err := h.rec.Branch(ctx, loom.BranchRecordInput{To: to, At: h.head(from)}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *hist) head(ref string) warp.ID {
	id, ok, err := h.s.Resolve(ctx, ref)
	if err != nil || !ok {
		h.t.Fatalf("resolve %s: %v %v", ref, ok, err)
	}
	return id
}

func (h *hist) merge(into, from, resolve string) (loom.MergeResult, error) {
	id, at := h.next()
	return h.rec.Merge(ctx, loom.MergeRecordInput{Into: into, Theirs: h.head(from), Resolve: resolve,
		WorkflowID: "wf-merge", RunID: id, At: at})
}

// beliefs maps question text to the answer believed at commit c.
func (h *hist) beliefs(c warp.ID) map[string]string {
	h.t.Helper()
	out := map[string]string{}
	for _, b := range must(loom.Beliefs(ctx, h.s, c)) {
		out[b.Program] = b.Outputs["n0"]
	}
	return out
}

// runs is the set of run IDs recorded in c's history (merges excluded).
func (h *hist) runs(c warp.ID) []string {
	h.t.Helper()
	out := gitOut(h.t, h.s, "log", "--format=%(trailers:key=Shuttle-Run,valueonly)%x00%(trailers:key=Loom-Merge,valueonly)%x01", string(c))
	var ids []string
	for _, rec := range strings.Split(out, "\x01") {
		run, merge, _ := strings.Cut(rec, "\x00")
		if strings.TrimSpace(run) != "" && strings.TrimSpace(merge) == "" {
			ids = append(ids, strings.TrimSpace(run))
		}
	}
	sort.Strings(ids)
	return ids
}

func union(a, b []string) []string {
	m := map[string]bool{}
	for _, x := range append(append([]string{}, a...), b...) {
		m[x] = true
	}
	var out []string
	for x := range m {
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}

// TestLaw_MergeIsUnionOfHistories: on random histories, a merge keeps
// every run of both sides; every belief is one side's whole run; and each
// question follows the three-way rule against the common ancestor: changed
// on one side only takes that side, changed differently on both is a
// conflict resolved as asked.
func TestLaw_MergeIsUnionOfHistories(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	seen := map[string]int{}
	defer func() {
		if seen["conflict"] == 0 || seen["ours"] == 0 || seen["theirs"] == 0 {
			t.Errorf("cases exercised %v; the law needs conflicts and one-sided changes from both sides", seen)
		}
	}()
	for trial := 0; trial < 8; trial++ {
		h := newHist(t)
		qs := []string{"q1", "q2", "q3", "q4"}
		h.ask("refs/loom/main", qs[r.Intn(4)], "base")
		for i := 0; i < r.Intn(3); i++ {
			h.ask("refs/loom/main", qs[r.Intn(4)], fmt.Sprint("base", i))
		}
		h.branch("refs/loom/main", "refs/loom/side")
		base := h.beliefs(h.head("refs/loom/main"))
		for i := 0; i < 1+r.Intn(3); i++ {
			h.ask("refs/loom/main", qs[r.Intn(4)], fmt.Sprint("ours", r.Intn(2)))
			h.ask("refs/loom/side", qs[r.Intn(4)], fmt.Sprint("theirs", r.Intn(2)))
		}
		ours, theirs := h.head("refs/loom/main"), h.head("refs/loom/side")
		ob, tb := h.beliefs(ours), h.beliefs(theirs)
		resolve := []string{loom.ResolveOurs, loom.ResolveTheirs}[r.Intn(2)]
		res, err := h.merge("refs/loom/main", "refs/loom/side", resolve)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := h.runs(res.Commit), union(h.runs(ours), h.runs(theirs)); !reflect.DeepEqual(got, want) {
			t.Fatalf("runs after merge %v, want union %v", got, want)
		}
		mb := h.beliefs(res.Commit)
		for _, q := range union(keys(ob), keys(tb)) {
			o, ook := ob[q]
			th, tok := tb[q]
			b, bok := base[q]
			var want string
			switch {
			case !tok || (ook && o == th) || (tok && bok && th == b):
				want = o
				seen["ours"]++
			case !ook || (ook && bok && o == b):
				want = th
				seen["theirs"]++
			default: // a conflict
				seen["conflict"]++
				want = map[string]string{loom.ResolveOurs: o, loom.ResolveTheirs: th}[resolve]
				if res.Resolved[mustQ(t, q)] != resolve {
					t.Fatalf("conflict on %s not recorded as %s: %v", q, resolve, res.Resolved)
				}
			}
			if mb[q] != want {
				t.Fatalf("trial %d %s: base %q ours %q theirs %q -> %q, want %q", trial, q, b, o, th, mb[q], want)
			}
		}
		if len(mb) != len(union(keys(ob), keys(tb))) {
			t.Fatalf("merged beliefs %v aren't exactly the union of %v and %v", mb, ob, tb)
		}
	}
}

func keys(m map[string]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func mustQ(t *testing.T, program string) string {
	p := must(loom.Compile(ctx, draft.DefaultRegistry(), program))
	return must(loom.QuestionID(ctx, program, p))
}

// TestLaw_MergeIsCommutative: merging b into a and a into b believe the
// same things (resolving ours on one side equals theirs on the other).
func TestLaw_MergeIsCommutative(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q1", "1")
	h.branch("refs/loom/a", "refs/loom/b")
	h.branch("refs/loom/a", "refs/loom/a2")
	h.branch("refs/loom/a", "refs/loom/b2")
	for _, ref := range []string{"refs/loom/a", "refs/loom/a2"} {
		h.ask(ref, "q1", "from a")
		h.ask(ref, "q2", "only a")
	}
	for _, ref := range []string{"refs/loom/b", "refs/loom/b2"} {
		h.ask(ref, "q1", "from b")
		h.ask(ref, "q3", "only b")
	}
	ab := must(h.merge("refs/loom/a", "refs/loom/b", loom.ResolveOurs))
	ba := must(h.merge("refs/loom/b2", "refs/loom/a2", loom.ResolveTheirs))
	if x, y := h.beliefs(ab.Commit), h.beliefs(ba.Commit); !reflect.DeepEqual(x, y) {
		t.Fatalf("a+b believes %v, b+a believes %v", x, y)
	}
}

// TestLaw_MergeIsAssociative: (a+b)+c and a+(b+c) believe the same things
// when no question conflicts.
func TestLaw_MergeIsAssociative(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q0", "root")
	for _, ref := range []string{"refs/loom/b", "refs/loom/c", "refs/loom/a2", "refs/loom/b2", "refs/loom/c2"} {
		h.branch("refs/loom/a", ref)
	}
	for _, p := range [][2]string{{"a", "q1"}, {"b", "q2"}, {"c", "q3"}, {"c", "q0"}} {
		h.ask("refs/loom/"+p[0], p[1], "by "+p[0])
		h.ask("refs/loom/"+p[0]+"2", p[1], "by "+p[0])
	}
	must(h.merge("refs/loom/a", "refs/loom/b", ""))
	left := must(h.merge("refs/loom/a", "refs/loom/c", ""))
	must(h.merge("refs/loom/b2", "refs/loom/c2", ""))
	right := must(h.merge("refs/loom/a2", "refs/loom/b2", ""))
	if x, y := h.beliefs(left.Commit), h.beliefs(right.Commit); !reflect.DeepEqual(x, y) {
		t.Fatalf("(a+b)+c believes %v, a+(b+c) believes %v", x, y)
	}
	if got := h.beliefs(left.Commit); got[`(echo "q0")`] != "by c" {
		t.Fatalf("q0 changed only on c, merged as %q", got[`(echo "q0")`])
	}
}

// TestLaw_MergeIsIdempotent: merging a history into itself or into one
// that already contains it changes nothing; merging into an ancestor
// fast-forwards without a new commit.
func TestLaw_MergeIsIdempotent(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q1", "1")
	h.branch("refs/loom/a", "refs/loom/b")
	h.ask("refs/loom/b", "q1", "2")
	before := h.head("refs/loom/a")
	if res := must(h.merge("refs/loom/a", "refs/loom/a", "")); res.Kind != loom.MergeUpToDate || res.Commit != before {
		t.Errorf("a+a = %+v", res)
	}
	res := must(h.merge("refs/loom/a", "refs/loom/b", ""))
	if res.Kind != loom.MergeFastForward || res.Commit != h.head("refs/loom/b") || h.head("refs/loom/a") != res.Commit {
		t.Errorf("merging a descendant = %+v, want a fast-forward to b", res)
	}
	if again := must(h.merge("refs/loom/a", "refs/loom/b", "")); again.Kind != loom.MergeUpToDate || again.Commit != res.Commit {
		t.Errorf("merging again = %+v, want up to date", again)
	}
}

// TestLaw_MergeRecordsStaySelfVerifying: a merge commit's own files are
// exactly its first parent's run, so every commit's program.draft,
// plan.json and outputs/ are a real run.
func TestLaw_MergeRecordsStaySelfVerifying(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q1", "1")
	h.branch("refs/loom/a", "refs/loom/b")
	ours := h.ask("refs/loom/a", "q2", "2")
	h.ask("refs/loom/b", "q3", "3")
	res := must(h.merge("refs/loom/a", "refs/loom/b", ""))
	if d := gitOut(t, h.s, "diff", "--name-only", string(ours), string(res.Commit), "--", ".", ":!beliefs"); d != "" {
		t.Fatalf("merge commit's run files differ from ours: %s", d)
	}
	if got := strings.Fields(gitOut(t, h.s, "log", "-1", "--format=%P", string(res.Commit))); len(got) != 2 || got[0] != string(ours) {
		t.Fatalf("parents %v, want ours then theirs", got)
	}
}

func appErrType(err error) string {
	var ae *temporal.ApplicationError
	if errors.As(err, &ae) {
		return ae.Type()
	}
	return ""
}

// TestMerge_ConflictIsRefusedUntilResolved: a question answered
// differently on both sides stops the merge and names it; nothing moves.
// Resolving takes one side's whole run and records how.
func TestMerge_ConflictIsRefusedUntilResolved(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "late?", "base")
	h.branch("refs/loom/a", "refs/loom/b")
	h.ask("refs/loom/a", "late?", "yes")
	h.ask("refs/loom/b", "late?", "no")
	before := h.head("refs/loom/a")
	_, err := h.merge("refs/loom/a", "refs/loom/b", "")
	q := mustQ(t, `(echo "late?")`)
	if appErrType(err) != "MergeConflict" || !strings.Contains(err.Error(), q) || !strings.Contains(err.Error(), `(echo "late?")`) {
		t.Fatalf("err = %v, want MergeConflict naming %s and its program", err, q)
	}
	if h.head("refs/loom/a") != before {
		t.Fatal("a refused merge moved the ref")
	}
	res := must(h.merge("refs/loom/a", "refs/loom/b", loom.ResolveTheirs))
	if got := h.beliefs(res.Commit)[`(echo "late?")`]; got != "no" {
		t.Fatalf("resolved theirs, believes %q", got)
	}
	if tr := gitOut(t, h.s, "log", "-1", "--format=%(trailers:key=Loom-Resolve,valueonly)", string(res.Commit)); tr != q+" theirs" {
		t.Fatalf("Loom-Resolve = %q", tr)
	}
}

// TestMerge_RetryReturnsTheSameMerge: a retried merge activity (same run
// ID) finds its own merge commit instead of merging again.
func TestMerge_RetryReturnsTheSameMerge(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q1", "1")
	h.branch("refs/loom/a", "refs/loom/b")
	h.ask("refs/loom/a", "q2", "2")
	h.ask("refs/loom/b", "q3", "3")
	in := loom.MergeRecordInput{Into: "refs/loom/a", Theirs: h.head("refs/loom/b"), WorkflowID: "wf", RunID: "merge-1", At: time.Unix(1800000000, 0)}
	first := must(h.rec.Merge(ctx, in))
	h.ask("refs/loom/a", "q4", "4") // the ref moves on before the retry
	again := must(h.rec.Merge(ctx, in))
	if again.Commit != first.Commit || again.Kind != loom.MergeCommitted {
		t.Fatalf("retry = %+v, first = %+v", again, first)
	}
}

// TestMerge_CrissCrossMerges: after a criss-cross there are two best
// common ancestors. The base is what they agree on, so questions they agree
// on merge normally, and a question they disagree on conflicts unless both
// sides now agree. (Review 4: criss-crossed lines could never merge again.)
func TestMerge_CrissCrossMerges(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q1", "1")
	h.branch("refs/loom/a", "refs/loom/b")
	h.ask("refs/loom/a", "q2", "a")
	h.ask("refs/loom/b", "q2", "b") // the bases will disagree on q2
	h.ask("refs/loom/b", "q3", "b")
	h.branch("refs/loom/a", "refs/loom/a-before")
	h.branch("refs/loom/b", "refs/loom/b-before")
	must(h.merge("refs/loom/a", "refs/loom/b-before", loom.ResolveOurs))
	must(h.merge("refs/loom/b", "refs/loom/a-before", loom.ResolveOurs))
	if n := len(must(h.s.MergeBases(ctx, h.head("refs/loom/a"), h.head("refs/loom/b")))); n != 2 {
		t.Fatalf("setup: %d merge bases, want a criss-cross", n)
	}
	h.ask("refs/loom/b", "q1", "changed on b") // the bases agree on q1
	res := must(h.merge("refs/loom/a", "refs/loom/b", loom.ResolveTheirs))
	got := h.beliefs(res.Commit)
	if got[`(echo "q1")`] != "changed on b" || got[`(echo "q3")`] != "b" {
		t.Fatalf("beliefs = %v", got)
	}
	// q2: a says "a", b says "b", the bases disagree: a conflict, settled
	// as asked (theirs), never silently.
	if got[`(echo "q2")`] != "b" || res.Resolved[mustQ(t, `(echo "q2")`)] != loom.ResolveTheirs {
		t.Fatalf("q2 = %q, resolved %v", got[`(echo "q2")`], res.Resolved)
	}
}

// TestMerge_ReansweringIsAChange: a side that ran a question again counts
// as having answered it, even if its answer happens to equal the common
// ancestor's. (Review 4: content-based beliefs let such a side lose
// without a conflict.)
func TestMerge_ReansweringIsAChange(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q", "0")
	h.branch("refs/loom/a", "refs/loom/b")
	h.ask("refs/loom/b", "q", "1")
	h.ask("refs/loom/b", "q", "0") // back to the base's answer, by a new run
	h.ask("refs/loom/a", "q", "2")
	if _, err := h.merge("refs/loom/a", "refs/loom/b", ""); appErrType(err) != "MergeConflict" {
		t.Fatalf("err = %v, want MergeConflict", err)
	}
}

// TestMerge_IdenticalRunsAgree: two different runs that recorded the same
// files answer a question the same way, so they don't conflict.
func TestMerge_IdenticalRunsAgree(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q", "0")
	h.branch("refs/loom/a", "refs/loom/b")
	h.ask("refs/loom/a", "q", "same")
	h.ask("refs/loom/b", "q", "same")
	h.ask("refs/loom/b", "other", "x")
	res := must(h.merge("refs/loom/a", "refs/loom/b", ""))
	if got := h.beliefs(res.Commit); got[`(echo "q")`] != "same" || len(res.Resolved) != 0 {
		t.Fatalf("beliefs %v, resolved %v", got, res.Resolved)
	}
}

// TestInv_BeliefsSurviveGarbageCollection: beliefs name runs by run ID,
// which resolves to a commit in the history, so git gc keeps everything a
// belief needs. (Review 4: beliefs named trees no commit reached.)
func TestInv_BeliefsSurviveGarbageCollection(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q1", "1")
	h.branch("refs/loom/a", "refs/loom/b")
	h.ask("refs/loom/a", "q2", "2")
	h.ask("refs/loom/b", "q3", "3")
	res := must(h.merge("refs/loom/a", "refs/loom/b", ""))
	gitOut(t, h.s, "gc", "--prune=now", "-q")
	if got := h.beliefs(res.Commit); len(got) != 3 || got[`(echo "q3")`] != "3" {
		t.Fatalf("after gc: %v", got)
	}
	// A clone carries them too.
	clone := t.TempDir() + "/clone.git"
	if out, err := exec.Command("git", "clone", "-q", "--mirror", h.s.Dir(), clone).CombinedOutput(); err != nil {
		t.Fatalf("clone: %v %s", err, out)
	}
	c := must(warp.Open(ctx, clone))
	if bs := must(loom.Beliefs(ctx, c, res.Commit)); len(bs) != 3 {
		t.Fatalf("clone beliefs = %v", bs)
	}
}

// TestInv_CraftedIndexIsRefused: an index entry that isn't a blob named
// q-<ID> holding one run ID is refused by every reader, never merged line
// by line or followed. (Review 4: Git merges multi-line blobs and subtrees
// path by path, and crafted entries crashed the tools.)
func TestInv_CraftedIndexIsRefused(t *testing.T) {
	q := "q-" + strings.Repeat("a", 40)
	for name, entries := range map[string][]warp.Entry{
		"two lines": {{Name: q, Kind: warp.KindBlob, ID: "BLOB:r1\nr2\n"}},
		"bad name":  {{Name: "x", Kind: warp.KindBlob, ID: "BLOB:r1\n"}},
		"subtree":   {{Name: q, Kind: warp.KindTree, ID: "EMPTY"}},
		"no run ID": {{Name: q, Kind: warp.KindBlob, ID: "BLOB:\n"}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHist(t)
			h.ask("refs/loom/a", "q1", "1")
			h.branch("refs/loom/a", "refs/loom/b")
			h.ask("refs/loom/a", "q2", "2")
			for i, e := range entries {
				switch {
				case strings.HasPrefix(string(e.ID), "BLOB:"):
					entries[i].ID = must(h.s.PutBlob(ctx, []byte(strings.TrimPrefix(string(e.ID), "BLOB:"))))
				case e.ID == "EMPTY":
					entries[i].ID = must(h.s.PutTree(ctx, nil))
				}
			}
			idx := must(h.s.PutTree(ctx, entries))
			root := must(h.s.PutTree(ctx, []warp.Entry{{Name: "beliefs", Kind: warp.KindTree, ID: idx}}))
			bad := must(h.s.Commit(ctx, warp.CommitSpec{Tree: root, Parents: []warp.ID{h.head("refs/loom/b")}, Message: "crafted",
				Author: warp.Signature{Name: "x", Email: "x@x", When: time.Unix(1, 0)}}))
			must(0, h.s.Advance(ctx, "refs/loom/b", h.head("refs/loom/b"), bad))
			if _, err := loom.Beliefs(ctx, h.s, bad); !errors.Is(err, warp.ErrInvalid) {
				t.Errorf("Beliefs: err = %v, want ErrInvalid", err)
			}
			_, err := h.merge("refs/loom/a", "refs/loom/b", loom.ResolveOurs)
			if appErrType(err) != "InvalidInput" {
				t.Errorf("merge: err = %v, want non-retryable InvalidInput", err)
			}
		})
	}
}

// TestInv_MergePostconditionIsChecked: the merge's postcondition rejects a
// lost question, an invented answer and a duplicate. (Review 4: disabling
// the check didn't fail any test.)
func TestInv_MergePostconditionIsChecked(t *testing.T) {
	e := func(q, id string) warp.Entry {
		return warp.Entry{Name: q, Kind: warp.KindBlob, ID: warp.ID(strings.Repeat(id, 40))}
	}
	ours := map[string]warp.Entry{"q1": e("q1", "a"), "q2": e("q2", "b")}
	theirs := map[string]warp.Entry{"q2": e("q2", "c"), "q3": e("q3", "d")}
	for name, final := range map[string][]warp.Entry{
		"lost":      {e("q1", "a"), e("q2", "b")},
		"invented":  {e("q1", "a"), e("q2", "f"), e("q3", "d")},
		"duplicate": {e("q1", "a"), e("q2", "b"), e("q2", "c"), e("q3", "d")},
	} {
		if err := loom.CheckUnion(final, ours, theirs); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := loom.CheckUnion([]warp.Entry{e("q1", "a"), e("q2", "c"), e("q3", "d")}, ours, theirs); err != nil {
		t.Errorf("a valid merge: %v", err)
	}
}

// TestMerge_UnrelatedHistoriesUnite: two refs that never shared a run
// merge as a union against an empty base.
func TestMerge_UnrelatedHistoriesUnite(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "q1", "1")
	h.ask("refs/loom/b", "q2", "2")
	res := must(h.merge("refs/loom/a", "refs/loom/b", ""))
	if got := h.beliefs(res.Commit); len(got) != 2 {
		t.Fatalf("beliefs = %v", got)
	}
}

// TestBranch_CreatesOnlyNewRefs: a branch starts at the pinned head; a
// retry is harmless even if the source moved meanwhile (review 4); an
// existing ref elsewhere is never moved.
func TestBranch_CreatesOnlyNewRefs(t *testing.T) {
	h := newHist(t)
	c := h.ask("refs/loom/a", "q1", "1")
	in := loom.BranchRecordInput{To: "refs/loom/b", At: c}
	must(0, h.rec.Branch(ctx, in))
	h.ask("refs/loom/a", "q2", "2") // the source moves on
	if err := h.rec.Branch(ctx, in); err != nil {
		t.Fatalf("retry after the source moved: %v", err)
	}
	if err := h.rec.Branch(ctx, loom.BranchRecordInput{To: "refs/loom/b", At: h.head("refs/loom/a")}); appErrType(err) != "BranchExists" {
		t.Fatalf("err = %v, want BranchExists", err)
	}
	if h.head("refs/loom/b") != c {
		t.Fatal("an existing branch moved")
	}
	if _, err := h.rec.Pin(ctx, "refs/loom/missing"); appErrType(err) != "InvalidInput" {
		t.Errorf("pin a missing ref: %v", err)
	}
	if err := h.rec.Branch(ctx, loom.BranchRecordInput{To: "bad", At: c}); appErrType(err) != "InvalidInput" {
		t.Errorf("bad ref: %v", err)
	}
}

// TestMergeWorkflow_RerunsConflicts runs the whole thing on Temporal's test
// environment: branch, run on both sides, merge with rerun. The conflicting
// question is run again on the merged ref, and its new run is the belief.
func TestMergeWorkflow_RerunsConflicts(t *testing.T) {
	h := newHist(t)
	src := `(echo "late?")`
	h.ask("refs/loom/a", "late?", "base")
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	loom.Register(env, h.s, draft.DefaultRegistry())
	env.ExecuteWorkflow(loom.BranchWorkflowName, loom.BranchInput{From: "refs/loom/a", To: "refs/loom/b"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	h.ask("refs/loom/a", "late?", "yes")
	h.ask("refs/loom/b", "late?", "no")

	env = suite.NewTestWorkflowEnvironment()
	loom.Register(env, h.s, draft.DefaultRegistry())
	env.ExecuteWorkflow(loom.MergeWorkflowName, loom.MergeInput{Into: "refs/loom/a", From: "refs/loom/b", Resolve: loom.ResolveRerun})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	// Temporal's test environment doesn't record child memos, so check the
	// options the workflow starts each re-run with.
	if o := loom.RerunOptions("refs/loom/a", "wf/rerun-0"); o.Memo[loom.MemoRef] != "refs/loom/a" || o.WorkflowID != "wf/rerun-0" {
		t.Errorf("re-run options = %+v", o)
	}
	var res loom.MergeResult
	must(0, env.GetWorkflowResult(&res))
	q := mustQ(t, src)
	if res.Kind != loom.MergeCommitted || res.Resolved[q] != loom.ResolveRerun || res.Commit != h.head("refs/loom/a") {
		t.Fatalf("result = %+v", res)
	}
	// echo of "late?" with no upstream outputs its argument: the rerun's
	// fresh answer, recorded on main before the merge and kept by it.
	if got := h.beliefs(res.Commit)[src]; got != "late?" {
		t.Fatalf("after rerun believes %q", got)
	}
	parents := strings.Fields(gitOut(t, h.s, "log", "-1", "--format=%P", string(res.Commit)))
	if len(parents) != 2 || parents[0] != string(res.RerunCommits[q]) {
		t.Fatalf("the merge's first parent %v isn't the rerun %s", parents, res.RerunCommits[q])
	}

	env = suite.NewTestWorkflowEnvironment()
	loom.Register(env, h.s, draft.DefaultRegistry())
	env.ExecuteWorkflow(loom.MergeWorkflowName, loom.MergeInput{Into: "refs/loom/a", From: "refs/loom/b", Resolve: "mine"})
	if appErrType(env.GetWorkflowError()) != "InvalidInput" {
		t.Fatalf("bad resolve: %v", env.GetWorkflowError())
	}
}

// TestMergeWorkflow_FailedRerunMergesNothing: re-runs happen before the
// merge, so a failed re-run leaves the target untouched and submitting the
// merge again re-runs and merges. (Review 4: the merge was written first,
// and a failed re-run was lost on the next attempt.)
func TestMergeWorkflow_FailedRerunMergesNothing(t *testing.T) {
	h := newHist(t)
	h.ask("refs/loom/a", "late?", "base")
	h.branch("refs/loom/a", "refs/loom/b")
	h.ask("refs/loom/a", "late?", "yes")
	h.ask("refs/loom/b", "late?", "no")
	before := h.head("refs/loom/a")
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	// A worker whose registry lacks echo: the re-run's check fails.
	loom.Register(env, h.s, registry.New())
	env.ExecuteWorkflow(loom.MergeWorkflowName, loom.MergeInput{Into: "refs/loom/a", From: "refs/loom/b", Resolve: loom.ResolveRerun})
	if env.GetWorkflowError() == nil {
		t.Fatal("merge succeeded although its re-run failed")
	}
	if h.head("refs/loom/a") != before {
		t.Fatal("a failed re-run left a merge behind")
	}
	env = suite.NewTestWorkflowEnvironment()
	loom.Register(env, h.s, draft.DefaultRegistry())
	env.ExecuteWorkflow(loom.MergeWorkflowName, loom.MergeInput{Into: "refs/loom/a", From: "refs/loom/b", Resolve: loom.ResolveRerun})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if got := h.beliefs(h.head("refs/loom/a"))[`(echo "late?")`]; got != "late?" {
		t.Fatalf("believes %q after the second merge", got)
	}
}

func gitOut(t *testing.T, s *warp.Store, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"--git-dir", s.Dir()}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// countingHistory counts blob reads.
type countingHistory struct {
	*warp.Store
	blob, blobs int
}

func (c *countingHistory) Blob(ctx context.Context, id warp.ID) ([]byte, error) {
	c.blob++
	return c.Store.Blob(ctx, id)
}

func (c *countingHistory) Blobs(ctx context.Context, ids []warp.ID) ([][]byte, error) {
	c.blobs++
	return c.Store.Blobs(ctx, ids)
}

// TestInv_IndexIsReadInOneCall: every Record reads the beliefs index, which
// holds every question ever asked, so it is read with one Blobs call however
// many questions it holds. (drawdown CI on macOS: one Blob per entry made
// record N cost 2N git processes.)
func TestInv_IndexIsReadInOneCall(t *testing.T) {
	h := newHist(t)
	const n = 30
	for i := 0; i < n; i++ {
		h.ask("refs/loom/a", fmt.Sprintf("q%d", i), "1")
	}
	c := &countingHistory{Store: h.s}
	runs := must(loom.ReadIndex(ctx, c, h.head("refs/loom/a")))
	if len(runs) != n || c.blob != 0 || c.blobs != 1 {
		t.Errorf("index of %d questions: %d read, %d Blob and %d Blobs calls; want %d, 0 and 1", n, len(runs), c.blob, c.blobs, n)
	}
}

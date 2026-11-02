package loom

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/heedle-ai/warp"
)

// FilesDir is the record's directory of step files: files/<node>/<path>.
const FilesDir = "files"

// MaxRecordedFiles is how many step files one record holds, in all: each
// costs a git process, and Record must finish well inside its timeout
// (writeOptions), on macOS too. pirn keeps at most 20 files an agent step.
const MaxRecordedFiles = 500

// StepFiles reads the files a step's output carries, by path. An output
// carries files when it is a JSON object whose key "files" (exactly) is an
// array of objects with "path" and either "text" or "base64" (neither is
// an empty file): the shape of pirn's agent result, whose files are what
// the agent left in /work/out. Keys are matched exactly; for a repeated
// key, the last wins, as in Go's JSON decoding. Any other output carries
// none.
//
// Each entry is read on its own, and left out (listed in skipped, by path,
// or "#i" for the i'th entry if it has no path) when:
//   - it isn't an object with a string path, string text or base64;
//   - it has both text and base64, or bad base64;
//   - a segment of its path is one Git refuses or treats specially: empty,
//     ".", "..", anything starting ".git" or "git~" in any case (".git",
//     ".gitmodules", ".gitattributes", NTFS short names), anything with a
//     "~" and a digit, ending in "." or " ", or holding a character macOS
//     ignores in names (U+200C to U+200F, U+202A to U+202E, U+206A to
//     U+206F, U+FEFF), so a store always passes git fsck --strict;
//   - its path appears more than once (all of them are left out);
//   - in sorted path order, its path matches an earlier file's or
//     directory's, or runs through an earlier file, ignoring case, so it
//     can be checked out on macOS's case-insensitive disks too.
//
// The result is a function of the set of entries alone, whatever their
// order. Left-out files are still in the output.
func StepFiles(out string) (files map[string][]byte, skipped []string) {
	var top map[string]json.RawMessage
	var raw []json.RawMessage
	if !strings.HasPrefix(strings.TrimSpace(out), "{") || json.Unmarshal([]byte(out), &top) != nil ||
		top["files"] == nil || json.Unmarshal(top["files"], &raw) != nil {
		return nil, nil
	}
	type entry struct {
		path string
		data []byte
	}
	var es []entry
	count := map[string]int{}
	for i, r := range raw {
		path, data, ok := readEntry(r)
		if !ok {
			if path == "" {
				path = fmt.Sprintf("#%d", i)
			}
			skipped = append(skipped, path)
			continue
		}
		es = append(es, entry{path, data})
		count[path]++
	}
	sort.Slice(es, func(i, j int) bool { return es[i].path < es[j].path })
	files = map[string][]byte{}
	taken, dirs := map[string]bool{}, map[string]bool{} // case-folded
	for _, e := range es {
		segs := strings.Split(strings.ToLower(e.path), "/")
		ok := count[e.path] == 1 && !taken[strings.Join(segs, "/")] && !dirs[strings.Join(segs, "/")]
		for i := 1; ok && i < len(segs); i++ {
			ok = !taken[strings.Join(segs[:i], "/")]
		}
		if !ok {
			skipped = append(skipped, e.path)
			continue
		}
		files[e.path] = e.data
		taken[strings.Join(segs, "/")] = true
		for i := 1; i < len(segs); i++ {
			dirs[strings.Join(segs[:i], "/")] = true
		}
	}
	sort.Strings(skipped)
	return files, skipped
}

// readEntry reads one entry of a step's files: its path, and its contents
// if it is well-formed.
func readEntry(r json.RawMessage) (path string, data []byte, ok bool) {
	var m map[string]json.RawMessage
	if json.Unmarshal(r, &m) != nil || json.Unmarshal(m["path"], &path) != nil {
		return "", nil, false
	}
	var text, b64 *string
	if m["text"] != nil && json.Unmarshal(m["text"], &text) != nil || m["base64"] != nil && json.Unmarshal(m["base64"], &b64) != nil {
		return path, nil, false
	}
	if !validPath(path) || text != nil && b64 != nil {
		return path, nil, false
	}
	switch {
	case b64 != nil:
		b, err := base64.StdEncoding.DecodeString(*b64)
		return path, b, err == nil
	case text != nil:
		return path, []byte(*text), true
	}
	return path, []byte{}, true
}

var ntfsShortName = regexp.MustCompile(`~[0-9]`)

func validPath(p string) bool {
	for _, seg := range strings.Split(p, "/") {
		low := strings.ToLower(seg)
		if warp.ValidName(seg) != nil || strings.HasPrefix(low, ".git") || strings.HasPrefix(low, "git~") ||
			ntfsShortName.MatchString(seg) || strings.HasSuffix(seg, ".") || strings.HasSuffix(seg, " ") ||
			strings.ContainsFunc(seg, hfsIgnored) {
			return false
		}
	}
	return true
}

// hfsIgnored is whether macOS's file systems ignore r in names, so that
// ".g\u200cit" would be ".git" there.
func hfsIgnored(r rune) bool {
	return r >= 0x200c && r <= 0x200f || r >= 0x202a && r <= 0x202e || r >= 0x206a && r <= 0x206f || r == 0xfeff
}

// filesTree stores each node's step files as a tree, files/<node>/<path>,
// and returns the files tree's ID, or "" if no step carried any files.
func filesTree(ctx context.Context, s *warp.Store, outputs map[string]string) (warp.ID, error) {
	var names []string
	for node := range outputs {
		names = append(names, node)
	}
	sort.Strings(names)
	var nodes []warp.Entry
	left := MaxRecordedFiles
	for _, node := range names {
		files, _ := StepFiles(outputs[node])
		files = firstFiles(files, left)
		left -= len(files)
		if len(files) == 0 {
			continue
		}
		id, err := putDir(ctx, s, files, "")
		if err != nil {
			return "", err
		}
		nodes = append(nodes, warp.Entry{Name: node, Kind: warp.KindTree, ID: id})
	}
	if len(nodes) == 0 {
		return "", nil
	}
	return s.PutTree(ctx, nodes)
}

// firstFiles is the first n of files in path order.
func firstFiles(files map[string][]byte, n int) map[string][]byte {
	if len(files) <= n {
		return files
	}
	var paths []string
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	out := map[string][]byte{}
	for _, p := range paths[:max(n, 0)] {
		out[p] = files[p]
	}
	return out
}

// putDir stores the files under prefix (relative paths, "/"-separated) as
// a tree of trees.
func putDir(ctx context.Context, s *warp.Store, files map[string][]byte, prefix string) (warp.ID, error) {
	subdirs := map[string]bool{}
	var es []warp.Entry
	for p, data := range files {
		rest, ok := strings.CutPrefix(p, prefix)
		if !ok {
			continue
		}
		if name, _, nested := strings.Cut(rest, "/"); nested {
			subdirs[name] = true
			continue
		}
		id, err := s.PutBlob(ctx, data)
		if err != nil {
			return "", err
		}
		es = append(es, warp.Entry{Name: rest, Kind: warp.KindBlob, ID: id})
	}
	for name := range subdirs {
		id, err := putDir(ctx, s, files, prefix+name+"/")
		if err != nil {
			return "", err
		}
		es = append(es, warp.Entry{Name: name, Kind: warp.KindTree, ID: id})
	}
	return s.PutTree(ctx, es)
}

package loom

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strings"

	"github.com/heedle-ai/warp"
)

// FilesDir is the record's directory of step files: files/<node>/<path>.
const FilesDir = "files"

// StepFiles reads the files a step's output carries, by path. An output
// carries files when it is a JSON object with a "files" array of
// {"path", and "text" or "base64"; neither is an empty file}: the shape of pirn's agent result,
// whose files are what the agent left in /work/out. Any other output
// carries none.
//
// A file is left out of the result (and listed in skipped) when its path
// can't be a Git path (an empty, ".", ".." or ".git" segment, or a
// leading or doubled "/"), when it has both text and base64, or bad base64, or when its path repeats an earlier one or runs through
// an earlier file. Paths are taken in sorted order, so the result is a
// function of the output alone. Left-out files are still in the output.
func StepFiles(out string) (files map[string][]byte, skipped []string) {
	var v struct {
		Files []struct {
			Path   string  `json:"path"`
			Text   *string `json:"text"`
			Base64 *string `json:"base64"`
		} `json:"files"`
	}
	if !strings.HasPrefix(strings.TrimSpace(out), "{") || json.Unmarshal([]byte(out), &v) != nil {
		return nil, nil
	}
	sort.SliceStable(v.Files, func(i, j int) bool { return v.Files[i].Path < v.Files[j].Path })
	files = map[string][]byte{}
	dirs := map[string]bool{}
	for _, f := range v.Files {
		data, ok := []byte(nil), validPath(f.Path)
		switch {
		case !ok:
		case f.Text != nil && f.Base64 != nil:
			ok = false
		case f.Base64 != nil:
			b, err := base64.StdEncoding.DecodeString(*f.Base64)
			data, ok = b, err == nil
		case f.Text != nil:
			data = []byte(*f.Text)
		}
		// The path must not be an earlier file or directory, nor run
		// through an earlier file.
		segs := strings.Split(f.Path, "/")
		if ok && (files[f.Path] != nil || dirs[f.Path]) {
			ok = false
		}
		for i := 1; ok && i < len(segs); i++ {
			if _, isFile := files[strings.Join(segs[:i], "/")]; isFile {
				ok = false
			}
		}
		if !ok {
			skipped = append(skipped, f.Path)
			continue
		}
		if data == nil {
			data = []byte{}
		}
		files[f.Path] = data
		for i := 1; i < len(segs); i++ {
			dirs[strings.Join(segs[:i], "/")] = true
		}
	}
	return files, skipped
}

func validPath(p string) bool {
	for _, seg := range strings.Split(p, "/") {
		if warp.ValidName(seg) != nil {
			return false
		}
	}
	return true
}

// filesTree stores each node's step files as a tree, files/<node>/<path>,
// and returns the files tree's ID, or "" if no step carried any files.
func filesTree(ctx context.Context, s *warp.Store, outputs map[string]string) (warp.ID, error) {
	var nodes []warp.Entry
	for node, out := range outputs {
		files, _ := StepFiles(out)
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

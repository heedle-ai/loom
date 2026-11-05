package loom

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/heedle-ai/pirn"
)

// Memory you can see (roadmap 10.1): short facts about you that you've
// asked heedle to remember, such as "I'm vegetarian" or "my home airport
// is IAD". They go to Claude with each front-door request, as a source
// (never as instructions), and the proposal says which were sent.
//
// They live in one file, ~/.heedle/memory.json for heedle, never in the
// Git store, whose history keeps everything: forgetting one rewrites the
// file without it, and nothing else heedle writes holds it. The file is a
// JSON list of {id, text, added, used}. It is written only through
// ChangeMemory: a new file, synced, renamed over the old one, keeping its
// mode, and only if it still holds what was read, so a hand edit made
// meanwhile is never lost.
const (
	// MaxMemories bounds how many things heedle remembers.
	MaxMemories = 200
	// MaxMemoryText bounds one memory, in characters.
	MaxMemoryText = 500
	// MaxMemoryTotal bounds all memories' text together, in bytes.
	MaxMemoryTotal = 16 << 10
	// MaxRemember bounds the suggestions one proposal makes.
	MaxRemember = 5
	// MemorySourceID is the source memories reach Claude as.
	MemorySourceID = "memory"
	// maxMemoryFile bounds the file read: the texts and their fields.
	maxMemoryFile = MaxMemoryTotal + MaxMemories*256
)

// Memory is one thing heedle remembers about you: its text, when you
// added it, and when it last went to Claude with a request (zero: never).
type Memory struct {
	ID    string    `json:"id"`
	Text  string    `json:"text"`
	Added time.Time `json:"added"`
	Used  time.Time `json:"used,omitzero"`
}

// Memories are everything heedle remembers, in the order added.
type Memories []Memory

// ErrMemoryInvalid is something heedle won't remember, or a memory file
// it can't read as one.
var ErrMemoryInvalid = errors.New("heedle can't remember that")

// ErrMemoryChanged is a change refused because the memory file isn't what
// the change was based on any more: edited by hand, or by another page.
var ErrMemoryChanged = errors.New("the memory file changed since it was read (edited by hand, or elsewhere); look again, and try again")

var memoryID = regexp.MustCompile(`^m[0-9a-f]{12}$`)

// CheckMemoryText refuses what heedle mustn't remember: an empty text or
// one longer than MaxMemoryText characters, more than one line, control
// characters, and anything that looks like a secret, a card number or a
// password (LooksSecret). Those belong in your keychain, or your profile.
func CheckMemoryText(text string) error {
	if why := memoryProblem(text); why != "" {
		return fmt.Errorf("%w: %s", ErrMemoryInvalid, why)
	}
	return nil
}

// memoryProblem says why text can't be a memory, without repeating it,
// or "".
func memoryProblem(text string) string {
	t := strings.TrimSpace(text)
	switch {
	case t == "":
		return "a memory can't be empty"
	case !utf8.ValidString(t):
		return "a memory must be text"
	case utf8.RuneCountInString(t) > MaxMemoryText:
		return fmt.Sprintf("a memory is at most %d characters", MaxMemoryText)
	case strings.ContainsFunc(t, unicode.IsControl):
		return "a memory is one line of text"
	}
	if why := LooksSecret(t); why != "" {
		return "that looks like " + why + ", which heedle never remembers (keep keys in your keychain: heedle secret set NAME)"
	}
	return ""
}

// Check is whether ms can be heedle's memory: each ID well formed and
// unique, each text one CheckMemoryText allows (and stored trimmed), each
// added at some time, and the bounds.
func (ms Memories) Check() error {
	if len(ms) > MaxMemories {
		return fmt.Errorf("%w: heedle remembers at most %d things; forget some first", ErrMemoryInvalid, MaxMemories)
	}
	seen, total := map[string]bool{}, 0
	for _, m := range ms {
		if !memoryID.MatchString(m.ID) || seen[m.ID] {
			return fmt.Errorf("%w: memory id %q isn't m and 12 hex digits, once", ErrMemoryInvalid, m.ID)
		}
		seen[m.ID] = true
		if err := CheckMemoryText(m.Text); err != nil {
			return fmt.Errorf("memory %s: %w", m.ID, err)
		}
		if m.Text != strings.TrimSpace(m.Text) || m.Added.IsZero() {
			return fmt.Errorf("%w: memory %s needs trimmed text and when it was added", ErrMemoryInvalid, m.ID)
		}
		total += len(m.Text)
	}
	if total > MaxMemoryTotal {
		return fmt.Errorf("%w: heedle remembers at most %d bytes of text in all; forget some first", ErrMemoryInvalid, MaxMemoryTotal)
	}
	return nil
}

// Version names what ms remembers (each ID, text and when it was added,
// in order), so a change can say what it was based on. When each was last
// used isn't part of it: a request using memory doesn't make a page's
// view of it stale.
func (ms Memories) Version() string {
	h := sha256.New()
	for _, m := range ms {
		fmt.Fprintf(h, "%s\x00%s\x00%s\n", m.ID, m.Text, m.Added.UTC().Format(time.RFC3339Nano))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Source is ms as Claude is shown it: the memory source, one line each,
// texts only (an ID or a date would tell Claude nothing about you), or
// false when there's nothing to show.
func (ms Memories) Source() (pirn.Source, bool) {
	if len(ms) == 0 {
		return pirn.Source{}, false
	}
	var b strings.Builder
	for _, m := range ms {
		b.WriteString("- " + m.Text + "\n")
	}
	return pirn.Source{ID: MemorySourceID, Text: b.String()}, true
}

// IDs are ms's IDs, in order.
func (ms Memories) IDs() []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

// ReadMemory reads the memory file at path strictly: one JSON list of
// memories with no other fields, each as Check allows. A file that isn't
// there is nothing remembered yet.
func ReadMemory(path string) (Memories, error) {
	ms, _, err := readMemory(path)
	return ms, err
}

func readMemory(path string) (Memories, []byte, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return Memories{}, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxMemoryFile+1))
	if err != nil {
		return nil, nil, err
	}
	if len(b) > maxMemoryFile {
		return nil, nil, fmt.Errorf("%w: %s is bigger than heedle's memory can be", ErrMemoryInvalid, path)
	}
	ms := Memories{}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ms); err != nil {
		return nil, nil, fmt.Errorf("%w: %s isn't a list of memories: %v", ErrMemoryInvalid, path, err)
	}
	if dec.More() {
		return nil, nil, fmt.Errorf("%w: %s holds more than one list", ErrMemoryInvalid, path)
	}
	if ms == nil {
		ms = Memories{} // "null"
	}
	if err := ms.Check(); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	return ms, b, nil
}

// memoryMu serializes this process's changes to memory files; the file's
// own contents, checked just before the rename, guard against everything
// else (another process, or a hand edit).
var memoryMu sync.Mutex

// ChangeMemory changes the memory file at path with edit, if what it
// remembers is still version (Memories.Version; "" for whatever it holds
// now), and returns what it remembers afterwards. edit gets a copy of what
// the file holds now, so what it leaves alone stays exactly as it was,
// hand edits included; nothing is written unless the result passes Check,
// or if the file changed meanwhile.
func ChangeMemory(path, version string, edit func(Memories) (Memories, error)) (Memories, error) {
	memoryMu.Lock()
	defer memoryMu.Unlock()
	ms, before, err := readMemory(path)
	if err != nil {
		return nil, err
	}
	if version != "" && ms.Version() != version {
		return nil, ErrMemoryChanged
	}
	next, err := edit(slices.Clone(ms))
	if err != nil {
		return nil, err
	}
	if next == nil {
		next = Memories{}
	}
	if err := next.Check(); err != nil {
		return nil, err
	}
	if err := writeMemory(path, next, before); err != nil {
		return nil, err
	}
	return next, nil
}

// writeMemory replaces the file at path with ms, all at once: a new file,
// written and synced, renamed over it, so a reader sees the old file or
// the new one, never half of either. It keeps the file's mode (0600 for a
// new one), and writes through a symbolic link to the file it names. If
// the file no longer holds before (nil: it wasn't there), nothing is
// written.
func writeMemory(path string, ms Memories, before []byte) error {
	b, err := json.MarshalIndent(ms, "", "  ")
	if err != nil {
		return err
	}
	if real, err := filepath.EvalSymlinks(path); err == nil {
		path = real
	} else if fi, lerr := os.Lstat(path); lerr == nil && fi.Mode()&os.ModeSymlink != 0 {
		// A link to a file not made yet: make the file it names.
		target, err := os.Readlink(path)
		if err != nil {
			return err
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(path), target)
		}
		path = target
	}
	mode := os.FileMode(0o600)
	if fi, err := os.Stat(path); err == nil {
		mode = fi.Mode().Perm()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".memory-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(append(b, '\n'))
	if err == nil {
		err = f.Chmod(mode)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	now, rerr := os.ReadFile(path)
	if before == nil && !errors.Is(rerr, os.ErrNotExist) || before != nil && !bytes.Equal(now, before) {
		return ErrMemoryChanged
	}
	return os.Rename(f.Name(), path)
}

// newMemoryID is an ID no memory in ms has.
func newMemoryID(ms Memories) (string, error) {
	for {
		var b [6]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		id := "m" + hex.EncodeToString(b[:])
		if !slices.ContainsFunc(ms, func(m Memory) bool { return m.ID == id }) {
			return id, nil
		}
	}
}

// AddMemory remembers text, added at now. Something already remembered
// (the same words, whatever their case) isn't added again: that memory is
// returned, and nothing is written.
func AddMemory(path, version, text string, now time.Time) (Memory, error) {
	if err := CheckMemoryText(text); err != nil {
		return Memory{}, err
	}
	text = strings.TrimSpace(text)
	var added Memory
	_, err := ChangeMemory(path, version, func(ms Memories) (Memories, error) {
		if i := slices.IndexFunc(ms, func(m Memory) bool { return strings.EqualFold(m.Text, text) }); i >= 0 {
			added = ms[i]
			return nil, errAlready
		}
		id, err := newMemoryID(ms)
		if err != nil {
			return nil, err
		}
		added = Memory{ID: id, Text: text, Added: now.UTC()}
		return append(ms, added), nil
	})
	if errors.Is(err, errAlready) {
		return added, nil
	}
	return added, err
}

var errAlready = errors.New("already remembered")

// EditMemory changes the text of the memory id.
func EditMemory(path, version, id, text string) (Memories, error) {
	if err := CheckMemoryText(text); err != nil {
		return nil, err
	}
	return ChangeMemory(path, version, func(ms Memories) (Memories, error) {
		i := slices.IndexFunc(ms, func(m Memory) bool { return m.ID == id })
		if i < 0 {
			return nil, fmt.Errorf("%w: nothing remembered as %s", ErrNoMemory, id)
		}
		ms[i].Text = strings.TrimSpace(text)
		return ms, nil
	})
}

// ErrNoMemory is a memory ID that isn't remembered.
var ErrNoMemory = errors.New("no such memory")

// ForgetMemory forgets the memory id: the file is rewritten without it.
func ForgetMemory(path, version, id string) (Memories, error) {
	return ChangeMemory(path, version, func(ms Memories) (Memories, error) {
		i := slices.IndexFunc(ms, func(m Memory) bool { return m.ID == id })
		if i < 0 {
			return nil, fmt.Errorf("%w: nothing remembered as %s", ErrNoMemory, id)
		}
		return slices.Delete(ms, i, i+1), nil
	})
}

// ForgetAllMemory forgets everything: the file is rewritten empty.
func ForgetAllMemory(path, version string) (Memories, error) {
	return ChangeMemory(path, version, func(Memories) (Memories, error) { return Memories{}, nil })
}

// MarkMemoryUsed records that the memories ids went to Claude at now.
// Only those still remembered are marked: one forgotten meanwhile stays
// forgotten.
func MarkMemoryUsed(path string, ids []string, now time.Time) error {
	_, err := ChangeMemory(path, "", func(ms Memories) (Memories, error) {
		for i := range ms {
			if slices.Contains(ids, ms[i].ID) {
				ms[i].Used = now.UTC()
			}
		}
		return ms, nil
	})
	return err
}

// What heedle never remembers, because it would go to Claude: keys and
// tokens (by their well-known shapes, or any long run of letters and
// digits), private keys, card numbers, and passwords.
var secretShapes = []struct {
	re   *regexp.Regexp
	what string
}{
	{regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY`), "a private key"},
	{regexp.MustCompile(`\b(sk-ant-|sk-proj-|sk-[A-Za-z0-9]{20}|ghp_|gho_|ghu_|ghs_|ghr_|github_pat_|glpat-|xox[abposr]-|AIza[0-9A-Za-z_-]{20}|(AKIA|ASIA)[0-9A-Z]{16}|npm_[A-Za-z0-9]{20})`), "an API key or token"},
	{regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.`), "a sign-in token"},
	{regexp.MustCompile(`(?i)\b(pass ?words?|passwd|pass ?codes?|pass ?phrases?)\b`), "a password"},
	{regexp.MustCompile(`(?i)\b(pin|cvv|cvc|security code)\b\s*(is|:|=)?\s*\d{3,}`), "a PIN or card code"},
}

var opaque = regexp.MustCompile(`[A-Za-z0-9+/_=-]{32,}`)
var cardDigits = regexp.MustCompile(`\d(?:[ -]?\d){12,18}`)

// LooksSecret says what a text looks like if it looks like something
// heedle must never remember or publish (such as "an API key or token",
// "a card number" or "a password"), else "".
func LooksSecret(text string) string {
	for _, s := range secretShapes {
		if s.re.MatchString(text) {
			return s.what
		}
	}
	for _, run := range opaque.FindAllString(text, -1) {
		if strings.ContainsAny(run, "0123456789") && strings.IndexFunc(run, unicode.IsLetter) >= 0 {
			return "an API key or token"
		}
	}
	for _, run := range cardDigits.FindAllString(text, -1) {
		if luhn(strings.NewReplacer(" ", "", "-", "").Replace(run)) {
			return "a card number"
		}
	}
	return ""
}

// luhn is whether digits pass the Luhn check card numbers carry.
func luhn(digits string) bool {
	sum, double := 0, false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i] - '0')
		if double {
			if d *= 2; d > 9 {
				d -= 9
			}
		}
		sum, double = sum+d, !double
	}
	return sum%10 == 0
}

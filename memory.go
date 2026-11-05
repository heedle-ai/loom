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
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/heedle-ai/pirn"
)

// Memory you can see (roadmap 10.1): short facts about you that you've
// asked heedle to remember, such as "Vegetarian." or "Home airport:
// IAD.". They go to Claude with each front-door request, as a source
// (never as instructions), and the proposal says which were sent.
//
// They live in one file, ~/.heedle/memory.json for heedle, never in the
// Git store, whose history keeps everything: forgetting one rewrites the
// file without it. The file is a JSON list of {id, text, added, used}. It
// is written only through ChangeMemory: under a lock every heedle process
// takes, a new file, synced, renamed over the old one, keeping its mode,
// and only if it still holds what was read, so a hand edit made meanwhile
// is never lost.
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
	// maxMemoryFile bounds the file read: the texts, each byte escaped at
	// worst into two (a quote or a backslash; nothing else is escaped),
	// and the other fields (review 47: heedle's own file outgrew it).
	maxMemoryFile = 2*MaxMemoryTotal + MaxMemories*256
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

// ErrMemoryInvalid is something heedle won't remember.
var ErrMemoryInvalid = errors.New("heedle can't remember that")

// ErrMemoryFile is a memory file heedle can't read as one.
var ErrMemoryFile = errors.New("heedle's memory file has a problem")

// ErrMemoryChanged is a change refused because the memory file isn't what
// the change was based on any more: edited by hand, or by another page.
var ErrMemoryChanged = errors.New("the memory file changed since it was read (edited by hand, or elsewhere); look again, and try again")

// ErrNoMemory is a memory ID that isn't remembered.
var ErrNoMemory = errors.New("no such memory")

var memoryID = regexp.MustCompile(`^m[0-9a-f]{12}$`)

// CheckMemoryText refuses what heedle mustn't remember: an empty text or
// one longer than MaxMemoryText characters, more than one line, control,
// format and other invisible characters, and anything that looks like a
// secret, a card number or a password (LooksSecret). Those belong in your
// keychain.
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
	case strings.ContainsFunc(t, hidden):
		// Review 47: tag characters and bidirectional overrides hid text
		// the page didn't show, and a zero-width space split "password".
		return "a memory is one line of plain text, with nothing invisible in it"
	}
	if why := LooksSecret(t); why != "" {
		return "that looks like " + why + ", which heedle never remembers (keep keys in your keychain: heedle secret set NAME)"
	}
	return ""
}

// hidden is a character a memory may not hold: one that isn't shown as
// itself (controls, format characters such as zero-width ones and
// bidirectional overrides, line and paragraph separators, private-use and
// surrogate code points, noncharacters).
func hidden(r rune) bool {
	return unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp, unicode.Co, unicode.Cs) ||
		r == utf8.RuneError || r&0xFFFE == 0xFFFE || (r >= 0xFDD0 && r <= 0xFDEF)
}

// Problems are, for each memory that couldn't be remembered as it is now
// (edited by hand into one), why not; and, under "", a bound the whole
// list is over. None: everything can go to Claude.
func (ms Memories) Problems() map[string]string {
	out := map[string]string{}
	total := 0
	for _, m := range ms {
		if why := memoryProblem(m.Text); why != "" {
			out[m.ID] = why
		} else if m.Text != strings.TrimSpace(m.Text) {
			out[m.ID] = "a memory's text has no spaces around it"
		} else if m.Added.IsZero() {
			out[m.ID] = "a memory needs when it was added"
		}
		total += len(m.Text)
	}
	switch {
	case len(ms) > MaxMemories:
		out[""] = fmt.Sprintf("heedle remembers at most %d things; forget some first", MaxMemories)
	case total > MaxMemoryTotal:
		out[""] = fmt.Sprintf("heedle remembers at most %d bytes of text in all; forget some first", MaxMemoryTotal)
	}
	return out
}

// Check is whether ms can be heedle's memory: its shape (checkShape), and
// no Problems.
func (ms Memories) Check() error {
	if err := ms.checkShape(); err != nil {
		return err
	}
	if p := ms.Problems(); len(p) > 0 {
		if why, ok := p[""]; ok {
			return fmt.Errorf("%w: %s", ErrMemoryInvalid, why)
		}
		for _, m := range ms {
			if why, ok := p[m.ID]; ok {
				return fmt.Errorf("memory %s: %w: %s", m.ID, ErrMemoryInvalid, why)
			}
		}
	}
	return nil
}

// checkShape is whether ms can be read and changed at all: each ID well
// formed and once.
func (ms Memories) checkShape() error {
	seen := map[string]bool{}
	for _, m := range ms {
		if !memoryID.MatchString(m.ID) || seen[m.ID] {
			return fmt.Errorf("%w: memory id %q isn't m and 12 hex digits, once", ErrMemoryFile, m.ID)
		}
		seen[m.ID] = true
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

// ReadMemory reads what heedle remembers, to send to Claude: the memory
// file at path, read strictly, each memory as Check allows. A file that
// isn't there is nothing remembered yet.
func ReadMemory(path string) (Memories, error) {
	ms, _, err := readMemory(path)
	if err == nil {
		err = ms.Check()
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return ms, nil
}

// ReadMemoryAsIs reads the memory file at path as it is, for you to see
// and change: one JSON list of memories with no other fields, each ID
// once, but each text as it is, even one heedle wouldn't send to Claude
// (Problems says which). So a memory edited by hand into one heedle won't
// keep can still be seen, and forgotten (review 47).
func ReadMemoryAsIs(path string) (Memories, error) {
	ms, _, err := readMemory(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return ms, nil
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
		return nil, nil, fmt.Errorf("%w: it's bigger than heedle's memory can be", ErrMemoryFile)
	}
	ms := Memories{}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ms); err != nil {
		return nil, nil, fmt.Errorf("%w: it isn't a list of memories: %v", ErrMemoryFile, err)
	}
	if dec.More() {
		return nil, nil, fmt.Errorf("%w: it holds more than one list", ErrMemoryFile)
	}
	if ms == nil {
		ms = Memories{} // "null"
	}
	if err := ms.checkShape(); err != nil {
		return nil, nil, err
	}
	return ms, b, nil
}

// ChangeMemory changes the memory file at path with edit, if what it
// remembers is still version (Memories.Version; "" for whatever it holds
// now), and returns what it remembers afterwards. edit gets a copy of what
// the file holds now, so what it leaves alone stays exactly as it was,
// hand edits included; nothing is written unless the result passes Check,
// or if the file changed meanwhile.
func ChangeMemory(path, version string, edit func(Memories) (Memories, error)) (Memories, error) {
	return changeMemory(path, version, edit, Memories.Check)
}

// changeMemory is ChangeMemory, checking the result with check. Every
// heedle process takes the same lock first, so no two change the file at
// once (review 47: a worker marking memories used, in another process,
// wrote a forgotten one back).
func changeMemory(path, version string, edit func(Memories) (Memories, error), check func(Memories) error) (Memories, error) {
	path, err := memoryPath(path)
	if err != nil {
		return nil, err
	}
	unlock, err := lockMemory(path)
	if err != nil {
		return nil, err
	}
	defer unlock()
	ms, before, err := readMemory(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
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
	if err := check(next); err != nil {
		return nil, err
	}
	if err := writeMemory(path, next, before); err != nil {
		return nil, err
	}
	return next, nil
}

// memoryPath is the file path names: through a link, the file it names,
// made or not.
func memoryPath(path string) (string, error) {
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return real, nil
	}
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(path)
		if err != nil {
			return "", err
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(path), target)
		}
		return target, nil
	}
	return path, nil
}

// lockMemory takes the lock on the memory file at path, which every heedle
// process takes before changing it: a file beside it, made only if it
// isn't there (os.O_EXCL; loom uses no system calls of its own,
// TestInv_OnlyWarpRunsGit), removed when done. One left by a process that
// died is taken over once it's older than memoryLockStale. Holding it, any
// temporary file left beside the memory file by a write that died is
// removed: it may hold memories since forgotten (review 47).
func lockMemory(path string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	lock := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".lock")
	deadline := time.Now().Add(memoryLockWait)
	for {
		f, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			f.Close()
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if fi, err := os.Stat(lock); err == nil && time.Since(fi.ModTime()) > memoryLockStale {
			os.Remove(lock) // left by a process that died
			continue
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("heedle's memory is being changed elsewhere (%s); try again", lock)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if stale, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".memory-*.json")); len(stale) > 0 {
		for _, s := range stale {
			os.Remove(s)
		}
	}
	return func() { os.Remove(lock) }, nil
}

// A change to the memory file takes milliseconds: a lock older than
// memoryLockStale was left by a process that died, and a change waits at
// most memoryLockWait for another.
const (
	memoryLockStale = 30 * time.Second
	memoryLockWait  = 10 * time.Second
)

// writeMemory replaces the file at path with ms, all at once: a new file,
// written and synced, renamed over it, so a reader sees the old file or
// the new one, never half of either. It keeps the file's mode (0600 for a
// new one). If the file no longer holds before (nil: it wasn't there),
// nothing is written.
func writeMemory(path string, ms Memories, before []byte) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // <, > and & as themselves: six bytes each otherwise
	enc.SetIndent("", "  ")
	if err := enc.Encode(ms); err != nil {
		return err
	}
	mode := os.FileMode(0o600)
	if fi, err := os.Stat(path); err == nil {
		mode = fi.Mode().Perm()
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".memory-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(buf.Bytes())
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
	text = strings.TrimSpace(text)
	return ChangeMemory(path, version, func(ms Memories) (Memories, error) {
		i := slices.IndexFunc(ms, func(m Memory) bool { return m.ID == id })
		if i < 0 {
			return nil, fmt.Errorf("%w: nothing remembered as %s", ErrNoMemory, id)
		}
		if slices.ContainsFunc(ms, func(m Memory) bool { return m.ID != id && strings.EqualFold(m.Text, text) }) {
			return nil, fmt.Errorf("%w: heedle already remembers that", ErrMemoryInvalid)
		}
		ms[i].Text = text
		return ms, nil
	})
}

// ForgetMemory forgets the memory id: the file is rewritten without it.
// It works on a file whatever its memories say, so long as heedle can
// read it as a list, so nothing written by hand can't be forgotten.
func ForgetMemory(path, version, id string) (Memories, error) {
	return changeMemory(path, version, func(ms Memories) (Memories, error) {
		i := slices.IndexFunc(ms, func(m Memory) bool { return m.ID == id })
		if i < 0 {
			return nil, fmt.Errorf("%w: nothing remembered as %s", ErrNoMemory, id)
		}
		return slices.Delete(ms, i, i+1), nil
	}, Memories.checkShape)
}

// ForgetAllMemory forgets everything: the file is rewritten empty, from
// whatever it held, even a file heedle can't read (review 47: one heedle
// itself had written couldn't be forgotten).
func ForgetAllMemory(path, version string) (Memories, error) {
	if version == "" {
		path, err := memoryPath(path)
		if err != nil {
			return nil, err
		}
		unlock, err := lockMemory(path)
		if err != nil {
			return nil, err
		}
		defer unlock()
		before, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			before = nil
		} else if err != nil {
			return nil, err
		}
		return Memories{}, writeMemory(path, Memories{}, before)
	}
	return changeMemory(path, version, func(Memories) (Memories, error) { return Memories{}, nil }, Memories.checkShape)
}

// MarkMemoryUsed records that the memories sent went to Claude at now.
// Only those still remembered with the same words are marked: one
// forgotten or changed meanwhile isn't (review 47).
func MarkMemoryUsed(path string, sent Memories, now time.Time) error {
	_, err := changeMemory(path, "", func(ms Memories) (Memories, error) {
		for i := range ms {
			if slices.ContainsFunc(sent, func(s Memory) bool { return s.ID == ms[i].ID && s.Text == ms[i].Text }) {
				ms[i].Used = now.UTC()
			}
		}
		return ms, nil
	}, Memories.checkShape)
	return err
}

// What heedle never remembers, because it would go to Claude: keys and
// tokens (by their well-known shapes, or any long run of letters and
// digits), private keys, card numbers, identity numbers and passwords.
var secretShapes = []struct {
	re   *regexp.Regexp
	what string
}{
	{regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY`), "a private key"},
	{regexp.MustCompile(`\b(sk-ant-|sk-proj-|sk-[A-Za-z0-9]{20}|(sk|rk|pk)_(live|test)_|ghp_|gho_|ghu_|ghs_|ghr_|github_pat_|glpat-|xox[abposr]-|AIza[0-9A-Za-z_-]{20}|(AKIA|ASIA)[0-9A-Z]{16}|npm_[A-Za-z0-9]{20})`), "an API key or token"},
	{regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.`), "a sign-in token"},
	// A password said: "password is …", "pw: …", "passcode for the gym: …";
	// not "I use a password manager" (review 47).
	{regexp.MustCompile(`(?i)\b(pass ?words?|passwd|pwd|pw|pass ?codes?|pass ?phrases?|passwort|kennwort|mot de passe|contraseña|senha|login)\b(\s+for\b[^:=]{0,40})?\s*(is|ist|est|es|é|:|=)\s*\S`), "a password"},
	{regexp.MustCompile(`(?i)\b(pin|cvv|cvc|security code)\s*(is|:|=)\s*\d{3,}`), "a PIN or card code"},
	{regexp.MustCompile(`(?i)\b(pin|door|gate|alarm|access|entry|garage|lock|safe)[ -]?code\b\s*(is|:|=)?\s*\d{3,}`), "a PIN or door code"},
	{regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`), "an identity number"},
	{regexp.MustCompile(`(?i)\b(ssn|social security( number)?|passport( number| no\.?)?)\s*(is|:|=|#)?\s*[A-Z0-9]{6,}`), "an identity number"},
}

var opaque = regexp.MustCompile(`[A-Za-z0-9+/_=.-]{24,}`)
var cardDigits = regexp.MustCompile(`\d(?:[ -]?\d){12,18}`)
var cardPrefix = regexp.MustCompile(`^(4|5[1-5]|2[2-7]|3[47]|6(011|5|4[4-9])|35)`)

// LooksSecret says what a text looks like if it looks like something
// heedle must never remember or publish (such as "an API key or token",
// "a card number" or "a password"), else "".
func LooksSecret(text string) string {
	// Every kind of space as a space, so no-break spaces don't hide a card.
	text = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, text)
	for _, s := range secretShapes {
		if s.re.MatchString(text) {
			return s.what
		}
	}
	// A long piece of mixed letters and digits, between the separators a
	// URL, a date or a name has (review 47: a dated URL path was a "key").
	for _, run := range opaque.FindAllString(text, -1) {
		for _, piece := range strings.FieldsFunc(run, func(r rune) bool { return strings.ContainsRune("/.-_", r) }) {
			if len(piece) >= 24 && strings.ContainsAny(piece, "0123456789") && strings.IndexFunc(piece, unicode.IsLetter) >= 0 {
				return "an API key or token"
			}
		}
	}
	// A card: a card issuer's number of digits and prefix, passing the
	// check cards carry (not any loyalty number that happens to).
	for _, run := range cardDigits.FindAllString(text, -1) {
		d := strings.NewReplacer(" ", "", "-", "").Replace(run)
		if (len(d) == 15 || len(d) == 16 || len(d) == 19) && cardPrefix.MatchString(d) && luhn(d) {
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

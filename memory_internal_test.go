package loom

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestInv_MemoryLockIsRemovedOnlyByItsHolder: a lock left by a process
// that died is taken over once stale, and a holder whose lock was taken
// over never removes the new holder's. (Review 47, round 3: the token
// check had no guard.)
func TestInv_MemoryLockIsRemovedOnlyByItsHolder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.json")
	lock := filepath.Join(filepath.Dir(path), ".memory.json.lock")
	unlock, err := lockMemory(path)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(lock, []byte("another holder"), 0o600) // taken over meanwhile
	unlock()
	if b, err := os.ReadFile(lock); err != nil || string(b) != "another holder" {
		t.Errorf("the old holder removed the new one's lock: %q %v", b, err)
	}
	old := time.Now().Add(-2 * memoryLockStale)
	os.Chtimes(lock, old, old)
	unlock, err = lockMemory(path)
	if err != nil {
		t.Fatalf("a stale lock wasn't taken over: %v", err)
	}
	unlock()
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Errorf("the holder's own lock is still there: %v", err)
	}
}

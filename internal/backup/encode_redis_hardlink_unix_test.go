//go:build unix

package backup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
)

// TestRedisEncodeRejectsHardLinkedBlob pins that a *.bin entry which is
// a hard link (nlink > 1) — potentially naming an inode outside the
// dump subtree — fails closed with ErrRedisEncodeHardLink rather than
// having its bytes ingested into the snapshot. Unix-only: the link
// count guard (refuseHardLink) is a no-op on platforms without
// syscall.Stat_t.Nlink.
func TestRedisEncodeRejectsHardLinkedBlob(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	// A target file outside the strings/ subdir.
	target := filepath.Join(in, "external.txt")
	if err := os.WriteFile(target, []byte("external bytes"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	blobDir := filepath.Join(in, "redis", "db_0", "strings")
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatalf("mkdir strings: %v", err)
	}
	enc := EncodeSegment([]byte("k"))
	// Hard-link the external file into the dump as a blob (nlink == 2).
	if err := os.Link(target, filepath.Join(blobDir, enc+".bin")); err != nil {
		t.Fatalf("os.Link: %v", err)
	}
	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeHardLink) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeHardLink", err)
	}
}

// TestRedisEncodeRejectsHardLinkedKeymap pins the same guard for the
// KEYMAP.jsonl sidecar (read via openDumpSidecar's Lstat-based check).
func TestRedisEncodeRejectsHardLinkedKeymap(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	target := filepath.Join(in, "external.jsonl")
	if err := os.WriteFile(target, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	dbDir := filepath.Join(in, "redis", "db_0")
	if err := os.MkdirAll(filepath.Join(dbDir, "strings"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// A real string so Encode reaches loadKeymap.
	enc := EncodeSegment([]byte("k"))
	if err := os.WriteFile(filepath.Join(dbDir, "strings", enc+".bin"), []byte("v"), 0o600); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	if err := os.Link(target, filepath.Join(dbDir, "KEYMAP.jsonl")); err != nil {
		t.Fatalf("os.Link: %v", err)
	}
	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeHardLink) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeHardLink", err)
	}
}

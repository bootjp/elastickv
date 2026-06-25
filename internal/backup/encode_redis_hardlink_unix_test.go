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

// TestRedisEncodeRejectsSymlinkedBlobSubdir pins that a symlinked
// strings/ (or hll/) subdir is refused before os.OpenRoot follows it
// outside the dump tree. Unix-only (os.Symlink + the threat model).
func TestRedisEncodeRejectsSymlinkedBlobSubdir(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	// External directory holding a .bin the attacker wants ingested.
	external := filepath.Join(in, "external")
	if err := os.MkdirAll(external, 0o755); err != nil {
		t.Fatalf("mkdir external: %v", err)
	}
	if err := os.WriteFile(filepath.Join(external, EncodeSegment([]byte("k"))+".bin"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write external blob: %v", err)
	}
	dbDir := filepath.Join(in, "redis", "db_0")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("mkdir dbDir: %v", err)
	}
	if err := os.Symlink(external, filepath.Join(dbDir, "strings")); err != nil {
		t.Fatalf("symlink strings: %v", err)
	}
	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeNotDir) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeNotDir", err)
	}
}

// TestRedisEncodeRejectsSymlinkedCollectionSubdir pins that a symlinked
// collection subdir (hashes/) is refused before walkJSONDir's
// os.OpenRoot follows it outside the dump tree — the .json/.jsonl
// counterpart of the blob-subdir guard.
func TestRedisEncodeRejectsSymlinkedCollectionSubdir(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	external := filepath.Join(in, "external_hashes")
	if err := os.MkdirAll(external, 0o755); err != nil {
		t.Fatalf("mkdir external: %v", err)
	}
	dbDir := filepath.Join(in, "redis", "db_0")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatalf("mkdir dbDir: %v", err)
	}
	if err := os.Symlink(external, filepath.Join(dbDir, "hashes")); err != nil {
		t.Fatalf("symlink hashes: %v", err)
	}
	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeNotDir) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeNotDir", err)
	}
}

// TestRedisEncodeRejectsSymlinkedDbDir pins that a symlinked
// redis/db_<n> directory is refused (Lstat in Encode) rather than
// followed outside the dump tree.
func TestRedisEncodeRejectsSymlinkedDbDir(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	external := filepath.Join(in, "external_db")
	if err := os.MkdirAll(filepath.Join(external, "strings"), 0o755); err != nil {
		t.Fatalf("mkdir external db: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(in, "redis"), 0o755); err != nil {
		t.Fatalf("mkdir redis: %v", err)
	}
	if err := os.Symlink(external, filepath.Join(in, "redis", "db_0")); err != nil {
		t.Fatalf("symlink db_0: %v", err)
	}
	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeNotDir) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeNotDir", err)
	}
}

// TestRedisEncodeRejectsHardLinkedTTLSidecar pins the same guard for a
// TTL sidecar (strings_ttl.jsonl) — the same openDumpSidecar path as
// the KEYMAP case, exercised separately for coverage symmetry.
func TestRedisEncodeRejectsHardLinkedTTLSidecar(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	target := filepath.Join(in, "external_ttl.jsonl")
	if err := os.WriteFile(target, []byte(`{"key":"x","expire_at_ms":1}`+"\n"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	dbDir := filepath.Join(in, "redis", "db_0")
	if err := os.MkdirAll(filepath.Join(dbDir, "strings"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	enc := EncodeSegment([]byte("k"))
	if err := os.WriteFile(filepath.Join(dbDir, "strings", enc+".bin"), []byte("v"), 0o600); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	if err := os.Link(target, filepath.Join(dbDir, "strings_ttl.jsonl")); err != nil {
		t.Fatalf("os.Link: %v", err)
	}
	b := newSnapshotBuilder(redisEncTS)
	err := NewRedisEncoder(in, 0).Encode(b)
	if !errors.Is(err, ErrRedisEncodeHardLink) {
		t.Fatalf("Encode err = %v, want ErrRedisEncodeHardLink", err)
	}
}

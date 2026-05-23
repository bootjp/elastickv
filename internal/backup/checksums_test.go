package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWriteChecksums_BasicListing pins the on-disk shape of a
// dump tree's CHECKSUMS file: lexicographically sorted lines, two-
// space separator, forward-slash paths, lowercase hex digest.
func TestWriteChecksums_BasicListing(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "a.txt"), []byte("hello"))
	mustWrite(t, filepath.Join(root, "nested", "b.txt"), []byte("world"))

	if err := WriteChecksums(root); err != nil {
		t.Fatalf("WriteChecksums: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, CHECKSUMSFilename))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// sha256("hello") and sha256("world"), pinned so a future
	// encoder change that accidentally normalises file bytes
	// (line endings, BOMs, etc.) surfaces as a hash mismatch.
	want := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824  a.txt\n" +
		"486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7  nested/b.txt\n"
	if string(got) != want {
		t.Fatalf("CHECKSUMS body:\nhave %q\nwant %q", got, want)
	}
}

// TestWriteChecksums_ExcludesSelf pins that the CHECKSUMS file is
// not listed inside its own body — sha256sum -c CHECKSUMS otherwise
// errors on the unreadable circular reference.
func TestWriteChecksums_ExcludesSelf(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "a.txt"), []byte("x"))
	if err := WriteChecksums(root); err != nil {
		t.Fatalf("WriteChecksums: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(root, CHECKSUMSFilename))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Contains(string(body), CHECKSUMSFilename) {
		t.Fatalf("CHECKSUMS lists itself; body=%q", body)
	}
}

// TestWriteChecksums_DeterministicOrder pins line ordering — two
// invocations on the same tree must produce byte-identical
// CHECKSUMS files.
func TestWriteChecksums_DeterministicOrder(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// Names chosen so the alphabetic order would differ from the
	// directory-walk order on some filesystems.
	for _, name := range []string{"zz", "aa", "mm"} {
		mustWrite(t, filepath.Join(root, name), []byte(name))
	}
	if err := WriteChecksums(root); err != nil {
		t.Fatalf("WriteChecksums: %v", err)
	}
	first, err := os.ReadFile(filepath.Join(root, CHECKSUMSFilename))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := WriteChecksums(root); err != nil {
		t.Fatalf("WriteChecksums (2nd): %v", err)
	}
	second, err := os.ReadFile(filepath.Join(root, CHECKSUMSFilename))
	if err != nil {
		t.Fatalf("read (2nd): %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("CHECKSUMS not deterministic across runs:\nrun1=%q\nrun2=%q", first, second)
	}
}

// TestVerifyChecksums_HappyPath round-trips a freshly written
// CHECKSUMS file. Used to back the Phase 0b encoder's self-test.
func TestVerifyChecksums_HappyPath(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "a.txt"), []byte("hello"))
	mustWrite(t, filepath.Join(root, "b.txt"), []byte("world"))
	if err := WriteChecksums(root); err != nil {
		t.Fatalf("WriteChecksums: %v", err)
	}
	if err := VerifyChecksums(root); err != nil {
		t.Fatalf("VerifyChecksums: %v", err)
	}
}

// TestVerifyChecksums_DetectsTampering pins the failure mode: a
// post-checksum write to one of the listed files must surface as
// a verification error.
func TestVerifyChecksums_DetectsTampering(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "a.txt"), []byte("hello"))
	if err := WriteChecksums(root); err != nil {
		t.Fatalf("WriteChecksums: %v", err)
	}
	mustWrite(t, filepath.Join(root, "a.txt"), []byte("tampered"))
	if err := VerifyChecksums(root); err == nil {
		t.Fatalf("expected VerifyChecksums to detect tampering, got nil")
	}
}

func mustWrite(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

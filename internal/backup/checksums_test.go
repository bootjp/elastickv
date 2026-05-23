package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
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

// TestVerifyChecksums_RejectsTraversalPath is the regression for
// the coderabbit critical finding on PR #810. A CHECKSUMS file
// shipped alongside a backup must not be able to direct the
// verifier to fingerprint files outside the dump root via a
// `..`-laden path. The verifier rejects with
// ErrChecksumsPathTraversal before any sha256 is computed.
func TestVerifyChecksums_RejectsTraversalPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		relPath string
	}{
		{"parent traversal", "../etc/passwd"},
		{"deep traversal", "a/../../b"},
		{"absolute path", "/etc/passwd"},
		{"current dir", "."},
		{"empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			// Write a CHECKSUMS that references the traversal path.
			// The digest is the sha256 of an arbitrary fixture so
			// shape parsing succeeds and the path guard is the
			// first thing to fire.
			body := "0000000000000000000000000000000000000000000000000000000000000000  " + tc.relPath + "\n"
			mustWrite(t, filepath.Join(root, CHECKSUMSFilename), []byte(body))
			err := VerifyChecksums(root)
			if err == nil {
				t.Fatalf("expected error for relPath=%q, got nil", tc.relPath)
			}
			// `.` and "" surface as malformed-line; the rest surface
			// as path-traversal. Both signal "do not trust the
			// CHECKSUMS contents".
			if !errors.Is(err, ErrChecksumsPathTraversal) && !errors.Is(err, ErrChecksumsMalformedLine) {
				t.Fatalf("err = %v, want ErrChecksumsPathTraversal or ErrChecksumsMalformedLine", err)
			}
		})
	}
}

// TestVerifyChecksums_RejectsTerminalSymlink is the regression for
// codex r2 P1 on PR #810: after the textual `..` guard, a
// CHECKSUMS line `<hex>  safe/file` whose `safe/file` is a symlink
// pointing outside the dump root would have been silently
// followed by os.Open, hashing the host-side target. The
// per-component lstat walk in refuseSymlinkComponents catches
// both terminal and intermediate symlinks.
func TestVerifyChecksums_RejectsTerminalSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		// Windows symlink creation requires
		// SeCreateSymbolicLinkPrivilege which is rarely available
		// to CI users. The lstat-walk fix runs on every platform;
		// the test only exercises Linux/macOS where os.Symlink is
		// unprivileged.
		t.Skip("symlink test requires unprivileged os.Symlink (skipped on Windows)")
	}
	t.Parallel()
	root := t.TempDir()
	outsideDir := t.TempDir()
	outsideFile := filepath.Join(outsideDir, "secret")
	mustWrite(t, outsideFile, []byte("host-only"))
	// Build a CHECKSUMS that lists `safe/file` with a digest that
	// would match the outside file's contents — proving the
	// verifier did NOT follow the symlink.
	mustWrite(t, filepath.Join(root, "safe", "honest"), []byte("dump-only"))
	if err := WriteChecksums(root); err != nil {
		t.Fatalf("WriteChecksums: %v", err)
	}
	// Now plant a symlink at safe/escape -> outsideFile and add an
	// entry for it to CHECKSUMS. WriteChecksums followed the link
	// during its own walk too, but VerifyChecksums must reject.
	link := filepath.Join(root, "safe", "escape")
	if err := os.Symlink(outsideFile, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	// Append a CHECKSUMS entry for the symlinked file.
	checksumsPath := filepath.Join(root, CHECKSUMSFilename)
	body, err := os.ReadFile(checksumsPath) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read CHECKSUMS: %v", err)
	}
	// Use the digest of the outside file's actual content so the
	// shape-parse + path-traversal checks succeed; the symlink
	// guard is the first thing that must reject.
	body = append(body, []byte("e3d1ac1b09a9e88c0c12e9b1d31d6a92e2ec43cf2bda7bcd58e3a3b2c50e2dd2  safe/escape\n")...)
	if err := os.WriteFile(checksumsPath, body, 0o600); err != nil {
		t.Fatalf("write CHECKSUMS: %v", err)
	}
	err = VerifyChecksums(root)
	if err == nil {
		t.Fatalf("expected ErrChecksumsSymlinkEscape, got nil")
	}
	if !errors.Is(err, ErrChecksumsSymlinkEscape) {
		t.Fatalf("err = %v, want ErrChecksumsSymlinkEscape", err)
	}
}

// TestVerifyChecksums_RejectsIntermediateSymlink pins that a
// symlinked PARENT component is also caught — without the
// per-component walk, refusing only the terminal symlink would
// still leave the verifier vulnerable to a dump with a top-level
// symlinked directory.
func TestVerifyChecksums_RejectsIntermediateSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink test requires unprivileged os.Symlink")
	}
	t.Parallel()
	root := t.TempDir()
	outsideDir := t.TempDir()
	mustWrite(t, filepath.Join(outsideDir, "target.txt"), []byte("host-side"))
	// safe/ is a symlink to a directory outside the dump root.
	if err := os.Symlink(outsideDir, filepath.Join(root, "safe")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	body := "0000000000000000000000000000000000000000000000000000000000000000  safe/target.txt\n"
	mustWrite(t, filepath.Join(root, CHECKSUMSFilename), []byte(body))
	err := VerifyChecksums(root)
	if err == nil {
		t.Fatalf("expected ErrChecksumsSymlinkEscape, got nil")
	}
	if !errors.Is(err, ErrChecksumsSymlinkEscape) {
		t.Fatalf("err = %v, want ErrChecksumsSymlinkEscape", err)
	}
}

// TestVerifyChecksums_StreamsLargeFile pins the bufio.Scanner path
// — the previous implementation slurped the entire CHECKSUMS file
// via os.ReadFile, which OOMs on large dump trees (gemini
// security-high on PR #810). We do not exercise an actual large
// file (CI cost) but we confirm the streaming code path tolerates
// many lines without buffering them all.
func TestVerifyChecksums_StreamsLargeFile(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const fileCount = 64
	for i := 0; i < fileCount; i++ {
		mustWrite(t, filepath.Join(root, fmt.Sprintf("f-%03d", i)), []byte{byte(i)})
	}
	if err := WriteChecksums(root); err != nil {
		t.Fatalf("WriteChecksums: %v", err)
	}
	if err := VerifyChecksums(root); err != nil {
		t.Fatalf("VerifyChecksums: %v", err)
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

package kek_test

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/cockroachdb/errors"
)

func writeKEK(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "kek.bin")
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func TestFileWrapper_RoundTrip(t *testing.T) {
	path := writeKEK(t, 32)
	w, err := kek.NewFileWrapper(path)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	wrapped, err := w.Wrap(dek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if got, want := len(wrapped), 12+32+16; got != want {
		t.Fatalf("wrapped len = %d, want %d", got, want)
	}
	got, err := w.Unwrap(wrapped)
	if err != nil {
		t.Fatalf("Unwrap: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("Unwrap returned different bytes than Wrap input")
	}
}

func TestFileWrapper_DifferentNonceEachWrap(t *testing.T) {
	path := writeKEK(t, 32)
	w, err := kek.NewFileWrapper(path)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	dek := make([]byte, 32)
	w1, err := w.Wrap(dek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	w2, err := w.Wrap(dek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	// Even with the same DEK, distinct nonces must produce distinct
	// wrapped outputs.
	if bytes.Equal(w1, w2) {
		t.Fatal("two Wrap calls produced identical output (nonce reuse)")
	}
	// First 12 bytes are the random nonce; they must differ.
	if bytes.Equal(w1[:12], w2[:12]) {
		t.Fatal("nonce did not change between Wrap calls")
	}
}

func TestFileWrapper_RejectsBadKEKLength(t *testing.T) {
	for _, n := range []int{0, 1, 16, 24, 31, 33, 64} {
		path := writeKEK(t, n)
		_, err := kek.NewFileWrapper(path)
		if err == nil {
			t.Fatalf("expected error for %d-byte KEK, got nil", n)
		}
	}
}

func TestFileWrapper_RejectsBadDEKLength(t *testing.T) {
	path := writeKEK(t, 32)
	w, err := kek.NewFileWrapper(path)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	for _, n := range []int{0, 1, 16, 31, 33} {
		_, err := w.Wrap(make([]byte, n))
		if err == nil {
			t.Fatalf("expected error for %d-byte DEK, got nil", n)
		}
	}
}

func TestFileWrapper_Unwrap_TamperedNonce(t *testing.T) {
	path := writeKEK(t, 32)
	w, err := kek.NewFileWrapper(path)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	dek := make([]byte, 32)
	wrapped, err := w.Wrap(dek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	wrapped[0] ^= 0x01 // flip a nonce bit
	_, err = w.Unwrap(wrapped)
	if err == nil {
		t.Fatal("expected error on tampered nonce, got nil")
	}
}

func TestFileWrapper_Unwrap_TamperedTag(t *testing.T) {
	path := writeKEK(t, 32)
	w, err := kek.NewFileWrapper(path)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	dek := make([]byte, 32)
	wrapped, err := w.Wrap(dek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	wrapped[len(wrapped)-1] ^= 0x01 // flip a tag bit
	_, err = w.Unwrap(wrapped)
	if err == nil {
		t.Fatal("expected error on tampered tag, got nil")
	}
}

func TestFileWrapper_Unwrap_TooShort(t *testing.T) {
	path := writeKEK(t, 32)
	w, err := kek.NewFileWrapper(path)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	for _, n := range []int{0, 11, 12, 30, 59} { // < 12+32+16
		_, err := w.Unwrap(make([]byte, n))
		if err == nil {
			t.Fatalf("expected error for %d-byte wrapped, got nil", n)
		}
	}
}

func TestFileWrapper_Unwrap_TooLong(t *testing.T) {
	path := writeKEK(t, 32)
	w, err := kek.NewFileWrapper(path)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	dek := make([]byte, 32)
	wrapped, err := w.Wrap(dek)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	wrapped = append(wrapped, 0x00)
	_, err = w.Unwrap(wrapped)
	if err == nil {
		t.Fatal("expected error for over-length wrapped, got nil")
	}
}

func TestFileWrapper_Name(t *testing.T) {
	path := writeKEK(t, 32)
	w, err := kek.NewFileWrapper(path)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	want := "file:" + path
	if w.Name() != want {
		t.Fatalf("Name() = %q, want %q", w.Name(), want)
	}
}

func TestFileWrapper_NewFileWrapper_MissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := kek.NewFileWrapper(filepath.Join(dir, "does-not-exist.bin"))
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// TestFileWrapper_RejectsInsecureMode covers the PR #719 CodeRabbit
// Major finding: a 0o644 / 0o666 KEK file would still be loaded under
// the previous implementation, weakening the at-rest encryption
// boundary on a multi-user host. NewFileWrapper now stat()s the file
// and refuses anything with bits in 0o077.
func TestFileWrapper_RejectsInsecureMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits not enforced on Windows")
	}
	cases := []struct {
		name string
		mode os.FileMode
	}{
		{"group-readable 0o640", 0o640},
		{"world-readable 0o644", 0o644},
		{"group/world-writable 0o666", 0o666},
		{"group-execute 0o610", 0o610},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "kek.bin")
			buf := make([]byte, 32)
			if _, err := rand.Read(buf); err != nil {
				t.Fatalf("rand.Read: %v", err)
			}
			// Write at the secure mode first (gosec G306 forbids
			// >0o600 in WriteFile), then chmod to the insecure target.
			if err := os.WriteFile(path, buf, 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			if err := os.Chmod(path, tc.mode); err != nil {
				t.Fatalf("Chmod %o: %v", tc.mode, err)
			}
			st, err := os.Stat(path)
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			if st.Mode().Perm() != tc.mode {
				t.Skipf("environment refused mode %o (got %o)", tc.mode, st.Mode().Perm())
			}
			_, err = kek.NewFileWrapper(path)
			if !errors.Is(err, kek.ErrInsecureKEKFile) {
				t.Fatalf("expected ErrInsecureKEKFile for mode %o, got %v", tc.mode, err)
			}
		})
	}
}

func TestFileWrapper_AcceptsOwnerOnlyMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits not enforced on Windows")
	}
	for _, mode := range []os.FileMode{0o400, 0o600} {
		dir := t.TempDir()
		path := filepath.Join(dir, "kek.bin")
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			t.Fatalf("rand.Read: %v", err)
		}
		if err := os.WriteFile(path, buf, 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatalf("Chmod %o: %v", mode, err)
		}
		w, err := kek.NewFileWrapper(path)
		if err != nil {
			t.Fatalf("NewFileWrapper(mode=%o): %v", mode, err)
		}
		if w == nil {
			t.Fatalf("nil wrapper for mode %o", mode)
		}
	}
}

// TestFileWrapper_RejectsOversizedFile is the regression for the PR
// #719 codex P2 finding: io.ReadAll without a size bound would read
// gigabytes of garbage before checking length. NewFileWrapper now
// caps the read at fileKEKSize+1 so a too-long file fails fast on
// the length check without exhausting memory.
func TestFileWrapper_RejectsOversizedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kek.bin")
	// 64 KiB — large enough that an unbounded ReadAll would observably
	// pull more than fileKEKSize+1; small enough to not be a real
	// resource problem if the bound regresses.
	buf := make([]byte, 64<<10)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := kek.NewFileWrapper(path)
	if err == nil {
		t.Fatal("NewFileWrapper accepted an oversized file")
	}
	if !strings.Contains(err.Error(), "want exactly 32") {
		t.Fatalf("expected length error, got: %v", err)
	}
}

// TestFileWrapper_RejectsDirectory is the lower-cost cousin to the
// FIFO test: a directory is also a non-regular file. Works on every
// platform (no syscall.Mkfifo dependency).
func TestFileWrapper_RejectsDirectory(t *testing.T) {
	dir := t.TempDir()
	_, err := kek.NewFileWrapper(dir)
	if err == nil {
		t.Fatal("NewFileWrapper accepted a directory path")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular-file error, got: %v", err)
	}
}

// TestFileWrapper_NilOrZeroValueRejected covers the PR #719 codex P2
// finding: a wiring/configuration mistake in a bootstrap or rotation
// path could hand callers a nil *FileWrapper or a zero-value
// FileWrapper{}, and Wrap/Unwrap dereference w.aead unconditionally.
// They now return ErrNilFileWrapper symmetrically with the
// encryption.Cipher / encryption.Keystore zero-value contract.
func TestFileWrapper_NilOrZeroValueRejected(t *testing.T) {
	t.Parallel()
	dek := make([]byte, 32)
	wrapped := make([]byte, 60)
	cases := []struct {
		name string
		w    *kek.FileWrapper
	}{
		{"nil receiver", (*kek.FileWrapper)(nil)},
		{"zero-value", &kek.FileWrapper{}},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/Wrap", func(t *testing.T) {
			_, err := tc.w.Wrap(dek)
			if !errors.Is(err, kek.ErrNilFileWrapper) {
				t.Fatalf("expected ErrNilFileWrapper, got %v", err)
			}
		})
		t.Run(tc.name+"/Unwrap", func(t *testing.T) {
			_, err := tc.w.Unwrap(wrapped)
			if !errors.Is(err, kek.ErrNilFileWrapper) {
				t.Fatalf("expected ErrNilFileWrapper, got %v", err)
			}
		})
	}
}

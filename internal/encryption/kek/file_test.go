package kek_test

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/kek"
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

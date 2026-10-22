package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/kek"
)

// writeTestKEKFile writes a random 32-byte owner-only KEK file and
// returns its path, matching the kek.NewFileWrapper contract.
func writeTestKEKFile(t *testing.T) string {
	t.Helper()
	buf := make([]byte, dekSize)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	path := filepath.Join(t.TempDir(), "kek.bin")
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func TestRunEncryptionWrapDEK_FileKEKRoundTrip(t *testing.T) {
	t.Parallel()
	kekPath := writeTestKEKFile(t)
	var out bytes.Buffer
	if err := runEncryptionWrapDEK([]string{"--kek-file", kekPath}, &out); err != nil {
		t.Fatalf("runEncryptionWrapDEK: %v", err)
	}
	line := strings.TrimSpace(out.String())
	wrapped, err := base64.StdEncoding.DecodeString(line)
	if err != nil {
		t.Fatalf("output %q is not standard base64: %v", line, err)
	}
	wrapper, err := kek.NewFileWrapper(kekPath)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	dek, err := wrapper.Unwrap(wrapped)
	if err != nil {
		t.Fatalf("Unwrap(wrapped) under the same KEK: %v", err)
	}
	if len(dek) != dekSize {
		t.Fatalf("unwrapped dek is %d bytes, want %d", len(dek), dekSize)
	}
}

func TestRunEncryptionWrapDEK_EachCallDrawsFreshDEK(t *testing.T) {
	t.Parallel()
	kekPath := writeTestKEKFile(t)
	wrapper, err := kek.NewFileWrapper(kekPath)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	seen := map[string]struct{}{}
	for i := 0; i < 3; i++ {
		var out bytes.Buffer
		if err := runEncryptionWrapDEK([]string{"--kek-file", kekPath}, &out); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		wrapped, err := base64.StdEncoding.DecodeString(strings.TrimSpace(out.String()))
		if err != nil {
			t.Fatalf("call %d: decode: %v", i, err)
		}
		dek, err := wrapper.Unwrap(wrapped)
		if err != nil {
			t.Fatalf("call %d: Unwrap: %v", i, err)
		}
		if _, dup := seen[string(dek)]; dup {
			t.Fatalf("call %d produced a DEK already seen; wrap-dek must draw fresh random material per call", i)
		}
		seen[string(dek)] = struct{}{}
	}
}

func TestRunEncryptionWrapDEK_RequiresKEKSource(t *testing.T) {
	// Not parallel: the env-backed source is read from the process
	// environment, so this test must observe it unset.
	t.Setenv(kek.EnvVar, "")
	if err := os.Unsetenv(kek.EnvVar); err != nil {
		t.Fatalf("Unsetenv: %v", err)
	}
	var out bytes.Buffer
	err := runEncryptionWrapDEK(nil, &out)
	if err == nil {
		t.Fatalf("runEncryptionWrapDEK() with no KEK source returned nil, want error")
	}
	if !strings.Contains(err.Error(), "no KEK source configured") {
		t.Errorf("err=%v, want substring 'no KEK source configured'", err)
	}
	if out.Len() != 0 {
		t.Errorf("stdout=%q, want nothing written on error", out.String())
	}
}

func TestRunEncryptionWrapDEK_RejectsInsecureKEKFile(t *testing.T) {
	t.Parallel()
	kekPath := writeTestKEKFile(t)
	if err := os.Chmod(kekPath, 0o644); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	var out bytes.Buffer
	err := runEncryptionWrapDEK([]string{"--kek-file", kekPath}, &out)
	if err == nil {
		t.Fatalf("runEncryptionWrapDEK with a 0644 KEK file returned nil, want ErrInsecureKEKFile")
	}
	if !strings.Contains(err.Error(), kek.ErrInsecureKEKFile.Error()) {
		t.Errorf("err=%v, want it to wrap ErrInsecureKEKFile", err)
	}
}

func TestRunEncryptionWrapDEK_RejectsMultipleSources(t *testing.T) {
	t.Parallel()
	kekPath := writeTestKEKFile(t)
	var out bytes.Buffer
	err := runEncryptionWrapDEK([]string{"--kek-file", kekPath, "--kek-uri", "aws-kms://arn"}, &out)
	if err == nil {
		t.Fatalf("runEncryptionWrapDEK with two KEK sources returned nil, want ErrMultipleKEKSources")
	}
	if !strings.Contains(err.Error(), kek.ErrMultipleKEKSources.Error()) {
		t.Errorf("err=%v, want it to wrap ErrMultipleKEKSources", err)
	}
}

func TestRunEncryptionWrapDEK_HelpFlagExitsZero(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	if err := runEncryptionWrapDEK([]string{"-h"}, &out); err != nil {
		t.Fatalf("runEncryptionWrapDEK(-h) = %v, want nil", err)
	}
}

func TestEncryptionMain_WrapDEKSubcommand(t *testing.T) {
	t.Parallel()
	// Routed through the dispatch table; -h keeps it RPC-free and
	// output-free so the test only pins that the subcommand exists.
	if err := encryptionMain([]string{"wrap-dek", "-h"}); err != nil {
		t.Fatalf("encryptionMain(wrap-dek -h) = %v, want nil", err)
	}
}

//go:build !windows

package kek_test

import (
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption/kek"
)

// TestFileWrapper_RejectsFIFO covers the unbounded-read DoS vector
// codex flagged on PR #719: a path pointing at a FIFO without a
// writer would hang NewFileWrapper indefinitely if either the
// O_NONBLOCK open flag or the IsRegular() stat reject were
// removed. The goroutine+timeout makes a regression in either
// surface as a deterministic test failure rather than a hung run.
//
// Unix-only: syscall.Mkfifo is not portable to Windows, and Windows
// named pipes use a different syscall path that we don't try to
// model here.
func TestFileWrapper_RejectsFIFO(t *testing.T) {
	dir := t.TempDir()
	fifoPath := filepath.Join(dir, "kek.fifo")
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		t.Skipf("mkfifo unsupported in this environment: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := kek.NewFileWrapper(fifoPath)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("NewFileWrapper accepted a FIFO")
		}
		if !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("expected regular-file error, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("NewFileWrapper hung on a FIFO (O_NONBLOCK or IsRegular missing?)")
	}
}

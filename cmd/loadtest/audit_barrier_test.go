package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuditWaitsForExternalDoneAndRejectsStaleMarkers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready")
	o := options{AuditReadyFile: path, AuditWaitTimeout: time.Second}
	require.NoError(t, checkAuditMarker(o))
	done := make(chan error, 1)
	go func() { done <- waitForAudit(t.Context(), o, json.NewEncoder(&bytes.Buffer{})) }()
	select {
	case err := <-done:
		t.Fatalf("audit started before Done: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, os.WriteFile(path, []byte("done"), 0600))
	require.NoError(t, <-done)
	require.ErrorContains(t, checkAuditMarker(o), "already exists")
}
func TestAuditWaitStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	o := options{AuditReadyFile: filepath.Join(t.TempDir(), "ready"), AuditWaitTimeout: time.Second}
	err := waitForAudit(ctx, o, json.NewEncoder(&bytes.Buffer{}))
	require.ErrorIs(t, err, context.Canceled)
}

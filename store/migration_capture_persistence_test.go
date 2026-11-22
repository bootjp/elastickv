package store

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestMigrationCaptureSnapshotAndRestore(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"native", "sst"} {
		t.Run(format, func(t *testing.T) {
			s := openSSTSnapshotTestStore(t, t.TempDir(), WithSSTIngestSnapshots(format == "sst"))
			id := captureTestIdentity(11)
			record, err := ApplyMigrationCaptureEntry(t.Context(), s, id, []MigrationCaptureRequest{captureTestRequest("a", "before-snapshot", 100)}, MigrationCaptureLimits{})
			require.NoError(t, err)
			snapshot, err := s.Snapshot()
			require.NoError(t, err)
			_, err = ApplyMigrationCaptureEntry(t.Context(), s, captureTestIdentity(12), []MigrationCaptureRequest{captureTestRequest("a", "after-snapshot", 101)}, MigrationCaptureLimits{})
			require.NoError(t, err)
			var payload bytes.Buffer
			_, err = snapshot.WriteTo(&payload)
			require.NoError(t, err)
			require.NoError(t, snapshot.Close())
			target := openSSTSnapshotTestStore(t, t.TempDir())
			require.NoError(t, target.Restore(bytes.NewReader(payload.Bytes())))
			stored, err := ReadMigrationCaptureEntry(t.Context(), target, id)
			require.NoError(t, err)
			require.Equal(t, record, stored)
			_, err = ReadMigrationCaptureEntry(t.Context(), target, captureTestIdentity(12))
			require.ErrorIs(t, err, ErrKeyNotFound)
			value, err := target.GetAt(t.Context(), []byte("a"), 102)
			require.NoError(t, err)
			require.Equal(t, []byte("before-snapshot"), value)
			index, present := raftEntryAppliedIndex(t, target)
			require.True(t, present)
			require.Equal(t, uint64(11), index)
			require.ErrorIs(t, target.RestoreExternal(t.Context(), bytes.NewReader(payload.Bytes())), ErrExternalSnapshotControlState)
		})
	}
}

func TestMigrationCaptureEncryptionAndKeyRotation(t *testing.T) {
	t.Parallel()
	fixture := newEncryptedStoreFixture(t, 1)
	id := captureTestIdentity(11)
	secret := "capture-payload-never-plaintext-32f83c"
	record, err := ApplyMigrationCaptureEntry(t.Context(), fixture.mvcc, id, []MigrationCaptureRequest{captureTestRequest("a", secret, 100)}, MigrationCaptureLimits{})
	require.NoError(t, err)
	physical, err := raftControlKey(migrationCaptureKey(id))
	require.NoError(t, err)
	stored, closer, err := pebbleStoreApplied(t, fixture.mvcc).db.Get(physical)
	require.NoError(t, err)
	require.NotContains(t, string(stored), secret)
	require.NoError(t, closer.Close())
	snapshot, err := fixture.mvcc.Snapshot()
	require.NoError(t, err)
	var payload bytes.Buffer
	_, err = snapshot.WriteTo(&payload)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	require.NotContains(t, payload.String(), secret)
	files, err := os.ReadDir(fixture.dir)
	require.NoError(t, err)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".log") {
			body, readErr := os.ReadFile(filepath.Join(fixture.dir, file.Name()))
			require.NoError(t, readErr)
			require.NotContains(t, string(body), secret)
		}
	}
	fixture.closeIfOpen(t)
	require.NoError(t, fixture.keystore.Set(2, bytes.Repeat([]byte{2}, encryption.KeySize)))
	fixture.keyID = 2
	fixture.reopen(t)
	got, err := ReadMigrationCaptureEntry(t.Context(), fixture.mvcc, id)
	require.NoError(t, err)
	require.Equal(t, record, got)
	_, err = ApplyMigrationCaptureEntry(t.Context(), fixture.mvcc, captureTestIdentity(12), []MigrationCaptureRequest{captureTestRequest("b", "new-key", 101)}, MigrationCaptureLimits{})
	require.NoError(t, err)
	require.NoError(t, fixture.mvcc.Restore(bytes.NewReader(payload.Bytes())))
	got, err = ReadMigrationCaptureEntry(t.Context(), fixture.mvcc, id)
	require.NoError(t, err)
	require.Equal(t, record, got)
}

const (
	captureCrashHelper = "ELASTICKV_CAPTURE_TEST_HELPER"
	captureCrashPhase  = "ELASTICKV_CAPTURE_TEST_PHASE"
	captureCrashExit   = 77
)

func TestMigrationCaptureCrashRecovery(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"before-record", "after-record", "after-commit"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestMigrationCaptureCrashHelper$")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), captureCrashHelper+"=1", captureCrashPhase+"="+phase)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(output))
			require.Equal(t, captureCrashExit, exit.ExitCode(), string(output))
			s := openSSTSnapshotTestStore(t, filepath.Join(dir, "pebble"))
			value, err := s.GetAt(t.Context(), []byte("a"), 101)
			_, recordErr := ReadMigrationCaptureEntry(t.Context(), s, captureTestIdentity(11))
			index, present := raftEntryAppliedIndex(t, s)
			if phase == "after-commit" {
				require.NoError(t, err)
				require.NoError(t, recordErr)
				require.Equal(t, []byte("durable"), value)
				require.True(t, present)
				require.Equal(t, uint64(11), index)
			} else {
				require.ErrorIs(t, err, ErrKeyNotFound)
				require.ErrorIs(t, recordErr, ErrKeyNotFound)
				require.False(t, present)
			}
		})
	}
}

func TestMigrationCaptureCrashHelper(t *testing.T) {
	if os.Getenv(captureCrashHelper) != "1" {
		t.Skip("subprocess crash helper")
	}
	s := newPebbleStoreWithFSMApplyWriteOptsForTest(t, "pebble", pebble.NoSync, fsmSyncModeNoSync)
	wrapped := &captureCrashMetadata{captureFailingMetadata: &captureFailingMetadata{MVCCStore: s, RaftEntryApplier: s, RaftControlMetadataStore: s, applied: s}, phase: os.Getenv(captureCrashPhase)}
	_, err := ApplyMigrationCaptureEntry(t.Context(), wrapped, captureTestIdentity(11), []MigrationCaptureRequest{captureTestRequest("a", "durable", 100)}, MigrationCaptureLimits{})
	require.NoError(t, err)
	os.Exit(captureCrashExit)
}

type captureCrashMetadata struct {
	*captureFailingMetadata
	phase string
}

func (s *captureCrashMetadata) StageRaftControlMetadata(ctx context.Context, key, value []byte) error {
	if s.phase == "before-record" {
		os.Exit(captureCrashExit)
	}
	err := s.RaftControlMetadataStore.StageRaftControlMetadata(ctx, key, value)
	if err == nil && s.phase == "after-record" {
		os.Exit(captureCrashExit)
	}
	return err
}

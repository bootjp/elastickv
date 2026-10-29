package store

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestExternalSnapshotEncryptedPebbleBoundary(t *testing.T) {
	for _, sst := range []bool{false, true} {
		for _, bearing := range []bool{false, true} {
			verifyExternalEncryptedSnapshot(t, sst, bearing)
		}
	}
}

func verifyExternalEncryptedSnapshot(t *testing.T, sst, bearing bool) {
	t.Helper()
	source := newEncryptedStoreFixture(t, 7)
	ps, ok := source.mvcc.(*pebbleStore)
	require.True(t, ok)
	ps.sstIngestSnapshots = sst
	require.NoError(t, source.mvcc.PutAt(t.Context(), []byte("encrypted"), []byte("payload !migstage|ownership"), 20, 0))
	seedExternalExistingEpoch(t, source.mvcc)
	if bearing {
		encoded, err := encodeMigrationOwnership([]MigrationOwnershipState{externalTestOwnership(MigrationOwner)})
		require.NoError(t, err)
		require.NoError(t, ps.db.Set(migrationOwnershipMetaKeyBytes, encoded, pebble.Sync))
	}
	raw := externalTestSnapshot(t, source.mvcc)
	dir := filepath.Join(t.TempDir(), "destination")
	options := []PebbleStoreOption{WithEncryption(source.cipher, encryption.NewDeterministicNonceFactory(0xABCD, 2),
		func() (uint32, bool) { return source.keyID, true })}
	destination, err := NewPebbleStore(dir, options...)
	require.NoError(t, err)
	defer func() { require.NoError(t, destination.Close()) }()
	seedExternalExistingEpoch(t, destination)
	require.NoError(t, destination.PutAt(t.Context(), []byte("sentinel"), []byte("unchanged"), 10, 0))
	before := externalTestSnapshot(t, destination)
	err = externalTestRestore(t, destination, raw)
	if bearing {
		require.ErrorIs(t, err, ErrExternalSnapshotMigrationState)
		require.Equal(t, before, externalTestSnapshot(t, destination))
	} else {
		require.NoError(t, err)
		_, readErr := destination.GetAt(t.Context(), MigrationConfigurationIndexKey(), ^uint64(0))
		require.ErrorIs(t, readErr, ErrKeyNotFound)
	}
	require.NoError(t, destination.Close())
	destination, err = NewPebbleStore(dir, options...)
	require.NoError(t, err)
	if bearing {
		require.Equal(t, before, externalTestSnapshot(t, destination))
		require.NoError(t, destination.Restore(bytes.NewReader(raw)))
	}
	value, err := destination.GetAt(t.Context(), []byte("encrypted"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("payload !migstage|ownership"), value)
}

type cancelAfterSnapshotReader struct {
	reader *bytes.Reader
	cancel context.CancelFunc
}

func (r *cancelAfterSnapshotReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	n, err := r.reader.Read(p)
	if r.reader.Len() == 0 {
		r.cancel()
	}
	return n, err
}

func TestExternalSnapshotCancellationAfterInputConsumed(t *testing.T) {
	for _, format := range []string{"mvcc", "native", "sst"} {
		source := externalTestMemory(t)
		require.NoError(t, source.PutAt(t.Context(), []byte("new"), []byte("value"), 20, 0))
		seedExternalExistingEpoch(t, source)
		raw := externalTestFormat(t, source, format)
		for _, memory := range []bool{false, true} {
			if memory && format != "mvcc" {
				continue
			}
			destination := externalTestDestination(t, memory)
			defer destination.Close()
			seedExternalExistingEpoch(t, destination)
			require.NoError(t, destination.PutAt(t.Context(), []byte("old"), []byte("value"), 10, 0))
			before := externalTestSnapshot(t, destination)
			ctx, cancel := context.WithCancel(t.Context())
			reader := &cancelAfterSnapshotReader{reader: bytes.NewReader(raw), cancel: cancel}
			restorer, ok := destination.(ExternalSnapshotRestorer)
			require.True(t, ok)
			require.ErrorIs(t, restorer.RestoreExternal(ctx, reader), context.Canceled)
			cancel()
			require.Zero(t, reader.reader.Len())
			require.Equal(t, before, externalTestSnapshot(t, destination))
		}
	}
}

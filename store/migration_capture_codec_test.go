package store

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/stretchr/testify/require"
)

func TestMigrationCaptureRejectsCorruptRecord(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"digest", "version", "identity", "ordinal", "timestamp", "outcome", "completeness", "trailing", "oversize"} {
		t.Run(name, func(t *testing.T) {
			s := openSSTSnapshotTestStore(t, t.TempDir())
			id := captureTestIdentity(11)
			record, err := ApplyMigrationCaptureEntry(t.Context(), s, id, []MigrationCaptureRequest{captureTestRequest("a", "value", 100)}, MigrationCaptureLimits{})
			require.NoError(t, err)
			corruptCaptureFields(name, &record)
			encoded, err := encodeMigrationCaptureEntry(&record)
			require.NoError(t, err)
			switch name {
			case "digest":
				encoded = bytes.Replace(encoded, []byte("dmFsdWU="), []byte("b3RoZXI="), 1)
			case "trailing":
				encoded = append(encoded, []byte(" {}")...)
			case "oversize":
				encoded = bytes.Repeat([]byte("x"), migrationCaptureMaxEncodedBytes+1)
			}
			require.NoError(t, s.ApplyRaftEntry(t.Context(), 12, func(ctx context.Context) error {
				return s.StageRaftControlMetadata(ctx, migrationCaptureKey(id), encoded)
			}))
			_, err = ReadMigrationCaptureEntry(t.Context(), s, id)
			require.ErrorIs(t, err, ErrMigrationCaptureCorrupt)
		})
	}
}

func corruptCaptureFields(name string, record *MigrationCaptureEntry) {
	switch name {
	case "version":
		record.Version++
	case "identity":
		record.Identity.Index++
	case "ordinal":
		record.Effects[0].Ordinal++
	case "timestamp":
		record.Effects[0].CommitTS = 0
	case "outcome":
		record.Outcomes[0] = MigrationCaptureRejected
	case "completeness":
		record.Complete = false
	}
}

func TestMigrationCaptureAuthenticatesTombstoneHeader(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 1)
	s := pebbleStoreApplied(t, f.mvcc)
	key := encodeKey([]byte("a"), 100)
	body, state, err := s.encryptForKey(key, []byte("protected-value"), 0, false)
	require.NoError(t, err)
	encoded := encodeValue(body, false, 0, state)
	encoded[0] ^= 1
	entry := MigrationCaptureEntry{Complete: true, Limits: MigrationCaptureLimits{MaxBytes: 64 << 10, MaxEffects: 1}}
	collector := migrationCaptureCollector{entry: &entry}
	_, err = collector.decodeEffect(&raftEntryScope{store: s}, encodedMutation{key: key, value: encoded})
	require.Error(t, err, "the header must be authenticated before treating the value as a tombstone")
	require.Empty(t, entry.Effects)
}

func TestMigrationCaptureChecksCompressedSizeBeforeExpansion(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 1)
	s := pebbleStoreApplied(t, f.mvcc)
	key := encodeKey([]byte("a"), 100)
	value := bytes.Repeat([]byte("compressed-data"), 1<<16)
	body, state, err := s.encryptForKey(key, value, 0, false)
	require.NoError(t, err)
	envelope, err := encryption.DecodeEnvelope(body)
	require.NoError(t, err)
	require.NotZero(t, envelope.Flag&encryption.FlagCompressed)
	entry := MigrationCaptureEntry{Complete: true, Limits: MigrationCaptureLimits{MaxBytes: 64 << 10, MaxEffects: 1}}
	collector := migrationCaptureCollector{entry: &entry}
	_, err = collector.decodeEffect(&raftEntryScope{store: s}, encodedMutation{key: key, value: encodeValue(body, false, 0, state)})
	require.NoError(t, err)
	require.False(t, entry.Complete)
	require.Equal(t, MigrationCaptureByteLimit, entry.Invalidation)
	require.Empty(t, entry.Effects)
}

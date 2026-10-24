package store

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func externalAliasValues() map[string][]byte {
	return map[string][]byte{
		"_migack": encodeMigrationImportAcks(map[migrationAckID]migrationImportAck{
			{jobID: 7, bracketID: 1}: {batchSeq: 1},
		}),
		"_mighlc":     encodeMigrationHLCFloors(map[uint64]uint64{7: 100}),
		"_migpromote": encodeMigrationPromotionStates(map[uint64]PromotionState{7: {Cursor: []byte("pending")}}),
	}
}

func externalAliasSource(t *testing.T, format string, encrypted bool) (MVCCStore, []PebbleStoreOption) {
	t.Helper()
	if encrypted {
		fixture := newEncryptedStoreFixture(t, 7)
		ps, ok := fixture.mvcc.(*pebbleStore)
		require.True(t, ok)
		ps.sstIngestSnapshots = format == "sst"
		return fixture.mvcc, []PebbleStoreOption{WithEncryption(fixture.cipher, NewCounterNonceFactory(0xABCD, 2),
			func() (uint32, bool) { return fixture.keyID, true })}
	}
	if format == "mvcc" {
		return NewMVCCStore(), nil
	}
	source, err := NewPebbleStore(t.TempDir(), WithSSTIngestSnapshots(format == "sst"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	return source, nil
}

func TestExternalSnapshotOrdinaryMigrationNamesRoundTrip(t *testing.T) {
	for _, format := range []string{"mvcc", "native", "sst"} {
		for _, encrypted := range []bool{false, true} {
			if encrypted && format == "mvcc" {
				continue
			}
			t.Run(format+map[bool]string{false: "", true: "-encrypted"}[encrypted], func(t *testing.T) {
				source, options := externalAliasSource(t, format, encrypted)
				seedExternalExistingEpoch(t, source)
				for key, value := range externalAliasValues() {
					require.NoError(t, source.PutAt(t.Context(), []byte(key), []byte("ordinary"), 10, 0))
					require.NoError(t, source.PutAt(t.Context(), []byte(key), value, 20, 0))
				}
				if indexed, ok := source.(interface{ SetDurableAppliedIndex(uint64) error }); ok {
					require.NoError(t, indexed.SetDurableAppliedIndex(9001))
				}
				if encrypted {
					assertExternalAliasEncrypted(t, source)
				}
				raw := externalTestSnapshot(t, source)
				t.Run("pebble", func(t *testing.T) { verifyExternalAliasDestination(t, raw, false, options) })
				if format == "mvcc" {
					t.Run("memory", func(t *testing.T) { verifyExternalAliasDestination(t, raw, true, nil) })
				}
			})
		}
	}
}

func assertExternalAliasEncrypted(t *testing.T, source MVCCStore) {
	t.Helper()
	ps, ok := source.(*pebbleStore)
	require.True(t, ok)
	for key := range externalAliasValues() {
		value, closer, err := ps.db.Get(encodeKey([]byte(key), 10))
		require.NoError(t, err)
		decoded, err := decodeValue(value)
		require.NoError(t, err)
		require.Equal(t, encStateEncrypted, decoded.EncState)
		require.NoError(t, closer.Close())
	}
}

func verifyExternalAliasDestination(t *testing.T, raw []byte, memory bool, options []PebbleStoreOption) {
	t.Helper()
	destination := NewMVCCStore()
	dir := filepath.Join(t.TempDir(), "destination")
	if !memory {
		var err error
		destination, err = NewPebbleStore(dir, options...)
		require.NoError(t, err)
	}
	defer func() { require.NoError(t, destination.Close()) }()
	require.NoError(t, externalTestRestore(t, destination, raw))
	assertExternalAliasValues(t, destination)
	if !memory {
		require.NoError(t, destination.Close())
		var err error
		destination, err = NewPebbleStore(dir, options...)
		require.NoError(t, err)
		assertExternalAliasValues(t, destination)
	}
}

func assertExternalAliasValues(t *testing.T, s MVCCStore) {
	t.Helper()
	for key, value := range externalAliasValues() {
		for ts, expected := range map[uint64][]byte{10: []byte("ordinary"), 20: value} {
			actual, err := s.GetAt(t.Context(), []byte(key), ts)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		}
	}
	_, err := s.GetAt(t.Context(), MigrationConfigurationIndexKey(), ^uint64(0))
	require.ErrorIs(t, err, ErrKeyNotFound)
	if indexed, ok := s.(interface{ LastAppliedIndex() (uint64, bool, error) }); ok {
		_, present, err := indexed.LastAppliedIndex()
		require.NoError(t, err)
		require.False(t, present)
	}
}

func TestExternalSnapshotRawMigrationNamesRemainMetadata(t *testing.T) {
	for _, format := range []string{"native", "sst"} {
		for _, encrypted := range []bool{false, true} {
			for key, value := range externalAliasValues() {
				t.Run(format+map[bool]string{false: "", true: "-encrypted"}[encrypted]+"/"+key, func(t *testing.T) {
					source, options := externalAliasSource(t, format, encrypted)
					ps, ok := source.(*pebbleStore)
					require.True(t, ok)
					require.NoError(t, source.PutAt(t.Context(), []byte("ordinary"), []byte("payload"), 10, 0))
					require.NoError(t, ps.db.Set([]byte(key), value, pebble.Sync))
					raw := externalTestSnapshot(t, source)
					verifyExternalAliasRefusal(t, raw, options, ErrExternalSnapshotMigrationState)
					require.NoError(t, ps.db.Set([]byte(key), []byte("corrupt"), pebble.Sync))
					verifyExternalAliasRefusal(t, externalTestSnapshot(t, source), options, nil)
				})
			}
		}
	}
}

func verifyExternalAliasRefusal(t *testing.T, raw []byte, options []PebbleStoreOption, expected error) {
	t.Helper()
	destination, err := NewPebbleStore(t.TempDir(), options...)
	require.NoError(t, err)
	defer destination.Close()
	seedExternalExistingEpoch(t, destination)
	require.NoError(t, destination.PutAt(t.Context(), []byte("old"), []byte("unchanged"), 30, 0))
	before := externalTestSnapshot(t, destination)
	err = externalTestRestore(t, destination, raw)
	if expected != nil {
		require.ErrorIs(t, err, expected)
	} else {
		require.Error(t, err)
	}
	require.Equal(t, before, externalTestSnapshot(t, destination))
}

func TestExternalSnapshotLogicalMigrationControlKeysRemainReserved(t *testing.T) {
	for _, key := range [][]byte{migrationOwnershipMetaKeyBytes, migrationRetiredMetaKeyBytes, migrationReadyKey(7)} {
		source := externalTestMemory(t)
		require.NoError(t, source.PutAt(t.Context(), key, []byte("legacy control"), 10, 0))
		for _, format := range []string{"mvcc", "native", "sst"} {
			raw := externalTestFormat(t, source, format)
			verifyExternalAliasRefusal(t, raw, nil, nil)
			if format == "mvcc" {
				destination := externalTestDestination(t, true)
				before := externalTestSnapshot(t, destination)
				require.Error(t, externalTestRestore(t, destination, raw))
				require.Equal(t, before, externalTestSnapshot(t, destination))
			}
		}
	}
}

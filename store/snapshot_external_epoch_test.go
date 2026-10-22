package store

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func seedExternalExistingEpoch(t *testing.T, s MVCCStore) {
	t.Helper()
	key := MigrationConfigurationIndexKey()
	require.NoError(t, s.PutAt(t.Context(), key, binary.BigEndian.AppendUint64(nil, 11), 10, 0))
	require.NoError(t, s.PutAt(t.Context(), key, binary.BigEndian.AppendUint64(nil, 22), 20, 0))
}

func TestMigrationConfigurationIndexKeyReturnsPrivateBytes(t *testing.T) {
	key := MigrationConfigurationIndexKey()
	key[0] = '?'
	require.Equal(t, []byte("!txn|meta|migration_configuration_index"), MigrationConfigurationIndexKey())
}

func TestExternalSnapshotDropsOnlyConfigurationEpochAllVersions(t *testing.T) {
	source := externalTestMemory(t)
	seedExternalExistingEpoch(t, source)
	adjacent := append(MigrationConfigurationIndexKey(), []byte("|user")...)
	require.NoError(t, source.PutAt(t.Context(), adjacent, []byte("keep"), 30, 0))
	for _, format := range []string{"mvcc", "native", "sst"} {
		t.Run(format, func(t *testing.T) {
			raw := externalTestFormat(t, source, format)
			for _, memory := range []bool{true, false} {
				if memory && format != "mvcc" {
					continue
				}
				verifyExternalEpochSanitization(t, raw, memory, adjacent)
			}
		})
	}
}

func verifyExternalEpochSanitization(t *testing.T, raw []byte, memory bool, adjacent []byte) {
	t.Helper()
	destination := externalTestDestination(t, memory)
	defer func() { require.NoError(t, destination.Close()) }()
	require.NoError(t, externalTestRestore(t, destination, raw))
	assertExternalEpochAbsent(t, destination, adjacent)
	destination = reopenExternalDestination(t, destination)
	assertExternalEpochAbsent(t, destination, adjacent)
	// Same-lineage restore retains both historical epoch versions verbatim.
	require.NoError(t, destination.Restore(bytes.NewReader(raw)))
	assertExternalEpochRetained(t, destination)
	destination = reopenExternalDestination(t, destination)
	assertExternalEpochRetained(t, destination)
}

func assertExternalEpochAbsent(t *testing.T, s MVCCStore, adjacent []byte) {
	t.Helper()
	for _, ts := range []uint64{10, 20, ^uint64(0)} {
		_, err := s.GetAt(t.Context(), MigrationConfigurationIndexKey(), ts)
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
	value, err := s.GetAt(t.Context(), adjacent, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), value)
	require.Equal(t, uint64(30), s.LastCommitTS(), "do not replace the epoch with the data frontier")
}

func assertExternalEpochRetained(t *testing.T, s MVCCStore) {
	t.Helper()
	for ts, epoch := range map[uint64]uint64{10: 11, 20: 22} {
		value, err := s.GetAt(t.Context(), MigrationConfigurationIndexKey(), ts)
		require.NoError(t, err)
		require.Equal(t, binary.BigEndian.AppendUint64(nil, epoch), value)
	}
}

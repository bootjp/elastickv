package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func externalTestMemory(t *testing.T) *mvccStore {
	t.Helper()
	s, ok := NewMVCCStore().(*mvccStore)
	require.True(t, ok)
	return s
}

func externalTestSnapshot(t *testing.T, s MVCCStore) []byte {
	t.Helper()
	snap, err := s.Snapshot()
	require.NoError(t, err)
	defer snap.Close()
	return snapshotBytes(t, snap)
}

func externalTestFormat(t *testing.T, source *mvccStore, format string) []byte {
	t.Helper()
	raw := externalTestSnapshot(t, source)
	if format == "mvcc" {
		return raw
	}
	s, err := NewPebbleStore(t.TempDir(), WithSSTIngestSnapshots(format == "sst"))
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.Restore(bytes.NewReader(raw)))
	return externalTestSnapshot(t, s)
}

func externalTestDestination(t *testing.T, memory bool) MVCCStore {
	t.Helper()
	if memory {
		return NewMVCCStore()
	}
	s, err := NewPebbleStore(filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err)
	return s
}

func externalTestRestore(t *testing.T, s MVCCStore, raw []byte) error {
	t.Helper()
	restorer, ok := s.(ExternalSnapshotRestorer)
	require.True(t, ok)
	return restorer.RestoreExternal(t.Context(), bytes.NewReader(raw))
}

func externalTestCases() map[string]func(*mvccStore) {
	return map[string]func(*mvccStore){
		"acks": func(s *mvccStore) {
			s.migrationAcks[migrationAckID{jobID: 7, bracketID: 2}] = migrationImportAck{batchSeq: 3, cursor: []byte("cursor")}
		},
		"floors": func(s *mvccStore) { s.migrationHLCFloors[7] = 100 },
		"promotion": func(s *mvccStore) {
			s.migrationPromotions[7] = PromotionState{Cursor: []byte("cursor"), PromotedRows: 2, MaxPromotedTS: 100}
		},
		"retirement": func(s *mvccStore) { s.migrationRetired[7] = struct{}{} },
		"readiness": func(s *mvccStore) {
			s.migrationReadinessCache = []TargetStagedReadinessState{{JobID: 7, MigrationJobID: 7, Armed: true, MinWriteTSExclusive: 100}}
		},
		"tracker-and-fences": func(s *mvccStore) {
			s.migrationReadinessCache = []TargetStagedReadinessState{{JobID: 7, MigrationJobID: 7,
				TrackWrites: true, SourceReadFence: true, SourceWriteFence: true, RetentionPinTS: 10, MinAdmittedTS: 20}}
		},
		"owner": func(s *mvccStore) {
			s.migrationOwnership = []MigrationOwnershipState{externalTestOwnership(MigrationOwner)}
		},
		"nonowner": func(s *mvccStore) {
			s.migrationOwnership = []MigrationOwnershipState{externalTestOwnership(MigrationNonOwner)}
		},
	}
}

func externalTestOwnership(role MigrationOwnershipRole) MigrationOwnershipState {
	owner := uint64(1)
	if role == MigrationNonOwner {
		owner = 2
	}
	return MigrationOwnershipState{JobID: 7, LocalGroupID: 1, OwnerGroupID: owner, CatalogVersion: 10,
		ConfigurationIndex: 3, AppliedIndex: 9, MinWriteTSExclusive: 100,
		RouteStart: []byte("a"), RouteEnd: []byte("z"), Role: role, Staged: role == MigrationOwner}
}

func TestExternalSnapshotMigrationMetadataRejectedBeforePublication(t *testing.T) {
	for name, populate := range externalTestCases() {
		t.Run(name, func(t *testing.T) {
			for _, format := range []string{"mvcc", "native", "sst"} {
				t.Run(format, func(t *testing.T) {
					source := externalTestMemory(t)
					populate(source)
					raw := externalTestFormat(t, source, format)
					for _, memory := range []bool{true, false} {
						if memory && format != "mvcc" {
							continue
						}
						verifyExternalMetadataRejection(t, raw, memory, source.migrationOwnership)
					}
				})
			}
		})
	}
}

func verifyExternalMetadataRejection(t *testing.T, raw []byte, memory bool, ownership []MigrationOwnershipState) {
	t.Helper()
	destination := externalTestDestination(t, memory)
	defer func() { require.NoError(t, destination.Close()) }()
	seedExternalExistingEpoch(t, destination)
	require.NoError(t, destination.PutAt(t.Context(), []byte("sentinel"), []byte("unchanged"), 50, 0))
	before := externalTestSnapshot(t, destination)
	require.ErrorIs(t, externalTestRestore(t, destination, raw), ErrExternalSnapshotMigrationState)
	require.Equal(t, before, externalTestSnapshot(t, destination))
	destination = reopenExternalDestination(t, destination)
	require.Equal(t, before, externalTestSnapshot(t, destination))
	// Same-lineage restore must retain every decoded field, not strip evidence.
	require.NoError(t, destination.Restore(bytes.NewReader(raw)))
	assertExternalNativeMetadata(t, destination, raw)
	assertExternalOwnershipFields(t, destination, ownership)
	destination = reopenExternalDestination(t, destination)
	assertExternalNativeMetadata(t, destination, raw)
	assertExternalOwnershipFields(t, destination, ownership)
}

func assertExternalOwnershipFields(t *testing.T, s MVCCStore, ownership []MigrationOwnershipState) {
	t.Helper()
	if len(ownership) == 0 {
		return
	}
	view, err := ownershipStore(t, s).MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Equal(t, ownership, view.Ownership)
}

func reopenExternalDestination(t *testing.T, s MVCCStore) MVCCStore {
	t.Helper()
	ps, ok := s.(*pebbleStore)
	if !ok {
		return s
	}
	require.NoError(t, ps.Close())
	reopened, err := NewPebbleStore(ps.dir)
	require.NoError(t, err)
	return reopened
}

func assertExternalNativeMetadata(t *testing.T, destination MVCCStore, raw []byte) {
	t.Helper()
	// Converting the original into Pebble gives a common decoder for all formats.
	expected := externalTestDestination(t, false)
	defer expected.Close()
	require.NoError(t, expected.Restore(bytes.NewReader(raw)))
	got := externalTestSnapshot(t, destination)
	actual := externalTestDestination(t, false)
	defer actual.Close()
	require.NoError(t, actual.Restore(bytes.NewReader(got)))
	require.Equal(t, externalTestSnapshot(t, expected), externalTestSnapshot(t, actual))
}

func TestExternalSnapshotTypedMigrationRows(t *testing.T) {
	for _, prefix := range []string{"!dist|migstage|", "!migwrite|", "!migfence|"} {
		t.Run(prefix, func(t *testing.T) {
			source := externalTestMemory(t)
			key := binary.BigEndian.AppendUint64([]byte(prefix), 7)
			key = append(key, '|', 'k')
			require.NoError(t, source.PutAt(t.Context(), key, []byte("payload"), 20, 0))
			for _, format := range []string{"mvcc", "native", "sst"} {
				raw := externalTestFormat(t, source, format)
				verifyExternalMetadataRejection(t, raw, false, nil)
				if format == "mvcc" {
					verifyExternalMetadataRejection(t, raw, true, nil)
				}
			}
		})
	}
}

func TestExternalSnapshotProofFreeAndEpochOnly(t *testing.T) {
	source := externalTestMemory(t)
	// Metadata-like bytes in ordinary values or non-typed keys are not evidence.
	require.NoError(t, source.PutAt(t.Context(), []byte("user"), []byte("!migstage|ownership !migwrite| _migack"), 20, 0))
	seedExternalExistingEpoch(t, source)
	for _, format := range []string{"mvcc", "native", "sst"} {
		raw := externalTestFormat(t, source, format)
		for _, memory := range []bool{false, true} {
			if memory && format != "mvcc" {
				continue
			}
			destination := externalTestDestination(t, memory)
			defer destination.Close()
			require.NoError(t, externalTestRestore(t, destination, raw))
			value, err := destination.GetAt(t.Context(), []byte("user"), 21)
			require.NoError(t, err)
			require.Equal(t, []byte("!migstage|ownership !migwrite| _migack"), value)
		}
	}
}

func TestExternalSnapshotEmptyV6Metadata(t *testing.T) {
	s := externalTestMemory(t)
	f, err := os.CreateTemp(t.TempDir(), "snapshot")
	require.NoError(t, err)
	defer f.Close()
	offset, err := writeMVCCSnapshotHeader(f, mvccSnapshotVersionV6)
	require.NoError(t, err)
	sum, err := s.writeSnapshotBodyLocked(f, mvccSnapshotVersionV6)
	require.NoError(t, err)
	require.NoError(t, finalizeMVCCSnapshotFile(f, offset, sum))
	raw, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	for _, memory := range []bool{true, false} {
		destination := externalTestDestination(t, memory)
		defer destination.Close()
		require.NoError(t, externalTestRestore(t, destination, raw))
	}
}

func TestExternalSnapshotCorruptMetadataLeavesDestinationUnchanged(t *testing.T) {
	for _, sst := range []bool{false, true} {
		for _, key := range [][]byte{migrationOwnershipMetaKeyBytes, migrationRetiredMetaKeyBytes,
			migrationAckMetaKeyBytes, migrationHLCFloorMetaKeyBytes, migrationPromoteMetaKeyBytes, migrationReadyKey(7)} {
			s, err := NewPebbleStore(t.TempDir(), WithSSTIngestSnapshots(sst))
			require.NoError(t, err)
			ps, ok := s.(*pebbleStore)
			require.True(t, ok)
			require.NoError(t, ps.db.Set(key, []byte("corrupt"), pebble.Sync))
			raw := externalTestSnapshot(t, s)
			require.NoError(t, s.Close())
			destination := externalTestDestination(t, false)
			defer destination.Close()
			seedExternalExistingEpoch(t, destination)
			require.NoError(t, destination.PutAt(t.Context(), []byte("sentinel"), []byte("unchanged"), 50, 0))
			before := externalTestSnapshot(t, destination)
			require.Error(t, externalTestRestore(t, destination, raw))
			require.Equal(t, before, externalTestSnapshot(t, destination))
		}
	}
}

func TestExternalSnapshotCanceledBeforePublication(t *testing.T) {
	source := externalTestMemory(t)
	raw := externalTestSnapshot(t, source)
	for _, memory := range []bool{true, false} {
		destination := externalTestDestination(t, memory)
		defer destination.Close()
		seedExternalExistingEpoch(t, destination)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		restorer, ok := destination.(ExternalSnapshotRestorer)
		require.True(t, ok)
		before := externalTestSnapshot(t, destination)
		require.ErrorIs(t, restorer.RestoreExternal(ctx, bytes.NewReader(raw)), context.Canceled)
		require.Equal(t, before, externalTestSnapshot(t, destination))
	}
}

func TestExternalSnapshotCorruptMVCCMetadata(t *testing.T) {
	source := externalTestMemory(t)
	source.migrationOwnership = []MigrationOwnershipState{externalTestOwnership(MigrationOwner)}
	raw := externalTestSnapshot(t, source)
	encoded, err := encodeMigrationOwnership(source.migrationOwnership)
	require.NoError(t, err)
	offset := bytes.Index(raw, encoded)
	require.Positive(t, offset)
	raw[offset] = 0 // Unknown ownership codec, with a valid outer checksum.
	const checksumOffset = 12
	const bodyOffset = 16
	binary.LittleEndian.PutUint32(raw[checksumOffset:bodyOffset], crc32.ChecksumIEEE(raw[bodyOffset:]))
	for _, memory := range []bool{true, false} {
		destination := externalTestDestination(t, memory)
		defer destination.Close()
		seedExternalExistingEpoch(t, destination)
		require.NoError(t, destination.PutAt(t.Context(), []byte("sentinel"), []byte("unchanged"), 50, 0))
		before := externalTestSnapshot(t, destination)
		require.ErrorContains(t, externalTestRestore(t, destination, raw), "invalid migration ownership encoding")
		require.Equal(t, before, externalTestSnapshot(t, destination))
	}
}

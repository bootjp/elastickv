package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftControlProjectionSnapshotWatermark(t *testing.T) {
	for _, format := range []string{"memory", "native-row", "native-sst"} {
		for _, blob := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/blob=%t", format, blob), func(t *testing.T) {
				source := projectionSnapshotSource(t, format, blob)
				snapshot, err := source.Snapshot()
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, snapshot.Close()) })
				var payload bytes.Buffer
				_, err = snapshot.WriteTo(&payload)
				require.NoError(t, err)
				require.NoError(t, snapshot.Close())
				assertProjectionSnapshotFormat(t, format, payload.Bytes())
				if format == "memory" {
					memory := NewMVCCStore()
					t.Cleanup(func() { require.NoError(t, memory.Close()) })
					require.NoError(t, memory.Restore(bytes.NewReader(payload.Bytes())))
					assertProjectionSnapshotState(t, memory, blob)
				}
				dir := t.TempDir()
				target := openSSTSnapshotTestStore(t, dir)
				require.NoError(t, target.Restore(bytes.NewReader(payload.Bytes())))
				assertProjectionSnapshotState(t, target, blob)
				index, present := raftEntryAppliedIndex(t, target)
				require.True(t, present)
				require.Equal(t, uint64(9), index)
				require.NoError(t, target.Close())
				target.db = nil
				reopened := openSSTSnapshotTestStore(t, dir)
				assertProjectionSnapshotState(t, reopened, blob)
				index, present = raftEntryAppliedIndex(t, reopened)
				require.True(t, present)
				require.Equal(t, uint64(9), index)
			})
		}
	}
}

func projectionSnapshotSource(t *testing.T, format string, blob bool) MVCCStore {
	t.Helper()
	var source MVCCStore
	if format == "memory" {
		source = NewMVCCStore()
		t.Cleanup(func() { require.NoError(t, source.Close()) })
	} else {
		source = openSSTSnapshotTestStore(t, t.TempDir(), WithSSTIngestSnapshots(format == "native-sst"))
	}
	api := raftMetadata(t, source)
	require.NoError(t, raftEntryApplier(t, source).ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
		if err := api.StageRaftEntryMutationsPreservingLastCommitTS(ctx, configurationProjection(9), nil, 10, 10); err != nil {
			return err
		}
		if err := source.ApplyMutationsRaftAt(ctx, raftEntryPut("data", "value"), nil, 100, 6, 9); err != nil {
			return err
		}
		return api.StageRaftControlMetadata(ctx, []byte("P"), []byte("policy-9"))
	}))
	if blob {
		auxiliary, ok := source.(interface {
			ApplyMutationsPreservingLastCommitTS(context.Context, []*KVPairMutation, [][]byte, uint64, uint64) error
		})
		require.True(t, ok)
		require.NoError(t, auxiliary.ApplyMutationsPreservingLastCommitTS(t.Context(), raftEntryPut("!s3|chunkblob|snapshot", "blob"), nil, 20, 20))
	}
	require.Equal(t, uint64(6), source.LastCommitTS())
	return source
}

func assertProjectionSnapshotFormat(t *testing.T, format string, payload []byte) {
	t.Helper()
	switch format {
	case "native-sst":
		require.True(t, bytes.HasPrefix(payload, pebbleSSTIngestSnapshotMagic[:]), "actual SST required; row fallback cannot pass")
	case "native-row":
		require.True(t, bytes.HasPrefix(payload, pebbleSnapshotMagic[:]), "actual native row stream required")
	case "memory":
		require.True(t, bytes.HasPrefix(payload, mvccSnapshotMagic[:]), "actual memory stream required")
	default:
		t.Fatalf("unknown projection snapshot format %q", format)
	}
}

func assertProjectionSnapshotState(t *testing.T, s MVCCStore, blob bool) {
	t.Helper()
	value, err := s.GetAt(t.Context(), MigrationConfigurationIndexKey(), 10)
	require.NoError(t, err)
	require.Equal(t, uint64(9), binary.BigEndian.Uint64(value))
	value, err = s.GetAt(t.Context(), []byte("data"), 6)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
	value, err = raftMetadata(t, s).ReadRaftControlMetadata(t.Context(), []byte("P"))
	require.NoError(t, err)
	require.Equal(t, []byte("policy-9"), value)
	if blob {
		value, err = s.GetAt(t.Context(), []byte("!s3|chunkblob|snapshot"), 20)
		require.NoError(t, err)
		require.Equal(t, []byte("blob"), value)
	}
	require.Equal(t, uint64(6), s.LastCommitTS(), "restoration must preserve the advertised watermark, not projection TS10 or auxiliary TS20")
}

package fskeys

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirEntryKeyRoundTripAndOrder(t *testing.T) {
	names := [][]byte{
		[]byte("a"),
		[]byte("a\x00"),
		[]byte("a/b"),
		[]byte("b"),
	}
	keys := make([][]byte, 0, len(names))
	for _, name := range names {
		key := DirEntryKey(7, name)
		got, ok := DirEntryNameFromKey(7, key)
		require.True(t, ok)
		require.Equal(t, name, got)
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	gotNames := make([][]byte, 0, len(keys))
	for _, key := range keys {
		name, ok := DirEntryNameFromKey(7, key)
		require.True(t, ok)
		gotNames = append(gotNames, name)
	}
	require.Equal(t, names, gotNames)
}

func TestExtractRouteKeyNormalizesChunkIndex(t *testing.T) {
	k1 := ChunkKey(11, 22, 1)
	k2 := ChunkKey(11, 22, 99)
	require.Equal(t, ExtractRouteKey(k1), ExtractRouteKey(k2))
	require.Equal(t, ChunkRouteKey(11, 22), ExtractRouteKey(k1))
	require.NotEqual(t, ExtractRouteKey(k1), ExtractRouteKey(ChunkKey(12, 22, 1)))
	require.Nil(t, ExtractRouteKey(InodeKey(22)))
}

func TestNormalizeSplitBoundarySnapsFilesystemChunkKeys(t *testing.T) {
	rawChunk := ChunkKey(11, 22, 99)
	routeKey := ChunkRouteKey(11, 22)
	insideRouteKey := append(append([]byte(nil), routeKey...), 0xff)

	require.Equal(t, routeKey, NormalizeSplitBoundary(rawChunk))
	require.Equal(t, routeKey, NormalizeSplitBoundary(insideRouteKey))
	require.Equal(t, routeKey, NormalizeSplitBoundary(routeKey))
	require.Equal(t, InodeKey(22), NormalizeSplitBoundary(InodeKey(22)))
}

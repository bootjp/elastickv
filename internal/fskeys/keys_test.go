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

func TestChunkScanRouteBounds(t *testing.T) {
	t.Parallel()

	start := ChunkKey(11, 22, 7)
	end := prefixEnd(ChunkPrefix(11, 22))
	routeStart, routeEnd, ok := ChunkScanRouteBounds(start, end)
	require.True(t, ok)
	require.Equal(t, ChunkRouteKey(11, 22), routeStart)
	require.Equal(t, prefixEnd(ChunkRouteKey(11, 22)), routeEnd)
}

func TestChunkScanRouteBoundsSameFileSubrange(t *testing.T) {
	t.Parallel()

	start := ChunkKey(11, 22, 7)
	end := scanCursorAfterForTest(ChunkKey(11, 22, 7))
	routeStart, routeEnd, ok := ChunkScanRouteBounds(start, end)
	require.True(t, ok)
	require.Equal(t, ChunkRouteKey(11, 22), routeStart)
	require.Equal(t, prefixEnd(ChunkRouteKey(11, 22)), routeEnd)
}

func TestChunkScanRouteBoundsCrossFileSubrangeIncludesEndRoute(t *testing.T) {
	t.Parallel()

	start := ChunkKey(11, 22, 7)
	end := ChunkKey(11, 23, 5)
	routeStart, routeEnd, ok := ChunkScanRouteBounds(start, end)
	require.True(t, ok)
	require.Equal(t, ChunkRouteKey(11, 22), routeStart)
	require.Equal(t, prefixEnd(ChunkRouteKey(11, 23)), routeEnd)
}

func TestChunkScanRouteBoundsCrossFileCursorAfterFirstChunkIncludesEndRoute(t *testing.T) {
	t.Parallel()

	start := ChunkKey(11, 22, 7)
	end := scanCursorAfterForTest(ChunkKey(11, 23, 0))
	routeStart, routeEnd, ok := ChunkScanRouteBounds(start, end)
	require.True(t, ok)
	require.Equal(t, ChunkRouteKey(11, 22), routeStart)
	require.Equal(t, prefixEnd(ChunkRouteKey(11, 23)), routeEnd)
}

func TestChunkScanRouteBoundsCrossFilePrefixEndExcludesEndRoute(t *testing.T) {
	t.Parallel()

	start := ChunkKey(11, 22, 7)
	end := ChunkPrefix(11, 23)
	routeStart, routeEnd, ok := ChunkScanRouteBounds(start, end)
	require.True(t, ok)
	require.Equal(t, ChunkRouteKey(11, 22), routeStart)
	require.Equal(t, ChunkRouteKey(11, 23), routeEnd)
}

func TestChunkScanRouteBoundsCarriedFilePrefixEnd(t *testing.T) {
	t.Parallel()

	start := ChunkPrefix(11, 0xff)
	end := prefixEnd(start)
	require.Less(t, len(end), len(start))

	routeStart, routeEnd, ok := ChunkScanRouteBounds(start, end)
	require.True(t, ok)
	require.Equal(t, ChunkRouteKey(11, 0xff), routeStart)
	require.Equal(t, prefixEnd(ChunkRouteKey(11, 0xff)), routeEnd)
}

func TestChunkScanRouteBoundsCrossFileCarriedPrefixEnd(t *testing.T) {
	t.Parallel()

	start := ChunkKey(11, 0xfe, 7)
	end := prefixEnd(ChunkPrefix(11, 0xff))
	require.Less(t, len(end), len(ChunkPrefix(11, 0xff)))

	routeStart, routeEnd, ok := ChunkScanRouteBounds(start, end)
	require.True(t, ok)
	require.Equal(t, ChunkRouteKey(11, 0xfe), routeStart)
	require.Equal(t, prefixEnd(ChunkRouteKey(11, 0xff)), routeEnd)
}

func TestChunkScanRouteBounds_AllChunks(t *testing.T) {
	t.Parallel()

	start := ChunkAllPrefix()
	end := prefixEnd(start)
	routeStart, routeEnd, ok := ChunkScanRouteBounds(start, end)
	require.True(t, ok)
	require.Equal(t, []byte(chunkRoutePrefix), routeStart)
	require.Equal(t, prefixEnd([]byte(chunkRoutePrefix)), routeEnd)
}

func TestChunkScanRouteBoundsUnboundedScanUsesChunkRoutes(t *testing.T) {
	t.Parallel()

	start := scanCursorAfterForTest(ChunkKey(11, 22, 7))
	routeStart, routeEnd, ok := ChunkScanRouteBounds(start, nil)
	require.True(t, ok)
	require.Equal(t, ChunkRouteKey(11, 22), routeStart)
	require.Equal(t, prefixEnd([]byte(chunkRoutePrefix)), routeEnd)
}

func TestRefFenceKeyIsPerInode(t *testing.T) {
	require.Equal(t, RefFenceKey(7), RefFenceKey(7))
	require.NotEqual(t, RefFenceKey(7), RefFenceKey(8))
	require.False(t, bytes.HasPrefix(RefKey(7, []byte("client"), 9), RefFenceKey(7)))
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

func scanCursorAfterForTest(key []byte) []byte {
	out := append([]byte(nil), key...)
	return append(out, 0)
}

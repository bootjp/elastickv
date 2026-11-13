package store

import (
	"context"
	"crypto/sha256"
	"math"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/stretchr/testify/require"
)

type localBlobWriter interface {
	ApplyMutationsPreservingLastCommitTS(context.Context, []*KVPairMutation, [][]byte, uint64, uint64) error
}

func writeLocalBlob(t *testing.T, st MVCCStore, sha [32]byte, ts uint64) {
	t.Helper()
	key := s3keys.ChunkBlobKey(sha)
	start, found, err := st.LatestCommitTS(context.Background(), key)
	require.NoError(t, err)
	if !found {
		start = 0
	}
	writer, ok := st.(localBlobWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyMutationsPreservingLastCommitTS(context.Background(), []*KVPairMutation{{
		Key: key, Value: []byte("payload"), Op: OpTypePut,
	}}, nil, start, ts))
}

func TestChunkBlobLocalStore(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) MVCCStore
	}{
		{"memory", func(*testing.T) MVCCStore { return NewMVCCStore() }},
		{"pebble", func(t *testing.T) MVCCStore {
			st, err := NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := tc.open(t)
			local, err := NewChunkBlobLocalStore(st)
			require.NoError(t, err)
			sha := sha256.Sum256([]byte("blob"))
			other := sha256.Sum256([]byte("other"))
			written, found, err := local.ChunkBlobWrittenAt(ctx, sha)
			require.NoError(t, err)
			require.False(t, found)
			require.Zero(t, written)

			writeLocalBlob(t, st, sha, 10)
			writeLocalBlob(t, st, other, 12)
			suffixKey := append(s3keys.ChunkBlobKey(sha), 'x')
			writer, ok := st.(localBlobWriter)
			require.True(t, ok)
			require.NoError(t, writer.ApplyMutationsPreservingLastCommitTS(ctx, []*KVPairMutation{{
				Key: suffixKey, Value: []byte("not a blob"), Op: OpTypePut,
			}}, nil, 0, 15))
			require.Equal(t, uint64(0), st.LastCommitTS())
			blobs, err := local.ListLocalChunkBlobs(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []s3keys.LocalChunkBlob{
				{ContentSHA256: sha, WrittenAtTS: 10},
				{ContentSHA256: other, WrittenAtTS: 12},
			}, blobs)

			writeLocalBlob(t, st, sha, 20) // A new upload re-anchors the same payload.
			deleted, err := local.DeleteChunkBlobIfUnchanged(ctx, sha, 10)
			require.NoError(t, err)
			require.False(t, deleted)
			written, found, err = local.ChunkBlobWrittenAt(ctx, sha)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, uint64(20), written)

			deleted, err = local.DeleteChunkBlobIfUnchanged(ctx, sha, 20)
			require.NoError(t, err)
			require.True(t, deleted)
			deleted, err = local.DeleteChunkBlobIfUnchanged(ctx, sha, 20)
			require.NoError(t, err)
			require.False(t, deleted)
			_, found, err = st.LatestCommitTS(ctx, s3keys.ChunkBlobKey(sha))
			require.NoError(t, err)
			require.False(t, found) // Every historical payload version was removed.
			_, found, err = st.LatestCommitTS(ctx, suffixKey)
			require.NoError(t, err)
			require.True(t, found) // A longer key with the same prefix is untouched.
			_, err = st.GetAt(ctx, s3keys.ChunkBlobKey(sha), math.MaxUint64)
			require.ErrorIs(t, err, ErrKeyNotFound)
			blobs, err = local.ListLocalChunkBlobs(ctx)
			require.NoError(t, err)
			require.Equal(t, []s3keys.LocalChunkBlob{{ContentSHA256: other, WrittenAtTS: 12}}, blobs)
			require.Equal(t, uint64(0), st.LastCommitTS())
		})
	}
}

func TestChunkBlobConditionalDeleteRacesReanchor(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) MVCCStore
	}{
		{"memory", func(*testing.T) MVCCStore { return NewMVCCStore() }},
		{"pebble", func(t *testing.T) MVCCStore {
			st, err := NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := tc.open(t)
			local, err := NewChunkBlobLocalStore(st)
			require.NoError(t, err)
			writer, ok := st.(localBlobWriter)
			require.True(t, ok)
			for i := 0; i < 20; i++ {
				sha := sha256.Sum256([]byte{byte(i)})
				writeLocalBlob(t, st, sha, 10)
				start := make(chan struct{})
				var wg sync.WaitGroup
				var deleteErr, writeErr error
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					_, deleteErr = local.DeleteChunkBlobIfUnchanged(ctx, sha, 10)
				}()
				go func() {
					defer wg.Done()
					<-start
					writeErr = writer.ApplyMutationsPreservingLastCommitTS(ctx, []*KVPairMutation{{
						Key: s3keys.ChunkBlobKey(sha), Value: []byte("payload"), Op: OpTypePut,
					}}, nil, 10, 20)
				}()
				close(start)
				wg.Wait()
				require.NoError(t, deleteErr)
				require.NoError(t, writeErr)
				written, found, err := local.ChunkBlobWrittenAt(ctx, sha)
				require.NoError(t, err)
				require.True(t, found, "acknowledged re-anchor must remain present")
				require.Equal(t, uint64(20), written)
			}
		})
	}
}

func TestPebbleChunkBlobLocalDeleteSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	sha := sha256.Sum256([]byte("durable"))
	writeLocalBlob(t, st, sha, 42)
	local, err := NewChunkBlobLocalStore(st)
	require.NoError(t, err)
	deleted, err := local.DeleteChunkBlobIfUnchanged(ctx, sha, 42)
	require.NoError(t, err)
	require.True(t, deleted)
	require.NoError(t, st.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer reopened.Close()
	local, err = NewChunkBlobLocalStore(reopened)
	require.NoError(t, err)
	_, found, err := local.ChunkBlobWrittenAt(ctx, sha)
	require.NoError(t, err)
	require.False(t, found)
}

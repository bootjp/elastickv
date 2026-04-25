package adapter

import (
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// raftMaxSizePerMsgPostPR593 is the post-PR-#593 default for
// etcd/raft's MaxSizePerMsg setting. Hardcoded here (rather than
// imported from internal/raftengine/etcd) so the test does not pull
// the engine package; the value is intentionally duplicated and pinned
// because the entire point of this test is to detect when an S3 batch
// silently grows past it.
const raftMaxSizePerMsgPostPR593 = 4 << 20

// TestS3ChunkBatchFitsInRaftMaxSize is the byte-budget invariant the
// s3ChunkBatchOps comment advertises: a worst-case S3 PutObject /
// UploadPart batch must encode strictly under
// raftMaxSizePerMsgPostPR593, plus the 1-byte raft framing prefix
// added by marshalRaftCommand.
//
// Without this guard, raising s3ChunkBatchOps or growing s3ChunkSize
// would silently route Raft entries through etcd/raft's
// oversized-first-entry path (util.go:limitSize, the "as an
// exception, if the size of the first entry exceeds maxSize, a
// non-empty slice with just this entry is returned" branch), which
// inflates the documented `MaxInflight × MaxSizePerMsg` per-peer
// memory bound.
func TestS3ChunkBatchFitsInRaftMaxSize(t *testing.T) {
	t.Parallel()

	// Worst-case key: a kilobyte-scale objectKey amplifies the
	// per-Mutation envelope. Choose 1 KiB to model a deeply nested
	// S3 path; longer keys are unusual but the headroom is generous.
	bucket := "test-bucket"
	objectKey := strings.Repeat("a", 1024)
	uploadID := "upload-12345678901234567890"
	const generation uint64 = 1
	const partNo uint64 = 1

	// Fill the chunk value with non-zero bytes so protobuf does not
	// elide trailing zeros and underestimate the encoded size.
	value := make([]byte, s3ChunkSize)
	for i := range value {
		value[i] = 0xAB
	}

	muts := make([]*pb.Mutation, 0, s3ChunkBatchOps)
	for i := uint64(0); i < uint64(s3ChunkBatchOps); i++ {
		key := s3keys.BlobKey(bucket, generation, objectKey, uploadID, partNo, i)
		muts = append(muts, &pb.Mutation{
			Op:    pb.Op_PUT,
			Key:   key,
			Value: value,
		})
	}

	req := &pb.Request{
		IsTxn:     false,
		Phase:     pb.Phase_NONE,
		Ts:        1234567890,
		Mutations: muts,
	}

	encoded, err := proto.Marshal(req)
	require.NoError(t, err)

	// marshalRaftCommand prepends one framing byte (raftEncodeSingle
	// or raftEncodeBatch). Account for it explicitly.
	const raftFramingPrefix = 1
	totalEntrySize := len(encoded) + raftFramingPrefix

	require.Lessf(t,
		totalEntrySize, raftMaxSizePerMsgPostPR593,
		"S3 chunk batch entry must fit strictly under MaxSizePerMsg=%d to avoid the etcd/raft oversized-first-entry path; got %d (s3ChunkBatchOps=%d, s3ChunkSize=%d, objectKey=%dB)",
		raftMaxSizePerMsgPostPR593, totalEntrySize, s3ChunkBatchOps, s3ChunkSize, len(objectKey),
	)

	// Sanity: the headroom should be meaningful (at least 64 KiB) so
	// future small bumps in key length or Request envelope fields do
	// not silently push past the limit. This is the constant we
	// document in the s3ChunkBatchOps comment.
	const minHeadroom = 64 << 10
	require.Greaterf(t,
		raftMaxSizePerMsgPostPR593-totalEntrySize, minHeadroom,
		"S3 chunk batch headroom under MaxSizePerMsg has fallen below %d B (got %d B); reduce s3ChunkBatchOps or s3ChunkSize",
		minHeadroom, raftMaxSizePerMsgPostPR593-totalEntrySize,
	)
}

// TestS3MetaBatchFitsInRaftMaxSize is the same byte-budget invariant
// for the cleanup paths (cleanupPartBlobsAsync, deleteByPrefix,
// cleanupManifestBlobs). These ops carry no chunk payload so the
// batch is dominated by key bytes; even at the worst-case key length
// the total stays well under the cap. The test pins the headroom
// margin so a future bump in s3MetaBatchOps that pushes too far is
// caught at PR time.
func TestS3MetaBatchFitsInRaftMaxSize(t *testing.T) {
	t.Parallel()

	bucket := "test-bucket"
	objectKey := strings.Repeat("a", 1024)
	uploadID := "upload-12345678901234567890"
	const generation uint64 = 1
	const partNo uint64 = 1

	muts := make([]*pb.Mutation, 0, s3MetaBatchOps)
	for i := uint64(0); i < uint64(s3MetaBatchOps); i++ {
		key := s3keys.BlobKey(bucket, generation, objectKey, uploadID, partNo, i)
		muts = append(muts, &pb.Mutation{
			Op:  pb.Op_DEL,
			Key: key,
		})
	}

	req := &pb.Request{
		IsTxn:     false,
		Phase:     pb.Phase_NONE,
		Ts:        1234567890,
		Mutations: muts,
	}

	encoded, err := proto.Marshal(req)
	require.NoError(t, err)

	const raftFramingPrefix = 1
	totalEntrySize := len(encoded) + raftFramingPrefix

	require.Lessf(t,
		totalEntrySize, raftMaxSizePerMsgPostPR593,
		"S3 meta batch entry must fit under MaxSizePerMsg=%d; got %d (s3MetaBatchOps=%d, objectKey=%dB)",
		raftMaxSizePerMsgPostPR593, totalEntrySize, s3MetaBatchOps, len(objectKey),
	)
}

// TestAppendPartBlobKeys_FlushFiresEveryS3MetaBatchOps is the
// regression guard for the slice-by-value bug Gemini caught: a
// previous version of appendPartBlobKeys took `pending` by value, so
// the flush closure (captured from cleanupManifestBlobs's enclosing
// scope) saw the outer slice header at length 0 and never fired,
// silently accumulating every chunk into one giant batch. This test
// pins the contract that the helper drains via flush exactly every
// s3MetaBatchOps appends, never building a slice longer than the cap.
func TestAppendPartBlobKeys_FlushFiresEveryS3MetaBatchOps(t *testing.T) {
	t.Parallel()

	// Build a manifest part with chunkCount > 2× s3MetaBatchOps so the
	// flush closure must fire at least twice, plus a tail flush from
	// the caller's final flush() in cleanupManifestBlobs.
	const chunkCount = 2*s3MetaBatchOps + 7
	chunkSizes := make([]uint64, chunkCount)
	for i := range chunkSizes {
		chunkSizes[i] = 1
	}
	part := s3ObjectPart{
		PartNo:      1,
		PartVersion: 1,
		ChunkSizes:  chunkSizes,
	}

	pending := make([]*kv.Elem[kv.OP], 0, s3MetaBatchOps)
	flushCalls := 0
	flushBatchSizes := make([]int, 0, 4)
	flush := func() {
		// Mirror cleanupManifestBlobs's flush: record the batch size
		// and then truncate. If the helper's pointer plumbing is
		// broken, len(pending) here would always be 0 and the
		// recorded batch sizes would never match s3MetaBatchOps.
		flushCalls++
		flushBatchSizes = append(flushBatchSizes, len(pending))
		pending = pending[:0]
	}

	srv := (*S3Server)(nil) // method body does not touch s
	ok := srv.appendPartBlobKeys(&pending, "bucket", 1, "key", "upload", part, flush)
	require.True(t, ok)

	// Exactly two threshold-triggered flushes inside the helper:
	// at append #s3MetaBatchOps and #2×s3MetaBatchOps. The 7-entry
	// remainder is left in pending for the caller's tail flush().
	require.Equal(t, 2, flushCalls,
		"expected flush to fire twice (at append %d and %d); slice-by-value bug regressed?",
		s3MetaBatchOps, 2*s3MetaBatchOps)
	require.Equal(t, []int{s3MetaBatchOps, s3MetaBatchOps}, flushBatchSizes,
		"each flush must drain exactly s3MetaBatchOps entries; pending must not silently overflow the cap")
	require.Len(t, pending, 7,
		"trailing 7 entries should remain for the caller's final flush()")
}

package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	rand "math/rand/v2"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *S3Server) ensureS3ObjectRangeLocal(
	ctx context.Context,
	bucket string,
	generation uint64,
	objectKey string,
	manifest *s3ObjectManifest,
	readTS uint64,
	offset int64,
	length int64,
) error {
	remaining := length
	position := int64(0)
	for _, part := range manifest.Parts {
		for chunkIndex, chunkSize := range part.ChunkSizes {
			chunkEnd := position + int64(chunkSize) //nolint:gosec // Chunk size is bounded by s3ChunkSize.
			if remaining > 0 && chunkEnd > offset {
				index, err := uint64FromInt(chunkIndex)
				if err != nil {
					return err
				}
				if _, err := s.readS3ObjectChunk(ctx, bucket, generation, objectKey, manifest.UploadID, part, index, chunkSize, readTS); err != nil {
					return err
				}
				consumedStart := position
				if consumedStart < offset {
					consumedStart = offset
				}
				consumed := chunkEnd - consumedStart
				if consumed > remaining {
					consumed = remaining
				}
				remaining -= consumed
			}
			position = chunkEnd
			if remaining <= 0 {
				return nil
			}
		}
	}
	return nil
}

func (s *S3Server) readS3ObjectChunk(
	ctx context.Context,
	bucket string,
	generation uint64,
	objectKey string,
	uploadID string,
	part s3ObjectPart,
	chunkIndex uint64,
	expectedSize uint64,
	readTS uint64,
) ([]byte, error) {
	if !part.Offloaded {
		return s.readLegacyS3ObjectChunk(ctx, bucket, generation, objectKey, uploadID, part, chunkIndex, readTS)
	}
	ref, refCommitTS, err := s.loadS3ChunkRef(ctx, bucket, generation, objectKey, uploadID, part.PartNo, chunkIndex, expectedSize, readTS)
	if err != nil {
		return nil, err
	}
	payload, found, err := s.localS3ChunkBlob(ctx, ref.ContentSHA256)
	if err != nil || found {
		return payload, err
	}
	return s.fetchAndStoreS3ChunkBlob(ctx, ref, refCommitTS)
}

func (s *S3Server) readLegacyS3ObjectChunk(
	ctx context.Context,
	bucket string,
	generation uint64,
	objectKey string,
	uploadID string,
	part s3ObjectPart,
	chunkIndex uint64,
	readTS uint64,
) ([]byte, error) {
	key := s3keys.VersionedBlobKey(bucket, generation, objectKey, uploadID, part.PartNo, chunkIndex, part.PartVersion)
	payload, err := s.store.GetAt(ctx, key, readTS)
	return payload, errors.WithStack(err)
}

func (s *S3Server) loadS3ChunkRef(
	ctx context.Context,
	bucket string,
	generation uint64,
	objectKey string,
	uploadID string,
	partNo uint64,
	chunkIndex uint64,
	expectedSize uint64,
	readTS uint64,
) (s3keys.ChunkRefValue, uint64, error) {
	refKey := s3keys.ChunkRefKey(bucket, generation, objectKey, uploadID, partNo, chunkIndex)
	rawRef, err := s.store.GetAt(ctx, refKey, readTS)
	if err != nil {
		return s3keys.ChunkRefValue{}, 0, errors.Wrap(err, "read s3 chunkref")
	}
	ref, ok := s3keys.DecodeChunkRefValue(rawRef)
	if !ok {
		return s3keys.ChunkRefValue{}, 0, errors.New("decode s3 chunkref: invalid value")
	}
	if ref.Size != expectedSize {
		s.observeS3ChunkBlobMismatch()
		return s3keys.ChunkRefValue{}, 0, errors.WithStack(errors.Newf(
			"s3 chunkref size %d does not match manifest size %d",
			ref.Size,
			expectedSize,
		))
	}
	refCommitTS, exists, err := s.store.LatestCommitTS(ctx, refKey)
	if err != nil {
		return s3keys.ChunkRefValue{}, 0, errors.Wrap(err, "read s3 chunkref commit timestamp")
	}
	if !exists || refCommitTS == 0 {
		return s3keys.ChunkRefValue{}, 0, errors.New("s3 chunkref commit timestamp is unavailable")
	}
	return ref, refCommitTS, nil
}

func (s *S3Server) fetchAndStoreS3ChunkBlob(
	ctx context.Context,
	ref s3keys.ChunkRefValue,
	refCommitTS uint64,
) ([]byte, error) {
	payload, err := s.fetchS3ChunkBlob(ctx, ref)
	if err != nil {
		return nil, err
	}
	if uint64(len(payload)) != ref.Size { //nolint:gosec // len is bounded by s3ChunkSize.
		s.observeS3ChunkBlobMismatch()
		return nil, errors.New("fetched s3 chunkblob size does not match chunkref")
	}
	if err := s.storeFetchedS3ChunkBlob(ctx, ref.ContentSHA256, payload, refCommitTS); err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *S3Server) localS3ChunkBlob(ctx context.Context, digest [s3ChunkBlobSHA256Bytes]byte) ([]byte, bool, error) {
	if s == nil || s.blobLocalStores == nil {
		return nil, false, s3BlobUnavailable("s3 chunkblob local store is not configured")
	}
	key := s3keys.ChunkBlobKey(digest)
	localStore, ok := s.blobLocalStores.LocalStoreForKey(key)
	if !ok || localStore == nil {
		return nil, false, s3BlobUnavailable("s3 chunkblob local store is unavailable")
	}
	payload, err := localStore.GetAt(ctx, key, math.MaxUint64)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	actual := sha256.Sum256(payload)
	if !bytes.Equal(actual[:], digest[:]) {
		s.observeS3ChunkBlobMismatch()
		return nil, false, nil
	}
	return payload, true, nil
}

func (s *S3Server) fetchS3ChunkBlob(ctx context.Context, ref s3keys.ChunkRefValue) ([]byte, error) {
	if s == nil || s.blobCluster == nil {
		return nil, s3BlobUnavailable("s3 chunkblob peer client is not configured")
	}
	key := s3keys.ChunkBlobKey(ref.ContentSHA256)
	replicas, err := s.blobCluster.ReplicasForChunk(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "resolve s3 chunkblob fetch replicas")
	}
	peers := orderedS3BlobPeers(replicas, s.blobCluster.SelfNodeID(), ref.SourcePeer)
	payload, err := s.fetchS3ChunkBlobFromPeers(ctx, peers, ref.ContentSHA256)
	if err == nil {
		return payload, nil
	}
	if s.blobOffloadObserver != nil {
		s.blobOffloadObserver.ObserveS3ChunkBlobUnrecoverable()
	}
	return nil, err
}

func orderedS3BlobPeers(replicas []S3BlobReplica, selfNodeID, sourcePeer string) []S3BlobReplica {
	peers := make([]S3BlobReplica, 0, len(replicas))
	var source *S3BlobReplica
	for i := range replicas {
		replica := replicas[i]
		if replica.NodeID == selfNodeID {
			continue
		}
		if replica.NodeID == sourcePeer {
			copyReplica := replica
			source = &copyReplica
			continue
		}
		peers = append(peers, replica)
	}
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	if source != nil {
		peers = append([]S3BlobReplica{*source}, peers...)
	}
	return peers
}

func (s *S3Server) fetchS3ChunkBlobFromPeers(
	ctx context.Context,
	peers []S3BlobReplica,
	digest [s3ChunkBlobSHA256Bytes]byte,
) ([]byte, error) {
	for _, peer := range peers {
		payload, fetchErr := s.blobCluster.FetchChunkBlob(ctx, peer, digest)
		if fetchErr == nil {
			return payload, nil
		}
		if status.Code(fetchErr) == codes.InvalidArgument {
			s.observeS3ChunkBlobMismatch()
		}
		if ctx.Err() != nil {
			return nil, errors.WithStack(ctx.Err())
		}
	}
	return nil, errors.New("s3 chunkblob is unavailable on every replica")
}

func (s *S3Server) storeFetchedS3ChunkBlob(ctx context.Context, digest [s3ChunkBlobSHA256Bytes]byte, payload []byte, commitTS uint64) error {
	key := s3keys.ChunkBlobKey(digest)
	localStore, ok := s.blobLocalStores.LocalStoreForKey(key)
	if !ok || localStore == nil {
		return s3BlobUnavailable("s3 chunkblob local store is unavailable")
	}
	repairTS, err := s.nextTxnCommitTS(ctx, commitTS)
	if err != nil {
		return errors.Wrap(err, "allocate s3 chunkblob repair timestamp")
	}
	server := NewS3BlobFetchServer(localStore, s.blobOffloadObserver, WithS3BlobFetchClock(s.clock()))
	return server.storeChunkBlob(ctx, digest, payload, repairTS)
}

func (s *S3Server) observeS3ChunkBlobMismatch() {
	if s != nil && s.blobOffloadObserver != nil {
		s.blobOffloadObserver.ObserveS3ChunkBlobSHAMismatch()
	}
}

func s3ManifestHasOffloadedParts(manifest *s3ObjectManifest) bool {
	if manifest == nil {
		return false
	}
	for _, part := range manifest.Parts {
		if part.Offloaded {
			return true
		}
	}
	return false
}

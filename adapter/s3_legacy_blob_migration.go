package adapter

import (
	"context"
	"crypto/sha256"
	"log/slog"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

const (
	s3MigrationStaging          = "staging"
	s3MigrationAborting         = "aborting"
	s3MigrationFlipped          = "flipped"
	s3MigrationMaxChunksPerPart = (s3MaxPartSizeBytes + s3ChunkSize - 1) / s3ChunkSize
)

type s3MigrationPart struct {
	PartNo      uint64 `json:"part_no"`
	PartVersion uint64 `json:"part_version"`
	ChunkCount  uint64 `json:"chunk_count"`
}

// The marker is replicated with the manifest. It contains enough information
// to remove every staged reference or old BlobKey without reading a historical
// manifest after a restart or an overwrite.
type s3MigrationIntent struct {
	Phase          string            `json:"phase"`
	AttemptVersion uint64            `json:"attempt_version"`
	ManifestSHA256 [32]byte          `json:"manifest_sha256"`
	UploadID       string            `json:"upload_id"`
	LegacyParts    []s3MigrationPart `json:"legacy_parts"`
}

type s3MigrationCandidate struct {
	manifest   *s3ObjectManifest
	raw        []byte
	generation uint64
	parts      []s3MigrationPart
}

type s3MigrationActiveKey struct {
	bucket string
	object string
}

func decodeS3MigrationIntent(raw []byte) (s3MigrationIntent, error) {
	var intent s3MigrationIntent
	if err := json.Unmarshal(raw, &intent); err != nil {
		return intent, errors.Wrap(err, "decode s3 migration intent")
	}
	if err := validateS3MigrationIntent(intent); err != nil {
		return intent, err
	}
	return intent, nil
}

func validateS3MigrationIntent(intent s3MigrationIntent) error {
	if intent.AttemptVersion == 0 || intent.UploadID == "" || len(intent.LegacyParts) == 0 || len(intent.LegacyParts) > s3MaxPartsPerUpload {
		return errors.New("s3 migration intent has invalid identity or part count")
	}
	switch intent.Phase {
	case s3MigrationStaging, s3MigrationAborting, s3MigrationFlipped:
	default:
		return errors.New("s3 migration intent has invalid phase")
	}
	return validateS3MigrationParts(intent.LegacyParts)
}

func validateS3MigrationParts(parts []s3MigrationPart) error {
	seen := make(map[uint64]struct{}, len(parts))
	for _, part := range parts {
		if part.PartNo == 0 || part.PartNo > s3MaxPartNumber || part.ChunkCount == 0 || part.ChunkCount > s3MigrationMaxChunksPerPart {
			return errors.New("s3 migration intent has invalid part")
		}
		if _, exists := seen[part.PartNo]; exists {
			return errors.New("s3 migration intent has duplicate part")
		}
		seen[part.PartNo] = struct{}{}
	}
	return nil
}

func encodeS3MigrationIntent(intent s3MigrationIntent) ([]byte, error) {
	raw, err := json.Marshal(intent)
	return raw, errors.WithStack(err)
}

// migrateS3LegacyObject is called by the default-off migration worker.
func (s *S3Server) migrateS3LegacyObject(ctx context.Context, bucket, object string) (bool, error) {
	finish, err := s.beginActiveS3Migration(ctx, bucket, object)
	if err != nil {
		return false, err
	}
	defer finish()
	intent, manifest, generation, readTS, pin, err := s.beginS3LegacyMigration(ctx, bucket, object)
	if err != nil || intent == nil {
		return false, err
	}
	defer pin.Release()
	markerKey := s3keys.MigrationIntentKey(bucket, generation, object)
	for _, part := range manifest.Parts {
		if part.Offloaded {
			continue
		}
		for chunkNo, size := range part.ChunkSizes {
			if err := s.stageS3LegacyChunk(ctx, bucket, generation, object, manifest.UploadID, part, uint64(chunkNo), size, readTS, *intent); err != nil { //nolint:gosec // Chunk index is bounded by the manifest slice.
				return false, errors.WithStack(err)
			}
		}
	}
	if err := s.flipS3LegacyManifest(ctx, bucket, generation, object, *intent); err != nil {
		return false, err
	}
	// The flip and cleanup obligation are atomic. A failed cleanup leaves the
	// flipped marker for startup recovery; it never rolls the manifest back.
	if err := s.recoverS3MigrationIntent(ctx, markerKey); err != nil {
		return true, err
	}
	return true, nil
}

func (s *S3Server) beginActiveS3Migration(ctx context.Context, bucket, object string) (func(), error) {
	if err := s.ensureS3MigrationReady(ctx); err != nil {
		return nil, err
	}
	activeKey := s3MigrationActiveKey{bucket: bucket, object: object}
	if _, loaded := s.activeMigrations.LoadOrStore(activeKey, struct{}{}); loaded {
		return nil, errors.New("s3 migration is already active for this object")
	}
	return func() { s.activeMigrations.Delete(activeKey) }, nil
}

func (s *S3Server) beginS3LegacyMigration(ctx context.Context, bucket, object string) (*s3MigrationIntent, *s3ObjectManifest, uint64, uint64, *kv.ActiveTimestampToken, error) {
	read, err := s.beginTxnReadTimestamp(ctx, s.readTS(), "s3 migration: begin")
	if err != nil {
		return nil, nil, 0, 0, nil, err
	}
	ts := read.Timestamp()
	pin := s.pinReadTS(ts)
	fail := func(err error) (*s3MigrationIntent, *s3ObjectManifest, uint64, uint64, *kv.ActiveTimestampToken, error) {
		pin.Release()
		return nil, nil, 0, 0, nil, err
	}
	candidate, err := s.loadS3MigrationCandidateAt(ctx, bucket, object, ts)
	if err != nil {
		return fail(err)
	}
	if candidate == nil {
		pin.Release()
		return nil, nil, 0, 0, nil, nil
	}
	commitTS, err := s.nextTxnCommitTS(ctx, ts)
	if err != nil {
		return fail(err)
	}
	intent := s3MigrationIntent{
		Phase: s3MigrationStaging, AttemptVersion: commitTS,
		ManifestSHA256: sha256.Sum256(candidate.raw), UploadID: candidate.manifest.UploadID, LegacyParts: candidate.parts,
	}
	encoded, err := encodeS3MigrationIntent(intent)
	if err != nil {
		return fail(err)
	}
	headKey := s3keys.ObjectManifestKey(bucket, candidate.generation, object)
	markerKey := s3keys.MigrationIntentKey(bucket, candidate.generation, object)
	err = s.dispatchS3MetadataMutation(read.WithDispatchVoucher(ctx), &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: ts, CommitTS: commitTS,
		ReadKeys: [][]byte{headKey, s3keys.BucketMetaKey(bucket), s3keys.BucketGenerationKey(bucket), markerKey},
		Elems:    []*kv.Elem[kv.OP]{{Op: kv.Put, Key: markerKey, Value: encoded}},
	})
	if err != nil {
		return fail(err)
	}
	return &intent, candidate.manifest, candidate.generation, ts, pin, nil
}

func (s *S3Server) loadS3MigrationCandidateAt(ctx context.Context, bucket, object string, ts uint64) (*s3MigrationCandidate, error) {
	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, ts)
	if err != nil || !exists || meta == nil {
		return nil, err
	}
	markerKey := s3keys.MigrationIntentKey(bucket, meta.Generation, object)
	if err := s.ensureS3MigrationIntentAbsentAt(ctx, markerKey, ts); err != nil {
		return nil, err
	}
	manifest, raw, err := s.loadS3MigrationManifestAt(ctx, bucket, meta.Generation, object, ts)
	if err != nil || manifest == nil {
		return nil, err
	}
	parts, err := s3LegacyMigrationParts(manifest)
	if err != nil || len(parts) == 0 {
		return nil, err
	}
	if err := validateS3MigrationCandidate(manifest, parts); err != nil {
		return nil, err
	}
	return &s3MigrationCandidate{manifest: manifest, raw: raw, generation: meta.Generation, parts: parts}, nil
}

func (s *S3Server) ensureS3MigrationIntentAbsentAt(ctx context.Context, markerKey []byte, ts uint64) error {
	existing, err := s.loadS3MigrationIntentAt(ctx, markerKey, ts)
	if err != nil {
		return err
	}
	if existing != nil {
		return errors.New("s3 migration intent already exists; recover it before retrying")
	}
	return nil
}

func validateS3MigrationCandidate(manifest *s3ObjectManifest, parts []s3MigrationPart) error {
	if manifest.UploadID == "" || len(parts) > s3MaxPartsPerUpload {
		return errors.New("s3 migration manifest has invalid upload or part count")
	}
	return validateS3MigrationParts(parts)
}

func (s *S3Server) loadS3MigrationManifestAt(ctx context.Context, bucket string, generation uint64, object string, ts uint64) (*s3ObjectManifest, []byte, error) {
	raw, err := s.store.GetAt(ctx, s3keys.ObjectManifestKey(bucket, generation, object), ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	manifest, err := decodeS3ObjectManifest(raw)
	if err != nil {
		return nil, nil, err
	}
	if manifest == nil {
		return nil, nil, errors.New("s3 migration manifest is empty")
	}
	return manifest, raw, nil
}

func s3LegacyMigrationParts(manifest *s3ObjectManifest) ([]s3MigrationPart, error) {
	parts := make([]s3MigrationPart, 0, len(manifest.Parts))
	for _, part := range manifest.Parts {
		if part.Offloaded {
			continue
		}
		if part.ChunkCount != uint64(len(part.ChunkSizes)) {
			return nil, errors.New("s3 migration legacy part has inconsistent chunk count")
		}
		if part.ChunkCount != 0 {
			parts = append(parts, s3MigrationPart{PartNo: part.PartNo, PartVersion: part.PartVersion, ChunkCount: part.ChunkCount})
		}
	}
	return parts, nil
}

func (s *S3Server) stageS3LegacyChunk(ctx context.Context, bucket string, generation uint64, object, uploadID string, part s3ObjectPart, chunkNo, size, sourceTS uint64, intent s3MigrationIntent) error {
	refKey := s3keys.VersionedChunkRefKey(bucket, generation, object, uploadID, part.PartNo, chunkNo, intent.AttemptVersion)
	legacyKey := s3keys.VersionedBlobKey(bucket, generation, object, uploadID, part.PartNo, chunkNo, part.PartVersion)
	value, err := s.prepareS3MigrationChunkRef(ctx, legacyKey, refKey, size, sourceTS)
	if err != nil {
		return err
	}
	read, err := s.beginTxnReadTimestamp(ctx, s.readTS(), "s3 migration: stage ref")
	if err != nil {
		return err
	}
	ts := read.Timestamp()
	pin := s.pinReadTS(ts)
	defer pin.Release()
	markerKey := s3keys.MigrationIntentKey(bucket, generation, object)
	current, err := s.loadS3MigrationIntentAt(ctx, markerKey, ts)
	if err != nil || current == nil || current.Phase != s3MigrationStaging || current.AttemptVersion != intent.AttemptVersion {
		return errors.New("s3 migration stage lost its intent fence")
	}
	commitTS, err := s.nextTxnCommitTS(ctx, ts)
	if err != nil {
		return err
	}
	return s.dispatchS3MetadataMutation(read.WithDispatchVoucher(ctx), &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: ts, CommitTS: commitTS,
		ReadKeys: [][]byte{markerKey, s3keys.ObjectManifestKey(bucket, generation, object), s3keys.BucketGenerationKey(bucket)},
		Elems:    []*kv.Elem[kv.OP]{{Op: kv.Put, Key: refKey, Value: value}},
	})
}

func (s *S3Server) prepareS3MigrationChunkRef(ctx context.Context, legacyKey, refKey []byte, size, sourceTS uint64) ([]byte, error) {
	payload, err := s.store.GetAt(ctx, legacyKey, sourceTS)
	if err != nil {
		return nil, errors.Wrap(err, "read legacy blob for migration")
	}
	if uint64(len(payload)) != size || len(payload) > s3ChunkSize {
		return nil, errors.New("s3 migration legacy chunk size mismatch")
	}
	digest := sha256.Sum256(payload)
	blobTS, err := s.nextTxnCommitTS(ctx, 0)
	if err != nil {
		return nil, err
	}
	ref, err := s.persistS3ChunkBlob(ctx, refKey, digest, payload, blobTS)
	if err != nil {
		return nil, err
	}
	value, err := s3keys.EncodeChunkRefValue(ref)
	return value, errors.WithStack(err)
}

func (s *S3Server) flipS3LegacyManifest(ctx context.Context, bucket string, generation uint64, object string, intent s3MigrationIntent) error {
	if err := s.ensureS3MigrationReady(ctx); err != nil {
		return err
	}
	read, err := s.beginTxnReadTimestamp(ctx, s.readTS(), "s3 migration: flip")
	if err != nil {
		return err
	}
	ts := read.Timestamp()
	pin := s.pinReadTS(ts)
	defer pin.Release()
	markerKey := s3keys.MigrationIntentKey(bucket, generation, object)
	current, err := s.loadS3MigrationIntentAt(ctx, markerKey, ts)
	if err != nil || current == nil || current.Phase != s3MigrationStaging || current.AttemptVersion != intent.AttemptVersion {
		return errors.New("s3 migration flip lost its intent fence")
	}
	encodedManifest, err := s.buildS3MigrationManifestAt(ctx, bucket, generation, object, intent, ts)
	if err != nil {
		return err
	}
	intent.Phase = s3MigrationFlipped
	encodedIntent, err := encodeS3MigrationIntent(intent)
	if err != nil {
		return err
	}
	commitTS, err := s.nextTxnCommitTS(ctx, ts)
	if err != nil {
		return err
	}
	headKey := s3keys.ObjectManifestKey(bucket, generation, object)
	return s.dispatchS3MetadataMutation(read.WithDispatchVoucher(ctx), &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: ts, CommitTS: commitTS,
		ReadKeys: [][]byte{markerKey, headKey, s3keys.BucketMetaKey(bucket), s3keys.BucketGenerationKey(bucket)},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: headKey, Value: encodedManifest},
			{Op: kv.Put, Key: markerKey, Value: encodedIntent},
		},
	})
}

func (s *S3Server) ensureS3MigrationReady(ctx context.Context) error {
	if !s.isVerifiedS3Leader(ctx) {
		return errors.New("s3 migration requires a verified leader")
	}
	if s.s3BlobOffloadDecision(ctx).mode != s3BlobOffloadModeOffload {
		return errors.New("s3 migration requires the ready M3 offload path")
	}
	return nil
}

func (s *S3Server) buildS3MigrationManifestAt(ctx context.Context, bucket string, generation uint64, object string, intent s3MigrationIntent, ts uint64) ([]byte, error) {
	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, ts)
	if err != nil || !exists || meta == nil || meta.Generation != generation {
		return nil, errors.New("s3 migration bucket generation changed")
	}
	headKey := s3keys.ObjectManifestKey(bucket, generation, object)
	raw, err := s.store.GetAt(ctx, headKey, ts)
	if err != nil || sha256.Sum256(raw) != intent.ManifestSHA256 {
		return nil, errors.New("s3 migration manifest changed before flip")
	}
	manifest, err := decodeS3ObjectManifest(raw)
	if err != nil {
		return nil, err
	}
	for i := range manifest.Parts {
		if !manifest.Parts[i].Offloaded {
			manifest.Parts[i].Offloaded = true
			manifest.Parts[i].ChunkRefVersion = intent.AttemptVersion
		}
	}
	return encodeS3ObjectManifest(manifest)
}

func (s *S3Server) loadS3MigrationIntentAt(ctx context.Context, key []byte, ts uint64) (*s3MigrationIntent, error) {
	raw, err := s.store.GetAt(ctx, key, ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	intent, err := decodeS3MigrationIntent(raw)
	return &intent, err
}

func (s *S3Server) abortS3MigrationIntent(ctx context.Context, key []byte, intent s3MigrationIntent) error {
	read, err := s.beginTxnReadTimestamp(ctx, s.readTS(), "s3 migration: abort")
	if err != nil {
		return err
	}
	ts := read.Timestamp()
	pin := s.pinReadTS(ts)
	defer pin.Release()
	current, err := s.loadS3MigrationIntentAt(ctx, key, ts)
	if err != nil || current == nil || current.Phase != s3MigrationStaging || current.AttemptVersion != intent.AttemptVersion {
		return errors.New("s3 migration abort lost its intent fence")
	}
	intent.Phase = s3MigrationAborting
	value, err := encodeS3MigrationIntent(intent)
	if err != nil {
		return err
	}
	commitTS, err := s.nextTxnCommitTS(ctx, ts)
	if err != nil {
		return err
	}
	return s.dispatchS3MetadataMutation(read.WithDispatchVoucher(ctx), &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: ts, CommitTS: commitTS, ReadKeys: [][]byte{key},
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: value}},
	})
}

func (s *S3Server) recoverS3MigrationIntent(ctx context.Context, key []byte) error {
	bucket, generation, object, ok := s3keys.ParseMigrationIntentKey(key)
	if !ok {
		return errors.New("s3 migration marker key is malformed")
	}
	read, err := s.beginTxnReadTimestamp(ctx, s.readTS(), "s3 migration: recover")
	if err != nil {
		return err
	}
	intent, err := s.loadS3MigrationIntentAt(ctx, key, read.Timestamp())
	if err != nil || intent == nil {
		return err
	}
	if intent.Phase == s3MigrationStaging {
		if !s.isVerifiedS3Leader(ctx) {
			return nil
		}
		if _, active := s.activeMigrations.Load(s3MigrationActiveKey{bucket: bucket, object: object}); active {
			return nil
		}
		if err := s.abortS3MigrationIntent(ctx, key, *intent); err != nil {
			return err
		}
		intent.Phase = s3MigrationAborting
	}
	if err := s.cleanupS3MigrationParts(ctx, key, bucket, generation, object, *intent); err != nil {
		return err
	}
	return s.deleteS3MigrationIntent(ctx, key, intent.Phase, intent.AttemptVersion)
}

func (s *S3Server) cleanupS3MigrationParts(ctx context.Context, markerKey []byte, bucket string, generation uint64, object string, intent s3MigrationIntent) error {
	var pending []*kv.Elem[kv.OP]
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		err := s.dispatchS3MigrationCleanupBatch(ctx, markerKey, intent.Phase, intent.AttemptVersion, pending)
		pending = nil
		return err
	}
	for _, part := range intent.LegacyParts {
		for chunkNo := uint64(0); chunkNo < part.ChunkCount; chunkNo++ {
			chunkKey := s3MigrationCleanupKey(bucket, generation, object, intent, part, chunkNo)
			pending = append(pending, &kv.Elem[kv.OP]{Op: kv.Del, Key: chunkKey})
			if len(pending) == s3MetaBatchOps {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

func s3MigrationCleanupKey(bucket string, generation uint64, object string, intent s3MigrationIntent, part s3MigrationPart, chunkNo uint64) []byte {
	if intent.Phase == s3MigrationFlipped {
		return s3keys.VersionedBlobKey(bucket, generation, object, intent.UploadID, part.PartNo, chunkNo, part.PartVersion)
	}
	return s3keys.VersionedChunkRefKey(bucket, generation, object, intent.UploadID, part.PartNo, chunkNo, intent.AttemptVersion)
}

func (s *S3Server) dispatchS3MigrationCleanupBatch(ctx context.Context, markerKey []byte, phase string, version uint64, elems []*kv.Elem[kv.OP]) error {
	return s.retryS3Mutation(ctx, func() error {
		read, err := s.beginTxnReadTimestamp(ctx, s.readTS(), "s3 migration: cleanup batch")
		if err != nil {
			return errors.WithStack(err)
		}
		ts := read.Timestamp()
		pin := s.pinReadTS(ts)
		defer pin.Release()
		intent, err := s.loadS3MigrationIntentAt(ctx, markerKey, ts)
		if err != nil || intent == nil || intent.Phase != phase || intent.AttemptVersion != version {
			return errors.New("s3 migration cleanup lost its intent fence")
		}
		commitTS, err := s.nextTxnCommitTS(ctx, ts)
		if err != nil {
			return err
		}
		return s.dispatchS3MetadataMutation(read.WithDispatchVoucher(ctx), &kv.OperationGroup[kv.OP]{
			IsTxn: true, StartTS: ts, CommitTS: commitTS, ReadKeys: [][]byte{markerKey},
			Elems: append([]*kv.Elem[kv.OP](nil), elems...),
		})
	})
}

func (s *S3Server) deleteS3MigrationIntent(ctx context.Context, key []byte, phase string, version uint64) error {
	read, err := s.beginTxnReadTimestamp(ctx, s.readTS(), "s3 migration: finish")
	if err != nil {
		return err
	}
	ts := read.Timestamp()
	pin := s.pinReadTS(ts)
	defer pin.Release()
	current, err := s.loadS3MigrationIntentAt(ctx, key, ts)
	if err != nil || current == nil || current.Phase != phase || current.AttemptVersion != version {
		return errors.New("s3 migration cleanup lost its intent fence")
	}
	commitTS, err := s.nextTxnCommitTS(ctx, ts)
	if err != nil {
		return err
	}
	return s.dispatchS3MetadataMutation(read.WithDispatchVoucher(ctx), &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: ts, CommitTS: commitTS, ReadKeys: [][]byte{key},
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Del, Key: key}},
	})
}

// recoverPendingS3Migrations scans a bounded page at a time. It is called by
// startup recovery even when new migration admission is disabled.
func (s *S3Server) recoverPendingS3Migrations(ctx context.Context) error {
	start := []byte(s3keys.MigrationIntentPrefix)
	end := prefixScanEnd(start)
	for {
		read, err := s.beginTxnReadTimestamp(ctx, s.readTS(), "s3 migration: scan")
		if err != nil {
			return err
		}
		pin := s.pinReadTS(read.Timestamp())
		rows, err := s.store.ScanKeysAt(ctx, start, end, s3MetaBatchOps, read.Timestamp())
		pin.Release()
		if err != nil {
			return errors.WithStack(err)
		}
		if len(rows) == 0 {
			return nil
		}
		for _, key := range rows {
			if err := s.recoverS3MigrationIntent(ctx, key); err != nil {
				slog.WarnContext(ctx, "s3 migration recovery incomplete", "key", key, "err", err)
				return err
			}
		}
		start = nextScanCursor(rows[len(rows)-1])
	}
}

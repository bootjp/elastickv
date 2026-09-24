package snapshotoffload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
)

type PublishOptions struct {
	Store         PublishStore
	DataDir       string
	Prefix        string
	GroupID       uint64
	SourceCluster string
	BinaryVersion string
	CreatedAt     time.Time
	SpoolDir      string
	// VerifyLeader, when set, is re-checked immediately before the manifest is
	// committed. §4 requires leadership to hold at that instant, not merely when
	// the snapshot was opened: spooling a multi-gigabyte payload takes long
	// enough to lose an election. Failing here can leave an unreferenced
	// content-addressed payload, which GC reclaims, but never a committed
	// manifest naming a snapshot this node no longer had the right to publish.
	VerifyLeader func(context.Context) error
	// SkipIfNotNewerThan suppresses the publish when the persisted
	// snapshot's index is not greater than this value. Zero disables
	// the check.
	//
	// The comparison happens after the export is opened but BEFORE
	// the payload is spooled, which is the whole point: a scheduler
	// that ticks every 15 minutes over an unchanged snapshot would
	// otherwise re-read, re-hash and re-fsync a multi-gigabyte
	// payload every tick just to discover the object store already
	// has it.
	SkipIfNotNewerThan uint64
}

func PublishPersistedSnapshot(ctx context.Context, opts PublishOptions) (result *Manifest, retErr error) {
	if err := validatePublishOptions(opts); err != nil {
		return nil, err
	}
	export, err := openPublishExport(opts.DataDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = export.Close() }()

	metadata := export.Metadata()
	if err := rejectSnapshotNotNewer(metadata.Index, opts.SkipIfNotNewerThan); err != nil {
		return nil, err
	}
	payloadFile, payloadSHA, payloadBytes, err := spoolExport(ctx, export, publishSpoolDir(opts))
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = payloadFile.Close()
		_ = os.Remove(payloadFile.Name())
	}()
	if payloadBytes != metadata.PayloadBytes {
		return nil, errors.Wrapf(ErrIntegrity, "exported %d bytes, metadata expected %d", payloadBytes, metadata.PayloadBytes)
	}
	payloadObjectKey, err := payloadKey(opts.Prefix, payloadSHA)
	if err != nil {
		return nil, err
	}
	payloadClaim, err := acquireObjectClaimWaiting(ctx, opts.Store, payloadObjectKey)
	if err != nil {
		return nil, errors.Wrap(err, "claim snapshot payload")
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, releaseObjectClaim(ctx, payloadClaim))
	}()
	if err := putPayload(ctx, opts.Store, payloadObjectKey, payloadFile, payloadBytes, payloadSHA); err != nil {
		return nil, err
	}
	return commitManifest(ctx, opts, metadata, payloadObjectKey, payloadSHA)
}

func rejectSnapshotNotNewer(index uint64, threshold uint64) error {
	if threshold > 0 && index <= threshold {
		return errors.Wrapf(ErrSnapshotNotNewer,
			"persisted snapshot index %d is not newer than %d", index, threshold)
	}
	return nil
}

// commitManifest builds, validates and commits the manifest once the payload is
// durable. Split out of PublishPersistedSnapshot to keep that function inside
// the cyclop budget after the leadership re-check landed.
func commitManifest(
	ctx context.Context,
	opts PublishOptions,
	metadata etcdraftengine.PersistedSnapshotExportMetadata,
	payloadObjectKey string,
	payloadSHA string,
) (*Manifest, error) {
	manifest, err := buildManifest(opts, metadata, payloadObjectKey, payloadSHA)
	if err != nil {
		return nil, err
	}
	if err := validateManifest(*manifest); err != nil {
		return nil, err
	}
	if err := putManifest(ctx, opts.Store, manifest, opts.CreatedAt.IsZero(), opts.VerifyLeader); err != nil {
		return nil, err
	}
	return manifest, nil
}

func openPublishExport(dataDir string) (*etcdraftengine.PersistedSnapshotExport, error) {
	export, ok, err := etcdraftengine.OpenPersistedSnapshotExport(dataDir)
	if err != nil {
		return nil, errors.Wrap(err, "open persisted snapshot export")
	}
	if !ok {
		return nil, errors.WithStack(ErrNoPersistedSnapshot)
	}
	return export, nil
}

func buildManifest(
	opts PublishOptions,
	metadata etcdraftengine.PersistedSnapshotExportMetadata,
	payloadObjectKey string,
	payloadSHA string,
) (*Manifest, error) {
	manifestObjectKey, err := manifestKey(opts.Prefix, opts.GroupID, metadata.Index, metadata.Term)
	if err != nil {
		return nil, err
	}
	createdAt := opts.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	return &Manifest{
		SchemaVersion: ManifestSchemaVersion,
		CreatedAt:     createdAt.UTC(),
		SourceCluster: stringsTrim(opts.SourceCluster),
		GroupID:       opts.GroupID,
		SnapshotIndex: metadata.Index,
		SnapshotTerm:  metadata.Term,
		ConfState:     manifestConfState(metadata.ConfState),
		Payload: PayloadDescriptor{
			Key:          payloadObjectKey,
			Bytes:        metadata.PayloadBytes,
			SHA256:       payloadSHA,
			SourceCRC32C: metadata.CRC32C,
		},
		BinaryVersion: stringsTrim(opts.BinaryVersion),
		ManifestKey:   manifestObjectKey,
	}, nil
}

func putManifest(
	ctx context.Context,
	store PublishStore,
	manifest *Manifest,
	reuseExistingCreatedAt bool,
	verifyLeader func(context.Context) error,
) (retErr error) {
	claim, err := acquireObjectClaimWaiting(ctx, store, manifest.ManifestKey)
	if err != nil {
		return errors.Wrap(err, "claim snapshot manifest")
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, releaseObjectClaim(ctx, claim))
	}()

	data, manifestSHA, err := manifest.MarshalCanonical()
	if err != nil {
		return err
	}
	size := int64(len(data))
	objectSHA := hexSHA256Bytes(data)
	if exists, err := verifyExistingManifest(ctx, store, manifest, reuseExistingCreatedAt); err != nil {
		return err
	} else if exists {
		if err := verifyPublishLeadership(ctx, verifyLeader, "manifest reuse"); err != nil {
			return err
		}
		return refreshExistingManifest(ctx, store, manifest)
	}
	// §4: leadership must hold at the instant the manifest is created,
	// not merely before the absence probe above. That probe is a remote
	// read whose latency is unbounded by anything the caller controls,
	// so checking before it leaves a window in which a demoted node
	// still commits a manifest — precisely the guarantee this
	// scheduler exists to provide.
	if err := verifyPublishLeadership(ctx, verifyLeader, "manifest commit"); err != nil {
		return err
	}
	if err := createManifestObject(ctx, store, manifest, data, size, objectSHA, reuseExistingCreatedAt); err != nil {
		return err
	}
	manifest.ManifestSHA256 = manifestSHA
	return verifyCommittedManifest(ctx, store, manifest, reuseExistingCreatedAt)
}

func verifyPublishLeadership(ctx context.Context, verify func(context.Context) error, operation string) error {
	if verify == nil {
		return nil
	}
	if err := verify(ctx); err != nil {
		return errors.Wrapf(err, "snapshot offload: leadership lost before %s", operation)
	}
	return nil
}

// refreshExistingManifest changes the storage-visible state of a reused
// manifest while preserving its canonical bytes. The manifest is small, and
// the refresh makes a retention scan taken before this publish skip the object
// after it acquires the shared claim and revalidates the listed state.
func refreshExistingManifest(ctx context.Context, store ObjectStore, manifest *Manifest) error {
	refresher, ok := store.(ObjectRefresher)
	if !ok {
		return errors.Wrapf(ErrInvalidOptions,
			"object store cannot refresh reused manifest %s", manifest.ManifestKey)
	}
	// CreatedAt is part of schema v1's canonical self-hash. Advancing the
	// stored value produces a distinct object version without adding a field
	// that older restore binaries would omit when recomputing that hash.
	manifest.CreatedAt = manifest.CreatedAt.Add(time.Nanosecond)
	data, manifestSHA, err := manifest.MarshalCanonical()
	if err != nil {
		return err
	}
	opts := PutOptions{
		Size:        int64(len(data)),
		SHA256:      hexSHA256Bytes(data),
		ContentType: "application/json",
	}
	info, err := refresher.RefreshObject(ctx, manifest.ManifestKey, bytes.NewReader(data), opts)
	if err != nil {
		return errors.Wrap(err, "refresh existing snapshot manifest")
	}
	if info.Size != opts.Size || (info.SHA256 != "" && info.SHA256 != opts.SHA256) {
		return errors.Wrapf(ErrIntegrity,
			"manifest object %s remote integrity mismatch after refresh", manifest.ManifestKey)
	}
	manifest.ManifestSHA256 = manifestSHA
	return nil
}

func createManifestObject(
	ctx context.Context,
	store ObjectStore,
	manifest *Manifest,
	data []byte,
	size int64,
	objectSHA string,
	reuseExistingCreatedAt bool,
) error {
	info, err := store.PutObject(ctx, manifest.ManifestKey, bytes.NewReader(data), PutOptions{
		Size:        size,
		SHA256:      objectSHA,
		ContentType: "application/json",
	})
	if err != nil {
		return handleManifestPutError(ctx, store, manifest, reuseExistingCreatedAt, err)
	}
	if info.Size != size || (info.SHA256 != "" && info.SHA256 != objectSHA) {
		return errors.Wrapf(ErrIntegrity, "manifest object %s remote integrity mismatch", manifest.ManifestKey)
	}
	return nil
}

func handleManifestPutError(
	ctx context.Context,
	store ObjectStore,
	manifest *Manifest,
	reuseExistingCreatedAt bool,
	err error,
) error {
	if !errors.Is(err, ErrIntegrity) {
		return errors.Wrap(err, "put snapshot manifest")
	}
	if exists, verifyErr := verifyExistingManifest(ctx, store, manifest, reuseExistingCreatedAt); verifyErr != nil {
		return errors.Wrap(verifyErr, "verify conflicting snapshot manifest")
	} else if exists {
		return refreshExistingManifest(ctx, store, manifest)
	}
	return errors.Wrap(err, "put snapshot manifest")
}

func verifyCommittedManifest(
	ctx context.Context,
	store ObjectStore,
	manifest *Manifest,
	reuseExistingCreatedAt bool,
) error {
	if exists, err := verifyExistingManifest(ctx, store, manifest, reuseExistingCreatedAt); err != nil {
		return errors.Wrap(err, "verify committed snapshot manifest")
	} else if !exists {
		return errors.Wrapf(ErrIntegrity, "manifest object %s missing after put", manifest.ManifestKey)
	}
	return nil
}

func verifyExistingManifest(
	ctx context.Context,
	store ObjectStore,
	manifest *Manifest,
	reuseExistingCreatedAt bool,
) (bool, error) {
	_, ok, err := store.HeadObject(ctx, manifest.ManifestKey)
	if err != nil {
		return false, errors.Wrap(err, "head existing snapshot manifest")
	}
	if !ok {
		return false, nil
	}
	existing, err := LoadManifest(ctx, store, manifest.ManifestKey)
	if err != nil {
		return true, errors.Wrapf(ErrIntegrity,
			"load existing snapshot manifest %s: %v", manifest.ManifestKey, err)
	}
	if !manifestMatchesCandidate(existing, *manifest, reuseExistingCreatedAt) {
		return true, errors.Wrapf(ErrIntegrity, "manifest object %s already exists with different content", manifest.ManifestKey)
	}
	*manifest = existing
	return true, nil
}

func manifestMatchesCandidate(existing Manifest, candidate Manifest, reuseExistingCreatedAt bool) bool {
	if reuseExistingCreatedAt {
		return sameManifestExceptCreation(existing, candidate)
	}
	originalCreatedAt := candidate.CreatedAt
	candidate.ManifestSHA256 = existing.ManifestSHA256
	if reflect.DeepEqual(existing, candidate) {
		return true
	}
	// Refreshing a reused schema-v1 manifest advances CreatedAt by one
	// nanosecond so the canonical bytes and self-hash form a new generation
	// without adding a field older restore binaries cannot verify. A later
	// retry with the caller's original explicit timestamp must still match
	// that refreshed object. Only accept a forward-advanced stored timestamp;
	// a candidate attempting to replace the audit timestamp with a newer value
	// remains a conflict.
	return existing.CreatedAt.After(originalCreatedAt) && sameManifestExceptCreation(existing, candidate)
}

// sameManifestExceptCreation compares a retry's candidate against the
// committed manifest, ignoring the fields that legitimately differ
// between two publishes of the SAME snapshot.
//
// BinaryVersion is one of them. It records which binary published the
// artifact, not anything about the snapshot itself, so after an
// upgrade a process that republishes an index it has not yet published
// locally would otherwise conflict with the manifest the previous
// binary committed — and keep failing every scan until Raft happens to
// produce a new snapshot. The committed manifest keeps the original
// publisher's version, which is the correct audit record for the
// bytes that actually exist.
func sameManifestExceptCreation(existing Manifest, candidate Manifest) bool {
	candidate.CreatedAt = existing.CreatedAt
	candidate.ManifestSHA256 = existing.ManifestSHA256
	candidate.BinaryVersion = existing.BinaryVersion
	return reflect.DeepEqual(existing, candidate)
}

func validatePublishOptions(opts PublishOptions) error {
	switch {
	case opts.Store == nil:
		return errors.Wrap(ErrInvalidOptions, "object store is required")
	case stringsTrim(opts.DataDir) == "":
		return errors.Wrap(ErrInvalidOptions, "data dir is required")
	case opts.GroupID == 0 && stringsTrim(opts.SourceCluster) == "":
		return errors.Wrap(ErrInvalidOptions, "source cluster is required for group 0 manifests")
	default:
		return nil
	}
}

func publishSpoolDir(opts PublishOptions) string {
	if stringsTrim(opts.SpoolDir) != "" {
		return filepath.Clean(opts.SpoolDir)
	}
	return filepath.Join(filepath.Dir(filepath.Clean(opts.DataDir)), ".snapshot-offload-spool")
}

func spoolExport(ctx context.Context, export *etcdraftengine.PersistedSnapshotExport, spoolDir string) (*os.File, string, int64, error) {
	if err := os.MkdirAll(spoolDir, localStoreDirPerm); err != nil {
		return nil, "", 0, errors.WithStack(err)
	}
	tmp, err := os.CreateTemp(spoolDir, "elastickv-snapshot-offload-*.fsm")
	if err != nil {
		return nil, "", 0, errors.WithStack(err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	hash := sha256.New()
	n, err := export.WriteTo(contextWriter{
		ctx:    ctx,
		writer: io.MultiWriter(tmp, hash),
	})
	if err != nil {
		return nil, "", n, errors.Wrap(err, "spool persisted snapshot export")
	}
	if err := ctx.Err(); err != nil {
		return nil, "", n, errors.WithStack(err)
	}
	if err := tmp.Sync(); err != nil {
		return nil, "", n, errors.WithStack(err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, "", n, errors.WithStack(err)
	}
	keep = true
	return tmp, hex.EncodeToString(hash.Sum(nil)), n, nil
}

func putPayload(ctx context.Context, store ObjectStore, key string, file *os.File, size int64, sha string) error {
	opts := PutOptions{
		Size:        size,
		SHA256:      sha,
		ContentType: "application/octet-stream",
	}
	exists, err := verifyExistingStoreObject(ctx, store, key, size, sha)
	if err != nil {
		return errors.Wrap(err, "verify existing snapshot payload")
	}
	if exists {
		// The caller holds the payload's storage-visible claim until the
		// manifest commits. Rewriting a multi-terabyte content-addressed
		// payload merely to move its mtime would defeat deduplication; the
		// shared claim is the lightweight coordination primitive instead.
		return nil
	}
	if err := seekPayloadFile(file); err != nil {
		return err
	}
	info, err := store.PutObject(ctx, key, file, opts)
	if err != nil {
		return errors.Wrap(err, "put snapshot payload")
	}
	return validatePayloadObjectInfo(key, info, opts)
}

func seekPayloadFile(file *os.File) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func validatePayloadObjectInfo(key string, info ObjectInfo, opts PutOptions) error {
	if info.Size != opts.Size || (info.SHA256 != "" && info.SHA256 != opts.SHA256) {
		return errors.Wrapf(ErrIntegrity, "payload object %s remote integrity mismatch", key)
	}
	return nil
}

func verifyExistingStoreObject(ctx context.Context, store ObjectStore, key string, size int64, sha string) (bool, error) {
	info, ok, err := store.HeadObject(ctx, key)
	if err != nil {
		return false, errors.Wrap(err, "head existing object")
	}
	if !ok {
		return false, nil
	}
	if info.Size != size {
		return true, errors.Wrapf(ErrIntegrity, "object %s already exists with different size", key)
	}
	if info.SHA256 != "" {
		if info.SHA256 == sha {
			return true, nil
		}
		return true, errors.Wrapf(ErrIntegrity, "object %s already exists with different sha256", key)
	}
	gotSize, gotSHA, err := hashExistingStoreObject(ctx, store, key)
	if err != nil {
		return true, err
	}
	if gotSize == size && gotSHA == sha {
		return true, nil
	}
	return true, errors.Wrapf(ErrIntegrity, "object %s already exists with different content", key)
}

func hashExistingStoreObject(ctx context.Context, store ObjectStore, key string) (int64, string, error) {
	body, _, err := store.GetObject(ctx, key)
	if err != nil {
		return 0, "", errors.Wrap(err, "get existing object")
	}
	defer func() { _ = body.Close() }()
	sum := sha256.New()
	n, err := io.Copy(sum, contextReader{ctx: ctx, reader: body})
	if err != nil {
		return 0, "", errors.WithStack(err)
	}
	return n, hex.EncodeToString(sum.Sum(nil)), nil
}

func stringsTrim(v string) string {
	return strings.TrimSpace(v)
}

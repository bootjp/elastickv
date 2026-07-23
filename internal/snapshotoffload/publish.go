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
	Store         ObjectStore
	DataDir       string
	Prefix        string
	GroupID       uint64
	SourceCluster string
	BinaryVersion string
	CreatedAt     time.Time
	SpoolDir      string
}

func PublishPersistedSnapshot(ctx context.Context, opts PublishOptions) (*Manifest, error) {
	if err := validatePublishOptions(opts); err != nil {
		return nil, err
	}
	export, err := openPublishExport(opts.DataDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = export.Close() }()

	metadata := export.Metadata()
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
	if err := putPayload(ctx, opts.Store, payloadObjectKey, payloadFile, payloadBytes, payloadSHA); err != nil {
		return nil, err
	}
	manifest, err := buildManifest(opts, metadata, payloadObjectKey, payloadSHA)
	if err != nil {
		return nil, err
	}
	if err := validateManifest(*manifest); err != nil {
		return nil, err
	}
	if err := putManifest(ctx, opts.Store, manifest, opts.CreatedAt.IsZero()); err != nil {
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
		return nil, errors.Wrap(ErrObjectNotFound, "no persisted snapshot available")
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

func putManifest(ctx context.Context, store ObjectStore, manifest *Manifest, reuseExistingCreatedAt bool) error {
	data, manifestSHA, err := manifest.MarshalCanonical()
	if err != nil {
		return err
	}
	size := int64(len(data))
	objectSHA := hexSHA256Bytes(data)
	if exists, err := verifyExistingManifest(ctx, store, manifest, size, objectSHA, reuseExistingCreatedAt); err != nil {
		return err
	} else if exists {
		return nil
	}
	if err := createManifestObject(ctx, store, manifest, data, size, objectSHA, reuseExistingCreatedAt); err != nil {
		return err
	}
	manifest.ManifestSHA256 = manifestSHA
	return verifyCommittedManifest(ctx, store, manifest, size, objectSHA, reuseExistingCreatedAt)
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
		return handleManifestPutError(ctx, store, manifest, size, objectSHA, reuseExistingCreatedAt, err)
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
	size int64,
	objectSHA string,
	reuseExistingCreatedAt bool,
	err error,
) error {
	if !errors.Is(err, ErrIntegrity) {
		return errors.Wrap(err, "put snapshot manifest")
	}
	if exists, verifyErr := verifyExistingManifest(ctx, store, manifest, size, objectSHA, reuseExistingCreatedAt); verifyErr != nil {
		return errors.Wrap(verifyErr, "verify conflicting snapshot manifest")
	} else if exists {
		return nil
	}
	return errors.Wrap(err, "put snapshot manifest")
}

func verifyCommittedManifest(
	ctx context.Context,
	store ObjectStore,
	manifest *Manifest,
	size int64,
	objectSHA string,
	reuseExistingCreatedAt bool,
) error {
	if exists, err := verifyExistingManifest(ctx, store, manifest, size, objectSHA, reuseExistingCreatedAt); err != nil {
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
	size int64,
	sha string,
	reuseExistingCreatedAt bool,
) (bool, error) {
	info, ok, err := store.HeadObject(ctx, manifest.ManifestKey)
	if err != nil {
		return false, errors.Wrap(err, "head existing snapshot manifest")
	}
	if !ok {
		return false, nil
	}
	if info.Size != size && !reuseExistingCreatedAt {
		return true, errors.Wrapf(ErrIntegrity, "manifest object %s already exists with different size", manifest.ManifestKey)
	}
	if info.SHA256 != "" && info.SHA256 != sha && !reuseExistingCreatedAt {
		return true, errors.Wrapf(ErrIntegrity, "manifest object %s already exists with different sha256", manifest.ManifestKey)
	}
	existing, err := LoadManifest(ctx, store, manifest.ManifestKey)
	if err != nil {
		return true, errors.Wrap(err, "load existing snapshot manifest")
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
	candidate.ManifestSHA256 = existing.ManifestSHA256
	return reflect.DeepEqual(existing, candidate)
}

func sameManifestExceptCreation(existing Manifest, candidate Manifest) bool {
	candidate.CreatedAt = existing.CreatedAt
	candidate.ManifestSHA256 = existing.ManifestSHA256
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
	if exists, err := verifyExistingStoreObject(ctx, store, key, size, sha); err != nil {
		return errors.Wrap(err, "verify existing snapshot payload")
	} else if exists {
		return nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	info, err := store.PutObject(ctx, key, file, PutOptions{
		Size:        size,
		SHA256:      sha,
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return errors.Wrap(err, "put snapshot payload")
	}
	if info.Size != size || (info.SHA256 != "" && info.SHA256 != sha) {
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

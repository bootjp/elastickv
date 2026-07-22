package snapshotoffload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
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
	payloadFile, payloadSHA, payloadBytes, err := spoolExport(ctx, export)
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
	if err := putManifest(ctx, opts.Store, manifest); err != nil {
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

func putManifest(ctx context.Context, store ObjectStore, manifest *Manifest) error {
	data, manifestSHA, err := manifest.MarshalCanonical()
	if err != nil {
		return err
	}
	if _, err := store.PutObject(ctx, manifest.ManifestKey, bytes.NewReader(data), PutOptions{
		Size:        int64(len(data)),
		SHA256:      hexSHA256Bytes(data),
		ContentType: "application/json",
	}); err != nil {
		return errors.Wrap(err, "put snapshot manifest")
	}
	manifest.ManifestSHA256 = manifestSHA
	return nil
}

func validatePublishOptions(opts PublishOptions) error {
	switch {
	case opts.Store == nil:
		return errors.Wrap(ErrInvalidOptions, "object store is required")
	case stringsTrim(opts.DataDir) == "":
		return errors.Wrap(ErrInvalidOptions, "data dir is required")
	default:
		return nil
	}
}

func spoolExport(ctx context.Context, export *etcdraftengine.PersistedSnapshotExport) (*os.File, string, int64, error) {
	tmp, err := os.CreateTemp("", "elastickv-snapshot-offload-*.fsm")
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
	n, err := export.WriteTo(io.MultiWriter(tmp, hash))
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
	if info, ok, err := store.HeadObject(ctx, key); err != nil {
		return errors.Wrap(err, "head snapshot payload")
	} else if ok {
		if info.Size == size && info.SHA256 == sha {
			return nil
		}
		return errors.Wrapf(ErrIntegrity, "payload object %s already exists with different content", key)
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
	if info.Size != size || info.SHA256 != sha {
		return errors.Wrapf(ErrIntegrity, "payload object %s remote integrity mismatch", key)
	}
	return nil
}

func stringsTrim(v string) string {
	return strings.TrimSpace(v)
}

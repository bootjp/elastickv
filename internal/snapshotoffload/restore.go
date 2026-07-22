package snapshotoffload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
)

type RestoreOptions struct {
	Store       ObjectStore
	ManifestKey string
	Manifest    *Manifest
	DataDir     string
	Peers       []etcdraftengine.Peer
}

func LoadManifest(ctx context.Context, store ObjectStore, key string) (Manifest, error) {
	if store == nil {
		return Manifest{}, errors.Wrap(ErrInvalidOptions, "object store is required")
	}
	body, _, err := store.GetObject(ctx, key)
	if err != nil {
		return Manifest{}, errors.Wrap(err, "get snapshot manifest")
	}
	defer func() { _ = body.Close() }()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, contextReader{ctx: ctx, reader: body}); err != nil {
		return Manifest{}, errors.WithStack(err)
	}
	manifest, err := DecodeManifest(buf.Bytes())
	if err != nil {
		return Manifest{}, err
	}
	if normalizeObjectKey(key) != normalizeObjectKey(manifest.ManifestKey) {
		return Manifest{}, errors.Wrapf(ErrIntegrity, "manifest key mismatch: loaded %s, body says %s", key, manifest.ManifestKey)
	}
	return manifest, nil
}

func RestorePhysicalSnapshot(ctx context.Context, opts RestoreOptions) (*etcdraftengine.ExternalSnapshotRestoreResult, error) {
	if err := validateRestoreOptions(opts); err != nil {
		return nil, err
	}
	manifest, err := restoreManifest(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err := validateManifest(manifest); err != nil {
		return nil, err
	}
	downloadDir, err := os.MkdirTemp(filepath.Dir(filepath.Clean(opts.DataDir)), ".snapshot-offload-restore-*")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = os.RemoveAll(downloadDir) }()
	payloadPath := filepath.Join(downloadDir, "payload.fsm")
	if err := downloadVerifiedPayload(ctx, opts.Store, manifest, payloadPath); err != nil {
		return nil, err
	}
	result, err := etcdraftengine.PreparePhysicalSnapshotRestore(etcdraftengine.PhysicalSnapshotRestoreOptions{
		InputFSMPath:          payloadPath,
		DataDir:               opts.DataDir,
		Index:                 manifest.SnapshotIndex,
		Term:                  manifest.SnapshotTerm,
		Peers:                 opts.Peers,
		ExpectedPayloadSHA256: manifest.Payload.SHA256,
	})
	if err != nil {
		return nil, errors.Wrap(err, "prepare physical snapshot restore")
	}
	return result, nil
}

func validateRestoreOptions(opts RestoreOptions) error {
	switch {
	case opts.Store == nil:
		return errors.Wrap(ErrInvalidOptions, "object store is required")
	case opts.Manifest == nil && stringsTrim(opts.ManifestKey) == "":
		return errors.Wrap(ErrInvalidOptions, "manifest key is required")
	case stringsTrim(opts.DataDir) == "":
		return errors.Wrap(ErrInvalidOptions, "data dir is required")
	case len(opts.Peers) == 0:
		return errors.Wrap(ErrInvalidOptions, "restore peers are required")
	default:
		return nil
	}
}

func restoreManifest(ctx context.Context, opts RestoreOptions) (Manifest, error) {
	if opts.Manifest != nil {
		return *opts.Manifest, nil
	}
	return LoadManifest(ctx, opts.Store, opts.ManifestKey)
}

func downloadVerifiedPayload(ctx context.Context, store ObjectStore, manifest Manifest, finalPath string) error {
	body, info, err := store.GetObject(ctx, manifest.Payload.Key)
	if err != nil {
		return errors.Wrap(err, "get snapshot payload")
	}
	defer func() { _ = body.Close() }()
	if err := validatePayloadInfo(manifest, info); err != nil {
		return err
	}
	tmpPath, err := writeDownloadedPayloadTemp(ctx, filepath.Dir(finalPath), manifest, body)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return errors.WithStack(err)
	}
	return syncDir(filepath.Dir(finalPath))
}

func validatePayloadInfo(manifest Manifest, info ObjectInfo) error {
	if info.Size != manifest.Payload.Bytes || info.SHA256 != manifest.Payload.SHA256 {
		return errors.Wrapf(ErrIntegrity, "payload head mismatch for %s", manifest.Payload.Key)
	}
	return nil
}

func writeDownloadedPayloadTemp(ctx context.Context, dir string, manifest Manifest, body io.Reader) (string, error) {
	tmp, err := os.CreateTemp(dir, ".payload-*")
	if err != nil {
		return "", errors.WithStack(err)
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hash), contextReader{ctx: ctx, reader: body})
	if err != nil {
		return "", errors.WithStack(err)
	}
	gotSHA := hex.EncodeToString(hash.Sum(nil))
	if n != manifest.Payload.Bytes {
		return "", errors.Wrapf(ErrIntegrity, "payload %s downloaded %d bytes, expected %d", manifest.Payload.Key, n, manifest.Payload.Bytes)
	}
	if gotSHA != manifest.Payload.SHA256 {
		return "", errors.Wrapf(ErrIntegrity, "payload %s sha256 %s, expected %s", manifest.Payload.Key, gotSHA, manifest.Payload.SHA256)
	}
	if err := tmp.Sync(); err != nil {
		return "", errors.WithStack(err)
	}
	if err := tmp.Close(); err != nil {
		return "", errors.WithStack(err)
	}
	keep = true
	return tmpPath, nil
}

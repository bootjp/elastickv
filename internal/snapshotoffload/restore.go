package snapshotoffload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"time"

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

const (
	maxManifestBytes         = 1 << 20
	restoreTempDirMode       = 0o755
	restoreTempDirPattern    = ".snapshot-offload-restore-*"
	restoreTempDirStaleAfter = 7 * 24 * time.Hour
)

func LoadManifest(ctx context.Context, store ObjectStore, key string) (Manifest, error) {
	ctx = restoreContext(ctx)
	if store == nil {
		return Manifest{}, errors.Wrap(ErrInvalidOptions, "object store is required")
	}
	body, _, err := store.GetObject(ctx, key)
	if err != nil {
		return Manifest{}, errors.Wrap(err, "get snapshot manifest")
	}
	defer func() { _ = body.Close() }()
	data, err := readLimitedManifest(ctx, body)
	if err != nil {
		return Manifest{}, err
	}
	manifest, err := DecodeManifest(data)
	if err != nil {
		return Manifest{}, err
	}
	if normalizeObjectKey(key) != normalizeObjectKey(manifest.ManifestKey) {
		return Manifest{}, errors.Wrapf(ErrIntegrity, "manifest key mismatch: loaded %s, body says %s", key, manifest.ManifestKey)
	}
	return manifest, nil
}

func RestorePhysicalSnapshot(ctx context.Context, opts RestoreOptions) (*etcdraftengine.ExternalSnapshotRestoreResult, error) {
	ctx = restoreContext(ctx)
	if err := validateRestoreOptions(opts); err != nil {
		return nil, err
	}
	manifest, payloadPath, cleanup, err := prepareRestorePayload(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	result, err := etcdraftengine.PreparePhysicalSnapshotRestore(etcdraftengine.PhysicalSnapshotRestoreOptions{
		Context:               ctx,
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

func prepareRestorePayload(ctx context.Context, opts RestoreOptions) (Manifest, string, func(), error) {
	if err := checkRestoreContext(ctx); err != nil {
		return Manifest{}, "", nil, err
	}
	manifest, err := restoreManifest(ctx, opts)
	if err != nil {
		return Manifest{}, "", nil, err
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, "", nil, err
	}
	if err := checkRestorePreflight(ctx, opts.DataDir); err != nil {
		return Manifest{}, "", nil, err
	}
	downloadDir, err := prepareRestoreDownloadDir(opts.DataDir)
	if err != nil {
		return Manifest{}, "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(downloadDir) }
	payloadPath := filepath.Join(downloadDir, "payload.fsm")
	if err := downloadRestorePayload(ctx, opts.Store, manifest, payloadPath); err != nil {
		cleanup()
		return Manifest{}, "", nil, err
	}
	return manifest, payloadPath, cleanup, nil
}

func checkRestorePreflight(ctx context.Context, dataDir string) error {
	if err := checkRestoreContext(ctx); err != nil {
		return err
	}
	return ensureRestoreDestinationAbsent(dataDir)
}

func downloadRestorePayload(ctx context.Context, store ObjectStore, manifest Manifest, payloadPath string) error {
	if err := checkRestoreContext(ctx); err != nil {
		return err
	}
	if err := downloadVerifiedPayload(ctx, store, manifest, payloadPath); err != nil {
		return err
	}
	return checkRestoreContext(ctx)
}

func checkRestoreContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func restoreContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func readLimitedManifest(ctx context.Context, body io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	limited := io.LimitReader(contextReader{ctx: ctx, reader: body}, maxManifestBytes+1)
	if _, err := io.Copy(&buf, limited); err != nil {
		return nil, errors.WithStack(err)
	}
	if buf.Len() > maxManifestBytes {
		return nil, errors.Wrapf(ErrInvalidOptions, "manifest exceeds %d bytes", maxManifestBytes)
	}
	return buf.Bytes(), nil
}

func ensureRestoreDestinationAbsent(dataDir string) error {
	cleaned := filepath.Clean(dataDir)
	if _, err := os.Stat(cleaned); err == nil {
		return errors.Wrapf(etcdraftengine.ErrExternalSnapshotRestoreExists, "destination exists: %s", cleaned)
	} else if !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	return nil
}

func prepareRestoreDownloadDir(dataDir string) (string, error) {
	parent := filepath.Dir(filepath.Clean(dataDir))
	if err := os.MkdirAll(parent, restoreTempDirMode); err != nil {
		return "", errors.WithStack(err)
	}
	if err := cleanupRestoreDownloadDirs(parent); err != nil {
		return "", err
	}
	downloadDir, err := os.MkdirTemp(parent, restoreTempDirPattern)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return downloadDir, nil
}

func cleanupRestoreDownloadDirs(parent string) error {
	matches, err := filepath.Glob(filepath.Join(parent, restoreTempDirPattern))
	if err != nil {
		return errors.WithStack(err)
	}
	staleBefore := time.Now().Add(-restoreTempDirStaleAfter)
	for _, match := range matches {
		info, err := os.Lstat(match)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return errors.WithStack(err)
		}
		if !info.IsDir() {
			continue
		}
		if info.ModTime().After(staleBefore) {
			continue
		}
		if err := os.RemoveAll(match); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
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
		return validateRestorePeers(opts.Peers)
	}
}

func validateRestorePeers(peers []etcdraftengine.Peer) error {
	seenNodeIDs := make(map[uint64]struct{}, len(peers))
	seenIDs := make(map[string]struct{}, len(peers))
	voters := 0
	for i, peer := range peers {
		isVoter, err := validateRestorePeer(i, peer, seenNodeIDs, seenIDs)
		if err != nil {
			return err
		}
		if isVoter {
			voters++
		}
	}
	if voters == 0 {
		return errors.Wrap(ErrInvalidOptions, "restore peers require at least one voter")
	}
	return nil
}

func validateRestorePeer(
	i int,
	peer etcdraftengine.Peer,
	seenNodeIDs map[uint64]struct{},
	seenIDs map[string]struct{},
) (bool, error) {
	if err := validateRestorePeerShape(i, peer); err != nil {
		return false, err
	}
	if _, ok := seenNodeIDs[peer.NodeID]; ok {
		return false, errors.Wrapf(ErrInvalidOptions, "restore peer[%d] has duplicate node id %d", i, peer.NodeID)
	}
	seenNodeIDs[peer.NodeID] = struct{}{}
	peerID := restorePeerIdentity(peer)
	if _, ok := seenIDs[peerID]; ok {
		return false, errors.Wrapf(ErrInvalidOptions, "restore peer[%d] has duplicate id %q", i, peerID)
	}
	seenIDs[peerID] = struct{}{}
	return peer.Suffrage != etcdraftengine.SuffrageLearner, nil
}

func validateRestorePeerShape(i int, peer etcdraftengine.Peer) error {
	switch {
	case peer.NodeID == 0:
		return errors.Wrapf(ErrInvalidOptions, "restore peer[%d] has zero node id", i)
	case stringsTrim(peer.Address) == "":
		return errors.Wrapf(ErrInvalidOptions, "restore peer[%d] has empty address", i)
	case peer.Suffrage != "" &&
		peer.Suffrage != etcdraftengine.SuffrageVoter &&
		peer.Suffrage != etcdraftengine.SuffrageLearner:
		return errors.Wrapf(ErrInvalidOptions, "restore peer[%d] has invalid suffrage %q", i, peer.Suffrage)
	default:
		return nil
	}
}

func restorePeerIdentity(peer etcdraftengine.Peer) string {
	peerID := stringsTrim(peer.ID)
	if peerID != "" {
		return peerID
	}
	return stringsTrim(peer.Address)
}

func restoreManifest(ctx context.Context, opts RestoreOptions) (Manifest, error) {
	if opts.Manifest != nil {
		manifest := *opts.Manifest
		if err := validateManifest(manifest); err != nil {
			return Manifest{}, err
		}
		if err := verifyManifestSelfHash(manifest); err != nil {
			return Manifest{}, err
		}
		return manifest, nil
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
	if info.Size != manifest.Payload.Bytes {
		return errors.Wrapf(ErrIntegrity, "payload head mismatch for %s", manifest.Payload.Key)
	}
	if info.SHA256 != "" && info.SHA256 != manifest.Payload.SHA256 {
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

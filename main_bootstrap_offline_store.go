package main

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	bootstrapOfflineDataPath    = "fsm.db"
	bootstrapOfflineCacheSize   = 8 << 20
	bootstrapOfflineOpenFiles   = 64
	bootstrapOfflineTreeLimit   = 1 << 16
	bootstrapOfflineWALLimit    = 256 << 20
	bootstrapOfflineAliasChecks = 1 << 20
)

var errBootstrapOfflineStore = errors.New("offline data store verification failed")

// These helpers do not acquire a process fence or authenticate the native
// backup. Caller must retain offline ownership across native backup, callbacks,
// WAL migration and publication. They never stop a running server or create a DB.
func bootstrapOfflineDataStoreProbe(ctx context.Context, dataDir string) (etcd.BaselineStoreProof, error) {
	var result etcd.BaselineStoreProof
	err := bootstrapOfflineWithData(ctx, dataDir, true, func(db *pebble.DB) error {
		snapshot := db.NewSnapshot()
		proof, err := bootstrapOfflineSnapshotProof(ctx, snapshot)
		if err = errors.Join(err, snapshot.Close()); err != nil {
			return err
		}
		result = proof
		return nil
	})
	if err != nil {
		return etcd.BaselineStoreProof{}, err
	}
	return result, nil
}

func bootstrapOfflineSnapshotProof(ctx context.Context, snapshot *pebble.Snapshot) (etcd.BaselineStoreProof, error) {
	observation, err := store.ProbeBootstrapBaselineSnapshot(ctx, snapshot)
	if err != nil {
		return etcd.BaselineStoreProof{}, errors.WithStack(err)
	}
	if !observation.WatermarkPresent || observation.AppliedIndex == 0 {
		return etcd.BaselineStoreProof{}, errors.Wrap(errBootstrapOfflineStore, "durable watermark and applied index required")
	}
	// A missing ordered configuration stays absent. Only native complete-history
	// replay may establish it; never substitute the snapshot/data applied index.
	return etcd.BaselineStoreProof{AppliedIndex: observation.AppliedIndex,
		ConfigurationIndex: observation.ConfigurationIndex, Watermark: observation.Watermark,
		DataDigest: raftbootstrap.Digest(observation.DataDigest)}, nil
}

type bootstrapOfflineDataPlan struct {
	prepared kv.PreparedBootstrapActivation
	probe    []byte
	key      []byte
	value    []byte
}

// bootstrapOfflineDataPreparation freezes independently trusted preparation.
// Only StagePreparation opens RW, after verifying the complete preserved source
// and closing all RO handles. Restart accepts exactly original or exact prepared
// target images, never arbitrary partial edits. Backup/native ownership remains
// the orchestrator's responsibility; no caller-supplied bypass boolean exists.
func bootstrapOfflineDataPreparation(prepared kv.PreparedBootstrapActivation) (*etcd.BaselinePreparationCallbacks, error) {
	encoded, err := raftbootstrap.MarshalBaselineProbe(prepared.Probe)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	probe, err := raftbootstrap.ParseBaselineProbe(encoded)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	prepared.Probe = probe
	key, value, err := kv.PreparedBootstrapActivationMetadata(probe.Identity, prepared)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	physicalValue, err := store.EncodeOfflinePlainControlMetadata(value)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	plan := &bootstrapOfflineDataPlan{prepared: prepared, probe: encoded,
		key: append([]byte(store.RaftControlMetadataPrefix), key...), value: physicalValue}
	return &etcd.BaselinePreparationCallbacks{MutablePaths: []string{bootstrapOfflineDataPath},
		StagePreparation: plan.stage, VerifyPreparedStore: plan.verify}, nil
}

func (p *bootstrapOfflineDataPlan) validate(c etcd.BaselinePreparationContext) error {
	if err := raftbootstrap.VerifyBaselineManifest(c.Manifest, p.prepared.Probe.BaselineContext); err != nil {
		return errors.WithStack(err)
	}
	digest, err := raftbootstrap.BaselineManifestDigest(c.Manifest)
	if err != nil || digest != p.prepared.ManifestDigest {
		return errors.Wrap(errBootstrapOfflineStore, "manifest differs from trusted preparation")
	}
	for _, evidence := range c.Manifest.Members {
		if evidence.MemberID != p.prepared.Probe.Evidence.MemberID {
			continue
		}
		probe, err := raftbootstrap.MarshalBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: c.Manifest.BaselineContext, Evidence: evidence})
		if err != nil || !bytes.Equal(probe, p.probe) {
			return errors.Wrap(errBootstrapOfflineStore, "local evidence differs from trusted preparation")
		}
		return bootstrapOfflineDistinctStores(c.BackupDir, c.DataDir)
	}
	return errors.Wrap(errBootstrapOfflineStore, "local member absent from preparation")
}

func (p *bootstrapOfflineDataPlan) stage(ctx context.Context, c etcd.BaselinePreparationContext) error {
	proof, staged, err := p.inspect(ctx, c)
	if err != nil || staged {
		return err
	}
	// inspect returned only after BOTH RO databases and snapshots closed.
	err = bootstrapOfflineWithData(ctx, c.DataDir, false, func(db *pebble.DB) error {
		snapshot := db.NewSnapshot()
		current, err := bootstrapOfflineSnapshotProof(ctx, snapshot)
		if err = errors.Join(err, snapshot.Close()); err != nil {
			return err
		}
		if current.DataDigest != proof.Baseline.DataDigest {
			return errors.Wrap(errBootstrapOfflineStore, "target changed between RO verification and trusted stage")
		}
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		// Exact canonical KV preparation bytes; one Sync write, no timestamp,
		// watermark, applied-index update, generic metadata writer or cleanup.
		return errors.Wrap(db.Set(p.key, p.value, pebble.Sync), "sync trusted activation preparation")
	})
	if err != nil {
		return err // Retain the backup and any ambiguous target; never roll back.
	}
	_, err = p.verify(ctx, c)
	return err
}

func (p *bootstrapOfflineDataPlan) verify(ctx context.Context, c etcd.BaselinePreparationContext) (etcd.BaselinePreparedStoreProof, error) {
	proof, staged, err := p.inspect(ctx, c)
	if err != nil {
		return etcd.BaselinePreparedStoreProof{}, err
	}
	if !staged {
		return etcd.BaselinePreparedStoreProof{}, errors.Wrap(errBootstrapOfflineStore, "trusted preparation not staged")
	}
	return proof, nil
}

func (p *bootstrapOfflineDataPlan) inspect(ctx context.Context, c etcd.BaselinePreparationContext) (etcd.BaselinePreparedStoreProof, bool, error) {
	var result etcd.BaselinePreparedStoreProof
	if err := p.validate(c); err != nil {
		return result, false, err
	}
	staged := false
	err := bootstrapOfflineWithData(ctx, c.BackupDir, true, func(source *pebble.DB) error {
		return bootstrapOfflineWithData(ctx, c.DataDir, true, func(target *pebble.DB) (err error) {
			src, dst := source.NewSnapshot(), target.NewSnapshot()
			defer func() { err = errors.Join(err, dst.Close(), src.Close()) }()
			result, staged, err = p.compare(ctx, src, dst)
			return err
		})
	})
	if err != nil {
		return etcd.BaselinePreparedStoreProof{}, false, err
	}
	return result, staged, nil
}

func (p *bootstrapOfflineDataPlan) compare(ctx context.Context, source, target *pebble.Snapshot) (etcd.BaselinePreparedStoreProof, bool, error) {
	var result etcd.BaselinePreparedStoreProof
	baseline, err := bootstrapOfflineSnapshotProof(ctx, source)
	if err != nil {
		return result, false, err
	}
	if err := p.matchSource(baseline); err != nil {
		return result, false, err
	}
	if err := p.checkUnpreparedSource(source); err != nil {
		return result, false, err
	}
	current, err := bootstrapOfflineSnapshotProof(ctx, target)
	if err != nil {
		return result, false, err
	}
	raw, present, err := bootstrapOfflineGet(target, p.key)
	if err != nil {
		return result, false, err
	}
	if !present {
		if current.DataDigest != baseline.DataDigest {
			return result, false, errors.Wrap(errBootstrapOfflineStore, "unprepared target differs from preserved source")
		}
		return etcd.BaselinePreparedStoreProof{Baseline: baseline}, false, nil
	}
	if !bytes.Equal(raw, p.value) {
		return result, false, errors.Wrap(errBootstrapOfflineStore, "conflicting preparation bytes")
	}
	if err := store.VerifyBootstrapBaselinePreparedTransform(ctx, source, target, map[string][]byte{string(p.key): p.value}); err != nil {
		return result, false, errors.WithStack(err)
	}
	return etcd.BaselinePreparedStoreProof{Baseline: baseline, PreparedDataDigest: current.DataDigest}, true, nil
}

func (p *bootstrapOfflineDataPlan) matchSource(proof etcd.BaselineStoreProof) error {
	cut := p.prepared.Probe.BaselineContext
	if proof.AppliedIndex != cut.CommitIndex || proof.Watermark != cut.Watermark || proof.DataDigest != cut.DataDigest {
		return errors.Wrap(errBootstrapOfflineStore, "backup store differs from signed source cut")
	}
	if proof.ConfigurationIndex != 0 && proof.ConfigurationIndex != cut.Configuration.Index {
		return errors.Wrap(errBootstrapOfflineStore, "backup configuration projection differs from signed source cut")
	}
	return nil
}

func (p *bootstrapOfflineDataPlan) checkUnpreparedSource(source *pebble.Snapshot) error {
	policy, err := kv.BootstrapPolicyMetadataKey(p.prepared.Probe.Identity)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, key := range [][]byte{p.key, append([]byte(store.RaftControlMetadataPrefix), policy...)} {
		_, present, err := bootstrapOfflineGet(source, key)
		if err != nil {
			return err
		}
		if present {
			return errors.Wrap(errBootstrapOfflineStore, "source already contains preparation or active policy")
		}
	}
	return nil
}

func bootstrapOfflineGet(source *pebble.Snapshot, key []byte) ([]byte, bool, error) {
	raw, closer, err := source.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	value := bytes.Clone(raw)
	if err := closer.Close(); err != nil {
		return nil, false, errors.WithStack(err)
	}
	return value, true, nil
}

func bootstrapOfflineWithData(ctx context.Context, root string, readOnly bool, action func(*pebble.DB) error) error {
	if ctx == nil || action == nil {
		return errors.WithStack(errBootstrapOfflineStore)
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	dir, err := bootstrapOfflineExistingData(root)
	if err != nil {
		return err
	}
	cache := pebble.NewCache(bootstrapOfflineCacheSize)
	defer cache.Unref()
	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: readOnly, ErrorIfNotExists: true,
		Cache: cache, MaxOpenFiles: bootstrapOfflineOpenFiles, DisableAutomaticCompactions: true,
		FS: bootstrapOfflineFS{FS: vfs.Default, readOnly: readOnly}})
	if err != nil {
		return errors.Wrap(err, "open existing offline fsm.db")
	}
	return errors.WithStack(errors.Join(action(db), db.Close()))
}

func bootstrapOfflineExistingData(root string) (string, error) {
	if root == "" {
		return "", errors.WithStack(errBootstrapOfflineStore)
	}
	for _, path := range []string{root, filepath.Join(root, bootstrapOfflineDataPath)} {
		if err := bootstrapOfflineCheckDirectory(path); err != nil {
			return "", err
		}
	}
	dir := filepath.Join(root, bootstrapOfflineDataPath)
	if err := bootstrapOfflineCheckTree(dir); err != nil {
		return "", err
	}
	lock, err := bootstrapOfflineLstat(filepath.Join(dir, "LOCK"))
	if err != nil || !lock.Mode().IsRegular() {
		return "", errors.Wrap(errBootstrapOfflineStore, "existing regular Pebble LOCK required")
	}
	description, err := pebble.Peek(dir, vfs.Default)
	if err != nil {
		return "", errors.WithStack(err)
	}
	if !description.Exists {
		return "", errors.Wrap(errBootstrapOfflineStore, "existing Pebble manifest required")
	}
	return dir, nil
}

func bootstrapOfflineCheckDirectory(path string) error {
	info, err := bootstrapOfflineLstat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Wrap(errBootstrapOfflineStore, "store root must be an existing nonsymlink directory")
	}
	return nil
}

func bootstrapOfflineCheckTree(dir string) error {
	count, walBytes := 0, int64(0)
	err := bootstrapOfflineWalk(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		count++
		if count > bootstrapOfflineTreeLimit || (!entry.IsDir() && !entry.Type().IsRegular()) {
			return errors.Wrap(errBootstrapOfflineStore, "unsupported store tree or file-count bound exceeded")
		}
		if filepath.Ext(path) == ".log" {
			info, err := entry.Info()
			if err != nil {
				return errors.WithStack(err)
			}
			if info.Size() > bootstrapOfflineWALLimit-walBytes {
				return errors.Wrap(errBootstrapOfflineStore, "offline WAL replay byte bound exceeded")
			}
			walBytes += info.Size()
		}
		return nil
	})
	return errors.WithStack(err)
}

func bootstrapOfflineDistinctStores(source, target string) error {
	if err := bootstrapOfflineSeparateRoots(source, target); err != nil {
		return err
	}
	a, err := bootstrapOfflineExistingData(source)
	if err != nil {
		return err
	}
	b, err := bootstrapOfflineExistingData(target)
	if err != nil {
		return err
	}
	left, err := bootstrapOfflineLstat(a)
	if err != nil {
		return errors.WithStack(err)
	}
	right, err := bootstrapOfflineLstat(b)
	if err != nil {
		return errors.WithStack(err)
	}
	if os.SameFile(left, right) {
		return errors.Wrap(errBootstrapOfflineStore, "backup and target must be distinct stores")
	}
	return bootstrapOfflineDisjointFiles(a, b)
}

func bootstrapOfflineSeparateRoots(source, target string) error {
	a, err := filepath.Abs(source)
	if err != nil {
		return errors.WithStack(err)
	}
	b, err := filepath.Abs(target)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, pair := range [][2]string{{a, b}, {b, a}} {
		rel, err := filepath.Rel(pair[0], pair[1])
		if err != nil {
			return errors.WithStack(err)
		}
		if rel == "." || (rel != ".." && !bytes.HasPrefix([]byte(rel), []byte(".."+string(filepath.Separator)))) {
			return errors.Wrap(errBootstrapOfflineStore, "backup and target roots must not overlap")
		}
	}
	return nil
}

func bootstrapOfflineDisjointFiles(source, target string) error {
	// Mutable manifests/WALs must not alias the authenticated backup, even
	// under a different filename. Size buckets keep the portable SameFile
	// checks cheap; refuse pathological buckets instead of unbounded work.
	files, err := bootstrapOfflineFileIndex(source)
	if err != nil {
		return errors.WithStack(err)
	}
	checks := 0
	err = bootstrapOfflineWalk(target, func(_ string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errors.WithStack(walkErr)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return errors.WithStack(err)
		}
		for _, previous := range files[info.Size()] {
			checks++
			if checks > bootstrapOfflineAliasChecks || os.SameFile(previous, info) {
				return errors.Wrap(errBootstrapOfflineStore, "backup aliases target files or alias-check bound exceeded")
			}
		}
		return nil
	})
	return errors.WithStack(err)
}

func bootstrapOfflineFileIndex(root string) (map[int64][]os.FileInfo, error) {
	files := make(map[int64][]os.FileInfo)
	err := bootstrapOfflineWalk(root, func(_ string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errors.WithStack(walkErr)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return errors.WithStack(err)
		}
		files[info.Size()] = append(files[info.Size()], info)
		return nil
	})
	return files, errors.WithStack(err)
}

// Inspect the final component without following it, including roots supplied
// with a trailing separator. Parent traversal is scoped by an os.Root handle.
func bootstrapOfflineLstat(path string) (_ os.FileInfo, err error) {
	clean := filepath.Clean(path)
	root, err := os.OpenRoot(filepath.Dir(clean))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	name := filepath.Base(clean)
	if name == string(filepath.Separator) {
		name = "."
	}
	info, err := root.Lstat(name)
	return info, errors.WithStack(err)
}

// Entry names and Info reads stay relative to this root. WalkDir does not
// follow symlinks; the existing type checks still reject them explicitly.
func bootstrapOfflineWalk(path string, visit fs.WalkDirFunc) (err error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	return errors.WithStack(fs.WalkDir(root.FS(), ".", visit))
}

// The read path denies every write API. Trusted staging may create new files
// inside the existing DB, but never removes data or replaces an existing file.
// No NewPebbleStore startup cleanup/restore-backup deletion runs on this path.
type bootstrapOfflineFS struct {
	vfs.FS
	readOnly bool
}

func (f bootstrapOfflineFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if err := f.newFile(name); err != nil {
		return nil, err
	}
	file, err := f.FS.Create(name, category)
	return file, errors.WithStack(err)
}

func (f bootstrapOfflineFS) newFile(name string) error {
	if f.readOnly {
		return errors.WithStack(os.ErrPermission)
	}
	if _, err := bootstrapOfflineLstat(name); !errors.Is(err, os.ErrNotExist) {
		return errors.Wrap(errBootstrapOfflineStore, "refuse replacing existing file")
	}
	return nil
}

func (f bootstrapOfflineFS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	if f.readOnly {
		return nil, errors.WithStack(os.ErrPermission)
	}
	file, err := f.FS.OpenReadWrite(name, category, opts...)
	return file, errors.WithStack(err)
}

func (f bootstrapOfflineFS) Remove(string) error    { return errors.WithStack(os.ErrPermission) }
func (f bootstrapOfflineFS) RemoveAll(string) error { return errors.WithStack(os.ErrPermission) }
func (f bootstrapOfflineFS) Link(string, string) error {
	return errors.WithStack(os.ErrPermission)
}

func (f bootstrapOfflineFS) Rename(old, next string) error {
	if err := f.newFile(next); err != nil {
		return err
	}
	return errors.WithStack(f.FS.Rename(old, next))
}

func (f bootstrapOfflineFS) ReuseForWrite(string, string, vfs.DiskWriteCategory) (vfs.File, error) {
	return nil, errors.WithStack(os.ErrPermission)
}

func (f bootstrapOfflineFS) MkdirAll(path string, _ os.FileMode) error {
	if f.readOnly {
		return errors.WithStack(os.ErrPermission)
	}
	info, err := bootstrapOfflineLstat(path)
	if err != nil || !info.IsDir() {
		return errors.Wrap(errBootstrapOfflineStore, "offline preparation cannot create store directories")
	}
	return nil
}

func (f bootstrapOfflineFS) Lock(path string) (io.Closer, error) {
	info, err := bootstrapOfflineLstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.Wrap(errBootstrapOfflineStore, "offline lock file must already exist")
	}
	lock, err := f.FS.Lock(path)
	return lock, errors.WithStack(err)
}

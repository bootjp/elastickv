package etcd

import (
	"context"
	"io"
	"slices"
	"sync"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// NamespaceRecoverySource can only be populated from the selected, identity-
// verified native WAL. Its snapshot is CRC verified before the callback runs.
// The callback must independently reconstruct and compare the COMPLETE durable
// namespace, including receipts and applied/configuration indices, before skip.
type NamespaceRecoverySource struct {
	identity      WALIdentity
	source        OfflineBaselineWALSource
	configuration func() (*raftpb.ConfState, uint64, error)
}

func (s NamespaceRecoverySource) Identity() WALIdentity { return s.identity }
func (s NamespaceRecoverySource) Snapshot() io.Reader   { return s.source.Snapshot }
func (s NamespaceRecoverySource) SnapshotIndex() uint64 { return s.source.SnapshotIndex }
func (s NamespaceRecoverySource) SnapshotTerm() uint64  { return s.source.SnapshotTerm }
func (s NamespaceRecoverySource) CommitIndex() uint64   { return s.source.CommitIndex }
func (s NamespaceRecoverySource) CommitTerm() uint64    { return s.source.CommitTerm }
func (s NamespaceRecoverySource) Configuration() (*raftpb.ConfState, uint64, error) {
	return s.configuration()
}
func (s NamespaceRecoverySource) Replay() []OfflineBaselineReplayEntry {
	entries := slices.Clone(s.source.Replay)
	for i := range entries {
		entries[i].Payload = slices.Clone(entries[i].Payload)
	}
	return entries
}

type NamespaceRecoveryVerifier func(context.Context, NamespaceRecoverySource) (index uint64, handled bool, err error)

type namespaceRootGuard struct {
	closer io.Closer
	once   sync.Once
	err    error
}

func (g *namespaceRootGuard) Close() error {
	g.once.Do(func() { g.err = g.closer.Close() })
	return errors.WithStack(g.err)
}

// WithNamespaceRecovery carries exclusive offline ownership into native Open.
// The derived factory is single-use. No lock is dropped between offline store
// inspection and engine startup; the caller closes ownership on failure too.
func (f *Factory) WithNamespaceRecovery(ownership io.Closer, verify NamespaceRecoveryVerifier) *Factory {
	copy := *f
	copy.cfg.namespaceOwnership = ownership
	copy.cfg.namespaceRecovery = verify
	return &copy
}

func claimNamespaceRecoveryOwnership(cfg OpenConfig) (io.Closer, error) {
	if runtime, ok := cfg.namespaceOwnership.(*namespaceRuntimeOwnership); ok {
		return runtime.claim(cfg.DataDir)
	}
	o, err := verifyBaselineOwnership(OfflineBaselineOptions{DataDir: cfg.DataDir, Ownership: cfg.namespaceOwnership})
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.root == nil {
		return nil, errors.WithStack(ErrOfflineBaseline)
	}
	// The root directory flock remains held. Only segment locks are handed to
	// the ordinary WAL reader, which must take its own locks before proceeding.
	for _, file := range o.files {
		if err := file.Close(); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	o.files = nil
	root := o.root
	o.root = nil
	o.closed = true
	return &namespaceRootGuard{closer: root}, nil
}

func recoverVerifiedNamespace(selected *walRestoreState, image *baselineWALImage, dir string, verify NamespaceRecoveryVerifier) (index uint64, handled bool, err error) {
	if verify == nil {
		return 0, false, nil
	}
	entries, err := namespaceRecoveryEntries(selected, image)
	if err != nil {
		return 0, false, err
	}
	snapshot := selected.snapshot
	commit := selected.hardState.GetCommit()
	start := snapshot.GetMetadata().GetIndex()
	payload, err := openBaselineSnapshotPayload(dir, &snapshot)
	if err != nil {
		return 0, false, err
	}
	defer func() { err = errors.Join(err, payload.Close()) }()
	term := snapshot.GetMetadata().GetTerm()
	if entry := image.entries[commit]; entry != nil {
		term = entry.GetTerm()
	}
	source := NamespaceRecoverySource{identity: *selected.verifiedIdentity, source: OfflineBaselineWALSource{
		Snapshot: payload, SnapshotIndex: start, SnapshotTerm: snapshot.GetMetadata().GetTerm(),
		CommitIndex: commit, CommitTerm: term, Replay: entries}, configuration: func() (*raftpb.ConfState, uint64, error) {
		return baselineReplayedConfiguration(image, &snapshot, BaselineStoreProof{})
	}}
	index, handled, err = verify(context.Background(), source)
	if err == nil && handled && (index < start || index > commit) {
		return 0, false, errors.WithStack(ErrOfflineBaseline)
	}
	return index, handled, err
}

func namespaceRecoveryEntries(selected *walRestoreState, image *baselineWALImage) ([]OfflineBaselineReplayEntry, error) {
	if selected.verifiedIdentity == nil {
		return nil, errors.WithStack(ErrWALIdentity)
	}
	if err := verifyWALIdentity(image.metadata, selected.verifiedIdentity); err != nil {
		return nil, err
	}
	if image.hardState.GetCommit() != selected.hardState.GetCommit() || image.hardState.GetTerm() != selected.hardState.GetTerm() {
		return nil, errors.WithStack(ErrOfflineBaseline)
	}
	commit := selected.hardState.GetCommit()
	start := selected.snapshot.GetMetadata().GetIndex()
	if start > commit || commit-start > uint64(len(image.entries)) {
		return nil, errors.WithStack(ErrOfflineBaseline)
	}
	return baselineSourceReplayEntries(image, start, commit)
}

func readNamespaceRecoveryImage(walDir string, verify NamespaceRecoveryVerifier) (*baselineWALImage, error) {
	if verify == nil {
		return nil, nil
	}
	// Runtime preallocation leftovers are not offline migration evidence.
	// Read before wal.Open starts its allocator; strict offline readers remain unchanged.
	return readRuntimeNamespaceWAL(walDir)
}

func OfflineBaselineBackupDirectory(dataDir string, manifest raftbootstrap.BaselineManifest) string {
	return baselineMigrationPaths(OfflineBaselineOptions{DataDir: dataDir, Expected: manifest.BaselineContext}).BackupDir
}

func loadWalStateWithNamespace(logger *zap.Logger, walDir, snapDir, fsmSnapDir string, fsm StateMachine, cfg OpenConfig) (*diskState, error) {
	return loadWalStateRecovery(logger, walDir, snapDir, fsmSnapDir, fsm, cfg.ColdStartObserver, cfg.namespaceRecovery, cfg.NativeIdentity)
}

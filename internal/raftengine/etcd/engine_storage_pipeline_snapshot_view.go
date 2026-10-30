package etcd

import (
	"path/filepath"

	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// Owner-only Storage facade. Snapshot never discovers an inline body by
// cloning MemoryStorage: a published, preflighted token proof is required.
// InitialState/Entries/Term remain the real MemoryStorage's native methods.
type pipelineSnapshotStorageView struct {
	*etcdraft.MemoryStorage
	root         *storageWorkBudget
	failure      func(error) error
	ready        bool
	index        uint64
	cloneBytes   uint64
	clones       []*storageWorkPermit
	cloneCount   uint64
	outerUnknown uint64
	metaUnknown  uint64
}

func (v *pipelineSnapshotStorageView) Snapshot() (*raftpb.Snapshot, error) {
	if !v.ready {
		return nil, etcdraft.ErrSnapshotTemporarilyUnavailable
	}
	permit, err := v.root.acquire(v.cloneBytes, 0, true)
	if err != nil {
		_ = v.failure(errors.Wrap(errStoragePipelineResource, "snapshot clone reservation"))
		return nil, etcdraft.ErrSnapshotTemporarilyUnavailable
	}
	snapshot, err := v.MemoryStorage.Snapshot()
	if err != nil {
		permit.Release()
		return nil, errors.WithStack(err)
	}
	v.clones = append(v.clones, permit)
	v.cloneCount++
	return snapshot, nil
}

func (v *pipelineSnapshotStorageView) publish(snapshot *raftpb.Snapshot, replace bool) {
	v.index = snapshot.GetMetadata().GetIndex()
	if replace {
		v.outerUnknown = uint64(len(snapshot.ProtoReflect().GetUnknown()))
		v.metaUnknown = uint64(len(snapshot.GetMetadata().ProtoReflect().GetUnknown()))
	}
	// CreateSnapshot preserves the old outer/metadata unknowns; ApplySnapshot
	// replaces them. Certify that distinction before the next possible clone.
	v.cloneBytes = pipelineTaskOverhead*pipelineSnapshotDescriptorCopies + uint64(len(snapshot.Data))*outboundCapacityFactor
	v.cloneBytes = pipelineAddBytes(v.cloneBytes, (v.outerUnknown+v.metaUnknown)*outboundCapacityFactor)
	v.cloneBytes = pipelineAddBytes(v.cloneBytes, pipelineConfStateAccounting(snapshot.GetMetadata().GetConfState(), true))
	v.ready = true
}

func (v *pipelineSnapshotStorageView) retireClones() {
	for _, permit := range v.clones {
		permit.Release()
	}
	v.clones = nil
}

func (v *pipelineSnapshotStorageView) close() { v.retireClones() }

func pipelineSnapshotEnvironment(env pipelineConfigurationEnvironment) pipelineConfigurationEnvironment {
	if env.fsmSnapDir == "" && env.dataDir != "" {
		env.fsmSnapDir = filepath.Join(env.dataDir, fsmSnapDirName)
	}
	return env
}

// Called before the owner starts, with the actual selected storage snapshot.
// Only bounded descriptor bytes are captured; an inline recovery body is read
// from its existing verified Raft file by the disk actor, not retained/copied.
func (e *Engine) attachPipelineSnapshotView(cfg OpenConfig, selected *raftpb.Snapshot) error {
	p := e.storagePipeline
	if p == nil || e.rawNode == nil || selected == nil {
		return errors.WithStack(errInvalidStorageWork)
	}
	view := &pipelineSnapshotStorageView{MemoryStorage: e.storage, root: p.bulkOwnership, failure: p.firstFailure}
	p.snapshotView = view
	view.index = selected.GetMetadata().GetIndex()
	if err := e.seedPipelineSnapshotConfiguration(selected); err != nil {
		return err
	}
	cfg.asyncSnapshotView = view
	applied := e.rawNode.BasicStatus().Applied
	raw, err := newStorageRawNode(cfg, e.storage, applied)
	if err != nil {
		return err
	}
	e.rawNode = raw
	if len(selected.Data) == 0 {
		view.publish(selected, true)
		return nil
	}
	env := pipelineSnapshotEnvironment(pipelineConfigurationEnvironment{dataDir: e.dataDir, fsmSnapDir: e.fsmSnapDir, maxWALFiles: e.maxWALFiles})
	e.fsmSnapDir = env.fsmSnapDir
	normalization, err := e.newPipelineNormalization(selected, env, nil, true)
	if err != nil {
		return err
	}
	p.recoveryNormalization = normalization
	return nil
}

func (e *Engine) seedPipelineSnapshotConfiguration(selected *raftpb.Snapshot) error {
	if err := validatePipelineSnapshotDescriptor(selected); err != nil {
		return err
	}
	if len(e.snapshotConfigurations.entries) != 0 {
		return nil
	}
	return e.resetPipelineSnapshotConfiguration(selected)
}

func (e *Engine) resetPipelineSnapshotConfiguration(selected *raftpb.Snapshot) error {
	p := e.storagePipeline
	permit, err := p.configurationHistoryBudget.tryAcquire(pipelineConfigurationBytes(selected.GetMetadata().GetConfState()))
	if err != nil {
		return errors.Wrap(errStoragePipelineResource, "snapshot configuration base")
	}
	e.snapshotConfigurations.reset(selected.GetMetadata().GetIndex(), selected.GetMetadata().GetConfState())
	p.retireConfigurationHistory()
	p.configurationHistory = append(p.configurationHistory, pipelineConfigurationRetention{index: selected.GetMetadata().GetIndex(), permit: permit})
	return nil
}

func (e *Engine) storedSnapshotIndex() (uint64, error) {
	if p := e.storagePipeline; p != nil && p.snapshotView != nil {
		return p.snapshotView.index, nil
	}
	snapshot, err := e.storage.Snapshot()
	return snapshot.GetMetadata().GetIndex(), errors.WithStack(err)
}

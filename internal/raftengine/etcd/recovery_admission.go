package etcd

import (
	"context"
	"slices"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// A data snapshot is not a configuration entry. Only the ordered FSM record,
// restored and checked against that snapshot's membership, supplies its epoch.
func (e *Engine) recoverProtectedConfigurationIndex(prepared preparedOpenState) error {
	if prepared.cfg.RecoveredAdmission == nil {
		return nil
	}
	snapshotIndex := prepared.disk.LocalSnap.GetMetadata().GetIndex()
	if snapshotIndex == 0 {
		e.configIndex.Store(0) // Committed bootstrap ConfChanges will replay.
		return nil
	}
	index, err := e.provenSnapshotConfigurationIndex(snapshotIndex)
	if err != nil {
		return err
	}
	e.configIndex.Store(index)
	return nil
}

// The caller must first restore the FSM and reconcile its namespace membership
// against this exact snapshot's ConfState.
func (e *Engine) provenSnapshotConfigurationIndex(snapshotIndex uint64) (uint64, error) {
	if _, ok := e.fsm.(raftengine.NamespaceConfigurationAware); !ok {
		return 0, errors.New("protected snapshot lacks namespace configuration proof")
	}
	aware, ok := e.fsm.(raftengine.AppliedConfigurationAware)
	if !ok {
		return 0, errors.New("protected snapshot lacks applied configuration proof")
	}
	index := aware.AppliedConfigurationIndex()
	if index == 0 || index > snapshotIndex {
		return 0, errors.New("protected snapshot configuration index is inconsistent")
	}
	return index, nil
}

func (e *Engine) receivedSnapshotConfigurationIndex(snapshotIndex uint64) (uint64, error) {
	if e.recoveredAdmission == nil && e.admissionChanged == nil {
		return snapshotIndex, nil
	}
	index, err := e.provenSnapshotConfigurationIndex(snapshotIndex)
	if err != nil {
		return 0, err
	}
	if index < e.currentConfigIndex() {
		return 0, errors.New("protected snapshot configuration index regressed")
	}
	return index, nil
}

// RecoveredState is minted at a native post-persist/post-apply boundary.
// A zero value has no verified identity. Accessors return independent copies.
// It proves this recovery prefix, not future admission or historical data equality.
type RecoveredState struct {
	identity                    *WALIdentity
	hardState                   *raftpb.HardState
	confState                   *raftpb.ConfState
	applied, configurationIndex uint64
	peers                       []Peer
}

// VerifyPreparation must precede initial (floor-zero) policy publication. The
// digest is the complete independently accepted BaselineManifestDigest. A v1
// identity may recover activated policy, but cannot prove initial preparation.
func (s RecoveredState) VerifyPreparation(expected raftbootstrap.Digest) error {
	if expected == (raftbootstrap.Digest{}) || s.identity == nil || s.identity.PreparationDigest != expected {
		return errors.WithStack(ErrWALIdentity)
	}
	return nil
}

func (s RecoveredState) Identity() (WALIdentity, bool) {
	if s.identity == nil {
		return WALIdentity{}, false
	}
	return *s.identity, true
}
func (s RecoveredState) AppliedIndex() uint64       { return s.applied }
func (s RecoveredState) ConfigurationIndex() uint64 { return s.configurationIndex }
func (s RecoveredState) HardState() *raftpb.HardState {
	if s.hardState == nil {
		return nil
	}
	copy, _ := proto.Clone(s.hardState).(*raftpb.HardState)
	return copy
}
func (s RecoveredState) ConfState() *raftpb.ConfState {
	if s.confState == nil {
		return nil
	}
	copy, _ := proto.Clone(s.confState).(*raftpb.ConfState)
	return copy
}
func (s RecoveredState) Peers() []Peer { return slices.Clone(s.peers) }

func (e *Engine) publishRecoveredAdmission() error {
	if e.recoveredAdmission == nil {
		return nil
	}
	if e.recoveredIdentity == nil {
		return errors.WithStack(ErrWALIdentity)
	}
	e.storage.Lock()
	hardState, confState, err := e.storage.InitialState()
	if err == nil {
		hardState, _ = proto.Clone(hardState).(*raftpb.HardState)
		confState, _ = proto.Clone(confState).(*raftpb.ConfState)
	}
	e.storage.Unlock()
	if err != nil {
		return errors.WithStack(err)
	}
	snapshot, err := e.storage.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}
	if !validRecoveryPrefix(hardState, confState, e.applied, e.currentConfigIndex(), snapshot.GetMetadata().GetIndex()) {
		return errors.New("native recovery admission prefix is not reconciled")
	}
	peers := make([]Peer, 0, len(e.peers))
	for _, peer := range e.peers {
		peers = append(peers, peer)
	}
	state := RecoveredState{identity: copyWALIdentity([]*WALIdentity{e.recoveredIdentity}),
		hardState: hardState, confState: confState,
		applied: e.applied, configurationIndex: e.currentConfigIndex(), peers: peers}
	state.hardState, state.confState = state.HardState(), state.ConfState()
	// Seed A from actual native authority only after the durable recovery prefix
	// has been verified, and release A before policy/owner publication callbacks.
	if err := e.withNative(context.Background(), nil, func() error { return nil }); err != nil {
		return err
	}
	return errors.WithStack(e.recoveredAdmission(state))
}

func validRecoveryPrefix(hardState *raftpb.HardState, confState *raftpb.ConfState, applied, configIndex, snapshotIndex uint64) bool {
	return hardState != nil && confState != nil && hardState.GetTerm() != 0 && applied >= hardState.GetCommit() &&
		configIndex <= snapshotIndex && snapshotIndex <= applied
}

func recoveredRawNodeForOpen(prepared preparedOpenState) (*etcdraft.RawNode, uint64, uint64, error) {
	if prepared.cfg.RecoveredAdmission != nil && prepared.disk.VerifiedIdentity == nil {
		return nil, 0, 0, errors.Wrap(ErrWALIdentity, "recovery admission requires native identity")
	}
	initialApplied := coldStartApplied(prepared.disk)
	rawApplied, err := rawNodeAppliedForOpen(prepared.disk.Storage, initialApplied, prepared.cfg)
	if err != nil {
		return nil, 0, 0, err
	}
	node, err := newRawNode(prepared.cfg, prepared.disk.Storage, rawApplied)
	return node, initialApplied, rawApplied, err
}

// Called without e.mu or transport locks. Reconciliation, not this callback,
// owns reopening Control/Data after a native authority change.
func (e *Engine) invalidateAdmission() {
	e.invalidateNativeAdmission()
	e.admissionDirty = true
	if e.admissionChanged != nil {
		e.admissionChanged()
	}
}

func (e *Engine) invalidateNativeAuthority() {
	basic := e.rawNode.BasicStatus()
	e.mu.RLock()
	before := e.status
	e.mu.RUnlock()
	if before.Term != basic.GetTerm() || before.State != translateState(basic.RaftState) || before.Leader != e.leaderInfo(basic.Lead) {
		e.invalidateAdmission()
	}
}

// RecoveredAdmission must revalidate policy and publish IdentityReady only.
// A failed callback keeps admission closed and prevents Ready dispatch.
func (e *Engine) reconcileIdentityAdmission() error {
	if e.admissionPublished && !e.admissionDirty {
		return nil
	}
	if e.recoveredAdmission != nil {
		if err := e.persistStartupAppliedIndex(); err != nil {
			e.invalidateAdmission()
			return err
		}
	}
	if err := e.publishRecoveredAdmission(); err != nil {
		e.invalidateAdmission()
		return err
	}
	e.admissionPublished = true
	e.admissionDirty = false
	return nil
}

func (e *Engine) applyAndDispatchReady(rd etcdraft.Ready) error {
	// Keep the legacy send-before-apply path for unprotected transports.
	if e.admissionChanged == nil {
		if err := e.sendReadyMessages(rd); err != nil {
			return err
		}
		return e.applyCommitted(entryValues(rd.CommittedEntries))
	}
	if err := e.applyCommitted(entryValues(rd.CommittedEntries)); err != nil {
		return err
	}
	// Protected dispatch waits for durable native state and fresh policy
	// reconciliation. The callback may reopen Identity only, not Control/Data.
	e.refreshStatus()
	if err := e.reconcileIdentityAdmission(); err != nil {
		return err
	}
	return e.sendReadyMessages(rd)
}

func (e *Engine) invalidateChangedStatus(before, after raftengine.Status) {
	if before.Term != after.Term || before.State != after.State || before.Leader != after.Leader ||
		before.ConfigurationIndex != after.ConfigurationIndex {
		e.invalidateAdmission()
	}
}

func (e *Engine) beforeConfigurationChange() error {
	e.invalidateAdmission()
	// No exact applied-policy transition context is implemented at this native
	// boundary yet. Reject before mutating RawNode, including replayed entries.
	if e.recoveredAdmission != nil || e.admissionChanged != nil {
		return errors.New("protected configuration change requires exact policy context")
	}
	return nil
}

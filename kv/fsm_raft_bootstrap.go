package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

var ErrGuardedBootstrapApply = errors.New("guarded bootstrap apply failed")
var ErrGuardedBootstrapReceiptMissing = errors.New("guarded bootstrap receipt missing")
var ErrGuardedBootstrapReceiptConflict = errors.New("guarded bootstrap receipt conflict")

// applyGuardedBootstrap owns the reserved bootstrap-control prefix ahead of
// every legacy decoder. Exact durable receipts precede all current validation;
// new effects and their receipts share the store's indexed entry transaction.
func (f *kvFSM) applyGuardedBootstrap(ctx context.Context, payload []byte) (any, bool) {
	if !raftbootstrap.IsReservedControlPayload(payload) {
		return nil, false
	}
	identity, err := f.guardedBootstrapEntryIdentity()
	if err != nil {
		return guardedBootstrapHalt(err, "bind protected entry identity"), true
	}
	receipt, err := f.applyGuardedBootstrapEntry(ctx, payload, identity)
	if err != nil {
		return guardedBootstrapHalt(err, "apply protected command"), true
	}
	return receipt, true
}

func (f *kvFSM) applyGuardedBootstrapEntry(ctx context.Context, payload []byte, identity raftbootstrap.EntryIdentity) (any, error) {
	receipt, found, err := f.readBootstrapReceiptVariant(ctx, identity, payload)
	if err != nil {
		return receipt, err
	}
	if found {
		f.publishBootstrapReplay(receipt)
		return receipt, nil
	}
	if command, matched, err := raftbootstrap.ParseActivationCommand(payload); matched {
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return f.applyBootstrapActivation(ctx, identity, command, payload)
	}
	command, matched, err := ParseGuardedBootstrapCommand(payload)
	if err != nil || !matched {
		return receipt, errors.Join(err, raftbootstrap.ErrInvalid)
	}
	if command.Witness.Cluster != identity.Cluster || command.Witness.Group != identity.Group {
		return receipt, errors.WithStack(ErrGuardedBootstrapReceiptConflict)
	}
	return f.applyNewGuardedBootstrap(ctx, identity, command)
}

func (f *kvFSM) publishBootstrapReplay(receipt any) {
	if guarded, ok := receipt.(raftbootstrap.GuardedReceipt); ok && guarded.Verdict == raftbootstrap.GuardedReceiptApplied {
		f.publishGuardedBootstrapWatermark(guarded)
	}
}

// NativeGroupIdentity returns only the identity installed by the trusted FSM
// option. Payload witnesses and local policy caches cannot select this identity.
func (f *kvFSM) NativeGroupIdentity() raftbootstrap.Identity {
	return f.raftBootstrapIdentity
}

func (f *kvFSM) applyNewGuardedBootstrap(ctx context.Context, identity raftbootstrap.EntryIdentity, command raftbootstrap.GuardedCommand) (raftbootstrap.GuardedReceipt, error) {
	var receipt raftbootstrap.GuardedReceipt
	applier, ok := f.store.(store.RaftEntryApplier)
	if !ok {
		return receipt, errors.WithStack(ErrGuardedBootstrapApply)
	}
	f.beginDeferredApplyNotices()
	defer f.discardDeferredApplyNotices()
	err := applier.ApplyRaftEntry(ctx, identity.Index, func(tx context.Context) error {
		var err error
		receipt, err = f.stageGuardedBootstrap(tx, identity, command)
		return err
	})
	if err != nil {
		return raftbootstrap.GuardedReceipt{}, errors.WithStack(err)
	}
	if receipt.Verdict == raftbootstrap.GuardedReceiptApplied {
		f.publishGuardedBootstrapWatermark(receipt)
		f.flushDeferredApplyNotices()
	}
	return receipt, nil
}

func (f *kvFSM) stageGuardedBootstrap(ctx context.Context, identity raftbootstrap.EntryIdentity, command raftbootstrap.GuardedCommand) (raftbootstrap.GuardedReceipt, error) {
	receipt, found, err := f.readGuardedBootstrapReceipt(ctx, identity)
	if err != nil {
		return receipt, err
	}
	if found {
		if receipt.Entry != identity {
			return receipt, errors.WithStack(ErrGuardedBootstrapReceiptConflict)
		}
		return receipt, nil
	}
	if err := f.guardNewBootstrapIndex(identity.Index); err != nil {
		return receipt, err
	}
	metadata, ok := f.store.(store.RaftControlMetadataStore)
	if !ok {
		return receipt, errors.WithStack(ErrGuardedBootstrapApply)
	}
	policy, err := readAppliedBootstrapPolicy(ctx, metadata, f.NativeGroupIdentity(), identity.Index-1)
	if err != nil {
		return receipt, errors.Join(ErrGuardedBootstrapReceiptMissing, err)
	}
	receipt, err = guardedBootstrapVerdict(identity, command, policy)
	if err != nil {
		return receipt, err
	}
	if receipt.Verdict == raftbootstrap.GuardedReceiptApplied {
		if err := f.stageGuardedBootstrapInner(ctx, command, &receipt); err != nil {
			return receipt, err
		}
	}
	return receipt, stageGuardedBootstrapReceipt(ctx, metadata, receipt)
}

func stageGuardedBootstrapReceipt(ctx context.Context, metadata store.RaftControlMetadataStore, receipt raftbootstrap.GuardedReceipt) error {
	encoded, err := raftbootstrap.EncodeGuardedReceipt(receipt)
	if err != nil {
		return errors.WithStack(err)
	}
	key, err := raftbootstrap.ReceiptKey(receipt.Entry)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(metadata.StageRaftControlMetadata(ctx, key, encoded))
}

func (f *kvFSM) guardNewBootstrapIndex(index uint64) error {
	reader, ok := f.store.(durableBootstrapIndex)
	if !ok {
		return nil // Memory stores enforce their control index while staging.
	}
	applied, known, err := reader.LastAppliedIndex()
	if err != nil {
		return errors.WithStack(err)
	}
	if known && applied >= index {
		return errors.WithStack(ErrGuardedBootstrapReceiptConflict)
	}
	return nil
}

func guardedBootstrapVerdict(identity raftbootstrap.EntryIdentity, command raftbootstrap.GuardedCommand, policy raftbootstrap.PolicyRecord) (raftbootstrap.GuardedReceipt, error) {
	receipt := raftbootstrap.GuardedReceipt{Entry: identity, Kind: command.Kind,
		Verdict: raftbootstrap.GuardedReceiptApplied, OperationID: command.OperationID, Witness: command.Witness}
	if command.Kind == raftbootstrap.ControlGuardedWatermark {
		ts, _, err := DecodeWatermarkAdvance(command.Inner)
		if err != nil {
			return receipt, err
		}
		receipt.RequestedWatermark = ts
	}
	witness, err := policy.Witness()
	if err != nil {
		return receipt, errors.WithStack(err)
	}
	switch {
	case witness != command.Witness:
		receipt.Verdict = raftbootstrap.GuardedReceiptRejectedStaleWitness
	case policy.Reservation != nil:
		receipt.Verdict = raftbootstrap.GuardedReceiptRejectedReservation
	}
	return receipt, nil
}

func (f *kvFSM) stageGuardedBootstrapInner(ctx context.Context, command raftbootstrap.GuardedCommand, receipt *raftbootstrap.GuardedReceipt) error {
	switch command.Kind {
	case raftbootstrap.ControlGuardedWatermark:
		ts := receipt.RequestedWatermark
		if ts > f.store.LastCommitTS() {
			if err := f.store.ApplyMutationsRaftAt(ctx, nil, nil, ts, ts, receipt.Entry.Index); err != nil {
				return errors.WithStack(err)
			}
		}
		receipt.ReturnedWatermark = max(ts, f.store.LastCommitTS())
		return nil
	case raftbootstrap.ControlGuardedCatalog:
		var request pb.Request
		if err := proto.Unmarshal(command.Inner[1:], &request); err != nil {
			return errors.WithStack(err)
		}
		if err := validateGuardedCatalogCommit(&request); err != nil {
			return err
		}
		commitTS, err := requestCommitTS(&request)
		if err != nil {
			return err
		}
		if err := f.verifyBackupTimestampFloor(&request, commitTS); err != nil {
			return err
		}
		return f.stageGuardedCatalogTransaction(ctx, &request, commitTS)
	case raftbootstrap.ControlActivate, raftbootstrap.ControlReserve, raftbootstrap.ControlAbort:
		return errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	return errors.WithStack(ErrInvalidGuardedBootstrapInner)
}

func (f *kvFSM) stageGuardedCatalogTransaction(ctx context.Context, request *pb.Request, commitTS uint64) error {
	// These typed control records do not belong to user routes. Reuse the
	// one-phase lock/OCC primitives, not migration or watcher-derived gates.
	mutations := request.Mutations[1:]
	storeMutations, err := f.buildOnePhaseStoreMutations(ctx, mutations)
	if err != nil {
		return err
	}
	if err := f.store.ApplyMutationsRaftAtFenced(ctx, storeMutations, request.ReadKeys, request.Ts, commitTS, f.pendingApplyIdx); err != nil {
		return errors.WithStack(err)
	}
	f.notifyApplyObservers(commitTS, mutations)
	return nil
}

func (f *kvFSM) publishGuardedBootstrapWatermark(receipt raftbootstrap.GuardedReceipt) {
	if receipt.Kind != raftbootstrap.ControlGuardedWatermark || f.hlc == nil {
		return
	}
	ts := receipt.ReturnedWatermark
	f.hlc.SetPhysicalCeiling(clampUint64ToInt64(ts >> hlcLogicalBits))
	f.hlc.Observe(ts)
}

func (f *kvFSM) guardedBootstrapEntryIdentity() (raftbootstrap.EntryIdentity, error) {
	pending := f.pendingApplyIdentity
	trusted := f.raftBootstrapIdentity
	if trusted.Cluster == (raftbootstrap.UUID{}) || pending.Index == 0 || pending.Term == 0 || pending.Digest == ([32]byte{}) || pending.Type != 0 {
		return raftbootstrap.EntryIdentity{}, errors.WithStack(ErrGuardedBootstrapReceiptConflict)
	}
	return raftbootstrap.EntryIdentity{
		Cluster: trusted.Cluster,
		Group:   trusted.Group,
		Index:   pending.Index,
		Term:    pending.Term,
		Digest:  raftbootstrap.Digest(pending.Digest),
	}, nil
}

func (f *kvFSM) readGuardedBootstrapReceipt(ctx context.Context, identity raftbootstrap.EntryIdentity) (raftbootstrap.GuardedReceipt, bool, error) {
	metadata, ok := f.store.(store.RaftControlMetadataStore)
	if !ok {
		return raftbootstrap.GuardedReceipt{}, false, errors.WithStack(ErrGuardedBootstrapApply)
	}
	key, err := raftbootstrap.ReceiptKey(identity)
	if err != nil {
		return raftbootstrap.GuardedReceipt{}, false, errors.Wrap(err, "encode receipt key")
	}
	value, err := metadata.ReadRaftControlMetadata(ctx, key)
	if errors.Is(err, store.ErrKeyNotFound) {
		return raftbootstrap.GuardedReceipt{}, false, nil
	}
	if err != nil {
		return raftbootstrap.GuardedReceipt{}, false, errors.Wrap(err, "read receipt metadata")
	}
	receipt, err := raftbootstrap.ParseGuardedReceipt(value)
	if err != nil {
		return raftbootstrap.GuardedReceipt{}, false, errors.Join(errors.WithStack(err), ErrGuardedBootstrapReceiptConflict)
	}
	return receipt, true, nil
}

func guardedBootstrapHalt(err error, operation string) any {
	return haltErr(errors.Wrap(errors.Join(err, ErrGuardedBootstrapApply), "kv/fsm: "+operation))
}

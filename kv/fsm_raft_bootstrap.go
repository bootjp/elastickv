package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrGuardedBootstrapApply = errors.New("guarded bootstrap apply failed")
var ErrGuardedBootstrapReceiptMissing = errors.New("guarded bootstrap receipt missing")
var ErrGuardedBootstrapReceiptConflict = errors.New("guarded bootstrap receipt conflict")

// applyGuardedBootstrap owns the reserved bootstrap-control prefix ahead of
// every legacy decoder. This first production stage permits only exact durable
// receipt replay. New entries remain fail-closed until replicated P/W and the
// atomic inner-effect path are installed.
func (f *kvFSM) applyGuardedBootstrap(ctx context.Context, payload []byte) (any, bool) {
	if !raftbootstrap.IsReservedControlPayload(payload) {
		return nil, false
	}
	identity, err := f.guardedBootstrapEntryIdentity()
	if err != nil {
		return guardedBootstrapHalt(err, "bind protected entry identity"), true
	}
	receipt, found, err := f.readGuardedBootstrapReceipt(ctx, identity)
	if err != nil {
		return guardedBootstrapHalt(err, "read protected receipt"), true
	}
	if found {
		if receipt.Entry != identity {
			return guardedBootstrapHalt(ErrGuardedBootstrapReceiptConflict, "match protected receipt"), true
		}
		return receipt, true
	}
	command, matched, err := ParseGuardedBootstrapCommand(payload)
	if err != nil || !matched {
		return guardedBootstrapHalt(errors.Join(err, raftbootstrap.ErrInvalid), "parse protected command"), true
	}
	if command.Witness.Cluster != identity.Cluster || command.Witness.Group != identity.Group {
		return guardedBootstrapHalt(ErrGuardedBootstrapReceiptConflict, "match protected command identity"), true
	}
	return guardedBootstrapHalt(ErrGuardedBootstrapReceiptMissing, "read protected receipt"), true
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

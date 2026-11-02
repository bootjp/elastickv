package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func guardedBootstrapFixture(t *testing.T, localStore store.MVCCStore) (*kvFSM, []byte, raftbootstrap.GuardedCommand, raftbootstrap.EntryIdentity) {
	t.Helper()
	command := raftbootstrap.GuardedCommand{
		Kind: raftbootstrap.ControlGuardedWatermark,
		Witness: raftbootstrap.Witness{
			Cluster: raftbootstrap.UUID{1}, Group: 7, ActivationIndex: 11, Generation: 2,
			ConfigurationIndex: 9, ConfigurationDigest: raftbootstrap.Digest{3},
		},
		OperationID: raftbootstrap.OperationID{4},
		Inner:       EncodeWatermarkAdvance(99),
	}
	payload, err := EncodeGuardedBootstrapCommand(command)
	require.NoError(t, err)
	pending := raftengine.NewAppliedEntryIdentity(21, 8, 0, payload)
	identity := raftbootstrap.EntryIdentity{
		Cluster: command.Witness.Cluster, Group: command.Witness.Group,
		Index: pending.Index, Term: pending.Term, Digest: raftbootstrap.Digest(pending.Digest),
	}
	fsm, ok := NewKvFSMWithHLC(localStore, nil, WithRaftBootstrapIdentity(command.Witness.Cluster, command.Witness.Group)).(*kvFSM)
	require.True(t, ok)
	fsm.SetApplyEntryIdentity(pending)
	return fsm, payload, command, identity
}

func haltApplyError(t *testing.T, response any) error {
	t.Helper()
	halt, ok := response.(interface{ HaltApply() error })
	require.True(t, ok, "response type %T", response)
	return halt.HaltApply()
}

func TestGuardedBootstrapReservedPrefixAlwaysHaltsMalformed(t *testing.T) {
	localStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, localStore.Close()) })
	cluster := raftbootstrap.UUID{1}
	fsm, ok := NewKvFSMWithHLC(localStore, nil, WithRaftBootstrapIdentity(cluster, 7)).(*kvFSM)
	require.True(t, ok)
	payload := []byte{0x1f}
	fsm.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(21, 8, 0, payload))

	err := haltApplyError(t, fsm.Apply(payload))
	require.ErrorIs(t, err, ErrGuardedBootstrapApply)
	require.ErrorIs(t, err, raftbootstrap.ErrInvalid)
}

func TestGuardedBootstrapNewEntryFailsClosedWithoutReceipt(t *testing.T) {
	localStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, localStore.Close()) })
	fsm, payload, _, _ := guardedBootstrapFixture(t, localStore)

	err := haltApplyError(t, fsm.Apply(payload))
	require.ErrorIs(t, err, ErrGuardedBootstrapApply)
	require.ErrorIs(t, err, ErrGuardedBootstrapReceiptMissing)
	require.Zero(t, localStore.LastCommitTS())
}

func TestGuardedBootstrapExactReceiptReplay(t *testing.T) {
	localStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, localStore.Close()) })
	fsm, payload, command, identity := guardedBootstrapFixture(t, localStore)
	receipt := raftbootstrap.GuardedReceipt{
		Entry: identity, Kind: command.Kind, Verdict: raftbootstrap.GuardedReceiptApplied,
		OperationID: command.OperationID, Witness: command.Witness, RequestedWatermark: 99, ReturnedWatermark: 99,
	}
	encoded, err := raftbootstrap.EncodeGuardedReceipt(receipt)
	require.NoError(t, err)
	key, err := raftbootstrap.ReceiptKey(identity)
	require.NoError(t, err)
	metadata, ok := localStore.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := localStore.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), identity.Index, func(ctx context.Context) error {
		if err := localStore.ApplyMutationsRaftAt(ctx, nil, nil, 99, 99, identity.Index); err != nil {
			return err
		}
		return metadata.StageRaftControlMetadata(ctx, key, encoded)
	}))

	require.Equal(t, receipt, fsm.Apply(payload))
	require.Equal(t, uint64(99), localStore.LastCommitTS())
}

func TestGuardedBootstrapReceiptIdentityConflictHalts(t *testing.T) {
	localStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, localStore.Close()) })
	fsm, payload, command, identity := guardedBootstrapFixture(t, localStore)
	receipt := raftbootstrap.GuardedReceipt{
		Entry: identity, Kind: command.Kind, Verdict: raftbootstrap.GuardedReceiptApplied,
		OperationID: command.OperationID, Witness: command.Witness, RequestedWatermark: 99, ReturnedWatermark: 99,
	}
	receipt.Entry.Term++
	encoded, err := raftbootstrap.EncodeGuardedReceipt(receipt)
	require.NoError(t, err)
	key, err := raftbootstrap.ReceiptKey(identity)
	require.NoError(t, err)
	metadata, ok := localStore.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := localStore.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), identity.Index, func(ctx context.Context) error {
		return metadata.StageRaftControlMetadata(ctx, key, encoded)
	}))

	err = haltApplyError(t, fsm.Apply(payload))
	require.ErrorIs(t, err, ErrGuardedBootstrapReceiptConflict)
}

func TestGuardedBootstrapReceiptSlotUsesTrustedIdentity(t *testing.T) {
	localStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, localStore.Close()) })
	fsm, payload, _, _ := guardedBootstrapFixture(t, localStore)
	corrupt := append([]byte(nil), payload...)
	corrupt[9] ^= 0xff // witness cluster starts immediately after the frame header.
	pending := raftengine.NewAppliedEntryIdentity(21, 8, 0, corrupt)
	fsm.SetApplyEntryIdentity(pending)

	err := haltApplyError(t, fsm.Apply(corrupt))
	require.ErrorIs(t, err, ErrGuardedBootstrapReceiptConflict)
}

func TestGuardedBootstrapExactReceiptPrecedesCurrentParser(t *testing.T) {
	localStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, localStore.Close()) })
	cluster := raftbootstrap.UUID{1}
	malformed := []byte{0x1f}
	pending := raftengine.NewAppliedEntryIdentity(21, 8, 0, malformed)
	identity := raftbootstrap.EntryIdentity{Cluster: cluster, Group: 7, Index: 21, Term: 8, Digest: raftbootstrap.Digest(pending.Digest)}
	command := raftbootstrap.GuardedCommand{
		Kind: raftbootstrap.ControlGuardedWatermark,
		Witness: raftbootstrap.Witness{Cluster: cluster, Group: 7, ActivationIndex: 11, Generation: 2,
			ConfigurationIndex: 9, ConfigurationDigest: raftbootstrap.Digest{3}},
		OperationID: raftbootstrap.OperationID{4}, Inner: EncodeWatermarkAdvance(99),
	}
	receipt := raftbootstrap.GuardedReceipt{
		Entry: identity, Kind: command.Kind, Verdict: raftbootstrap.GuardedReceiptApplied,
		OperationID: command.OperationID, Witness: command.Witness, RequestedWatermark: 99, ReturnedWatermark: 99,
	}
	encoded, err := raftbootstrap.EncodeGuardedReceipt(receipt)
	require.NoError(t, err)
	key, err := raftbootstrap.ReceiptKey(identity)
	require.NoError(t, err)
	metadata, ok := localStore.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := localStore.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), identity.Index, func(ctx context.Context) error {
		if err := localStore.ApplyMutationsRaftAt(ctx, nil, nil, 99, 99, identity.Index); err != nil {
			return err
		}
		return metadata.StageRaftControlMetadata(ctx, key, encoded)
	}))
	fsm, ok := NewKvFSMWithHLC(localStore, nil, WithRaftBootstrapIdentity(cluster, 7)).(*kvFSM)
	require.True(t, ok)
	fsm.SetApplyEntryIdentity(pending)

	require.Equal(t, receipt, fsm.Apply(malformed), "exact durable receipt must replay before current command validation")
}

func TestGuardedBootstrapCorruptReceiptIsConflict(t *testing.T) {
	localStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, localStore.Close()) })
	fsm, payload, _, identity := guardedBootstrapFixture(t, localStore)
	key, err := raftbootstrap.ReceiptKey(identity)
	require.NoError(t, err)
	metadata, ok := localStore.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := localStore.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), identity.Index, func(ctx context.Context) error {
		return metadata.StageRaftControlMetadata(ctx, key, []byte("corrupt"))
	}))

	err = haltApplyError(t, fsm.Apply(payload))
	require.ErrorIs(t, err, ErrGuardedBootstrapReceiptConflict)
	require.ErrorIs(t, err, raftbootstrap.ErrInvalid)
}

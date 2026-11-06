package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestProtectedDataGroupFSMBindsIdentityBeforeReceiptReplay(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	cluster := raftbootstrap.UUID{1}
	command := raftbootstrap.GuardedCommand{Kind: raftbootstrap.ControlGuardedWatermark,
		Witness:     raftbootstrap.Witness{Cluster: cluster, Group: 7, ActivationIndex: 11, Generation: 2, ConfigurationIndex: 9, ConfigurationDigest: raftbootstrap.Digest{3}},
		OperationID: raftbootstrap.OperationID{4}, Inner: kv.EncodeWatermarkAdvance(99)}
	payload, err := kv.EncodeGuardedBootstrapCommand(command)
	require.NoError(t, err)
	pending := raftengine.NewAppliedEntryIdentity(21, 8, 0, payload)
	identity := raftbootstrap.EntryIdentity{Cluster: cluster, Group: 7, Index: 21, Term: 8, Digest: raftbootstrap.Digest(pending.Digest)}
	receipt := raftbootstrap.GuardedReceipt{Entry: identity, Kind: command.Kind, Verdict: raftbootstrap.GuardedReceiptApplied,
		OperationID: command.OperationID, Witness: command.Witness, RequestedWatermark: 99, ReturnedWatermark: 99}
	encoded, err := raftbootstrap.EncodeGuardedReceipt(receipt)
	require.NoError(t, err)
	key, err := raftbootstrap.ReceiptKey(identity)
	require.NoError(t, err)
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 21, func(ctx context.Context) error {
		if err := st.ApplyMutationsRaftAt(ctx, nil, nil, 99, 99, 21); err != nil {
			return err
		}
		return metadata.StageRaftControlMetadata(ctx, key, encoded)
	}))
	builder := shardGroupBuilder{clock: kv.NewHLC()}
	spec := &groupBootstrapSpec{config: &bootstrapPeerConfig{local: peersession.Node{Cluster: cluster}}}
	fsm := builder.newDataGroupFSM(st, nil, 7, spec)
	aware, ok := fsm.(raftengine.ApplyEntryIdentityAware)
	require.True(t, ok)
	aware.SetApplyEntryIdentity(pending)
	require.Equal(t, receipt, fsm.Apply(payload))
}

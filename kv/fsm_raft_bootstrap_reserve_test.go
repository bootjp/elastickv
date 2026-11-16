package kv

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func guardedReserveCommand(t *testing.T, previous raftbootstrap.PolicyRecord, index uint64) raftbootstrap.GuardedCommand {
	t.Helper()
	witness, err := previous.Witness()
	require.NoError(t, err)
	next := previous
	next.Generation++
	next.Reservation = &raftbootstrap.MembershipReservation{
		ID:       raftbootstrap.ReservationID{ReserveIndex: index, OperationID: raftbootstrap.OperationID{9}},
		Expected: witness, Transition: raftbootstrap.TransitionAddLearner,
		Candidate:      raftbootstrap.Member{ID: 2, Address: "node2:9000", PublicKeyDigest: raftbootstrap.Digest{4}},
		EvidenceDigest: raftbootstrap.Digest{5},
		Target: raftbootstrap.Configuration{Index: previous.Configuration.Index + 1,
			State: raftbootstrap.ConfState{Voters: []uint64{1}, Learners: []uint64{2}},
			Members: []raftbootstrap.Member{
				previous.Configuration.Members[0],
				{ID: 2, Address: "node2:9000", PublicKeyDigest: raftbootstrap.Digest{4}},
			}},
	}
	inner, err := raftbootstrap.MarshalPolicyRecord(next)
	require.NoError(t, err)
	return raftbootstrap.GuardedCommand{Kind: raftbootstrap.ControlReserve, Witness: witness,
		OperationID: next.Reservation.ID.OperationID, Inner: inner}
}

func TestGuardedMembershipReserveAtomicApply(t *testing.T) {
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			previous := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, previous, 10)
			fsm := newGuardedApplyFSM(t, st)
			command := guardedReserveCommand(t, previous, 11)
			changed, err := BootstrapApplyChangesAdmission(t.Context(), st, previous.Identity, mustGuardedReservePayload(t, command))
			require.NoError(t, err)
			require.True(t, changed)
			receipt, ok := applyGuardedNewCommand(t, fsm, command, 11).(raftbootstrap.GuardedReceipt)
			require.True(t, ok)
			require.Equal(t, raftbootstrap.GuardedReceiptApplied, receipt.Verdict)
			requireGuardedNewIndex(t, st, 11)
			stored := readReserveTestPolicy(t, st, previous.Identity, 11)
			require.Equal(t, uint64(2), stored.Generation)
			require.Equal(t, command.OperationID, stored.Reservation.ID.OperationID)
			require.Equal(t, uint64(11), stored.Reservation.ID.ReserveIndex)
			require.Equal(t, receipt, applyGuardedNewCommand(t, fsm, command, 11))
		})
	}
}

func TestGuardedMembershipReserveRejectsWrongEntryIdentity(t *testing.T) {
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			previous := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, previous, 10)
			fsm := newGuardedApplyFSM(t, st)
			for _, mutate := range []func(*raftbootstrap.GuardedCommand){
				func(c *raftbootstrap.GuardedCommand) { c.OperationID[0]++ },
				func(c *raftbootstrap.GuardedCommand) {
					policy, err := raftbootstrap.ParsePolicyRecord(c.Inner)
					require.NoError(t, err)
					policy.Reservation.ID.ReserveIndex++
					c.Inner, err = raftbootstrap.MarshalPolicyRecord(policy)
					require.NoError(t, err)
				},
			} {
				command := guardedReserveCommand(t, previous, 11)
				mutate(&command)
				err := haltApplyError(t, applyGuardedNewCommand(t, fsm, command, 11))
				require.ErrorIs(t, err, ErrGuardedBootstrapApply)
				stored := readReserveTestPolicy(t, st, previous.Identity, 10)
				require.Nil(t, stored.Reservation)
				requireGuardedNewIndex(t, st, 10)
			}
		})
	}
}

func TestGuardedMembershipReserveRejectsStaleWitness(t *testing.T) {
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			previous := guardedNewPolicy()
			seedGuardedNewPolicy(t, st, previous, 10)
			fsm := newGuardedApplyFSM(t, st)
			command := guardedReserveCommand(t, previous, 11)
			command.Witness.Generation++
			receipt, ok := applyGuardedNewCommand(t, fsm, command, 11).(raftbootstrap.GuardedReceipt)
			require.True(t, ok)
			require.Equal(t, raftbootstrap.GuardedReceiptRejectedStaleWitness, receipt.Verdict)
			stored := readReserveTestPolicy(t, st, previous.Identity, 11)
			require.Nil(t, stored.Reservation)
			require.Equal(t, previous.Generation, stored.Generation)
			storedReceipt, found, err := fsm.readGuardedBootstrapReceipt(t.Context(), receipt.Entry)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, receipt, storedReceipt)
		})
	}
}

func mustGuardedReservePayload(t *testing.T, command raftbootstrap.GuardedCommand) []byte {
	t.Helper()
	payload, err := EncodeGuardedBootstrapCommand(command)
	require.NoError(t, err)
	return payload
}

func readReserveTestPolicy(t *testing.T, st store.MVCCStore, identity raftbootstrap.Identity, applied uint64) raftbootstrap.PolicyRecord {
	t.Helper()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	policy, err := readAppliedBootstrapPolicy(t.Context(), metadata, identity, applied)
	require.NoError(t, err)
	return policy
}

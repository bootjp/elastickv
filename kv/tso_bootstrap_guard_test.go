package kv

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func tsoGuardFixture(t *testing.T, backing TSODurableBacking) (*GuardedTSOStateMachine, raftbootstrap.ActivationCommand) {
	t.Helper()
	_, signer, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(signer.Public())
	require.NoError(t, err)
	c := raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion,
		Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}}, OperationID: raftbootstrap.OperationID{2},
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, CommitIndex: 7, CommitTerm: 2,
		Watermark: 10 << hlcLogicalBits, DataDigest: raftbootstrap.Digest{3},
		Configuration: raftbootstrap.Configuration{Index: 3, State: raftbootstrap.ConfState{Voters: []uint64{1}},
			Members: []raftbootstrap.Member{{ID: 1, Address: "localhost:9000", PublicKeyDigest: sha256.Sum256(der)}}}}
	digest, err := raftbootstrap.ConfigurationDigest(c.Configuration)
	require.NoError(t, err)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: c,
		Evidence: raftbootstrap.BaselineMemberEvidence{MemberID: 1, CommitIndex: 7, CommitTerm: 2, AppliedIndex: 7, LastLogIndex: 7,
			ConfigurationDigest: digest, NativeStateDigest: raftbootstrap.Digest{4}, DataDigest: c.DataDigest, Watermark: c.Watermark}}, signer)
	require.NoError(t, err)
	m := raftbootstrap.BaselineManifest{BaselineContext: c, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
	manifestDigest, err := raftbootstrap.BaselineManifestDigest(m)
	require.NoError(t, err)
	state, err := NewDurableTSOStateMachine(NewHLC(), backing)
	require.NoError(t, err)
	snapshot := &tsoFSMSnapshot{ceilingMs: 20, allocationFloor: c.Watermark, cutoverActive: true, phaseDActive: true, phaseDFloor: 3, pendingFloor: 3}
	var image bytes.Buffer
	_, err = snapshot.WriteTo(&image)
	require.NoError(t, err)
	require.NoError(t, state.InitializeLegacyControl(t.Context(), 7, 3, &image, nil))
	require.NoError(t, StagePreparedBootstrapActivation(t.Context(), backing, c.Identity, PreparedBootstrapActivation{Probe: probe, ManifestDigest: manifestDigest}))
	preparedSnapshot, err := backing.Snapshot()
	require.NoError(t, err)
	preparedHash := sha256.New()
	_, err = preparedSnapshot.WriteTo(preparedHash)
	require.NoError(t, err)
	require.NoError(t, preparedSnapshot.Close())
	var preparedDigest raftbootstrap.Digest
	copy(preparedDigest[:], preparedHash.Sum(nil))
	ack, err := raftbootstrap.SignPreparedMemberAck(raftbootstrap.PreparedMemberAck{MemberID: 1,
		ManifestDigest: manifestDigest, Minimum: c.Minimum, PreparedStateDigest: preparedDigest}, m, signer)
	require.NoError(t, err)
	f, err := NewGuardedTSOStateMachine(state, c.Identity, func(context.Context) (BootstrapActivationNativeProof, error) {
		return BootstrapActivationNativeProof{Identity: c.Identity, Configuration: c.Configuration}, nil
	})
	require.NoError(t, err)
	return f, raftbootstrap.ActivationCommand{Manifest: m, Witness: raftbootstrap.Witness{Cluster: c.Identity.Cluster, Group: c.Identity.Group, ConfigurationIndex: 3, ConfigurationDigest: digest},
		Readiness: raftbootstrap.PreparedCertificate{ManifestDigest: manifestDigest, Members: []raftbootstrap.PreparedMemberAck{ack}}}
}

func applyTSOGuard(t *testing.T, f *GuardedTSOStateMachine, index uint64, payload []byte) any {
	t.Helper()
	f.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(index, 2, 0, payload))
	defer f.ClearApplyEntryIdentity()
	return f.Apply(payload)
}

func activateTSOGuard(t *testing.T, f *GuardedTSOStateMachine, command raftbootstrap.ActivationCommand) ([]byte, raftbootstrap.ActivationReceipt) {
	t.Helper()
	payload, err := raftbootstrap.EncodeActivationCommand(command)
	require.NoError(t, err)
	receipt, ok := applyTSOGuard(t, f, 8, payload).(raftbootstrap.ActivationReceipt)
	require.True(t, ok)
	require.Equal(t, raftbootstrap.ActivationReceiptApplied, receipt.Verdict)
	return payload, receipt
}

func tsoGuardWatermark(t *testing.T, f *GuardedTSOStateMachine, ts uint64) ([]byte, raftbootstrap.GuardedCommand) {
	t.Helper()
	policy, err := readAppliedBootstrapPolicy(t.Context(), f.backing, f.identity, 8)
	require.NoError(t, err)
	witness, err := policy.Witness()
	require.NoError(t, err)
	command := raftbootstrap.GuardedCommand{Kind: raftbootstrap.ControlGuardedWatermark, Witness: witness, OperationID: raftbootstrap.OperationID{5}, Inner: EncodeWatermarkAdvance(ts)}
	payload, err := EncodeGuardedBootstrapCommand(command)
	require.NoError(t, err)
	return payload, command
}

func TestTSOGuardActivationWatermarkAndExactReplay(t *testing.T) {
	for name, factory := range tsoControlStores(t) {
		t.Run(name, func(t *testing.T) {
			st := factory(t)
			f, command := tsoGuardFixture(t, tsoTestBacking(t, st))
			activation, first := activateTSOGuard(t, f, command)
			payload, _ := tsoGuardWatermark(t, f, 30<<hlcLogicalBits)
			r, ok := applyTSOGuard(t, f, 9, payload).(raftbootstrap.GuardedReceipt)
			require.True(t, ok)
			require.Equal(t, raftbootstrap.GuardedReceiptApplied, r.Verdict)
			require.Equal(t, uint64(30<<hlcLogicalBits), r.ReturnedWatermark)
			require.Equal(t, int64(30), f.ceilingMs.Load())
			require.Equal(t, uint64(3), f.PhaseDFloor())
			require.Equal(t, uint64(3), f.PrepareEvidenceProtocolFloor())
			require.True(t, f.CutoverActive())
			require.True(t, f.PhaseDActive())
			require.NoError(t, f.backing.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
				return DeletePreparedBootstrapActivation(ctx, f.backing, f.identity, first.ManifestDigest)
			}))
			f.SetBootstrapActivationNativeProof(nil)
			require.Equal(t, first, applyTSOGuard(t, f, 8, activation))
			require.Equal(t, r, applyTSOGuard(t, f, 9, payload))
			require.Zero(t, st.LastCommitTS())
			raw, err := f.backing.ReadRaftControlMetadata(t.Context(), []byte(tsoControlStateKey))
			require.NoError(t, err)
			c, index, configuration, err := decodeTSOControlState(raw)
			require.NoError(t, err)
			require.Equal(t, uint64(9), index)
			require.Equal(t, uint64(3), configuration)
			require.Equal(t, f.AllocationFloor(), c.AllocationFloor())
		})
	}
}

func TestTSOGuardRejectsStaleAndReceiptConflict(t *testing.T) {
	f, activation := tsoGuardFixture(t, tsoTestBacking(t, store.NewMVCCStore()))
	activateTSOGuard(t, f, activation)
	_, command := tsoGuardWatermark(t, f, 30<<hlcLogicalBits)
	command.Witness.Generation++
	payload, err := EncodeGuardedBootstrapCommand(command)
	require.NoError(t, err)
	r, ok := applyTSOGuard(t, f, 9, payload).(raftbootstrap.GuardedReceipt)
	require.True(t, ok)
	require.Equal(t, raftbootstrap.GuardedReceiptRejectedStaleWitness, r.Verdict)
	require.Equal(t, activation.Manifest.Watermark, f.AllocationFloor())
	require.Equal(t, int64(20), f.ceilingMs.Load())
	f.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(9, 3, 0, payload))
	require.ErrorIs(t, requireTSOHaltError(t, f.Apply(payload)), ErrGuardedBootstrapReceiptConflict)
	f.ClearApplyEntryIdentity()
	require.ErrorIs(t, requireTSOHaltError(t, f.Apply(payload)), ErrGuardedBootstrapReceiptConflict)
	f.SetApplyIndex(10)
	require.ErrorIs(t, requireTSOHaltError(t, f.Apply(payload)), ErrGuardedBootstrapReceiptConflict)
}

func TestTSOGuardActivationRequiresNativeProof(t *testing.T) {
	f, command := tsoGuardFixture(t, tsoTestBacking(t, store.NewMVCCStore()))
	payload, err := raftbootstrap.EncodeActivationCommand(command)
	require.NoError(t, err)
	f.SetBootstrapActivationNativeProof(nil)
	require.ErrorIs(t, requireTSOHaltError(t, applyTSOGuard(t, f, 8, payload)), ErrBootstrapActivationNativeProof)
	key, err := BootstrapPolicyMetadataKey(f.identity)
	require.NoError(t, err)
	_, err = f.backing.ReadRaftControlMetadata(t.Context(), key)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Equal(t, command.Manifest.Watermark, f.AllocationFloor())
}

func TestTSOGuardCommitFailurePublishesNothing(t *testing.T) {
	backing := tsoTestBacking(t, store.NewMVCCStore())
	f, command := tsoGuardFixture(t, backing)
	activateTSOGuard(t, f, command)
	payload, _ := tsoGuardWatermark(t, f, 30<<hlcLogicalBits)
	f.backing = tsoFailCommitBacking{TSODurableBacking: backing}
	require.Error(t, requireTSOHaltError(t, applyTSOGuard(t, f, 9, payload)))
	require.Equal(t, command.Manifest.Watermark, f.AllocationFloor())
	require.Equal(t, int64(20), f.ceilingMs.Load())
	identity := raftbootstrap.EntryIdentity{Cluster: f.identity.Cluster, Group: 0, Index: 9, Term: 2, Digest: raftbootstrap.Digest(raftengine.NewAppliedEntryIdentity(9, 2, 0, payload).Digest)}
	key, err := raftbootstrap.ReceiptKey(identity)
	require.NoError(t, err)
	_, err = backing.ReadRaftControlMetadata(t.Context(), key)
	require.True(t, errors.Is(err, store.ErrKeyNotFound))
}

func TestTSOGuardActivationCommitFailureIsAtomic(t *testing.T) {
	backing := tsoTestBacking(t, store.NewMVCCStore())
	f, command := tsoGuardFixture(t, backing)
	payload, err := raftbootstrap.EncodeActivationCommand(command)
	require.NoError(t, err)
	f.backing = tsoFailCommitBacking{TSODurableBacking: backing}
	require.Error(t, requireTSOHaltError(t, applyTSOGuard(t, f, 8, payload)))
	key, err := BootstrapPolicyMetadataKey(f.identity)
	require.NoError(t, err)
	_, err = backing.ReadRaftControlMetadata(t.Context(), key)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	raw, err := backing.ReadRaftControlMetadata(t.Context(), []byte(tsoControlStateKey))
	require.NoError(t, err)
	_, index, _, err := decodeTSOControlState(raw)
	require.NoError(t, err)
	require.Equal(t, uint64(7), index)
}

func TestTSOGuardReplayAfterNewPolicyAndAlreadyActive(t *testing.T) {
	f, command := tsoGuardFixture(t, tsoTestBacking(t, store.NewMVCCStore()))
	activation, first := activateTSOGuard(t, f, command)
	payload, guarded := tsoGuardWatermark(t, f, 30<<hlcLogicalBits)
	receipt := applyTSOGuard(t, f, 9, payload)
	policy, err := readAppliedBootstrapPolicy(t.Context(), f.backing, f.identity, 9)
	require.NoError(t, err)
	policy.Generation++
	raw, err := raftbootstrap.MarshalPolicyRecord(policy)
	require.NoError(t, err)
	key, err := BootstrapPolicyMetadataKey(f.identity)
	require.NoError(t, err)
	require.Nil(t, f.ApplyWithControl(t.Context(), 10, nil, func(ctx context.Context, metadata store.RaftControlMetadataStore) error {
		return metadata.StageRaftControlMetadata(ctx, key, raw)
	}))
	f.SetBootstrapActivationNativeProof(nil)
	require.Equal(t, first, applyTSOGuard(t, f, 8, activation))
	require.Equal(t, receipt, applyTSOGuard(t, f, 9, payload))
	guarded.Inner = EncodeWatermarkAdvance(40 << hlcLogicalBits)
	stale, err := EncodeGuardedBootstrapCommand(guarded)
	require.NoError(t, err)
	r, ok := applyTSOGuard(t, f, 11, stale).(raftbootstrap.GuardedReceipt)
	require.True(t, ok)
	require.Equal(t, raftbootstrap.GuardedReceiptRejectedStaleWitness, r.Verdict)
	r2, ok := applyTSOGuard(t, f, 12, activation).(raftbootstrap.ActivationReceipt)
	require.True(t, ok)
	require.Equal(t, raftbootstrap.ActivationReceiptAlreadyActive, r2.Verdict)
	require.Equal(t, uint64(30<<hlcLogicalBits), f.AllocationFloor())
}

func tsoActivationNativeImage(command raftbootstrap.ActivationCommand) raftengine.NativeConfiguration {
	c := command.Manifest.Configuration
	native := raftengine.NativeConfiguration{Index: c.Index, Voters: c.State.Voters, VotersOutgoing: c.State.VotersOutgoing,
		Learners: c.State.Learners, LearnersNext: c.State.LearnersNext, AutoLeave: c.State.AutoLeave}
	for _, member := range c.Members {
		native.Members = append(native.Members, raftengine.NativeConfigurationMember{ID: member.ID, Address: member.Address})
	}
	return native
}

func TestTSOGuardActivationFromDeliveredNativeImage(t *testing.T) {
	f, command := tsoGuardFixture(t, tsoTestBacking(t, store.NewMVCCStore()))
	f.SetBootstrapActivationNativeProof(nil)
	payload, err := raftbootstrap.EncodeActivationCommand(command)
	require.NoError(t, err)
	image := tsoActivationNativeImage(command)
	f.SetApplyNativeConfiguration(image)
	image.Voters[0] = 999
	image.Members[0].Address = "wrong:9000"
	r, ok := applyTSOGuard(t, f, 8, payload).(raftbootstrap.ActivationReceipt)
	require.True(t, ok)
	require.Equal(t, raftbootstrap.ActivationReceiptApplied, r.Verdict)
	require.Nil(t, f.pendingNativeConfiguration)
	// Replay has an exact receipt and needs neither a current image nor a callback.
	require.Equal(t, r, applyTSOGuard(t, f, 8, payload))
}

func TestTSOGuardRejectsInconsistentNativeImageWithoutFallback(t *testing.T) {
	for _, kind := range []string{"index", "roles", "address", "missing-member", "unknown-member", "autoleave", "cleared"} {
		t.Run(kind, func(t *testing.T) {
			f, command := tsoGuardFixture(t, tsoTestBacking(t, store.NewMVCCStore()))
			image := tsoActivationNativeImage(command)
			switch kind {
			case "index":
				image.Index++
			case "roles":
				image.Voters = nil
				image.Learners = []uint64{1}
			case "address":
				image.Members[0].Address = "wrong:9000"
			case "missing-member":
				image.Members = nil
			case "unknown-member":
				image.Members[0].ID = 999
			case "autoleave":
				image.AutoLeave = true
			case "cleared":
				f.SetBootstrapActivationNativeProof(nil)
			}
			f.SetApplyNativeConfiguration(image)
			if kind == "cleared" {
				f.ClearApplyNativeConfiguration()
			}
			payload, err := raftbootstrap.EncodeActivationCommand(command)
			require.NoError(t, err)
			require.Error(t, requireTSOHaltError(t, applyTSOGuard(t, f, 8, payload)))
			key, err := BootstrapPolicyMetadataKey(f.identity)
			require.NoError(t, err)
			_, err = f.backing.ReadRaftControlMetadata(t.Context(), key)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestTSOGuardNativeImageOwnershipAndClear(t *testing.T) {
	f, _ := tsoGuardFixture(t, tsoTestBacking(t, store.NewMVCCStore()))
	image := raftengine.NativeConfiguration{Index: 3, Voters: []uint64{1}, VotersOutgoing: []uint64{2}, Learners: []uint64{3}, LearnersNext: []uint64{4},
		Members: []raftengine.NativeConfigurationMember{{ID: 1, Address: "one"}}}
	f.SetApplyNativeConfiguration(image)
	image.Voters[0] = 10
	image.VotersOutgoing[0] = 20
	image.Learners[0] = 30
	image.LearnersNext[0] = 40
	image.Members[0].ID = 99
	require.Equal(t, []uint64{1}, f.pendingNativeConfiguration.Voters)
	require.Equal(t, []uint64{2}, f.pendingNativeConfiguration.VotersOutgoing)
	require.Equal(t, []uint64{3}, f.pendingNativeConfiguration.Learners)
	require.Equal(t, []uint64{4}, f.pendingNativeConfiguration.LearnersNext)
	require.Equal(t, uint64(1), f.pendingNativeConfiguration.Members[0].ID)
	f.SetApplyIndex(8)
	require.Nil(t, f.pendingNativeConfiguration)
	f.SetApplyNativeConfiguration(image)
	f.ClearApplyEntryIdentity()
	require.Nil(t, f.pendingNativeConfiguration)
}

func TestTSOGuardRejectsReceiptVariantCollision(t *testing.T) {
	f, command := tsoGuardFixture(t, tsoTestBacking(t, store.NewMVCCStore()))
	activation, applied := activateTSOGuard(t, f, command)
	payload, guarded := tsoGuardWatermark(t, f, 30<<hlcLogicalBits)
	native := raftengine.NewAppliedEntryIdentity(9, 2, 0, payload)
	receipt := applied
	receipt.Entry = raftbootstrap.EntryIdentity{Cluster: f.identity.Cluster, Index: 9, Term: 2, Digest: raftbootstrap.Digest(native.Digest)}
	receipt.Verdict = raftbootstrap.ActivationReceiptAlreadyActive
	raw, err := raftbootstrap.EncodeActivationReceipt(receipt)
	require.NoError(t, err)
	key, err := raftbootstrap.ReceiptKey(receipt.Entry)
	require.NoError(t, err)
	require.NoError(t, f.backing.ApplyRaftEntry(t.Context(), 9, func(ctx context.Context) error {
		return f.backing.StageRaftControlMetadata(ctx, key, raw)
	}))
	require.ErrorIs(t, requireTSOHaltError(t, applyTSOGuard(t, f, 9, payload)), ErrGuardedBootstrapReceiptConflict)
	wrongGuard := raftbootstrap.GuardedReceipt{Entry: applied.Entry, Kind: raftbootstrap.ControlGuardedWatermark,
		Verdict: raftbootstrap.GuardedReceiptApplied, OperationID: guarded.OperationID, Witness: guarded.Witness, RequestedWatermark: 1, ReturnedWatermark: 1}
	raw, err = raftbootstrap.EncodeGuardedReceipt(wrongGuard)
	require.NoError(t, err)
	key, err = raftbootstrap.ReceiptKey(wrongGuard.Entry)
	require.NoError(t, err)
	require.NoError(t, f.backing.ApplyRaftEntry(t.Context(), 10, func(ctx context.Context) error {
		return f.backing.StageRaftControlMetadata(ctx, key, raw)
	}))
	require.ErrorIs(t, requireTSOHaltError(t, applyTSOGuard(t, f, 8, activation)), ErrGuardedBootstrapReceiptConflict)
}

func TestTSOGuardSnapshotRestoresReceipts(t *testing.T) {
	for name, factory := range tsoControlStores(t) {
		t.Run(name, func(t *testing.T) {
			f, command := tsoGuardFixture(t, tsoTestBacking(t, factory(t)))
			activation, r1 := activateTSOGuard(t, f, command)
			payload, _ := tsoGuardWatermark(t, f, 30<<hlcLogicalBits)
			r2 := applyTSOGuard(t, f, 9, payload)
			snapshot, err := f.Snapshot()
			require.NoError(t, err)
			var image bytes.Buffer
			_, err = snapshot.WriteTo(&image)
			require.NoError(t, err)
			require.NoError(t, snapshot.Close())
			state, err := NewDurableTSOStateMachine(NewHLC(), tsoTestBacking(t, factory(t)))
			require.NoError(t, err)
			restored, err := NewGuardedTSOStateMachine(state, f.identity, nil)
			require.NoError(t, err)
			require.NoError(t, restored.Restore(&image))
			require.Equal(t, r1, applyTSOGuard(t, restored, 8, activation))
			require.Equal(t, r2, applyTSOGuard(t, restored, 9, payload))
			require.Equal(t, f.AllocationFloor(), restored.AllocationFloor())
			require.Equal(t, f.AppliedConfigurationIndex(), restored.AppliedConfigurationIndex())
		})
	}
}

package kv

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func activationApplyFixture(t *testing.T) (raftbootstrap.ActivationCommand, PreparedBootstrapActivation, BootstrapActivationNativeProof) {
	t.Helper()
	return activationApplyFixtureAtWatermark(t, 0)
}

func TestBootstrapActivationPolicySeparatesBaselineAndPreparationDigests(t *testing.T) {
	t.Parallel()
	command, prepared, proof := activationApplyFixture(t)
	policy, err := NewBootstrapActivationPolicy(proof.Identity, command.Manifest, command.Manifest.CommitIndex+1)
	require.NoError(t, err)
	baseline, err := raftbootstrap.ConfigurationDigest(command.Manifest.Configuration)
	require.NoError(t, err)
	require.Equal(t, baseline, policy.BaselineDigest, "P and WAL baseline identity use the original configuration digest")
	require.NotEqual(t, prepared.ManifestDigest, policy.BaselineDigest, "WAL-v2 PreparationDigest separately binds the full signed manifest")
	command.Manifest.Members[0].Signature[0] ^= 1
	_, err = NewBootstrapActivationPolicy(proof.Identity, command.Manifest, command.Manifest.CommitIndex+1)
	require.Error(t, err, "preserve complete baseline certificate validation")
}

func activationApplyFixtureAtWatermark(t *testing.T, watermark uint64) (raftbootstrap.ActivationCommand, PreparedBootstrapActivation, BootstrapActivationNativeProof) {
	t.Helper()
	c := raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion,
		Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 7}, OperationID: raftbootstrap.OperationID{2},
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, CommitIndex: 10, CommitTerm: 3, Watermark: watermark,
		DataDigest: raftbootstrap.Digest{4}, Configuration: raftbootstrap.Configuration{Index: 8, State: raftbootstrap.ConfState{Voters: []uint64{1, 2}}}}
	keys := make([]ed25519.PrivateKey, 2)
	for i := range keys {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		keys[i] = key
		der, err := x509.MarshalPKIXPublicKey(key.Public())
		require.NoError(t, err)
		c.Configuration.Members = append(c.Configuration.Members, raftbootstrap.Member{ID: uint64(i + 1), Address: "node:9000", PublicKeyDigest: sha256.Sum256(der)})
	}
	digest, err := raftbootstrap.ConfigurationDigest(c.Configuration)
	require.NoError(t, err)
	manifest := raftbootstrap.BaselineManifest{BaselineContext: c}
	var local raftbootstrap.BaselineProbe
	for i, key := range keys {
		probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: c, Evidence: raftbootstrap.BaselineMemberEvidence{
			MemberID: uint64(i + 1), CommitIndex: 10, CommitTerm: 3, AppliedIndex: 10, LastLogIndex: 10,
			ConfigurationDigest: digest, NativeStateDigest: raftbootstrap.Digest{byte(i + 1)}, DataDigest: c.DataDigest, Watermark: watermark}}, key)
		require.NoError(t, err)
		manifest.Members = append(manifest.Members, probe.Evidence)
		if i == 0 {
			local = probe
		}
	}
	manifestDigest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	command := raftbootstrap.ActivationCommand{Witness: raftbootstrap.Witness{Cluster: c.Identity.Cluster, Group: c.Identity.Group,
		ConfigurationIndex: c.Configuration.Index, ConfigurationDigest: digest}, Manifest: manifest}
	command.Readiness.ManifestDigest = manifestDigest
	for i, key := range keys {
		ack, err := raftbootstrap.SignPreparedMemberAck(raftbootstrap.PreparedMemberAck{
			MemberID: uint64(i + 1), ManifestDigest: manifestDigest, Minimum: c.Minimum,
			PreparedStateDigest: raftbootstrap.Digest{byte(i + 1), 9}}, manifest, key)
		require.NoError(t, err)
		command.Readiness.Members = append(command.Readiness.Members, ack)
	}
	return command, PreparedBootstrapActivation{Probe: local, ManifestDigest: manifestDigest},
		BootstrapActivationNativeProof{Identity: c.Identity, Configuration: c.Configuration}
}

func TestBootstrapActivationRejectsMissingOrBaselineOnlyReadiness(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"baseline_only", "missing_member", "baseline_signature"} {
		t.Run(scenario, func(t *testing.T) {
			command, prepared, proof := activationApplyFixture(t)
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			metadata := activationMetadata(t, st)
			require.NoError(t, StagePreparedBootstrapActivation(t.Context(), metadata, proof.Identity, prepared))
			before := activationStoreSnapshot(t, st)
			payload, err := raftbootstrap.EncodeActivationCommand(command)
			require.NoError(t, err)
			switch scenario {
			case "baseline_only":
				manifest, err := raftbootstrap.MarshalBaselineManifest(command.Manifest)
				require.NoError(t, err)
				// Keep the valid manifest but omit the mandatory readiness frame.
				const controlHeaderSize = 9
				payload = payload[:controlHeaderSize+raftbootstrap.WitnessSize+4+len(manifest)]
				binary.BigEndian.PutUint32(payload[5:9], uint32(len(payload)-controlHeaderSize)) // #nosec G115 -- encoded payload is bounded.
				command.Readiness = raftbootstrap.PreparedCertificate{}
			case "missing_member":
				command.Readiness.Members = command.Readiness.Members[:1]
			case "baseline_signature":
				command.Readiness.Members[0].Signature = command.Manifest.Members[0].Signature
			}
			_, err = raftbootstrap.EncodeActivationCommand(command)
			require.Error(t, err)
			entry := raftbootstrap.EntryIdentity{Cluster: proof.Identity.Cluster, Group: proof.Identity.Group, Index: 11}
			err = metadata.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
				_, err := StageBootstrapActivation(ctx, metadata, entry, command, proof, 0)
				return err
			})
			require.Error(t, err, "exported stage helper must also reject invalid readiness")
			if scenario == "baseline_only" {
				fsm := activationFSM(t, st, proof.Identity, proof)
				fsm.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(11, 4, 0, payload))
				haltApplyError(t, fsm.Apply(payload))
			}
			require.Equal(t, before, activationStoreSnapshot(t, st), "no P, R or applied index may escape")
		})
	}
}

func activationMetadata(t *testing.T, st store.MVCCStore) store.RaftControlMetadataStore {
	t.Helper()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	return metadata
}

func activationFSM(t *testing.T, st store.MVCCStore, identity raftbootstrap.Identity, proof BootstrapActivationNativeProof) *kvFSM {
	t.Helper()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC(), WithRaftBootstrapIdentity(identity.Cluster, identity.Group),
		WithBootstrapActivationNativeProof(func(context.Context) (BootstrapActivationNativeProof, error) { return proof, nil })).(*kvFSM)
	require.True(t, ok)
	return fsm
}

func activationApply(t *testing.T, fsm *kvFSM, command raftbootstrap.ActivationCommand, index uint64) any {
	t.Helper()
	payload, err := raftbootstrap.EncodeActivationCommand(command)
	require.NoError(t, err)
	fsm.SetApplyEntryIdentity(raftengine.NewAppliedEntryIdentity(index, 4, 0, payload))
	return fsm.Apply(payload)
}

func requireActivationReceipt(t *testing.T, response any) raftbootstrap.ActivationReceipt {
	t.Helper()
	receipt, ok := response.(raftbootstrap.ActivationReceipt)
	require.True(t, ok, "response: %#v", response)
	return receipt
}

func TestBootstrapActivationAtomicApplyAndReplay(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			command, prepared, proof := activationApplyFixture(t)
			metadata := activationMetadata(t, st)
			require.NoError(t, StagePreparedBootstrapActivation(t.Context(), metadata, proof.Identity, prepared))
			fsm := activationFSM(t, st, proof.Identity, proof)
			receipt := requireActivationReceipt(t, activationApply(t, fsm, command, 11))
			require.Equal(t, raftbootstrap.ActivationReceiptApplied, receipt.Verdict)
			requireGuardedNewIndex(t, st, 11)
			require.Zero(t, st.LastCommitTS())
			policy, err := readAppliedBootstrapPolicy(t.Context(), metadata, proof.Identity, 11)
			require.NoError(t, err)
			require.Equal(t, uint64(11), policy.ActivationIndex)
			require.Equal(t, uint64(1), policy.Generation)
			require.Equal(t, command.Witness.ConfigurationDigest, policy.BaselineDigest)
			require.NotEqual(t, prepared.ManifestDigest, policy.BaselineDigest)
			digest, err := raftbootstrap.PolicyDigest(policy)
			require.NoError(t, err)
			require.Equal(t, digest, receipt.PolicyDigest)
			key, err := bootstrapActivationMetadataKey("B/activation/", proof.Identity)
			require.NoError(t, err)
			baseline, err := metadata.ReadRaftControlMetadata(t.Context(), key)
			require.NoError(t, err)
			manifest, err := raftbootstrap.ParseBaselineManifest(baseline)
			require.NoError(t, err)
			require.NoError(t, raftbootstrap.VerifyBaselineManifest(manifest, prepared.Probe.BaselineContext))
			require.NoError(t, metadata.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
				return DeletePreparedBootstrapActivation(ctx, metadata, proof.Identity, prepared.ManifestDigest)
			}))
			fsm.bootstrapActivationNativeProof = nil
			require.Equal(t, receipt, activationApply(t, fsm, command, 11), "replay must not require current proof")
			rejected := requireActivationReceipt(t, activationApply(t, fsm, command, 12))
			require.Equal(t, raftbootstrap.ActivationReceiptAlreadyActive, rejected.Verdict)
			require.Equal(t, digest, rejected.PolicyDigest)
			requireGuardedNewIndex(t, st, 12)
		})
	}
}

func TestBootstrapActivationNeverTrustsIncomingManifest(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"no_preparation", "wrong_manifest_digest", "wrong_native_identity", "changed_native_configuration", "no_native_getter", "changed_watermark", "future_cut"} {
		t.Run(scenario, func(t *testing.T) { testActivationUntrusted(t, scenario) })
	}
}

func testActivationUntrusted(t *testing.T, scenario string) {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	command, prepared, proof := activationApplyFixture(t)
	identity := proof.Identity
	if scenario == "wrong_manifest_digest" {
		prepared.ManifestDigest[0] ^= 1
	}
	if scenario != "no_preparation" {
		require.NoError(t, StagePreparedBootstrapActivation(t.Context(), activationMetadata(t, st), identity, prepared))
	}
	switch scenario {
	case "wrong_native_identity":
		proof.Identity.Group++
	case "changed_native_configuration":
		proof.Configuration.Index++
	case "changed_watermark":
		require.NoError(t, st.PutAt(t.Context(), []byte("user"), []byte("changed"), 1, 0))
	}
	fsm := activationFSM(t, st, identity, proof)
	if scenario == "no_native_getter" {
		fsm.bootstrapActivationNativeProof = nil
	}
	index := uint64(11)
	if scenario == "future_cut" {
		index = 10
	}
	err := haltApplyError(t, activationApply(t, fsm, command, index))
	require.ErrorIs(t, err, ErrGuardedBootstrapApply)
	key, err := BootstrapPolicyMetadataKey(identity)
	require.NoError(t, err)
	_, err = activationMetadata(t, st).ReadRaftControlMetadata(t.Context(), key)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestBootstrapActivationPreparationExplicitAndImmutable(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			_, prepared, proof := activationApplyFixture(t)
			metadata := activationMetadata(t, st)
			require.NoError(t, StagePreparedBootstrapActivation(t.Context(), metadata, proof.Identity, prepared))
			require.NoError(t, StagePreparedBootstrapActivation(t.Context(), metadata, proof.Identity, prepared))
			got, err := ReadPreparedBootstrapActivation(t.Context(), metadata, proof.Identity)
			require.NoError(t, err)
			require.Equal(t, prepared.ManifestDigest, got.ManifestDigest)
			changed := prepared
			changed.ManifestDigest[0] ^= 1
			require.ErrorIs(t, StagePreparedBootstrapActivation(t.Context(), metadata, proof.Identity, changed), ErrBootstrapActivationPreparation)
			require.Zero(t, st.LastCommitTS())
			err = metadata.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
				return DeletePreparedBootstrapActivation(ctx, metadata, proof.Identity, changed.ManifestDigest)
			})
			require.ErrorIs(t, err, ErrBootstrapActivationPreparation)
			require.NoError(t, metadata.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
				return DeletePreparedBootstrapActivation(ctx, metadata, proof.Identity, prepared.ManifestDigest)
			}))
			_, err = ReadPreparedBootstrapActivation(t.Context(), metadata, proof.Identity)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestPreparedBootstrapActivationMetadataMatchesStaging(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			_, prepared, proof := activationApplyFixture(t)
			metadata := activationMetadata(t, st)
			before := activationStoreSnapshot(t, st)
			key, value, err := PreparedBootstrapActivationMetadata(proof.Identity, prepared)
			require.NoError(t, err)
			require.Equal(t, before, activationStoreSnapshot(t, st), "encoding must not change the store")
			expectedKey, err := bootstrapActivationMetadataKey("J/activation-prepared/", proof.Identity)
			require.NoError(t, err)
			require.Equal(t, expectedKey, key)
			require.NoError(t, StagePreparedBootstrapActivation(t.Context(), metadata, proof.Identity, prepared))
			stored, err := metadata.ReadRaftControlMetadata(t.Context(), key)
			require.NoError(t, err)
			require.Equal(t, value, stored)
			key[0], value[0] = 0, 0
			freshKey, freshValue, err := PreparedBootstrapActivationMetadata(proof.Identity, prepared)
			require.NoError(t, err)
			require.Equal(t, expectedKey, freshKey)
			require.Equal(t, stored, freshValue, "returned buffers must not alias preparation or persisted bytes")
			require.NoError(t, metadata.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
				return StagePreparedBootstrapActivationInEntry(ctx, metadata, proof.Identity, prepared)
			}))
		})
	}
}

func TestPreparedBootstrapActivationMetadataPreservesValidation(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"foreign_identity", "zero_digest", "invalid_signature"} {
		t.Run(scenario, func(t *testing.T) {
			_, prepared, proof := activationApplyFixture(t)
			switch scenario {
			case "foreign_identity":
				proof.Identity.Group++
			case "zero_digest":
				prepared.ManifestDigest = raftbootstrap.Digest{}
			case "invalid_signature":
				prepared.Probe.Evidence.Signature[0] ^= 1
			}
			_, originalErr := encodePreparedBootstrapActivation(proof.Identity, prepared)
			require.Error(t, originalErr)
			key, value, err := PreparedBootstrapActivationMetadata(proof.Identity, prepared)
			require.EqualError(t, err, originalErr.Error())
			if scenario == "invalid_signature" {
				require.ErrorIs(t, err, raftbootstrap.ErrInvalid)
			} else {
				require.ErrorIs(t, err, ErrBootstrapActivationPreparation)
			}
			require.Nil(t, key)
			require.Nil(t, value)
		})
	}
}

func TestBootstrapActivationReceiptPrecedesParserAndOtherVariantConflicts(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	command, prepared, proof := activationApplyFixture(t)
	metadata := activationMetadata(t, st)
	require.NoError(t, StagePreparedBootstrapActivation(t.Context(), metadata, proof.Identity, prepared))
	fsm := activationFSM(t, st, proof.Identity, proof)
	receipt := requireActivationReceipt(t, activationApply(t, fsm, command, 11))
	payload, err := raftbootstrap.EncodeActivationCommand(command)
	require.NoError(t, err)
	conflict := receipt.Entry
	conflict.Digest[0] ^= 1
	_, _, err = fsm.readBootstrapReceiptVariant(t.Context(), conflict, payload)
	require.ErrorIs(t, err, ErrGuardedBootstrapReceiptConflict)
	key, err := raftbootstrap.ReceiptKey(receipt.Entry)
	require.NoError(t, err)
	raw, err := metadata.ReadRaftControlMetadata(t.Context(), key)
	require.NoError(t, err)
	_, _, err = matchBootstrapReceiptVariant(raw, receipt.Entry, []byte{0x1f, 'B', 'C', 1, byte(raftbootstrap.ControlGuardedCatalog)})
	require.ErrorIs(t, err, ErrGuardedBootstrapReceiptConflict)
	guarded := raftbootstrap.GuardedReceipt{Entry: receipt.Entry, Kind: raftbootstrap.ControlGuardedCatalog,
		Verdict: raftbootstrap.GuardedReceiptApplied, OperationID: receipt.OperationID,
		Witness: raftbootstrap.Witness{Cluster: proof.Identity.Cluster, Group: proof.Identity.Group, ActivationIndex: 9, Generation: 1,
			ConfigurationIndex: command.Witness.ConfigurationIndex, ConfigurationDigest: command.Witness.ConfigurationDigest}}
	guardRaw, err := raftbootstrap.EncodeGuardedReceipt(guarded)
	require.NoError(t, err)
	_, _, err = matchBootstrapReceiptVariant(guardRaw, receipt.Entry, payload)
	require.ErrorIs(t, err, ErrGuardedBootstrapReceiptConflict)
	// A historical receipt is authoritative even if today's parser rejects the
	// original bytes. Deliberately seed that exact identity at a later slot.
	malformed := []byte{0x1f}
	pending := raftengine.NewAppliedEntryIdentity(12, 4, 0, malformed)
	receipt.Entry.Index, receipt.Entry.Digest = 12, raftbootstrap.Digest(pending.Digest)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 12, func(ctx context.Context) error {
		return stageBootstrapActivationReceipt(ctx, metadata, receipt)
	}))
	fsm.SetApplyEntryIdentity(pending)
	require.Equal(t, receipt, fsm.Apply(malformed))
}

func TestBootstrapActivationFailureRollsBackPolicyBaselineReceipt(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			for _, fault := range []string{"receipt-stage", "commit"} {
				t.Run(fault, func(t *testing.T) {
					st := backend.new(t)
					command, prepared, proof := activationApplyFixture(t)
					metadata := activationMetadata(t, st)
					require.NoError(t, StagePreparedBootstrapActivation(t.Context(), metadata, proof.Identity, prepared))
					wrapped := newGuardedFaultStore(t, st)
					wrapped.fault = fault
					fsm := activationFSM(t, wrapped, proof.Identity, proof)
					err := haltApplyError(t, activationApply(t, fsm, command, 11))
					require.ErrorIs(t, err, wrapped.cause)
					requireActivationAbsent(t, metadata, proof.Identity)
					require.Zero(t, st.LastCommitTS())
				})
			}
		})
	}
}

func requireActivationAbsent(t *testing.T, metadata store.RaftControlMetadataStore, identity raftbootstrap.Identity) {
	t.Helper()
	policyKey, err := BootstrapPolicyMetadataKey(identity)
	require.NoError(t, err)
	baselineKey, err := bootstrapActivationMetadataKey("B/activation/", identity)
	require.NoError(t, err)
	for _, key := range [][]byte{policyKey, baselineKey} {
		_, err := metadata.ReadRaftControlMetadata(t.Context(), key)
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

func TestBootstrapActivationSnapshotParity(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			command, prepared, proof := activationApplyFixture(t)
			require.NoError(t, StagePreparedBootstrapActivation(t.Context(), activationMetadata(t, st), proof.Identity, prepared))
			fsm := activationFSM(t, st, proof.Identity, proof)
			receipt := requireActivationReceipt(t, activationApply(t, fsm, command, 11))
			snapshot, err := fsm.Snapshot()
			require.NoError(t, err)
			var raw bytes.Buffer
			_, err = snapshot.WriteTo(&raw)
			require.NoError(t, err)
			require.NoError(t, snapshot.Close())
			clone := activationFSM(t, backend.new(t), proof.Identity, proof)
			require.NoError(t, clone.Restore(&raw))
			clone.bootstrapActivationNativeProof = nil
			require.Equal(t, receipt, activationApply(t, clone, command, 11))
			requireGuardedNewIndex(t, clone.store, 11)
			metadata := activationMetadata(t, clone.store)
			policy, err := readAppliedBootstrapPolicy(t.Context(), metadata, proof.Identity, 11)
			require.NoError(t, err)
			digest, err := raftbootstrap.PolicyDigest(policy)
			require.NoError(t, err)
			require.Equal(t, receipt.PolicyDigest, digest)
		})
	}
}

func TestBootstrapActivationNativeErrorRetainsCause(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	command, prepared, proof := activationApplyFixture(t)
	require.NoError(t, StagePreparedBootstrapActivation(t.Context(), activationMetadata(t, st), proof.Identity, prepared))
	fsm := activationFSM(t, st, proof.Identity, proof)
	cause := errors.New("native configuration read failed")
	fsm.bootstrapActivationNativeProof = func(context.Context) (BootstrapActivationNativeProof, error) {
		return BootstrapActivationNativeProof{}, cause
	}
	err := haltApplyError(t, activationApply(t, fsm, command, 11))
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, ErrBootstrapActivationNativeProof)
}

func TestBootstrapActivationSharedStageUsesSuppliedScalarWatermark(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			command, prepared, proof := activationApplyFixtureAtWatermark(t, 37)
			metadata := activationMetadata(t, st)
			require.NoError(t, StagePreparedBootstrapActivation(t.Context(), metadata, proof.Identity, prepared))
			payload, err := raftbootstrap.EncodeActivationCommand(command)
			require.NoError(t, err)
			entry := raftengine.NewAppliedEntryIdentity(11, 4, 0, payload)
			identity := raftbootstrap.EntryIdentity{Cluster: proof.Identity.Cluster, Group: proof.Identity.Group,
				Index: 11, Term: 4, Digest: raftbootstrap.Digest(entry.Digest)}
			applier, ok := st.(store.RaftEntryApplier)
			require.True(t, ok)
			cause := errors.New("scalar commit failed")
			err = applier.ApplyRaftEntry(t.Context(), 11, func(ctx context.Context) error {
				_, err := StageBootstrapActivation(ctx, metadata, identity, command, proof, 37)
				require.NoError(t, err)
				return cause
			})
			require.ErrorIs(t, err, cause)
			requireActivationAbsent(t, metadata, proof.Identity)
			require.NoError(t, applier.ApplyRaftEntry(t.Context(), 11, func(ctx context.Context) error {
				receipt, err := StageBootstrapActivation(ctx, metadata, identity, command, proof, 37)
				require.Equal(t, raftbootstrap.ActivationReceiptApplied, receipt.Verdict)
				return err
			}))
			require.Zero(t, st.LastCommitTS(), "TSO floor must not become a KV data watermark")
			requireGuardedNewIndex(t, st, 11)
		})
	}
}

type activationStageFaultStore struct {
	*guardedNewFaultStore
	prefix []byte
}

func (s *activationStageFaultStore) StageRaftControlMetadata(ctx context.Context, key, value []byte) error {
	if bytes.HasPrefix(key, s.prefix) {
		return s.cause
	}
	return s.guardedNewFaultStore.StageRaftControlMetadata(ctx, key, value)
}

func activationStoreSnapshot(t *testing.T, st store.MVCCStore) []byte {
	t.Helper()
	snapshot, err := st.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	return raw.Bytes()
}

func TestBootstrapActivationEveryMetadataStageAbortsAtomically(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			for _, prefix := range []string{"B/activation/", "P/", "R/"} {
				t.Run(prefix, func(t *testing.T) {
					st := backend.new(t)
					command, prepared, proof := activationApplyFixture(t)
					require.NoError(t, StagePreparedBootstrapActivation(t.Context(), activationMetadata(t, st), proof.Identity, prepared))
					before := activationStoreSnapshot(t, st)
					wrapped := &activationStageFaultStore{guardedNewFaultStore: newGuardedFaultStore(t, st), prefix: []byte(prefix)}
					fsm := activationFSM(t, wrapped, proof.Identity, proof)
					err := haltApplyError(t, activationApply(t, fsm, command, 11))
					require.ErrorIs(t, err, wrapped.cause)
					require.Equal(t, before, activationStoreSnapshot(t, st), "all metadata/data/index state must remain unchanged")
				})
			}
		})
	}
}

func activationNativeImage(proof BootstrapActivationNativeProof) raftengine.NativeConfiguration {
	c := proof.Configuration
	image := raftengine.NativeConfiguration{Index: c.Index, Voters: append([]uint64(nil), c.State.Voters...),
		VotersOutgoing: append([]uint64(nil), c.State.VotersOutgoing...), Learners: append([]uint64(nil), c.State.Learners...),
		LearnersNext: append([]uint64(nil), c.State.LearnersNext...), AutoLeave: c.State.AutoLeave}
	for _, member := range c.Members {
		image.Members = append(image.Members, raftengine.NativeConfigurationMember{ID: member.ID, Address: member.Address})
	}
	return image
}

func TestBootstrapActivationNativeImageWithoutRuntimeGetter(t *testing.T) {
	t.Parallel()
	for _, backend := range guardedNewApplyStores() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.new(t)
			command, prepared, proof := activationApplyFixture(t)
			require.NoError(t, StagePreparedBootstrapActivation(t.Context(), activationMetadata(t, st), proof.Identity, prepared))
			fsm := activationFSM(t, st, proof.Identity, proof)
			fsm.bootstrapActivationNativeProof = nil
			var aware raftengine.ApplyNativeConfigurationAware = fsm
			image := activationNativeImage(proof)
			aware.SetApplyNativeConfiguration(image)
			image.Voters[0] = 99
			image.Members[0].Address = "mutated-after-delivery:9000"
			receipt := requireActivationReceipt(t, activationApply(t, fsm, command, 11))
			require.Equal(t, raftbootstrap.ActivationReceiptApplied, receipt.Verdict)
			require.Nil(t, fsm.pendingNativeConfiguration, "native proof must expire after one Apply")
			aware.SetApplyNativeConfiguration(raftengine.NativeConfiguration{})
			require.Equal(t, receipt, activationApply(t, fsm, command, 11), "historical receipt precedes native validation")
			require.Nil(t, fsm.pendingNativeConfiguration)
			aware.SetApplyNativeConfiguration(activationNativeImage(proof))
			fsm.SetApplyIndex(13)
			require.Nil(t, fsm.pendingNativeConfiguration, "weak index provenance must discard native proof")
		})
	}
}

func TestBootstrapActivationInvalidNativeImageNeverFallsBack(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*raftengine.NativeConfiguration)
	}{
		{name: "configuration_index", change: func(c *raftengine.NativeConfiguration) { c.Index++ }},
		{name: "address", change: func(c *raftengine.NativeConfiguration) { c.Members[0].Address = "foreign:9000" }},
		{name: "member_identity", change: func(c *raftengine.NativeConfiguration) { c.Members[0].ID = 99 }},
		{name: "duplicate_member", change: func(c *raftengine.NativeConfiguration) { c.Members[1] = c.Members[0] }},
		{name: "role", change: func(c *raftengine.NativeConfiguration) { c.Learners = []uint64{2}; c.Voters = []uint64{1} }},
		{name: "auto_leave", change: func(c *raftengine.NativeConfiguration) { c.AutoLeave = true }},
		{name: "empty", change: func(c *raftengine.NativeConfiguration) { *c = raftengine.NativeConfiguration{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			command, prepared, proof := activationApplyFixture(t)
			require.NoError(t, StagePreparedBootstrapActivation(t.Context(), activationMetadata(t, st), proof.Identity, prepared))
			fsm := activationFSM(t, st, proof.Identity, proof)
			fallbacks := 0
			fsm.bootstrapActivationNativeProof = func(context.Context) (BootstrapActivationNativeProof, error) { fallbacks++; return proof, nil }
			image := activationNativeImage(proof)
			tc.change(&image)
			fsm.SetApplyNativeConfiguration(image)
			err := haltApplyError(t, activationApply(t, fsm, command, 11))
			require.ErrorIs(t, err, ErrBootstrapActivationNativeProof)
			require.Zero(t, fallbacks)
			require.Nil(t, fsm.pendingNativeConfiguration)
			requireActivationAbsent(t, activationMetadata(t, st), proof.Identity)
		})
	}
}

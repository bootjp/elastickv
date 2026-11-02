package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func encryptionAdmissionPayloads() [][]byte {
	registration := fsmwire.RegistrationPayload{DEKID: 7, FullNodeID: 9, LocalEpoch: 1}
	payloads := [][]byte{
		append([]byte{fsmwire.OpRegistration}, fsmwire.EncodeRegistration(registration)...),
		append([]byte{fsmwire.OpBootstrap}, fsmwire.EncodeBootstrap(fsmwire.BootstrapPayload{StorageDEKID: 7, RaftDEKID: 8, WrappedStorage: []byte("storage"), WrappedRaft: []byte("raft"), BatchRegistry: []fsmwire.RegistrationPayload{registration}})...),
	}
	for _, subtag := range []byte{fsmwire.RotateSubRotateDEK, fsmwire.RotateSubEnableStorageEnvelope, fsmwire.RotateSubEnableRaftEnvelope} {
		payloads = append(payloads, append([]byte{fsmwire.OpRotation}, fsmwire.EncodeRotation(fsmwire.RotationPayload{SubTag: subtag, DEKID: 7, Purpose: fsmwire.PurposeStorage, ProposerRegistration: registration})...))
	}
	return payloads
}

func TestEncryptionAdmissionCompletelyDecodesImplementedEffects(t *testing.T) {
	for _, payload := range encryptionAdmissionPayloads() {
		changed, err := BootstrapApplyChangesAdmission(t.Context(), nil, raftbootstrap.Identity{}, payload)
		require.NoError(t, err)
		require.True(t, changed)
		for _, invalid := range [][]byte{payload[:1], append(bytes.Clone(payload), 0), append([]byte{payload[0], 0xff}, payload[2:]...)} {
			changed, err := BootstrapApplyChangesAdmission(t.Context(), nil, raftbootstrap.Identity{}, invalid)
			require.Error(t, err)
			require.False(t, changed)
		}
	}
}

func TestEncryptionAdmissionDoesNotTreatReservedOrTSOTagsAsPermission(t *testing.T) {
	for _, payload := range [][]byte{nil, {0x06}, {0x07}, []byte("\x07TSOF\x01\x00\x00\x00\x00\x00\x00\x00\x2a"), {0, 1}, {2, 1}} {
		changed, err := EncryptionApplyChangesAdmission(payload)
		require.NoError(t, err)
		require.False(t, changed)
	}
	payload := append([]byte{fsmwire.OpRotation}, fsmwire.EncodeRotation(fsmwire.RotationPayload{SubTag: fsmwire.RotateSubRotateDEK, Purpose: fsmwire.PurposeStorage})...)
	payload[2] = 0xff
	changed, err := EncryptionApplyChangesAdmission(payload)
	require.ErrorIs(t, err, fsmwire.ErrFSMWireSubtag)
	require.False(t, changed)
}

func TestEncryptionAdmissionPreservesActivationNoop(t *testing.T) {
	command, _, proof := activationApplyFixture(t)
	payload, err := raftbootstrap.EncodeActivationCommand(command)
	require.NoError(t, err)
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	changed, err := BootstrapApplyChangesAdmission(t.Context(), st, proof.Identity, payload)
	require.NoError(t, err)
	require.True(t, changed)
	policy, err := NewBootstrapActivationPolicy(proof.Identity, command.Manifest, command.Manifest.CommitIndex+1)
	require.NoError(t, err)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), policy.ActivationIndex, func(ctx context.Context) error {
		stageGuardedNewPolicy(t, ctx, st, policy)
		return nil
	}))
	changed, err = BootstrapApplyChangesAdmission(t.Context(), st, proof.Identity, payload)
	require.NoError(t, err)
	require.False(t, changed, "already-active deterministic rejection must not revoke healthy sessions")
}

package etcd

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

type admissionEncryptionApplier struct{ effect func() }

func (a admissionEncryptionApplier) ApplyRegistration(fsmwire.RegistrationPayload) error {
	a.effect()
	return nil
}
func (a admissionEncryptionApplier) ApplyBootstrap(uint64, fsmwire.BootstrapPayload) error {
	a.effect()
	return nil
}
func (a admissionEncryptionApplier) ApplyRotation(uint64, fsmwire.RotationPayload) error {
	a.effect()
	return nil
}

func TestNativeEncryptionAdmissionRevokesBeforeDecryptedEffects(t *testing.T) {
	for _, payload := range nativeEncryptionAdmissionPayloads() {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%02x/%v", payload[0], encrypted), func(t *testing.T) {
				a := &recordingAdmission{}
				parentClosed, applied := false, false
				st := store.NewMVCCStore()
				t.Cleanup(func() { require.NoError(t, st.Close()) })
				fsm := kv.NewKvFSMWithHLC(st, nil, kv.WithEncryption(admissionEncryptionApplier{effect: func() {
					require.True(t, a.mu.TryLock(), "FSM effects must not execute under A")
					require.True(t, a.suspended)
					a.mu.Unlock()
					require.True(t, parentClosed, "permission generation closes before first effect")
					applied = true
				}}))
				e := identityElectionEngine(t, 1)
				e.raftCutoverIndex = orInertCutover(nil)
				e.fsm, e.nativeAdmission, e.recoveredIdentity = fsm, a, &WALIdentity{}
				entry := raftpb.Entry{Index: uint64Ptr(17), Term: uint64Ptr(5), Type: entryTypePtr(raftpb.EntryNormal), Data: encodeProposalEnvelope(7, payload)}
				if encrypted {
					cipher, kid := raftCipherFixture(t)
					e.raftCipher, e.raftCutoverIndex = cipher, func() uint64 { return 10 }
					entry = envelopeEntry(t, cipher, kid, 17, payload)
					entry.Term = uint64Ptr(5)
				}
				e.beforeAdmissionChange = func(identity raftengine.AppliedEntryIdentity, cleartext []byte) error {
					require.True(t, a.mu.TryLock(), "projection handoff must run outside A")
					require.True(t, a.suspended)
					a.mu.Unlock()
					require.False(t, applied)
					require.Equal(t, payload, cleartext)
					require.Equal(t, raftengine.NewAppliedEntryIdentity(17, 5, int32(raftpb.EntryNormal), payload), identity)
					parentClosed = true
					return nil
				}
				response, err := e.applyNormalEntry(entry, false)
				require.NoError(t, err)
				require.Nil(t, response)
				require.True(t, applied)
			})
		}
	}
}

func nativeEncryptionAdmissionPayloads() [][]byte {
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

func TestNativeEncryptionAdmissionRejectsBeforeEffects(t *testing.T) {
	for _, failure := range []string{"malformed", "handoff", "unwrap"} {
		t.Run(failure, func(t *testing.T) {
			applied := false
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			e := identityElectionEngine(t, 1)
			e.raftCutoverIndex = orInertCutover(nil)
			e.fsm = kv.NewKvFSMWithHLC(st, nil, kv.WithEncryption(admissionEncryptionApplier{effect: func() { applied = true }}))
			e.nativeAdmission, e.recoveredIdentity = &recordingAdmission{}, &WALIdentity{}
			payload := append([]byte{fsmwire.OpRegistration}, fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{DEKID: 7})...)
			entry := raftpb.Entry{Index: uint64Ptr(17), Term: uint64Ptr(5), Type: entryTypePtr(raftpb.EntryNormal), Data: encodeProposalEnvelope(7, payload)}
			handoff := false
			e.beforeAdmissionChange = func(raftengine.AppliedEntryIdentity, []byte) error {
				handoff = true
				return errors.New("projection unavailable")
			}
			switch failure {
			case "malformed":
				entry.Data = encodeProposalEnvelope(7, append(bytes.Clone(payload), 0))
			case "unwrap":
				e.raftCutoverIndex = func() uint64 { return 10 }
			}
			_, err := e.applyNormalEntry(entry, false)
			require.Error(t, err)
			require.False(t, applied)
			require.Equal(t, failure == "handoff", handoff)
		})
	}
}

func TestNativeEncryptionAdmissionDoesNotRevokeDroppedDuplicate(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	e := newTestEngine(kv.NewKvFSMWithHLC(st, nil), nil, nil)
	a := &recordingAdmission{}
	e.nativeAdmission, e.recoveredIdentity = a, &WALIdentity{Identity: raftbootstrap.Identity{Group: 1}}
	payload := append([]byte{fsmwire.OpRegistration}, fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{DEKID: 7})...)
	entry := raftpb.Entry{Index: uint64Ptr(17), Type: entryTypePtr(raftpb.EntryNormal), Data: encodeProposalEnvelope(7, payload)}
	_, err := e.applyNormalEntry(entry, true)
	require.NoError(t, err)
	require.False(t, a.suspended)
}

func TestNativeEncryptionAdmissionRequiresProtectedVerifierAndPreservesLegacy(t *testing.T) {
	payload := append([]byte{fsmwire.OpRegistration}, fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{DEKID: 7})...)
	entry := raftpb.Entry{Index: uint64Ptr(17), Type: entryTypePtr(raftpb.EntryNormal), Data: encodeProposalEnvelope(7, payload)}
	bare := &entryIdentityFSM{}
	e := newTestEngine(bare, nil, nil)
	e.recoveredIdentity = &WALIdentity{}
	_, err := e.applyNormalEntry(entry, false)
	require.ErrorIs(t, err, raftengine.ErrNativeAdmissionDenied)
	require.Empty(t, bare.payload)
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	applied := false
	legacy := newTestEngine(kv.NewKvFSMWithHLC(st, nil, kv.WithEncryption(admissionEncryptionApplier{effect: func() { applied = true }})), nil, nil)
	_, err = legacy.applyNormalEntry(entry, false)
	require.NoError(t, err)
	require.True(t, applied)
}

package main

import (
	"crypto/sha256"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func bootstrapEncryptionDomainWiring(f *encryptionRuntimeIOFixture) encryptionwiring.WriteWiring {
	w := f.cfg.Wiring
	w.NonceFactory = encryption.NewDeterministicNonceFactory(123, 7)
	w.RaftNonceFactory = f.runtime.NonceFactory
	w.RaftRegistration = f.runtime.Registration
	return w
}

func bootstrapEncryptionDomainRequest(enabled, requested bool, path string) noderuntime.BootstrapEncryptionRequest {
	return encryptionwiring.Flags{Enabled: enabled, RotateOnStartup: requested, SidecarPath: path}.BootstrapRequest()
}

func TestBootstrapEncryptionDomainRejectsMissingActualWiring(t *testing.T) {
	for _, tc := range []struct {
		name              string
		enabled, rotation bool
		path              string
	}{
		{"enabled", true, false, ""}, {"rotation", false, true, ""}, {"sidecar", false, false, "keys.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := bootstrapEncryptionDomainRequest(tc.enabled, tc.rotation, tc.path)
			_, err := newBootstrapDomain(raftbootstrap.UUID{1}, []noderuntime.GroupSpec{{ID: 1}}, request)
			require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionUnsupported)
			_, err = newBootstrapDomain(raftbootstrap.UUID{1}, []noderuntime.GroupSpec{{ID: 1}}, request, encryptionwiring.WriteWiring{})
			require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionUnsupported)
		})
	}
}

func newEncryptedDomainFixture(t *testing.T) (*bootstrapDomain, *encryptionRuntimeIOFixture) {
	t.Helper()
	f := newEncryptionRuntimeIOFixture(t)
	request := bootstrapEncryptionDomainRequest(true, true, f.cfg.SidecarPath)
	d, err := newBootstrapDomain(raftbootstrap.UUID{1}, []noderuntime.GroupSpec{{ID: 1}}, request, bootstrapEncryptionDomainWiring(f))
	require.NoError(t, err)
	t.Cleanup(d.admission.Close)
	bootstrapIngressObserve(t, d.admission, 1)
	ticket, err := d.admission.Ticket(1)
	require.NoError(t, err)
	configuration := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1}}, Members: []raftbootstrap.Member{{ID: 1, Address: "localhost:9000", PublicKeyDigest: raftbootstrap.Digest{1}}}}
	digest, err := raftbootstrap.ConfigurationDigest(configuration)
	require.NoError(t, err)
	require.NoError(t, d.admission.PublishRecovery(ticket, raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion,
		Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1}, BaselineDigest: digest, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		ActivationIndex: 1, Generation: 1, LastConfigurationIndex: 1, Configuration: configuration}, 1, raftadmission.RegistrationVerified))
	return d, f
}

func TestBootstrapEncryptionDomainAttachAndActualInspection(t *testing.T) {
	d, f := newEncryptedDomainFixture(t)
	plain := encryptionwiring.RegistrationEntry(2, 123, 9)
	final, err := f.group.RaftPayloadWrap()(plain)
	require.NoError(t, err)
	_, err = d.inspectPayload(raftadmission.Registration, 1, final, raftbootstrap.Digest{})
	require.ErrorIs(t, err, encryptionwiring.ErrBootstrapEncryptionProjectionPending)
	require.NoError(t, d.attachEncryptionGroup(1, f.group))
	proof, err := d.inspectPayload(raftadmission.Registration, 1, final, raftbootstrap.Digest{})
	require.NoError(t, err)
	require.Equal(t, raftbootstrap.Digest(sha256.Sum256(plain)), proof.PlaintextDigest)
	require.NotZero(t, proof.ProjectionGeneration)
	gen := proof.ProjectionGeneration
	require.NoError(t, d.attachEncryptionGroup(1, f.group))
	after, err := d.admission.EncryptionFence()
	require.NoError(t, err)
	require.Equal(t, gen, after, "same-group attach must not continuously invalidate")
	_, err = d.inspectPayload(raftadmission.Registration, 1, plain, raftbootstrap.Digest{})
	require.Error(t, err, "active envelope may not fall back to plaintext")
	final[len(final)-1] ^= 1
	_, err = d.inspectPayload(raftadmission.Registration, 1, final, raftbootstrap.Digest{})
	require.Error(t, err)
}

func TestBootstrapEncryptionDomainRecipeGrantsAreExactAndIsolated(t *testing.T) {
	d, f := newEncryptedDomainFixture(t)
	require.NoError(t, d.attachEncryptionGroup(1, f.group))
	makeRecipe := func(id uint32) *bootstrapRotationRecipe {
		wrapped, err := f.cfg.Wrapper.Wrap(make([]byte, encryption.KeySize))
		require.NoError(t, err)
		r := &bootstrapRotationRecipe{config: bootstrapRotationRecipeConfig{NodeID: 123, StorageEpoch: 7, RaftEpoch: 9}, baseline: encryption.ActiveKeys{Storage: 1, Raft: 2}, requests: []*pb.RotateDEKRequest{
			{Purpose: pb.RotateDEKRequest_PURPOSE_STORAGE, NewDekId: id, WrappedNewDek: wrapped, ProposerNodeId: 123, ProposerLocalEpoch: 7},
		}}
		require.NoError(t, r.hash())
		return r
	}
	first, second := makeRecipe(3), makeRecipe(4)
	require.NoError(t, d.authorizeRotationRecipe(first))
	require.NoError(t, d.authorizeRotationRecipe(first), "exact same recipe retry is idempotent")
	plain, err := bootstrapRotationPayloadFromRequest(first.Requests()[0])
	require.NoError(t, err)
	final, err := f.group.RaftPayloadWrap()(plain)
	require.NoError(t, err)
	_, err = d.inspectPayload(raftadmission.Rotation, 1, final, first.Digest())
	require.NoError(t, err)
	for _, digest := range []raftbootstrap.Digest{{}, second.Digest(), {9}} {
		_, err = d.inspectPayload(raftadmission.Rotation, 1, final, digest)
		require.Error(t, err, "a grant for another recipe must not authorize this payload")
	}
	first.requests[0].WrappedNewDek[0] ^= 1
	require.ErrorIs(t, d.authorizeRotationRecipe(first), errBootstrapRotationRecipe, "tampered recipe may not reuse committed digest")
	_, err = d.inspectPayload(raftadmission.Rotation, 1, final, first.Digest())
	require.NoError(t, err, "stored grant is immutable and does not alias recipe bytes")
	require.NoError(t, d.authorizeRotationRecipe(second))
	_, err = d.inspectPayload(raftadmission.Rotation, 1, final, first.Digest())
	require.Error(t, err, "replacing current recipe drops old grants rather than accumulating them")
	secondPlain, err := bootstrapRotationPayloadFromRequest(second.Requests()[0])
	require.NoError(t, err)
	secondFinal, err := f.group.RaftPayloadWrap()(secondPlain)
	require.NoError(t, err)
	_, err = d.inspectPayload(raftadmission.Rotation, 1, secondFinal, second.Digest())
	require.NoError(t, err)
	require.Len(t, d.encryption.grants, 1)
}

func TestBootstrapEncryptionDomainPlaintextCompatibility(t *testing.T) {
	d, err := newBootstrapDomain(raftbootstrap.UUID{1}, []noderuntime.GroupSpec{{ID: 1}}, noderuntime.BootstrapEncryptionRequest{}, encryptionwiring.WriteWiring{})
	require.NoError(t, err)
	defer d.admission.Close()
	require.Nil(t, d.encryption)
	require.NoError(t, d.attachEncryptionGroup(1, nil))
}

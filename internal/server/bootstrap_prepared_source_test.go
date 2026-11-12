package server

import (
	"crypto"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func bootstrapPreparedSourceFixture(t *testing.T) (kv.PreparedBootstrapActivation, etcd.WALIdentity, []byte, *clusterbootstrap.BootstrapPeerConfig) {
	t.Helper()
	manifest, configs := bootstrapArtifactsFixture(t)
	artifact, err := clusterbootstrap.AssembleBootstrapActivation(manifest, manifest.BaselineContext, bootstrapArtifactAcks(t, manifest, configs))
	require.NoError(t, err)
	prepared, err := clusterbootstrap.BootstrapLocalPreparation(configs[0].Local.Member, manifest)
	require.NoError(t, err)
	configuration, err := raftbootstrap.ConfigurationDigest(manifest.Configuration)
	require.NoError(t, err)
	expected := etcd.WALIdentity{Identity: manifest.Identity, NodeID: configs[1].Local.Member,
		BaselineDigest: configuration, PreparationDigest: prepared.ManifestDigest}
	return prepared, expected, artifact, configs[0]
}

func TestPreparedSnapshotSourceRequiresFullIndependentArtifact(t *testing.T) {
	t.Parallel()
	prepared, expected, artifact, _ := bootstrapPreparedSourceFixture(t)
	require.NotEqual(t, prepared.Probe.Evidence.MemberID, expected.NodeID)
	require.Error(t, clusterbootstrap.ValidatePreparedBootstrapIdentity(prepared, expected), "original local-preparation check remains strict")
	require.NoError(t, clusterbootstrap.ValidateRuntimePreparedBootstrapIdentity(prepared, expected, artifact))
	require.Error(t, clusterbootstrap.ValidateRuntimePreparedBootstrapIdentity(prepared, expected, nil), "source probe alone is not local preparation authority")
	command, matched, err := raftbootstrap.ParseActivationCommand(artifact)
	require.True(t, matched)
	require.NoError(t, err)
	manifestOnly, err := raftbootstrap.MarshalBaselineManifest(command.Manifest)
	require.NoError(t, err)
	require.Error(t, clusterbootstrap.ValidateRuntimePreparedBootstrapIdentity(prepared, expected, manifestOnly), "baseline signatures cannot substitute for final readiness")
	command.Readiness.Members = command.Readiness.Members[:len(command.Readiness.Members)-1]
	_, err = raftbootstrap.EncodeActivationCommand(command)
	require.Error(t, err, "missing even one current member cannot produce an accepted artifact")
}

func TestPreparedSnapshotSourceRejectsConflictingBindings(t *testing.T) {
	t.Parallel()
	changes := map[string]func(*kv.PreparedBootstrapActivation, *etcd.WALIdentity, []byte){
		"manifest digest": func(p *kv.PreparedBootstrapActivation, _ *etcd.WALIdentity, _ []byte) { p.ManifestDigest[0]++ },
		"WAL digest":      func(_ *kv.PreparedBootstrapActivation, w *etcd.WALIdentity, _ []byte) { w.PreparationDigest[0]++ },
		"v1 WAL": func(_ *kv.PreparedBootstrapActivation, w *etcd.WALIdentity, _ []byte) {
			w.PreparationDigest = raftbootstrap.Digest{}
		},
		"WAL member":        func(_ *kv.PreparedBootstrapActivation, w *etcd.WALIdentity, _ []byte) { w.NodeID = 9999 },
		"WAL group":         func(_ *kv.PreparedBootstrapActivation, w *etcd.WALIdentity, _ []byte) { w.Identity.Group++ },
		"WAL cluster":       func(_ *kv.PreparedBootstrapActivation, w *etcd.WALIdentity, _ []byte) { w.Identity.Cluster[0]++ },
		"WAL configuration": func(_ *kv.PreparedBootstrapActivation, w *etcd.WALIdentity, _ []byte) { w.BaselineDigest[0]++ },
		"source context":    func(p *kv.PreparedBootstrapActivation, _ *etcd.WALIdentity, _ []byte) { p.Probe.CommitIndex++ },
		"source signature": func(p *kv.PreparedBootstrapActivation, _ *etcd.WALIdentity, _ []byte) {
			p.Probe.Evidence.Signature[0]++
		},
		"readiness signature": func(_ *kv.PreparedBootstrapActivation, _ *etcd.WALIdentity, a []byte) { a[len(a)-1]++ },
	}
	for kind, change := range changes {
		t.Run(kind, func(t *testing.T) {
			prepared, expected, artifact, _ := bootstrapPreparedSourceFixture(t)
			change(&prepared, &expected, artifact)
			require.Error(t, clusterbootstrap.ValidateRuntimePreparedBootstrapIdentity(prepared, expected, artifact))
		})
	}
}

func TestPreparedSnapshotSourceRequiresExactSignedEvidence(t *testing.T) {
	t.Parallel()
	prepared, expected, artifact, source := bootstrapPreparedSourceFixture(t)
	prepared.Probe.Evidence.NativeStateDigest[0]++
	signer, ok := source.ClientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	var err error
	prepared.Probe, err = raftbootstrap.SignBaselineProbe(prepared.Probe, signer)
	require.NoError(t, err, "individually valid source signature is insufficient if exact manifest evidence differs")
	require.Error(t, clusterbootstrap.ValidateRuntimePreparedBootstrapIdentity(prepared, expected, artifact))
}

func TestPreparedSnapshotLocalCompatibilityDoesNotWeakenMemberCheck(t *testing.T) {
	t.Parallel()
	prepared, expected, _, _ := bootstrapPreparedSourceFixture(t)
	expected.NodeID = prepared.Probe.Evidence.MemberID
	require.NoError(t, clusterbootstrap.ValidateRuntimePreparedBootstrapIdentity(prepared, expected, nil))
	expected.PreparationDigest[0]++
	require.Error(t, clusterbootstrap.ValidateRuntimePreparedBootstrapIdentity(prepared, expected, nil))
	expected.PreparationDigest = raftbootstrap.Digest{}
	require.NoError(t, clusterbootstrap.ValidatePreparedBootstrapIdentity(prepared, expected), "original local helper compatibility is unchanged")
}

func TestPreparedSnapshotCannotPublishWithoutVerifiedLocalWAL(t *testing.T) {
	t.Parallel()
	_, expected, artifact, _ := bootstrapPreparedSourceFixture(t)
	err := clusterbootstrap.PublishPreparedBootstrapRecovery(nil, raftadmission.RecoveryTicket{}, nil, nil, nil, expected, etcd.RecoveredState{}, artifact)
	require.ErrorIs(t, err, etcd.ErrWALIdentity, "even a complete artifact cannot mint native lineage or readiness")
}

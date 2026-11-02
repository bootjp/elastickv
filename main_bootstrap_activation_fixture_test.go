package main

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
)

// Test-only final preparation attestations use real baseline-pinned signers,
// but a separate readiness signature domain. They do not perform disk migration.
func bootstrapActivationFixture(t testing.TB, group uint64) raftbootstrap.ActivationCommand {
	t.Helper()
	_, _, raw, signers := baselineCLIFixtureWithSigners(t)
	manifest, err := raftbootstrap.ParseBaselineManifest(raw)
	require.NoError(t, err)
	manifest.Identity.Group = group
	for i, signer := range signers {
		probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{
			BaselineContext: manifest.BaselineContext, Evidence: manifest.Members[i],
		}, signer)
		require.NoError(t, err)
		manifest.Members[i] = probe.Evidence
	}
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	ready := raftbootstrap.PreparedCertificate{ManifestDigest: digest}
	for i, member := range manifest.Configuration.Members {
		ack, err := raftbootstrap.SignPreparedMemberAck(raftbootstrap.PreparedMemberAck{
			MemberID: member.ID, ManifestDigest: digest, Minimum: manifest.Minimum,
			PreparedStateDigest: sha256.Sum256([]byte(fmt.Sprintf("fixture final preparation %d", member.ID))),
		}, manifest, signers[i])
		require.NoError(t, err)
		ready.Members = append(ready.Members, ack)
	}
	require.NoError(t, raftbootstrap.VerifyPreparedCertificate(ready, manifest))
	configuration, err := raftbootstrap.ConfigurationDigest(manifest.Configuration)
	require.NoError(t, err)
	return raftbootstrap.ActivationCommand{Manifest: manifest, Readiness: ready, Witness: raftbootstrap.Witness{
		Cluster: manifest.Identity.Cluster, Group: group, ConfigurationIndex: manifest.Configuration.Index,
		ConfigurationDigest: configuration,
	}}
}

func TestBootstrapActivationFixtureRequiresDistinctAllMemberReadiness(t *testing.T) {
	command := bootstrapActivationFixture(t, 17)
	require.Len(t, command.Readiness.Members, 4)
	for i, ack := range command.Readiness.Members {
		require.Equal(t, command.Manifest.Members[i].MemberID, ack.MemberID)
		require.Equal(t, command.Manifest.Members[i].PublicKeyDER, ack.PublicKeyDER)
		require.NotEqual(t, command.Manifest.Members[i].Signature, ack.Signature)
	}
	_, err := raftbootstrap.EncodeActivationCommand(command)
	require.NoError(t, err)
	command.Readiness.Members = command.Readiness.Members[:3]
	_, err = raftbootstrap.EncodeActivationCommand(command)
	require.Error(t, err, "missing learner readiness must not be supplied by old baseline probes")
}

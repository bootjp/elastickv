package raftbootstrap

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func activationFixture(t testing.TB) ActivationCommand {
	t.Helper()
	manifest, signers := baselineFixture(t)
	digest, err := ConfigurationDigest(manifest.Configuration)
	require.NoError(t, err)
	manifestDigest, err := BaselineManifestDigest(manifest)
	require.NoError(t, err)
	readiness := PreparedCertificate{ManifestDigest: manifestDigest}
	for i, signer := range signers {
		ack, err := SignPreparedMemberAck(PreparedMemberAck{MemberID: manifest.Members[i].MemberID,
			ManifestDigest: manifestDigest, Minimum: manifest.Minimum, PreparedStateDigest: Digest{byte(i + 1)}}, manifest, signer)
		require.NoError(t, err)
		readiness.Members = append(readiness.Members, ack)
	}
	return ActivationCommand{Manifest: manifest, Readiness: readiness, Witness: Witness{
		Cluster: manifest.Identity.Cluster, Group: manifest.Identity.Group,
		ConfigurationIndex: manifest.Configuration.Index, ConfigurationDigest: digest,
	}}
}

func TestActivationCommandRoundTrip(t *testing.T) {
	command := activationFixture(t)
	raw, err := EncodeActivationCommand(command)
	require.NoError(t, err)
	got, matched, err := ParseActivationCommand(raw)
	require.True(t, matched)
	require.NoError(t, err)
	require.Equal(t, command, got)
	_, _, err = ParseGuardedCommand(raw, func(ControlKind, []byte) error { return nil })
	require.Error(t, err, "activation must not be interpreted as an active-policy guarded write")
}

func TestActivationRequiresEveryMembersPostPreparationAcknowledgment(t *testing.T) {
	command := activationFixture(t)
	command.Readiness.Members = command.Readiness.Members[:len(command.Readiness.Members)-1]
	_, err := EncodeActivationCommand(command)
	require.Error(t, err, "a quorum or a complete pre-preparation baseline is not all-member readiness")
	command.Readiness = PreparedCertificate{}
	_, err = EncodeActivationCommand(command)
	require.Error(t, err)
}

func TestActivationCommandRejectsWrongWitness(t *testing.T) {
	for _, field := range []string{"cluster", "group", "activation", "generation", "configuration", "digest"} {
		t.Run(field, func(t *testing.T) {
			command := activationFixture(t)
			switch field {
			case "cluster":
				command.Witness.Cluster[0]++
			case "group":
				command.Witness.Group++
			case "activation":
				command.Witness.ActivationIndex = 1
			case "generation":
				command.Witness.Generation = 1
			case "configuration":
				command.Witness.ConfigurationIndex++
			case "digest":
				command.Witness.ConfigurationDigest[0]++
			}
			_, err := EncodeActivationCommand(command)
			require.Error(t, err)
		})
	}
}

func TestActivationCommandRejectsMalformedFrame(t *testing.T) {
	raw, err := EncodeActivationCommand(activationFixture(t))
	require.NoError(t, err)
	for _, mutation := range []func([]byte) []byte{
		func(b []byte) []byte { b[1] ^= 1; return b },
		func(b []byte) []byte { b[3]++; return b },
		func(b []byte) []byte { b[8]++; return b },
		func(b []byte) []byte { return b[:len(b)-1] },
		func(b []byte) []byte { return append(b, 0) },
		func(b []byte) []byte { b[len(b)-1] ^= 1; return b },
	} {
		_, matched, err := ParseActivationCommand(mutation(bytes.Clone(raw)))
		require.True(t, matched)
		require.Error(t, err)
	}
	_, matched, err := ParseActivationCommand([]byte("legacy"))
	require.False(t, matched)
	require.NoError(t, err)
}

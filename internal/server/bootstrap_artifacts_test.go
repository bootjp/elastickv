package server

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
)

func bootstrapArtifactsFixture(t testing.TB) (raftbootstrap.BaselineManifest, []*clusterbootstrap.BootstrapPeerConfig) {
	t.Helper()
	_, _, raw, signers := baselineCLIFixtureWithSigners(t)
	manifest, err := raftbootstrap.ParseBaselineManifest(raw)
	require.NoError(t, err)
	var provisions []peersession.Provision
	for _, member := range manifest.Configuration.Members {
		provisions = append(provisions, peersession.Provision{Node: peersession.Node{
			Cluster: manifest.Identity.Cluster, Member: member.ID, Address: member.Address, Key: member.PublicKeyDigest,
		}, Groups: []uint64{manifest.Identity.Group}})
	}
	configs := make([]*clusterbootstrap.BootstrapPeerConfig, len(signers))
	for i, signer := range signers {
		configs[i] = &clusterbootstrap.BootstrapPeerConfig{Local: provisions[i].Node, Provisions: provisions,
			ClientTLS: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{{PrivateKey: signer}}}}
	}
	return manifest, configs
}

func bootstrapArtifactAcks(t testing.TB, manifest raftbootstrap.BaselineManifest, configs []*clusterbootstrap.BootstrapPeerConfig) [][]byte {
	t.Helper()
	raw := make([][]byte, len(configs))
	for i, config := range configs {
		// Test-only input stands for a native final verifier's result, not a probe.
		ack, err := clusterbootstrap.SignBootstrapPreparedAck(config, manifest, config.Local.Member, raftbootstrap.Digest{byte(i + 1)})
		require.NoError(t, err)
		raw[i], err = clusterbootstrap.MarshalBootstrapPreparedAck(ack, manifest)
		require.NoError(t, err)
	}
	return raw
}

func TestBootstrapArtifactsAllMemberRoundTrip(t *testing.T) {
	t.Parallel()
	manifest, configs := bootstrapArtifactsFixture(t)
	raw := bootstrapArtifactAcks(t, manifest, configs)
	for i, data := range raw {
		ack, err := clusterbootstrap.ParseBootstrapPreparedAck(data, manifest)
		require.NoError(t, err)
		require.Equal(t, configs[i].Local.Member, ack.MemberID)
		require.Equal(t, raftbootstrap.Digest{byte(i + 1)}, ack.PreparedStateDigest)
		require.Equal(t, manifest.Members[i].PublicKeyDER, ack.PublicKeyDER)
		require.NotEqual(t, manifest.Members[i].Signature, ack.Signature)
		require.NotContains(t, string(data), "PRIVATE KEY")
		again, err := clusterbootstrap.MarshalBootstrapPreparedAck(ack, manifest)
		require.NoError(t, err)
		require.Equal(t, data, again)
	}
	encoded, err := clusterbootstrap.AssembleBootstrapActivation(manifest, manifest.BaselineContext, raw)
	require.NoError(t, err)
	command, matched, err := raftbootstrap.ParseActivationCommand(encoded)
	require.NoError(t, err)
	require.True(t, matched)
	require.Len(t, command.Readiness.Members, 4)
	require.Zero(t, command.Witness.ActivationIndex)
	require.Zero(t, command.Witness.Generation)
	require.Equal(t, manifest.Identity.Cluster, command.Witness.Cluster)
	require.NoError(t, raftbootstrap.VerifyPreparedCertificate(command.Readiness, manifest))
	// Imported acknowledgments may arrive in any order; canonical output is stable.
	raw[0], raw[3] = raw[3], raw[0]
	other, err := clusterbootstrap.AssembleBootstrapActivation(manifest, manifest.BaselineContext, raw)
	require.NoError(t, err)
	require.Equal(t, encoded, other)
}

func TestBootstrapArtifactsSignerRejectsUntrustedIdentity(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*clusterbootstrap.BootstrapPeerConfig, *raftbootstrap.BaselineManifest){
		"missing TLS": func(c *clusterbootstrap.BootstrapPeerConfig, _ *raftbootstrap.BaselineManifest) { c.ClientTLS = nil },
		"missing cert": func(c *clusterbootstrap.BootstrapPeerConfig, _ *raftbootstrap.BaselineManifest) {
			c.ClientTLS.Certificates = nil
		},
		"not signer": func(c *clusterbootstrap.BootstrapPeerConfig, _ *raftbootstrap.BaselineManifest) {
			c.ClientTLS.Certificates[0].PrivateKey = "not a signer"
		},
		"cluster": func(c *clusterbootstrap.BootstrapPeerConfig, _ *raftbootstrap.BaselineManifest) { c.Local.Cluster[0]++ },
		"member":  func(c *clusterbootstrap.BootstrapPeerConfig, _ *raftbootstrap.BaselineManifest) { c.Local.Member++ },
		"key pin": func(c *clusterbootstrap.BootstrapPeerConfig, _ *raftbootstrap.BaselineManifest) { c.Local.Key[0]++ },
		"address": func(c *clusterbootstrap.BootstrapPeerConfig, _ *raftbootstrap.BaselineManifest) {
			c.Local.Address = "foreign.test:1"
		},
		"roster missing": func(c *clusterbootstrap.BootstrapPeerConfig, _ *raftbootstrap.BaselineManifest) {
			c.Provisions = c.Provisions[:3]
		},
		"roster key": func(c *clusterbootstrap.BootstrapPeerConfig, _ *raftbootstrap.BaselineManifest) {
			c.Provisions[3].Node.Key[0]++
		},
		"wrong group":       func(_ *clusterbootstrap.BootstrapPeerConfig, m *raftbootstrap.BaselineManifest) { m.Identity.Group++ },
		"unsupported floor": func(_ *clusterbootstrap.BootstrapPeerConfig, m *raftbootstrap.BaselineManifest) { m.Minimum.Session++ },
		"bad manifest": func(_ *clusterbootstrap.BootstrapPeerConfig, m *raftbootstrap.BaselineManifest) {
			m.Members[0].Signature[0]++
		},
	} {
		t.Run(name, func(t *testing.T) {
			manifest, configs := bootstrapArtifactsFixture(t)
			change(configs[0], &manifest)
			_, err := clusterbootstrap.SignBootstrapPreparedAck(configs[0], manifest, 1, raftbootstrap.Digest{1})
			require.Error(t, err)
		})
	}
	manifest, configs := bootstrapArtifactsFixture(t)
	_, err := clusterbootstrap.SignBootstrapPreparedAck(nil, manifest, 1, raftbootstrap.Digest{1})
	require.Error(t, err)
	_, err = clusterbootstrap.SignBootstrapPreparedAck(configs[0], manifest, 1, raftbootstrap.Digest{})
	require.Error(t, err)
	configs[0].ClientTLS.Certificates[0].PrivateKey = configs[1].ClientTLS.Certificates[0].PrivateKey
	_, err = clusterbootstrap.SignBootstrapPreparedAck(configs[0], manifest, 1, raftbootstrap.Digest{1})
	require.Error(t, err)
}

func TestBootstrapArtifactsStrictJSON(t *testing.T) {
	t.Parallel()
	manifest, configs := bootstrapArtifactsFixture(t)
	valid := bootstrapArtifactAcks(t, manifest, configs)[0]
	for name, raw := range map[string][]byte{
		"empty": nil, "oversized": bytes.Repeat([]byte{' '}, clusterbootstrap.BootstrapPreparedAckLimit+1),
		"trailing object":  append(bytes.Clone(valid), []byte(" {}")...),
		"trailing byte":    append(bytes.Clone(valid), 'x'),
		"unknown":          bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"unknown":1`), 1),
		"duplicate":        bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"escaped alias":    bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"\u0076ersion":1`), 1),
		"case alias":       bytes.Replace(valid, []byte(`"version"`), []byte(`"Version"`), 1),
		"nested duplicate": bytes.Replace(valid, []byte(`"entry":1`), []byte(`"entry":1,"entry":1`), 1),
		"nested unknown":   bytes.Replace(valid, []byte(`"entry":1`), []byte(`"entry":1,"unknown":1`), 1),
		"nested missing":   bytes.Replace(valid, []byte(`"entry":1,`), nil, 1),
		"missing":          bytes.Replace(valid, []byte(`"version":1,`), nil, 1),
		"null":             bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":null`), 1),
		"wrong version":    bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":2`), 1),
		"numeric member":   bytes.Replace(valid, []byte(`"member_id":"1"`), []byte(`"member_id":1`), 1),
		"leading zero":     bytes.Replace(valid, []byte(`"member_id":"1"`), []byte(`"member_id":"01"`), 1),
		"null root":        []byte("null"), "array root": []byte("[]"), "bad utf8": {0xff},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := clusterbootstrap.ParseBootstrapPreparedAck(raw, manifest)
			require.Error(t, err)
		})
	}
}

func TestBootstrapArtifactsRejectsAlteredSignedFields(t *testing.T) {
	t.Parallel()
	manifest, configs := bootstrapArtifactsFixture(t)
	valid := bootstrapArtifactAcks(t, manifest, configs)[0]
	for name, change := range map[string]func(*clusterbootstrap.BootstrapPreparedAckJSON){
		"member":                func(r *clusterbootstrap.BootstrapPreparedAckJSON) { r.MemberID = "2" },
		"manifest":              func(r *clusterbootstrap.BootstrapPreparedAckJSON) { r.Manifest = strings.Repeat("01", 32) },
		"state":                 func(r *clusterbootstrap.BootstrapPreparedAckJSON) { r.PreparedState = strings.Repeat("02", 32) },
		"minimum":               func(r *clusterbootstrap.BootstrapPreparedAckJSON) { r.Minimum.Entry++ },
		"key":                   func(r *clusterbootstrap.BootstrapPreparedAckJSON) { r.PublicKeyDER[0]++ },
		"signature":             func(r *clusterbootstrap.BootstrapPreparedAckJSON) { r.Signature[0]++ },
		"baseline is not ready": func(r *clusterbootstrap.BootstrapPreparedAckJSON) { r.Signature = manifest.Members[0].Signature },
		"noncanonical digest":   func(r *clusterbootstrap.BootstrapPreparedAckJSON) { r.Manifest = strings.Repeat("AA", 32) },
	} {
		t.Run(name, func(t *testing.T) {
			var raw clusterbootstrap.BootstrapPreparedAckJSON
			require.NoError(t, json.Unmarshal(valid, &raw))
			change(&raw)
			data, err := json.Marshal(raw)
			require.NoError(t, err)
			_, err = clusterbootstrap.ParseBootstrapPreparedAck(data, manifest)
			require.Error(t, err)
		})
	}
	ack, err := clusterbootstrap.ParseBootstrapPreparedAck(valid, manifest)
	require.NoError(t, err)
	ack.Signature[0]++
	_, err = clusterbootstrap.MarshalBootstrapPreparedAck(ack, manifest)
	require.Error(t, err)
}

func TestBootstrapArtifactsAssemblerRejectsIncompleteOrForeignCut(t *testing.T) {
	t.Parallel()
	manifest, configs := bootstrapArtifactsFixture(t)
	acks := bootstrapArtifactAcks(t, manifest, configs)
	for name, imported := range map[string][][]byte{
		"none": nil, "missing learner": acks[:3], "extra": append(append([][]byte{}, acks...), acks[0]),
		"duplicate instead of learner": {acks[0], acks[1], acks[2], acks[0]},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := clusterbootstrap.AssembleBootstrapActivation(manifest, manifest.BaselineContext, imported)
			require.Error(t, err)
		})
	}
	for name, change := range map[string]func(*raftbootstrap.BaselineContext){
		"operation": func(c *raftbootstrap.BaselineContext) { c.OperationID[0]++ },
		"cluster":   func(c *raftbootstrap.BaselineContext) { c.Identity.Cluster[0]++ },
		"group":     func(c *raftbootstrap.BaselineContext) { c.Identity.Group++ },
		"cut":       func(c *raftbootstrap.BaselineContext) { c.CommitIndex++ },
		"data":      func(c *raftbootstrap.BaselineContext) { c.DataDigest[0]++ },
		"minimum":   func(c *raftbootstrap.BaselineContext) { c.Minimum.Entry++ },
	} {
		t.Run(name, func(t *testing.T) {
			expected := manifest.BaselineContext
			change(&expected)
			_, err := clusterbootstrap.AssembleBootstrapActivation(manifest, expected, acks)
			require.Error(t, err)
		})
	}
	foreign, foreignConfigs := bootstrapArtifactsFixture(t)
	foreignAcks := bootstrapArtifactAcks(t, foreign, foreignConfigs)
	_, err := clusterbootstrap.AssembleBootstrapActivation(foreign, manifest.BaselineContext, foreignAcks)
	require.Error(t, err, "self-consistent foreign keys are not operator authorization")
	_, err = clusterbootstrap.AssembleBootstrapActivation(manifest, manifest.BaselineContext, foreignAcks)
	require.Error(t, err)
}

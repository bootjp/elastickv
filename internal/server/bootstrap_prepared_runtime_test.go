package server

import (
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestPreparedBootstrapIdentityPinsMemberAndCompleteConfiguration(t *testing.T) {
	t.Parallel()
	_, _, raw := baselineCLIFixture(t)
	m, err := raftbootstrap.ParseBaselineManifest(raw)
	require.NoError(t, err)
	digest, err := raftbootstrap.ConfigurationDigest(m.Configuration)
	require.NoError(t, err)
	manifestDigest, err := raftbootstrap.BaselineManifestDigest(m)
	require.NoError(t, err)
	prepared := kv.PreparedBootstrapActivation{Probe: raftbootstrap.BaselineProbe{BaselineContext: m.BaselineContext, Evidence: m.Members[0]}, ManifestDigest: manifestDigest}
	identity := etcd.WALIdentity{Identity: m.Identity, NodeID: prepared.Probe.Evidence.MemberID, BaselineDigest: digest}
	require.NoError(t, clusterbootstrap.ValidatePreparedBootstrapIdentity(prepared, identity))
	wrongMember := identity
	wrongMember.NodeID++
	require.Error(t, clusterbootstrap.ValidatePreparedBootstrapIdentity(prepared, wrongMember))
	wrongGroup := identity
	wrongGroup.Identity.Group++
	require.Error(t, clusterbootstrap.ValidatePreparedBootstrapIdentity(prepared, wrongGroup))
	wrongConfig := identity
	wrongConfig.BaselineDigest[0]++
	require.Error(t, clusterbootstrap.ValidatePreparedBootstrapIdentity(prepared, wrongConfig))
}

func TestRecoveredPolicyMirrorsActivationWithoutDowngrade(t *testing.T) {
	t.Parallel()
	_, _, raw := baselineCLIFixture(t)
	m, err := raftbootstrap.ParseBaselineManifest(raw)
	require.NoError(t, err)
	digest, err := raftbootstrap.ConfigurationDigest(m.Configuration)
	require.NoError(t, err)
	files := raftbootstrap.NewStore(t.TempDir())
	intent := raftbootstrap.Intent{Identity: m.Identity, BaselineKind: raftbootstrap.Accepted,
		Baseline: m.Configuration, Minimum: m.Minimum}
	require.NoError(t, files.Publish(intent, raftbootstrap.Evidence{Identity: &m.Identity, BaselineDigest: &digest, RaftState: raftbootstrap.Intact}))
	policy := raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion, Identity: m.Identity,
		BaselineDigest: digest, Minimum: m.Minimum, ActivationIndex: m.CommitIndex + 1, Generation: 1,
		LastConfigurationIndex: m.Configuration.Index, Configuration: m.Configuration}
	require.NoError(t, clusterbootstrap.MirrorRecoveredBootstrapFloor(files, policy))
	got, present, err := files.LoadIntent()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, raftbootstrap.Floor{ActivationIndex: policy.ActivationIndex, Generation: policy.Generation}, got.Floor)
	regressed := policy
	regressed.ActivationIndex = 0
	require.Error(t, clusterbootstrap.MirrorRecoveredBootstrapFloor(files, regressed))
	unchanged, _, err := files.LoadIntent()
	require.NoError(t, err)
	require.Equal(t, got, unchanged)
}

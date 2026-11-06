package peersession

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
)

func TestRecoveredPolicyPublicationRequiresDurableMirroredIntent(t *testing.T) {
	h, _ := controlFixture(t)
	o, err := NewOwner(Config{Local: h.Client, Versions: h.View.Installed, Provisioned: []Provision{
		{Node: h.Client, Groups: []uint64{testGroup}}, {Node: h.Server, Groups: []uint64{testGroup}},
	}})
	require.NoError(t, err)
	t.Cleanup(o.Close)
	configuration := raftbootstrap.Configuration{Index: 7, State: raftbootstrap.ConfState{Voters: []uint64{1, 2}}, Members: []raftbootstrap.Member{
		{ID: h.Client.Member, Address: h.Client.Address, PublicKeyDigest: h.Client.Key},
		{ID: h.Server.Member, Address: h.Server.Address, PublicKeyDigest: h.Server.Key},
	}}
	digest, err := raftbootstrap.ConfigurationDigest(configuration)
	require.NoError(t, err)
	identity := raftbootstrap.Identity{Cluster: h.Client.Cluster, Group: testGroup}
	floor := raftbootstrap.Floor{ActivationIndex: 9, Generation: 1}
	policy := raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion, Identity: identity, BaselineDigest: digest,
		Minimum: h.View.Installed, ActivationIndex: 9, Generation: 1, LastConfigurationIndex: 7, Configuration: configuration}
	files := raftbootstrap.NewStore(t.TempDir())
	require.Error(t, o.PublishRecoveredPolicy(files, testGroup, policy, raftbootstrap.Intact, 1))
	intent := raftbootstrap.Intent{Identity: identity, BaselineKind: raftbootstrap.Accepted, Baseline: configuration, Minimum: policy.Minimum, Floor: floor}
	authoritative := raftbootstrap.Policy{Identity: identity, BaselineDigest: digest, Minimum: policy.Minimum, Floor: floor}
	require.NoError(t, files.Publish(intent, raftbootstrap.Evidence{Identity: &identity, BaselineDigest: &digest, RaftState: raftbootstrap.Intact, Policy: &authoritative}))
	require.NoError(t, o.PublishRecoveredPolicy(files, testGroup, policy, raftbootstrap.Intact, 1))
	require.Equal(t, IdentityReady, o.groups[testGroup].Stage)
	require.Equal(t, PendingClear, o.groups[testGroup].Pending)
	projection, found := o.GroupProjection(testGroup)
	require.True(t, found)
	projection.Configuration.Members[0].Address = "changed:9000"
	require.NotEqual(t, projection.Configuration.Members[0].Address, o.groups[testGroup].Configuration.Members[0].Address)
	s := &session{binding: Binding{Group: testGroup, Local: h.Client, Remote: h.Server}, local: h.View, remote: h.View}
	require.NoError(t, o.authorizeLocked(s, Vote))
	require.Error(t, o.authorizeLocked(s, Control))
	require.Error(t, o.authorizeLocked(s, Data))
	wrongGroup := policy
	wrongGroup.Identity.Group++
	require.Error(t, o.PublishRecoveredPolicy(files, testGroup, wrongGroup, raftbootstrap.Intact, 2))
	require.Equal(t, Reconciling, o.groups[testGroup].Stage)
	require.NoError(t, o.PublishRecoveredPolicy(files, testGroup, policy, raftbootstrap.Intact, 2))
	require.Error(t, o.PublishRecoveredPolicy(files, testGroup, policy, raftbootstrap.Lost, 3))
	require.Equal(t, Reconciling, o.groups[testGroup].Stage)
	require.Equal(t, PendingTransition, o.groups[testGroup].Pending)
	require.Equal(t, floor, o.groups[testGroup].Floor, "failure must retain the downgrade floor")
	require.Error(t, o.authorizeLocked(s, Vote), "new sessions cannot reuse quarantined authority")
}

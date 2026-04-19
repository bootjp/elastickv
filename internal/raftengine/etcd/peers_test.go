package etcd

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestNormalizePeers_EmptyWithoutBootstrapRejects(t *testing.T) {
	// peers == [] && persistedPeersOK == false && Bootstrap == false is the
	// scenario that produced a split-brain: a node whose data dir was wiped
	// started against an existing cluster and elected itself as a single-node
	// phantom. Refuse the start instead.
	_, _, err := normalizePeers(0, "n1", "host:1", nil, false, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errNoPeersConfigured))
}

func TestNormalizePeers_EmptyWithBootstrapAllowsSelf(t *testing.T) {
	// --raftBootstrap is an explicit opt-in: starting a brand-new single-node
	// cluster is legitimate.
	local, peers, err := normalizePeers(0, "n1", "host:1", nil, false, true)
	require.NoError(t, err)
	require.Equal(t, "n1", local.ID)
	require.Len(t, peers, 1)
	require.Equal(t, "n1", peers[0].ID)
}

func TestNormalizePeers_EmptyWithPersistedAllowsSelf(t *testing.T) {
	// Recovering from persisted peers where the current caller happens to be
	// the only surviving entry must still succeed.
	local, peers, err := normalizePeers(0, "n1", "host:1", nil, true, false)
	require.NoError(t, err)
	require.Equal(t, "n1", local.ID)
	require.Len(t, peers, 1)
	require.Equal(t, "n1", peers[0].ID)
}

func TestNormalizePeers_ExplicitPeerListIgnoresGuard(t *testing.T) {
	// When an explicit peer list is supplied, the guard does not apply — the
	// operator has stated cluster membership intent.
	in := []Peer{
		{ID: "n1", Address: "host1:1"},
		{ID: "n2", Address: "host2:1"},
	}
	_, peers, err := normalizePeers(0, "n1", "host1:1", in, false, false)
	require.NoError(t, err)
	require.Len(t, peers, 2)
}

func TestNormalizePeersConfig_DoesNotDefaultWithoutBootstrap(t *testing.T) {
	// Regression for the bypass of the self-bootstrap guard: without an
	// explicit Bootstrap flag, normalizePeersConfig must NOT fabricate a
	// single-peer self-list from LocalID/LocalAddress. Otherwise the guard
	// in normalizePeers (which triggers on len(peers) == 0) never fires,
	// and a node with a wiped data dir silently self-bootstraps.
	cfg := OpenConfig{
		NodeID:       DeriveNodeID("n1"),
		LocalID:      "n1",
		LocalAddress: "host:1",
		Bootstrap:    false,
	}
	out := normalizePeersConfig(cfg)
	require.Empty(t, out.Peers)
}

func TestNormalizePeersConfig_DefaultsWhenBootstrapSet(t *testing.T) {
	// With Bootstrap explicitly set, a single-peer self-list is legitimate.
	cfg := OpenConfig{
		NodeID:       DeriveNodeID("n1"),
		LocalID:      "n1",
		LocalAddress: "host:1",
		Bootstrap:    true,
	}
	out := normalizePeersConfig(cfg)
	require.Len(t, out.Peers, 1)
	require.Equal(t, "n1", out.Peers[0].ID)
}

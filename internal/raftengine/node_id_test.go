package raftengine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The node ID derivation is persisted cluster identity (Raft ConfState
// members, peer files, encryption nonce prefixes). Pin exact values so a
// change to the hash is caught before it strands existing clusters.
func TestDeriveNodeIDIsStable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		id   string
		want uint64
	}{
		{"", 0},
		{"n1", 0x08b37b07b558d4c0},
		{"n2", 0x08b37e07b558d9d9},
		{"node-a", 0xe6971cc22078cb53},
	} {
		require.Equal(t, tc.want, DeriveNodeID(tc.id), "id %q", tc.id)
	}
}

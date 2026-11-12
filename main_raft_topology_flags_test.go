package main

import (
	"flag"
	"testing"

	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/stretchr/testify/require"
)

func TestRaftTopologyConsistencyFlagsDefaultsAndParsing(t *testing.T) {
	t.Parallel()
	require.Equal(t, raftFlags{address: "localhost:50051", engine: string(noderuntime.RaftEngineEtcd), dataDir: "data/"}, defaultFlags[raftFlags]())
	require.Equal(t, topologyFlags{}, defaultFlags[topologyFlags]())
	require.Equal(t, consistencyFlags{}, defaultFlags[consistencyFlags]())

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var raft raftFlags
	var topology topologyFlags
	var consistency consistencyFlags
	raft.register(fs)
	topology.register(fs)
	consistency.register(fs)
	require.NoError(t, fs.Parse([]string{
		"--raftId", "n1", "--raftDataDir", "/var/lib/ekv",
		"--raftGroups", "1=127.0.0.1:50051", "--shardRanges", ":=1", "--raftSqsMap", "a=b",
		"--sqsFifoPartitionMap", "q.fifo:1=1", "--readWatermarkAdvance",
	}))
	require.Equal(t, "n1", raft.id)
	require.Equal(t, "/var/lib/ekv", raft.dataDir)
	require.Equal(t, topologyFlags{groups: "1=127.0.0.1:50051", shardRanges: ":=1", sqsMap: "a=b", sqsFifoPartitionMap: "q.fifo:1=1"}, topology)
	require.Equal(t, consistencyFlags{readWatermarkAdvance: true}, consistency)
}

func TestRuntimeInputsRequireRaftID(t *testing.T) {
	t.Parallel()
	_, _, _, _, err := resolveRuntimeInputs(defaultFlags[raftFlags](), defaultFlags[topologyFlags](), defaultFlags[listenerFlags](), defaultFlags[raftBootstrapFlags]())
	require.ErrorContains(t, err, "flag --raftId is required")
}

func TestRuntimeInputsCarryBootstrapFlagsIntoBootstrapConfig(t *testing.T) {
	t.Parallel()
	raft := defaultFlags[raftFlags]()
	raft.id = "n1"
	bootstrap := raftBootstrapFlags{peerConfigDir: "/etc/elastickv/peers", joinAsLearner: true}
	_, _, bootstrapCfg, bootstrapping, err := resolveRuntimeInputs(raft, defaultFlags[topologyFlags](), defaultFlags[listenerFlags](), bootstrap)
	require.NoError(t, err)
	require.Equal(t, "/etc/elastickv/peers", bootstrapCfg.PeerConfigDir)
	require.True(t, bootstrapCfg.JoinAsLearner)
	require.False(t, bootstrapping)

	bootstrap = raftBootstrapFlags{bootstrap: true}
	_, _, bootstrapCfg, bootstrapping, err = resolveRuntimeInputs(raft, defaultFlags[topologyFlags](), defaultFlags[listenerFlags](), bootstrap)
	require.NoError(t, err)
	require.Empty(t, bootstrapCfg.PeerConfigDir)
	require.False(t, bootstrapCfg.JoinAsLearner)
	require.True(t, bootstrapping)
}

func TestRaftBootstrapFlagsJoining(t *testing.T) {
	t.Parallel()
	require.False(t, raftBootstrapFlags{}.joining())
	require.False(t, raftBootstrapFlags{joinMembers: "  "}.joining())
	require.True(t, raftBootstrapFlags{joinMembers: "n1=127.0.0.1:50051"}.joining())
}

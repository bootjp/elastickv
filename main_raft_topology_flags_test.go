package main

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftTopologyConsistencyFlagsDefaultsAndParsing(t *testing.T) {
	t.Parallel()
	require.Equal(t, raftFlags{address: "localhost:50051", engine: string(raftEngineEtcd), dataDir: "data/"}, defaultFlags[raftFlags]())
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
	_, _, _, _, err := resolveRuntimeInputs(defaultFlags[raftFlags](), defaultFlags[topologyFlags](), defaultFlags[listenerFlags]())
	require.ErrorContains(t, err, "flag --raftId is required")
}

package etcd

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestEnginePipelineLocalSnapshotFailuresJoinAndReopen(t *testing.T) {
	for _, stage := range []string{"capture", "serialize", "snapshot-close", "durable", "save", "release"} {
		t.Run(stage, func(t *testing.T) { testPipelineLocalSnapshotFailure(t, stage) })
	}
}

func testPipelineLocalSnapshotFailure(t *testing.T, stage string) {
	t.Helper()
	c := newNativeSnapshotCluster(t, stage, "")
	failure := errors.New("injected local snapshot " + stage)
	c.failures[1] = failure
	c.gates[0].failure = failure
	index := c.propose(t, []byte("checkpoint-before-failure"))
	c.pumpUntil(t, func() bool { return c.gates[0].used.Load() }, "local fault stage")
	c.requireHeartbeats(t, func() { require.NoError(t, c.nodes[0].engine.Err()) })
	c.gates[0].unblock()
	e := c.nodes[0].engine
	c.pumpUntil(t, func() bool {
		select {
		case <-e.Done():
			return true
		default:
			return false
		}
	}, "local snapshot fault join")
	require.ErrorIs(t, e.Err(), failure)
	bytes, items := e.storagePipeline.snapshotBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
	require.Empty(t, e.storagePipeline.snapshotResources.leases)
	fsm := &testStateMachine{}
	reopened, err := Open(t.Context(), OpenConfig{NodeID: 1, LocalID: c.nodes[0].peer.ID,
		LocalAddress: c.nodes[0].peer.Address, DataDir: e.dataDir, StateMachine: fsm})
	require.NoError(t, err)
	require.GreaterOrEqual(t, reopened.AppliedIndex(), index)
	require.NoError(t, reopened.Close())
	require.Contains(t, fsm.Applied(), []byte("checkpoint-before-failure"))
}

func TestEnginePipelineLocalSnapshotCloseJoinsActualHandleClose(t *testing.T) {
	c := newNativeSnapshotCluster(t, "snapshot-close", "")
	c.propose(t, []byte("held-handle-close"))
	c.pumpUntil(t, func() bool { return c.gates[0].used.Load() }, "real snapshot handle Close")
	e := c.nodes[0].engine
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	// Pump real traffic from the remaining voters while Close waits for the
	// non-cancellable handle operation; do not synthesize its completion.
	for i := 0; i < 2; i++ {
		c.pumpUntil(t, func() bool { return len(c.wire) > 0 }, "traffic during held Close")
		message := <-c.wire
		if message.message.GetTo() != 1 {
			c.deliver(t, message)
		}
		select {
		case err := <-closed:
			t.Fatalf("Close returned before actual snapshot handle Close: %v", err)
		default:
		}
	}
	c.gates[0].unblock()
	require.NoError(t, <-closed)
	require.Empty(t, e.storagePipeline.snapshotResources.leases)
	bytes, items := e.storagePipeline.snapshotBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
}

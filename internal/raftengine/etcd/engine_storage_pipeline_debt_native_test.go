package etcd

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type debtSaveStorage struct {
	etcdstorage.Storage
	payload []byte
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *debtSaveStorage) Save(state *raftpb.HardState, entries []*raftpb.Entry) error {
	if err := d.Storage.Save(state, entries); err != nil {
		return err
	}
	for _, entry := range entries {
		_, payload, ok := decodeProposalEnvelope(entry.GetData())
		if ok && bytes.Equal(payload, d.payload) {
			d.once.Do(func() { close(d.entered) })
			<-d.release // Real Save returned; required Sync/ACK has not.
		}
	}
	return nil
}

func TestPipelineDebtNativeHeartbeatAndRetirement(t *testing.T) {
	payload := bytes.Repeat([]byte{0xa5}, 64<<10)
	disk := &debtSaveStorage{payload: payload, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(disk.release) }) }
	c := newNativeSnapshotClusterWithHook(t, "", "", nil, func(e *Engine, index int) {
		if index == 0 {
			disk.Storage = e.persist
			e.persist, e.storagePipeline.persist = disk, disk
			// Scale DOWN only the normal aggregate watermark, before owner start.
			// All production stage limits and the Ready allowance stay unchanged.
			e.storagePipeline.bulkOwnership.maxBytes = 256 << 10
		}
	})
	t.Cleanup(unblock)
	e := c.nodes[0].engine
	baseline := c.propose(t, []byte("durable-baseline"))
	c.pumpUntil(t, func() bool {
		return c.nodes[1].engine.AppliedIndex() >= baseline && e.Status().PerPeer[2].Match >= baseline
	}, "real other-voter durable baseline")
	c.dropThird = true // Quorum requires the real local durability receipt.
	done := make(chan proposalResult, 1)
	go func() {
		result, err := e.Propose(t.Context(), payload)
		done <- proposalResult{result: result, err: err}
	}()
	c.pumpUntil(t, func() bool {
		select {
		case <-disk.entered:
			return true
		default:
			return false
		}
	}, "real Save held before required Sync")
	usage := e.StoragePipelineUsage()
	require.Positive(t, usage.BulkDebtBytes)
	require.LessOrEqual(t, usage.BulkDebtBytes, usage.ReadyAllowance)
	before := e.nextRequestID.Load()
	result, err := e.Propose(t.Context(), []byte("must-not-enter-RawNode"))
	require.ErrorIs(t, err, errStorageWorkBackpressure)
	require.Nil(t, result)
	require.Equal(t, before, e.nextRequestID.Load())
	c.requireHeartbeats(t, func() {
		require.NoError(t, e.Err())
		select {
		case early := <-done:
			t.Fatalf("real required Sync was held but proposal returned: %+v", early)
		default:
		}
	})
	unblock()
	verdict := waitDebtNativeProposal(t, c, done)
	require.NoError(t, verdict.err)
	require.NotNil(t, verdict.result)
	c.pumpUntil(t, func() bool {
		usage := e.StoragePipelineUsage()
		return usage.BulkDebtBytes == 0 && usage.BulkDebtWork == 0
	}, "actual retired ownership repays debt")
	require.Equal(t, [][]byte{[]byte("durable-baseline"), payload}, c.fsms[0].Applied())
	closePipelineDispatchCluster(t, c)
	for _, node := range c.nodes {
		usage := node.engine.StoragePipelineUsage()
		require.Zero(t, usage.BulkBytes)
		require.Zero(t, usage.BulkWork)
		require.Zero(t, usage.ControlBytes)
		require.Zero(t, usage.ControlWork)
	}
}

func waitDebtNativeProposal(t *testing.T, c *nativeSnapshotCluster, done <-chan proposalResult) proposalResult {
	t.Helper()
	var verdict proposalResult
	c.pumpUntil(t, func() bool {
		select {
		case verdict = <-done:
			return true
		default:
			return false
		}
	}, "real durable/apply proposal receipt")
	return verdict
}

package etcd

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineDebtUsageIncludesIndependentOutbound(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	p := e.storagePipeline
	// Counters only: this does not allocate payloads or establish a Ready bound.
	for _, budget := range []*storageWorkBudget{p.proposalBudget, p.diskBulk, p.controlOwnership, p.outboundBulk, p.outboundControl} {
		permit, err := budget.tryAcquire(budget.maxBytes)
		require.NoError(t, err)
		t.Cleanup(permit.Release)
	}
	usage := e.StoragePipelineUsage()
	require.Equal(t, uint64(1024<<20), usage.BulkBytes)
	require.Equal(t, uint64(512<<20), usage.BulkDebtBytes)
	require.Equal(t, uint64(512<<20), usage.OutboundBulkBytes)
	require.Equal(t, uint64(8<<20), usage.OutboundControlBytes)
	require.Equal(t, uint64(1552<<20), usage.ReservedBytes)
	require.Equal(t, usage.ReservedBytes, usage.ReservationCeilingBytes)
	require.Equal(t, 775, usage.ReservationCeilingWork)
	require.LessOrEqual(t, usage.ReservedWork, usage.ReservationCeilingWork)
}

func TestPipelineDebtArrayArithmeticRejectsInvalidCapacity(t *testing.T) {
	require.Equal(t, uint64(math.MaxUint64), pipelineArrayBytes(-1, pipelinePointerBytes))
	require.Equal(t, uint64(math.MaxUint64), pipelineArrayBytes(1, 0))
	require.Equal(t, uint64(0), pipelineArrayBytes(0, pipelinePointerBytes))
	require.Equal(t, uint64(16), pipelineArrayBytes(2, pipelinePointerBytes))
	require.Equal(t, uint64(math.MaxUint64), pipelineAddBytes(math.MaxUint64-1, 2))
}

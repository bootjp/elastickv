package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// groupScanningStore scans one Raft group at a time, as *kv.ShardStore does in
// production; the scan result does not matter for planning.
type groupScanningStore struct {
	store.MVCCStore
}

func (groupScanningStore) ScanGroupAt(context.Context, uint64, []byte, []byte, int, uint64) ([]*store.KVPair, error) {
	return nil, nil
}

type ttlPlanLeaderGroupsCoordinator struct {
	kv.Coordinator

	groups []uint64
}

func (c ttlPlanLeaderGroupsCoordinator) LocalLeaderGroupIDs() []uint64 { return c.groups }

// In production the compactor holds WithKeyVizLabel(startupGatedCoordinator{
// sharded}); kv.CoordinatorForwarder stands in for the startup gate, which
// embeds it. The migration must plan one scan per group this node leads --
// when the gate did not forward LocalLeaderGroupIDs it planned none.
func TestTTLInlineMigrationPlansOneScanPerLocalLeaderGroupThroughWrappers(t *testing.T) {
	t.Parallel()

	gate := kv.CoordinatorForwarder{Inner: ttlPlanLeaderGroupsCoordinator{groups: []uint64{2, 5}}}
	compactor := NewDeltaCompactor(groupScanningStore{MVCCStore: store.NewMVCCStore()}, gate)

	h := compactor.ttlInlineMigrationHandlers()[0]
	plans := compactor.ttlInlineMigrationScanPlans(h)
	require.Len(t, plans, 2)
	require.Equal(t, uint64(2), plans[0].groupID)
	require.Equal(t, ttlInlineMigrationGroupCursorName(h.typeName, 2), plans[0].cursorName)
	require.Equal(t, uint64(5), plans[1].groupID)
	require.Equal(t, ttlInlineMigrationGroupCursorName(h.typeName, 5), plans[1].cursorName)
}

package kv

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestFSMOnePhaseStagedConflictsPreserveRaftReplay(t *testing.T) {
	t.Parallel()
	for _, writeStagedKey := range []bool{false, true} {
		t.Run(map[bool]string{false: "read footprint", true: "write footprint"}[writeStagedKey], func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			engine := distribution.NewEngine()
			applyComposed1Snapshot(t, engine, 2, []distribution.RouteDescriptor{{
				RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1,
				State: distribution.RouteStateActive, StagedVisibilityActive: true,
				MigrationJobID: 9, MinWriteTSExclusive: 100,
			}})
			st, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "fsm.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			fsm, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(WrapDistributionEngine(engine), 1)).(*kvFSM)
			require.True(t, ok)
			applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{
				JobID: 9, RouteStart: []byte("a"), RouteEnd: []byte("z"),
				ExpectedCutoverVersion: 2, MigrationJobID: 9, MinWriteTSExclusive: 100, Armed: true,
			})
			ownership, ok := st.(store.MigrationOwnershipStore)
			require.True(t, ok)
			require.NoError(t, ownership.AcquireMigrationOwnership(ctx, store.MigrationOwnershipState{
				JobID: 9, LocalGroupID: 1, OwnerGroupID: 1, CatalogVersion: 2, ConfigurationIndex: 1,
				RouteStart: []byte("a"), RouteEnd: []byte("z"), MinWriteTSExclusive: 100,
				Role: store.MigrationOwner, Staged: true,
			}, 1))
			require.NoError(t, st.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("n")), []byte("staged"), 20, 0))
			writeKey := []byte("b")
			if writeStagedKey {
				writeKey = []byte("n")
			}
			req := onePhaseReq(10, 120, 0, writeKey, []byte("new"))
			req.ObservedRouteVersion = 2
			if !writeStagedKey {
				req.ReadKeys = [][]byte{[]byte("n")}
			}
			fsm.SetApplyIndex(2)
			require.ErrorIs(t, applyFSMRequest(t, fsm, req), store.ErrWriteConflict)
			_, err = st.GetAt(ctx, writeKey, 120)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			req.Ts = 110
			fsm.SetApplyIndex(3)
			require.NoError(t, applyFSMRequest(t, fsm, req))
			require.NoError(t, applyFSMRequest(t, fsm, req), "same entry must replay even though its live version exceeds startTS")
			fsm.SetApplyIndex(4)
			require.ErrorIs(t, applyFSMRequest(t, fsm, req), store.ErrWriteConflict, "a different entry cannot reuse the commit timestamp")
		})
	}
}

package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestRawMigrationVersionPreservesAdmission(t *testing.T) {
	t.Parallel()
	f, engine, _ := frontierAdmissionSplitFixture(t)
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		2: {Store: f.store}, 3: {Store: f.store},
	}, 2, NewHLC(), nil)
	version := EncodeObservedRouteVersion(engine.Version())
	for _, tc := range []struct {
		name     string
		key      string
		group    uint64
		observed uint64
		want     uint64
		admitted bool
	}{
		{name: "migrated owner", key: "n", group: 2, want: version, admitted: true},
		{name: "ordinary key", key: "b", group: 2, admitted: true},
		{name: "explicit stale version", key: "n", group: 2, observed: version - 1, want: version - 1},
		{name: "wrong owner remains rejected", key: "n", group: 3, want: version},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := &pb.Request{Ts: 200, ObservedRouteVersion: tc.observed,
				Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(tc.key), Value: []byte("value")}}}
			require.NoError(t, coord.pinRawMigrationVersion(t.Context(), tc.group, request, version))
			require.Equal(t, tc.want, request.ObservedRouteVersion)
			err := CheckMigrationRequest(t.Context(), f.store, tc.group, request)
			if tc.admitted {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrRouteCutoverPending)
			}
		})
	}
}

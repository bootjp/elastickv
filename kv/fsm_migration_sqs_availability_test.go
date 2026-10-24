package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestCatalogMigrationPreservesSQSAvailabilityOutsideMovingRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{{RouteID: 1, End: []byte("z"), GroupID: 1}})
	fsm := newComposed1FSM(t, engine, 1)
	sharded := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: fsm.store}})
	keys := [][]byte{
		partitionedSQSMsgDataKey("orders", 3), []byte(sqsMsgVisPrefix + "p|orders|3"),
		[]byte(sqsMsgDedupPrefix + "p|orders|3"), []byte(sqsMsgGroupPrefix + "p|orders|3"),
		[]byte(sqsMsgByAgePrefix + "p|orders|3"), []byte(sqsMsgDataPrefix + "orders|legacy"),
		[]byte(sqsQueueMetaPrefix + "orders"),
	}
	partitionOwners := make(map[string]uint64)
	for _, key := range keys {
		if IsPartitionedSQSKey(key) {
			partitionOwners[string(key)] = 1
		}
	}
	resolver := &fakePartitionResolver{routes: partitionOwners}
	router := NewShardRouter(engine).WithPartitionResolver(resolver)
	sharded.WithPartitionResolver(resolver)
	version := uint64(2)
	for _, phase := range []string{"FENCE", "CUTOVER", "CLEANUP"} {
		t.Run(phase, func(t *testing.T) {
			moving := distribution.RouteDescriptor{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 1, State: distribution.RouteStateWriteFenced}
			if phase != "FENCE" {
				moving.GroupID, moving.State = 2, distribution.RouteStateActive
			}
			applyComposed1Snapshot(t, engine, version, []distribution.RouteDescriptor{
				{RouteID: 2, ParentRouteID: 1, End: []byte("m"), GroupID: 1}, moving,
			})
			if phase == "CLEANUP" {
				cleaner, ok := fsm.store.(store.MigrationCleaner)
				require.True(t, ok)
				require.NoError(t, cleaner.ClearMigrationState(ctx, 10, 0))
			} else {
				applySourceMigrationControlToFSM(t, fsm, phase == "CUTOVER")
			}
			for _, key := range keys {
				if IsPartitionedSQSKey(key) {
					groupID, found := router.ResolveGroup(key)
					require.True(t, found)
					require.Equal(t, uint64(1), groupID, "partition ownership must not change with catalog migration phases")
				}
				require.Equal(t, sqsGlobalRouteKey, routeKey(key))
				require.NoError(t, distribution.ValidateSplitMigrationRouteRange([]byte("m"), []byte("z")))
				ts := version * 100
				value := []byte(phase)
				require.NoError(t, fsm.handleRawRequest(ctx, &pb.Request{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: value}}}, ts))
				got, err := sharded.GetGroupAt(ctx, 1, key, ts)
				require.NoError(t, err)
				require.Equal(t, value, got)
				owner, found := engine.GetRoute(routeKey(key))
				require.True(t, found)
				require.Equal(t, uint64(1), owner.GroupID)
			}
		})
		version++
	}
}

func TestSQSShapeDoesNotBypassCatalogOrSourceFences(t *testing.T) {
	t.Parallel()
	for _, durable := range []bool{false, true} {
		t.Run(map[bool]string{false: "catalog", true: "source"}[durable], func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			engine := distribution.NewEngine()
			state := distribution.RouteStateWriteFenced
			if durable {
				state = distribution.RouteStateActive
			}
			applyComposed1Snapshot(t, engine, 1, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("!sqs|route|"), End: []byte("!sqs|route|z"), GroupID: 1, State: state}})
			fsm := newComposed1FSM(t, engine, 1)
			if durable {
				applyTargetReadinessToFSM(t, fsm, store.TargetStagedReadinessState{JobID: 10, MigrationJobID: 10,
					RouteStart: []byte("!sqs|route|"), RouteEnd: []byte("!sqs|route|z"), Armed: true,
					SourceWriteFence: true, MinWriteTSExclusive: 1, RetentionPinTS: 1})
			}
			for _, key := range [][]byte{partitionedSQSMsgDataKey("q", 3), []byte(sqsMsgDataPrefix + "q|legacy"), []byte(sqsQueueMetaPrefix + "q"), []byte("!sqs|route|global-user")} {
				err := fsm.handleRawRequest(ctx, &pb.Request{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("must-not-land")}}}, 100)
				require.ErrorIs(t, err, ErrRouteWriteFenced)
				_, err = fsm.store.GetAt(ctx, key, 100)
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			}
		})
	}
}

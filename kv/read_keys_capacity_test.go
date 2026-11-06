package kv

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const readKeysCapacityJobID = 9

// Observe the real proposal boundary without replacing Raft or the FSM.
type readKeysCapacityEngine struct {
	raftengine.Engine
	mu       sync.Mutex
	payloads [][]byte
}

func (e *readKeysCapacityEngine) record(data []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.payloads = append(e.payloads, bytes.Clone(data))
}

func (e *readKeysCapacityEngine) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	e.record(data)
	return e.Engine.Propose(ctx, data)
}

func (e *readKeysCapacityEngine) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	e.record(data)
	return e.Engine.ProposeAdmin(ctx, data)
}

func (e *readKeysCapacityEngine) proposals() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([][]byte(nil), e.payloads...)
}

func (e *readKeysCapacityEngine) requests(t *testing.T) []*pb.Request {
	t.Helper()
	var requests []*pb.Request
	for _, payload := range e.proposals() {
		if len(payload) == 0 || payload[0] != raftEncodeSingle {
			continue
		}
		request := &pb.Request{}
		require.NoError(t, proto.Unmarshal(payload[1:], request))
		requests = append(requests, request)
	}
	return requests
}

type readKeysCapacityHarness struct {
	coord   *ShardedCoordinator
	engines []*readKeysCapacityEngine
	stores  []store.MVCCStore
}

func newReadKeysCapacityHarness(t *testing.T, staged, multiShard bool) readKeysCapacityHarness {
	t.Helper()
	routes := distribution.NewEngine()
	descriptors := []distribution.RouteDescriptor{{
		RouteID: 1, GroupID: 1, State: distribution.RouteStateActive,
		StagedVisibilityActive: staged,
	}}
	if staged {
		descriptors[0].MigrationJobID = readKeysCapacityJobID
	}
	if multiShard {
		descriptors[0].End = []byte("m")
		descriptors = append(descriptors, distribution.RouteDescriptor{
			RouteID: 2, GroupID: 2, Start: []byte("m"), State: distribution.RouteStateActive,
		})
	}
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: descriptors}))
	clock := NewHLC()
	groups := make(map[uint64]*ShardGroup)
	h := readKeysCapacityHarness{}
	for _, descriptor := range descriptors {
		st := store.NewMVCCStore()
		fsm := NewKvFSMWithHLC(st, clock, WithRouteHistory(WrapDistributionEngine(routes), descriptor.GroupID))
		engine, stop := newSingleRaft(t, fmt.Sprintf("read-cap-%d", descriptor.GroupID), fsm)
		t.Cleanup(stop)
		recorder := &readKeysCapacityEngine{Engine: engine}
		groups[descriptor.GroupID] = &ShardGroup{Engine: recorder, Store: st, Txn: NewLeaderProxyWithEngine(recorder)}
		h.engines = append(h.engines, recorder)
		h.stores = append(h.stores, st)
	}
	h.coord = NewShardedCoordinator(routes, groups, 1, clock, NewShardStore(routes, groups))
	t.Cleanup(func() { require.NoError(t, h.coord.Close()) })
	return h
}

func readKeysCapacityParts(count int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = s3keys.UploadPartKey("capacity", 1, "object", "upload", uint64(i+1))
	}
	return keys
}

func readKeysCapacityRequest(count int) *OperationGroup[OP] {
	return &OperationGroup[OP]{
		IsTxn: true, StartTS: 10, CommitTS: 100,
		ReadKeys: readKeysCapacityParts(count),
		Elems: []*Elem[OP]{
			{Op: Put, Key: s3keys.BucketMetaKey("capacity"), Value: []byte("bucket")},
			{Op: Put, Key: s3keys.ObjectManifestKey("capacity", 1, "object"), Value: []byte("manifest")},
			{Op: Del, Key: s3keys.UploadMetaKey("capacity", 1, "object", "upload")},
			{Op: Del, Key: s3keys.GCUploadKey("capacity", 1, "object", "upload")},
		},
	}
}

func dispatchReadKeysCapacity(ctx context.Context, coord Coordinator, request *OperationGroup[OP], leaderStamped bool) (*CoordinateResponse, error) {
	if leaderStamped {
		request.CommitTS = 0
		return DispatchWithLeaderIssuedCommitTS(ctx, coord, request)
	}
	return coord.Dispatch(ctx, request)
}

func TestReadKeysCapacityInputBoundary(t *testing.T) {
	t.Parallel()
	require.Equal(t, 20_480, MaxReadKeys)
	for _, path := range []string{"coordinate", "sharded-explicit", "sharded-leader-stamped"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			h := newReadKeysCapacityHarness(t, false, false)
			var coord Coordinator = h.coord
			if path == "coordinate" {
				plain := NewCoordinatorWithEngine(NewLeaderProxyWithEngine(h.engines[0]), h.engines[0])
				t.Cleanup(func() { require.NoError(t, plain.Close()) })
				coord = plain
			}
			ctx := context.Background()
			response, err := dispatchReadKeysCapacity(ctx, coord, readKeysCapacityRequest(MaxReadKeys), path == "sharded-leader-stamped")
			require.NoError(t, err)
			require.Positive(t, response.CommitIndex)
			requests := h.engines[0].requests(t)
			require.Len(t, requests, 1)
			require.Len(t, requests[0].ReadKeys, MaxReadKeys)
			before := len(h.engines[0].proposals())
			_, err = dispatchReadKeysCapacity(ctx, coord, readKeysCapacityRequest(MaxReadKeys+1), path == "sharded-leader-stamped")
			require.ErrorIs(t, err, ErrInvalidRequest)
			require.Len(t, h.engines[0].proposals(), before, "input overflow must fail before any proposal")
		})
	}
}

func TestReadKeysCapacityRedirectBoundary(t *testing.T) {
	t.Parallel()
	coord := &Coordinate{}
	requests, err := coord.buildRedirectRequests(readKeysCapacityRequest(MaxReadKeys))
	require.NoError(t, err)
	require.Len(t, requests, 1)
	require.Len(t, requests[0].ReadKeys, MaxReadKeys)
	requests, err = coord.buildRedirectRequests(readKeysCapacityRequest(MaxReadKeys + 1))
	require.ErrorIs(t, err, ErrInvalidRequest)
	require.Nil(t, requests, "overflow must not produce a forwardable request")
}

func assertReadKeysCapacityAliases(t *testing.T, request *OperationGroup[OP], physical [][]byte) {
	t.Helper()
	want := make(map[string]int)
	for _, key := range request.ReadKeys {
		want[string(key)]++
		want[string(distribution.MigrationStagedDataKey(readKeysCapacityJobID, key))]++
	}
	for _, elem := range request.Elems {
		if bytes.Equal(elem.Key, []byte("z")) {
			continue // The second shard has no staged visibility.
		}
		want[string(distribution.MigrationStagedDataKey(readKeysCapacityJobID, elem.Key))]++
	}
	got := make(map[string]int)
	for _, key := range physical {
		got[string(key)]++
	}
	require.Equal(t, want, got, "every read, fence and write alias must reach Raft, including duplicates")
}

func TestReadKeysCapacityStagedVisibility(t *testing.T) {
	t.Parallel()
	for _, leaderStamped := range []bool{false, true} {
		t.Run(fmt.Sprintf("leader-stamped=%t", leaderStamped), func(t *testing.T) {
			t.Parallel()
			h := newReadKeysCapacityHarness(t, true, false)
			ctx := context.Background()
			request := readKeysCapacityRequest(10_000)
			request.ReadKeys = append(request.ReadKeys,
				s3keys.BucketMetaKey("capacity"), s3keys.BucketGenerationKey("capacity"),
				s3keys.UploadMetaKey("capacity", 1, "object", "upload"), s3keys.ObjectManifestKey("capacity", 1, "object"))
			// Build the read set from actual staged-only values, not synthetic aliases.
			for _, key := range request.ReadKeys {
				alias := distribution.MigrationStagedDataKey(readKeysCapacityJobID, key)
				require.NoError(t, h.stores[0].PutAt(ctx, alias, []byte("staged"), 1, 0))
				value, err := h.coord.store.GetAt(ctx, key, request.StartTS)
				require.NoError(t, err)
				require.Equal(t, []byte("staged"), value)
			}
			response, err := dispatchReadKeysCapacity(ctx, h.coord, request, leaderStamped)
			require.NoError(t, err)
			require.Greater(t, response.CommitTS, request.StartTS)
			requests := h.engines[0].requests(t)
			require.Len(t, requests, 1)
			require.Len(t, requests[0].ReadKeys, 2*(10_000+4)+4)
			assertReadKeysCapacityAliases(t, request, requests[0].ReadKeys)
			value, err := h.stores[0].GetAt(ctx, request.Elems[1].Key, response.CommitTS)
			require.NoError(t, err)
			require.Equal(t, []byte("manifest"), value)
		})
	}
}

func TestReadKeysCapacityStagedConflicts(t *testing.T) {
	t.Parallel()
	for _, leaderStamped := range []bool{false, true} {
		t.Run(fmt.Sprintf("leader-stamped=%t", leaderStamped), func(t *testing.T) {
			t.Parallel()
			for _, target := range []string{"last-part", "fence", "bucket-write", "manifest-write", "upload-delete", "gc-delete"} {
				t.Run(target, func(t *testing.T) {
					t.Parallel()
					h := newReadKeysCapacityHarness(t, true, false)
					request := readKeysCapacityRequest(10_000)
					fence := s3keys.BucketGenerationKey("capacity")
					conflicts := map[string][]byte{
						"last-part":      request.ReadKeys[len(request.ReadKeys)-1],
						"fence":          fence,
						"bucket-write":   request.Elems[0].Key,
						"manifest-write": request.Elems[1].Key,
						"upload-delete":  request.Elems[2].Key,
						"gc-delete":      request.Elems[3].Key,
					}
					request.ReadKeys = append(request.ReadKeys, fence)
					alias := distribution.MigrationStagedDataKey(readKeysCapacityJobID, conflicts[target])
					ctx := context.Background()
					require.NoError(t, h.stores[0].PutAt(ctx, alias, []byte("concurrent"), request.StartTS+1, 0))
					_, err := dispatchReadKeysCapacity(ctx, h.coord, request, leaderStamped)
					require.ErrorIs(t, err, store.ErrWriteConflict, "expanded capacity must not skip any OCC aliases")
					requests := h.engines[0].requests(t)
					require.Len(t, requests, 1, "the FSM, not the capacity guard, must reject the conflict")
					assertReadKeysCapacityAliases(t, request, requests[0].ReadKeys)
					_, err = h.stores[0].GetAt(ctx, request.Elems[1].Key, ^uint64(0))
					require.ErrorIs(t, err, store.ErrKeyNotFound, "a rejected completion must not publish the manifest")
				})
			}
		})
	}
}

func TestReadKeysCapacityStagedBoundary(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"explicit", "leader-stamped", "multi-shard"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name  string
				reads int
				fits  bool
			}{
				{name: "physical-at-cap", reads: (MaxReadKeys - 4) / 2, fits: true},
				{name: "physical-over-cap", reads: (MaxReadKeys-4)/2 + 1},
				{name: "input-at-cap-expands-over", reads: MaxReadKeys},
				{name: "input-over-cap", reads: MaxReadKeys + 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					h := newReadKeysCapacityHarness(t, true, path == "multi-shard")
					request := readKeysCapacityRequest(tc.reads)
					if path == "multi-shard" {
						request.Elems = append(request.Elems, &Elem[OP]{Op: Put, Key: []byte("z"), Value: []byte("second shard")})
					}
					response, err := dispatchReadKeysCapacity(context.Background(), h.coord, request, path == "leader-stamped")
					if !tc.fits {
						require.ErrorIs(t, err, ErrInvalidRequest)
						for _, engine := range h.engines {
							require.Empty(t, engine.proposals(), "overflow must fail before any participant proposes")
						}
						return
					}
					require.NoError(t, err)
					require.Positive(t, response.CommitIndex)
					requests := h.engines[0].requests(t)
					require.NotEmpty(t, requests)
					require.Len(t, requests[0].ReadKeys, MaxReadKeys)
					assertReadKeysCapacityAliases(t, request, requests[0].ReadKeys)
				})
			}
		})
	}
}

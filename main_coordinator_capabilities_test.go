package main

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var errSpyForwarded = errors.New("forwarded from the wrapped coordinator")

const (
	spyIndex  uint64 = 4242
	spyLeader        = "spy-leader"
)

// shardedCapabilitySpy stands in for the ShardedCoordinator under the startup
// wrappers. Every method records its name and returns a distinctive value or
// errSpyForwarded, so a forwarded call can be told apart from a wrapper's own
// fallback.
type shardedCapabilitySpy struct {
	clock *kv.HLC

	mu    sync.Mutex
	calls map[string]int
}

var _ kv.Coordinator = (*shardedCapabilitySpy)(nil)

func newShardedCapabilitySpy() *shardedCapabilitySpy {
	return &shardedCapabilitySpy{clock: kv.NewHLC(), calls: make(map[string]int)}
}

func (s *shardedCapabilitySpy) record(name string) {
	s.mu.Lock()
	s.calls[name]++
	s.mu.Unlock()
}

func (s *shardedCapabilitySpy) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[name]
}

func (s *shardedCapabilitySpy) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	s.record("Dispatch")
	return &kv.CoordinateResponse{CommitIndex: spyIndex}, errSpyForwarded
}

func (s *shardedCapabilitySpy) IsLeader() bool {
	s.record("IsLeader")
	return true
}

func (s *shardedCapabilitySpy) VerifyLeader(context.Context) error {
	s.record("VerifyLeader")
	return errSpyForwarded
}

func (s *shardedCapabilitySpy) LinearizableRead(context.Context) (uint64, error) {
	s.record("LinearizableRead")
	return spyIndex, errSpyForwarded
}

func (s *shardedCapabilitySpy) RaftLeader() string {
	s.record("RaftLeader")
	return spyLeader
}

func (s *shardedCapabilitySpy) IsLeaderForKey([]byte) bool {
	s.record("IsLeaderForKey")
	return true
}

func (s *shardedCapabilitySpy) VerifyLeaderForKey(context.Context, []byte) error {
	s.record("VerifyLeaderForKey")
	return errSpyForwarded
}

func (s *shardedCapabilitySpy) RaftLeaderForKey([]byte) string {
	s.record("RaftLeaderForKey")
	return spyLeader
}

func (s *shardedCapabilitySpy) Clock() *kv.HLC {
	s.record("Clock")
	return s.clock
}

func (s *shardedCapabilitySpy) LinearizableReadForKey(context.Context, []byte) (uint64, error) {
	s.record("LinearizableReadForKey")
	return spyIndex, errSpyForwarded
}

func (s *shardedCapabilitySpy) LeaseRead(context.Context) (uint64, error) {
	s.record("LeaseRead")
	return spyIndex, errSpyForwarded
}

func (s *shardedCapabilitySpy) LeaseReadForKey(context.Context, []byte) (uint64, error) {
	s.record("LeaseReadForKey")
	return spyIndex, errSpyForwarded
}

func (s *shardedCapabilitySpy) LeaseReadAllGroups(context.Context) error {
	s.record("LeaseReadAllGroups")
	return errSpyForwarded
}

func (s *shardedCapabilitySpy) LeaseReadAllGroupsTimestamp(context.Context) (uint64, error) {
	s.record("LeaseReadAllGroupsTimestamp")
	return spyIndex, errSpyForwarded
}

func (s *shardedCapabilitySpy) IsLeaderForGroup(uint64) bool {
	s.record("IsLeaderForGroup")
	return true
}

func (s *shardedCapabilitySpy) RaftLeaderForGroup(uint64) string {
	s.record("RaftLeaderForGroup")
	return spyLeader
}

func (s *shardedCapabilitySpy) LeaseReadForGroup(context.Context, uint64) (uint64, error) {
	s.record("LeaseReadForGroup")
	return spyIndex, errSpyForwarded
}

func (s *shardedCapabilitySpy) EngineGroupIDForKey([]byte) uint64 {
	s.record("EngineGroupIDForKey")
	return spyIndex
}

func (s *shardedCapabilitySpy) LocalLeaderGroupIDs() ([]uint64, bool) {
	s.record("LocalLeaderGroupIDs")
	return []uint64{spyIndex}, true
}

func (s *shardedCapabilitySpy) Next(context.Context) (uint64, error) {
	s.record("Next")
	return spyIndex, errSpyForwarded
}

func (s *shardedCapabilitySpy) NextAfter(context.Context, uint64) (uint64, error) {
	s.record("NextAfter")
	return spyIndex, errSpyForwarded
}

func (s *shardedCapabilitySpy) TimestampAllocator() kv.TimestampAllocator {
	s.record("TimestampAllocator")
	return s
}

func (s *shardedCapabilitySpy) RecoverHLCLease(context.Context) error {
	s.record("RecoverHLCLease")
	return errSpyForwarded
}

func (s *shardedCapabilitySpy) VouchAppliedReadTimestamp(uint64, kv.AppliedReadTimestampVoucherRef) error {
	s.record("VouchAppliedReadTimestamp")
	return errSpyForwarded
}

func (s *shardedCapabilitySpy) RevokeAppliedReadTimestamp(uint64, kv.AppliedReadTimestampVoucherRef) {
	s.record("RevokeAppliedReadTimestamp")
}

func (s *shardedCapabilitySpy) SupportsAppliedReadTimestampVoucher() bool {
	s.record("SupportsAppliedReadTimestampVoucher")
	return true
}

func (s *shardedCapabilitySpy) EnsureMutationsWriteAllowed([]*pb.Mutation, uint64) error {
	s.record("EnsureMutationsWriteAllowed")
	return errSpyForwarded
}

func (s *shardedCapabilitySpy) ObserveForwardedRequests([]*pb.Request) {
	s.record("ObserveForwardedRequests")
}

func (s *shardedCapabilitySpy) RaftMembers(context.Context) ([]kv.RaftMember, error) {
	s.record("RaftMembers")
	return []kv.RaftMember{{NodeID: spyLeader}}, errSpyForwarded
}

func (s *shardedCapabilitySpy) RaftMembersForKey(context.Context, []byte) ([]kv.RaftMember, error) {
	s.record("RaftMembersForKey")
	return []kv.RaftMember{{NodeID: spyLeader}}, errSpyForwarded
}

type startupWrapperSurface struct {
	name string
	wrap func(kv.Coordinator) kv.Coordinator
}

func startupGateOver(inner kv.Coordinator) kv.Coordinator {
	gate := &startupPublicKVGate{}
	gate.markReady()
	return newStartupGatedCoordinator(inner, gate)
}

// startupWrapperSurfaces are the values capability consumers type-assert in
// the server: main probes the startup gate itself, and every adapter probes
// WithKeyVizLabel(startupGatedCoordinator).
var startupWrapperSurfaces = []startupWrapperSurface{
	{name: "startupGatedCoordinator", wrap: startupGateOver},
	{
		name: "WithKeyVizLabel(startupGatedCoordinator)",
		wrap: func(inner kv.Coordinator) kv.Coordinator {
			return kv.WithKeyVizLabel(startupGateOver(inner), keyviz.LabelRedis)
		},
	},
}

// capabilityChecks replaces the default "call it, expect the same-named method
// on the wrapped coordinator" check for capabilities a wrapper serves
// differently on purpose.
var capabilityChecks = map[string]func(*testing.T, kv.Coordinator, *shardedCapabilitySpy){
	// The startup gate hands out itself as the allocator so it can refuse
	// timestamps until startup rotation completes. What must survive is that
	// the allocator consumers resolve through the wrappers still allocates
	// from the wrapped coordinator.
	"TimestampAllocator": func(t *testing.T, surface kv.Coordinator, spy *shardedCapabilitySpy) {
		t.Helper()
		alloc, ok := kv.TimestampAllocatorThrough(surface)
		require.True(t, ok)
		ts, err := alloc.Next(context.Background())
		require.ErrorIs(t, err, errSpyForwarded)
		require.Equal(t, spyIndex, ts)
		require.Positive(t, spy.count("Next"), "the resolved allocator must allocate from the wrapped coordinator")
	},
}

// TestStartupWrappersForwardShardedCoordinatorCapabilities calls every
// ShardedCoordinator method not listed in shardedMethodsNotForwarded through
// both wrappers, against a spy, and requires the call to reach the spy and
// return its result. TestAdapterCoordinatorWrappersForwardShardedCapabilities
// checks only that the methods exist. A method can exist and still not
// forward, when the wrapper probes its inner coordinator through an interface
// that no longer matches and answers with its own fallback. That is how
// LocalLeaderGroupIDs stopped the Redis TTL-inline migration while unit tests
// on an unwrapped coordinator stayed green. The wrappers' own
// timestamp-allocator surfaces (PhaseD*, ValidateDurableTimestamp,
// ConfiguredTimestampAllocator) are not ShardedCoordinator methods; the TSO
// routing tests cover them.
func TestStartupWrappersForwardShardedCoordinatorCapabilities(t *testing.T) {
	t.Parallel()

	shardedType := reflect.TypeOf((*kv.ShardedCoordinator)(nil))
	for name := range shardedMethodsNotForwarded {
		_, ok := shardedType.MethodByName(name)
		require.Truef(t, ok, "ShardedCoordinator.%s no longer exists; drop it from shardedMethodsNotForwarded", name)
	}
	sharded := reflect.ValueOf((*kv.ShardedCoordinator)(nil))
	spyMethods := reflect.ValueOf(newShardedCapabilitySpy())
	for i := range shardedType.NumMethod() {
		name := shardedType.Method(i).Name
		sig := sharded.Method(i).Type()
		if isShardedCoordinatorBuilder(name, sig, shardedType) {
			continue
		}
		if _, notForwarded := shardedMethodsNotForwarded[name]; notForwarded {
			continue
		}
		spyMethod := spyMethods.MethodByName(name)
		require.Truef(t, spyMethod.IsValid() && spyMethod.Type() == sig,
			"shardedCapabilitySpy.%s must mirror ShardedCoordinator.%s %s", name, name, sig)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, s := range startupWrapperSurfaces {
				t.Run(s.name, func(t *testing.T) {
					t.Parallel()
					requireForwardsCapability(t, s, name, sig)
				})
			}
		})
	}
}

func isShardedCoordinatorBuilder(name string, sig reflect.Type, shardedType reflect.Type) bool {
	return strings.HasPrefix(name, "With") && sig.NumOut() == 1 && sig.Out(0) == shardedType
}

func requireForwardsCapability(t *testing.T, s startupWrapperSurface, name string, sig reflect.Type) {
	t.Helper()
	spy := newShardedCapabilitySpy()
	surface := s.wrap(spy)
	method := reflect.ValueOf(surface).MethodByName(name)
	require.Truef(t, method.IsValid(), "%s does not expose %s", s.name, name)
	require.Equalf(t, sig, method.Type(), "%s.%s must keep the ShardedCoordinator signature", s.name, name)
	if check, ok := capabilityChecks[name]; ok {
		check(t, surface, spy)
		return
	}
	args := capabilityCallArgs(sig)
	got := method.Call(args)
	require.Positivef(t, spy.count(name), "%s.%s did not reach the wrapped coordinator", s.name, name)
	want := reflect.ValueOf(spy).MethodByName(name).Call(args)
	errType := reflect.TypeFor[error]()
	for i := range want {
		if sig.Out(i) == errType {
			gotErr, _ := got[i].Interface().(error)
			require.ErrorIsf(t, gotErr, errSpyForwarded, "%s.%s must return the wrapped coordinator's error", s.name, name)
			continue
		}
		require.Equalf(t, want[i].Interface(), got[i].Interface(), "%s.%s must return the wrapped coordinator's result", s.name, name)
	}
}

func capabilityCallArgs(sig reflect.Type) []reflect.Value {
	ctxType := reflect.TypeFor[context.Context]()
	keyType := reflect.TypeFor[[]byte]()
	args := make([]reflect.Value, sig.NumIn())
	for i := range args {
		switch in := sig.In(i); in {
		case ctxType:
			args[i] = reflect.ValueOf(context.Background())
		case keyType:
			args[i] = reflect.ValueOf([]byte("capability-key"))
		default:
			args[i] = reflect.Zero(in)
		}
	}
	return args
}

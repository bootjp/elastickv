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

// coordinatorSurface names the value a capability's consumers type-assert
// against in the server. main.go wraps the ShardedCoordinator in
// startupGatedCoordinator and hands that to the adapters, and every adapter
// wraps it again in WithKeyVizLabel.
type coordinatorSurface int

const (
	// adapterSurface capabilities are probed by adapters on
	// WithKeyVizLabel(startupGatedCoordinator), so both wrappers must forward
	// them.
	adapterSurface coordinatorSurface = iota + 1
	// gateSurface capabilities are probed by main on the startupGatedCoordinator
	// itself; no adapter probes them. If one starts to, move the capability to
	// adapterSurface and forward it on WithKeyVizLabel with a fallback that
	// matches what the consumer does when the capability is absent.
	gateSurface
	// rawOnly methods are called by main on the ShardedCoordinator it built,
	// never through a wrapper.
	rawOnly
)

// shardedCoordinatorSurfaces classifies every exported ShardedCoordinator
// method except the With* builders. Adding a method to ShardedCoordinator fails
// TestStartupWrappersForwardShardedCoordinatorCapabilities until it is listed
// here.
var shardedCoordinatorSurfaces = map[string]coordinatorSurface{
	// kv.Coordinator.
	"Dispatch":           adapterSurface,
	"IsLeader":           adapterSurface,
	"VerifyLeader":       adapterSurface,
	"LinearizableRead":   adapterSurface,
	"RaftLeader":         adapterSurface,
	"IsLeaderForKey":     adapterSurface,
	"VerifyLeaderForKey": adapterSurface,
	"RaftLeaderForKey":   adapterSurface,
	"Clock":              adapterSurface,
	// Key-routed read fence (kv.LeaderRoutedStore), exposed by WithKeyVizLabel.
	"LinearizableReadForKey": adapterSurface,
	// Lease reads.
	"LeaseRead":                   adapterSurface,
	"LeaseReadForKey":             adapterSurface,
	"LeaseReadAllGroups":          adapterSurface,
	"LeaseReadAllGroupsTimestamp": adapterSurface,
	// Group-keyed Redis read fence and per-group lease dedup.
	"IsLeaderForGroup":    adapterSurface,
	"RaftLeaderForGroup":  adapterSurface,
	"LeaseReadForGroup":   adapterSurface,
	"EngineGroupIDForKey": adapterSurface,
	// Scope of the Redis TTL-inline migration.
	"LocalLeaderGroupIDs": adapterSurface,
	// Timestamp allocation and applied-read vouchers.
	"Next":                                adapterSurface,
	"NextAfter":                           adapterSurface,
	"TimestampAllocator":                  adapterSurface,
	"RecoverHLCLease":                     adapterSurface,
	"VouchAppliedReadTimestamp":           adapterSurface,
	"RevokeAppliedReadTimestamp":          adapterSurface,
	"SupportsAppliedReadTimestampVoucher": adapterSurface,

	// internalServerOptions installs the route-floor gate and the
	// forwarded-write observer from the startup gate for Internal.Forward.
	"EnsureMutationsWriteAllowed": gateSurface,
	"ObserveForwardedRequests":    gateSurface,
	// newS3BlobCluster discovers peers through the startup gate.
	"RaftMembers":       gateSurface,
	"RaftMembersForKey": gateSurface,

	// Lifecycle and HLC lease renewal, driven by main on in.coordinate.
	"Close":                     rawOnly,
	"RunHLCLeaseRenewal":        rawOnly,
	"SetHLCLeaseRenewalBlocker": rawOnly,
	"ProposeHLCLease":           rawOnly,
	// Installed on the read tracker from the typed coordinator.
	"ObserveTimestampFloor": rawOnly,
	// Autosplit is configured with the typed *kv.ShardedCoordinator.
	"GroupLeadership":  rawOnly,
	"LeadershipForKey": rawOnly,
	// The legacy TSO bridge wraps the typed coordinator (configureLegacyCoordinatorTSO).
	"IsTimestampLeader": rawOnly,
}

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
	return startupGatedCoordinator{inner: inner, gate: gate}
}

var (
	startupGateSurface = startupWrapperSurface{
		name: "startupGatedCoordinator",
		wrap: startupGateOver,
	}
	adapterCoordinatorSurface = startupWrapperSurface{
		name: "WithKeyVizLabel(startupGatedCoordinator)",
		wrap: func(inner kv.Coordinator) kv.Coordinator {
			return kv.WithKeyVizLabel(startupGateOver(inner), keyviz.LabelRedis)
		},
	}
)

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

// main.go hands the adapters startupGatedCoordinator{ShardedCoordinator}, and
// each adapter wraps it in WithKeyVizLabel. Capabilities are optional
// interfaces found by type assertion, so a wrapper that does not forward one
// fails silently: the probe misses and the consumer quietly takes a fallback,
// or the outer wrapper answers on the inner's behalf. That has already cost the
// Redis read fence its group routing, Internal.Forward its route-floor gate,
// and the Redis TTL-inline migration all of its work, each time with green unit
// tests that used an unwrapped coordinator. This test requires every
// ShardedCoordinator capability to be classified by the surface its consumers
// probe, and every forwarded one to reach the wrapped coordinator and return
// its result through that surface. The wrappers' own timestamp-allocator
// surfaces (PhaseD*, ValidateDurableTimestamp, ConfiguredTimestampAllocator)
// are not ShardedCoordinator methods; the TSO routing tests cover them.
func TestStartupWrappersForwardShardedCoordinatorCapabilities(t *testing.T) {
	t.Parallel()

	shardedType := reflect.TypeOf((*kv.ShardedCoordinator)(nil))
	for name := range shardedCoordinatorSurfaces {
		_, ok := shardedType.MethodByName(name)
		require.Truef(t, ok, "ShardedCoordinator.%s no longer exists; drop it from shardedCoordinatorSurfaces", name)
	}
	sharded := reflect.ValueOf((*kv.ShardedCoordinator)(nil))
	spyMethods := reflect.ValueOf(newShardedCapabilitySpy())
	for i := range shardedType.NumMethod() {
		name := shardedType.Method(i).Name
		sig := sharded.Method(i).Type()
		if isShardedCoordinatorBuilder(name, sig, shardedType) {
			continue
		}
		surface, ok := shardedCoordinatorSurfaces[name]
		require.Truef(t, ok, "ShardedCoordinator.%s is not classified in shardedCoordinatorSurfaces: "+
			"forward it on the wrappers whose consumers probe it, or record why main keeps it on the raw coordinator", name)
		if surface == rawOnly {
			continue
		}
		spyMethod := spyMethods.MethodByName(name)
		require.Truef(t, spyMethod.IsValid() && spyMethod.Type() == sig,
			"shardedCapabilitySpy.%s must mirror ShardedCoordinator.%s %s", name, name, sig)
		surfaces := []startupWrapperSurface{startupGateSurface}
		if surface == adapterSurface {
			surfaces = append(surfaces, adapterCoordinatorSurface)
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, s := range surfaces {
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

// plainLinearizableReadCoordinator has a LinearizableRead but no key-routed
// fence. The embedded interface is nil on purpose: only LinearizableRead is
// exercised here.
type plainLinearizableReadCoordinator struct {
	kv.Coordinator

	reads int
}

func (c *plainLinearizableReadCoordinator) LinearizableRead(context.Context) (uint64, error) {
	c.reads++
	return spyIndex, nil
}

// An inner coordinator without a key-routed fence owns a single group, so the
// startup gate serves LinearizableReadForKey from its plain LinearizableRead,
// the same fallback WithKeyVizLabel uses.
func TestStartupGatedCoordinatorLinearizableReadForKeyFallsBackToLinearizableRead(t *testing.T) {
	t.Parallel()

	inner := &plainLinearizableReadCoordinator{}
	idx, err := startupGatedCoordinator{inner: inner}.LinearizableReadForKey(context.Background(), []byte("k"))
	require.NoError(t, err)
	require.Equal(t, spyIndex, idx)
	require.Equal(t, 1, inner.reads)
}

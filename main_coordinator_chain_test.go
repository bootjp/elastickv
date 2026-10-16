package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

// localLeaderGroupsCoordinator stands in for the ShardedCoordinator under the
// adapter wrapper chain. The embedded interface is nil on purpose: only
// LocalLeaderGroupIDs is exercised.
type localLeaderGroupsCoordinator struct {
	kv.Coordinator

	groups []uint64
}

func (c localLeaderGroupsCoordinator) LocalLeaderGroupIDs() []uint64 { return c.groups }

// Every adapter receives WithKeyVizLabel(startupGatedCoordinator{sharded}). The
// Redis delta compactor's TTL-inline migration plans one scan per group this
// node leads and reads the groups through that chain; when the gate did not
// forward LocalLeaderGroupIDs, the label wrapper answered nil and the
// migration planned no scans at all.
func TestAdapterCoordinatorChainForwardsLocalLeaderGroups(t *testing.T) {
	t.Parallel()

	inner := localLeaderGroupsCoordinator{groups: []uint64{1, 3}}
	chain := kv.WithKeyVizLabel(startupGatedCoordinator{inner: inner, gate: &startupPublicKVGate{}}, keyviz.LabelRedis)

	groups, ok := chain.(interface{ LocalLeaderGroupIDs() []uint64 })
	require.True(t, ok, "the adapter coordinator chain must expose LocalLeaderGroupIDs")
	require.Equal(t, []uint64{1, 3}, groups.LocalLeaderGroupIDs())
}

// shardedMethodsNotForwarded lists the *kv.ShardedCoordinator methods the
// adapter wrappers deliberately do not expose: main drives them on the concrete
// coordinator, never through the chain the adapters receive. Builder methods
// (With*) are skipped by prefix.
var shardedMethodsNotForwarded = map[string]string{
	"Close":                     "lifecycle, owned by main",
	"GroupLeadership":           "autosplit reads it from the concrete coordinator",
	"IsTimestampLeader":         "LocalTSOAllocator probes the concrete coordinator",
	"LeadershipForKey":          "autosplit reads it from the concrete coordinator",
	"ObserveTimestampFloor":     "TSO plumbing on the concrete coordinator",
	"ProposeHLCLease":           "HLC lease renewal runs on the concrete coordinator",
	"RunHLCLeaseRenewal":        "HLC lease renewal runs on the concrete coordinator",
	"SetHLCLeaseRenewalBlocker": "installed on the concrete coordinator at startup",
}

// Adapters type-assert optional coordinator capabilities against the wrapper
// chain main.go builds, not against the ShardedCoordinator inside it. A
// capability one wrapper forgets to forward fails that assertion silently at
// runtime -- the LocalLeaderGroupIDs gap above, and the write gate, read fence
// and membership gaps pinned elsewhere in this package. Comparing method sets
// catches the next one when it is added to ShardedCoordinator.
func TestAdapterCoordinatorWrappersForwardShardedCapabilities(t *testing.T) {
	t.Parallel()

	sharded := reflect.TypeOf(&kv.ShardedCoordinator{})
	wrappers := map[string]reflect.Type{
		"startupGatedCoordinator": reflect.TypeOf(startupGatedCoordinator{}),
		"WithKeyVizLabel":         reflect.TypeOf(kv.WithKeyVizLabel(startupGatedCoordinator{}, keyviz.LabelRedis)),
	}
	for i := range sharded.NumMethod() {
		method := sharded.Method(i)
		if strings.HasPrefix(method.Name, "With") {
			continue
		}
		if _, skip := shardedMethodsNotForwarded[method.Name]; skip {
			continue
		}
		for name, wrapper := range wrappers {
			forwarded, ok := wrapper.MethodByName(method.Name)
			if !ok {
				t.Errorf("%s does not forward ShardedCoordinator.%s", name, method.Name)
				continue
			}
			if !sameMethodSignature(method.Type, forwarded.Type) {
				t.Errorf("%s.%s has signature %v, ShardedCoordinator has %v",
					name, method.Name, forwarded.Type, method.Type)
			}
		}
	}
}

// sameMethodSignature compares two method types from reflect.Type.Method,
// ignoring the receiver in position 0.
func sameMethodSignature(a, b reflect.Type) bool {
	if a.NumIn() != b.NumIn() || a.NumOut() != b.NumOut() || a.IsVariadic() != b.IsVariadic() {
		return false
	}
	for i := 1; i < a.NumIn(); i++ {
		if a.In(i) != b.In(i) {
			return false
		}
	}
	for i := range a.NumOut() {
		if a.Out(i) != b.Out(i) {
			return false
		}
	}
	return true
}

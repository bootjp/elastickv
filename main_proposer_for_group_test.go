package main

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// stubEngineForProposerTest satisfies raftengine.Engine at the
// type level (via embedding) without implementing any method.
// proposerForGroup never calls into the engine — it only stores
// the reference as the return-typed raftengine.Proposer — so the
// nil-embedded shape is sufficient and keeps this test free of
// the full Engine surface area.
type stubEngineForProposerTest struct {
	raftengine.Engine
}

// TestProposerForGroup_UsesShardGroupProposerWhenPresent pins the
// Stage 6E-2c forwarded-write wiring at main.go:1573 (Internal.Forward
// TransactionManager construction) against silent regression: when
// shardGroups carries an entry for rt.spec.id, the returned
// proposer MUST be ShardGroup.Proposer() — i.e. the wrap-aware
// dynamicWrappedProposer chain installed by
// NewLeaderProxyForShardGroup — and NOT a raw rt.engine reference.
//
// A future refactor that swaps the helper back to rt.engine would
// silently reopen the cleartext-bypass path for forwarded follower
// writes after the EnableRaftEnvelope cutover (codex round-2 P1).
// This test fails the moment that regression lands.
func TestProposerForGroup_UsesShardGroupProposerWhenPresent(t *testing.T) {
	t.Parallel()
	engine := &stubEngineForProposerTest{}
	sg := &kv.ShardGroup{Engine: engine}
	// Install sg.proposer via the production constructor so the
	// Proposer() accessor returns the wrap-aware path, not the
	// Engine fallback. NewLeaderProxyForShardGroup is the only
	// public way to set the cached proposer; bypassing it would
	// test the fallback branch instead, which is covered by the
	// sibling test below.
	_ = kv.NewLeaderProxyForShardGroup(sg)

	rt := &raftGroupRuntime{spec: groupSpec{id: 7}, engine: engine}
	got := proposerForGroup(rt, map[uint64]*kv.ShardGroup{7: sg})
	if got == nil {
		t.Fatal("proposerForGroup returned nil")
	}
	// A raw engine reference here means the wrap-aware path is
	// silently bypassed. The Proposer() accessor wraps Engine in
	// a dynamicWrappedProposer after NewLeaderProxyForShardGroup,
	// so the returned value must NOT be the engine itself.
	if got == raftengine.Proposer(engine) {
		t.Error("proposerForGroup returned raw rt.engine; want sg.Proposer() (wrap-aware) — Internal.Forward bypass risk")
	}
	if got != sg.Proposer() {
		t.Error("proposerForGroup did not return sg.Proposer(); a future refactor could silently divert the forwarded-write path off the wrap chain")
	}
}

// TestProposerForGroup_FallsBackToEngineWhenShardGroupMissing
// pins the documented fallback: when shardGroups does not have
// an entry for rt.spec.id (test fixtures that pass a partial
// map), the raw rt.engine is returned so the Internal.Forward
// path still works. Production wires every runtime's shard group
// via buildShardGroups, so the fallback never fires in production;
// this test guards the test-fixture contract.
func TestProposerForGroup_FallsBackToEngineWhenShardGroupMissing(t *testing.T) {
	t.Parallel()
	engine := &stubEngineForProposerTest{}
	rt := &raftGroupRuntime{spec: groupSpec{id: 7}, engine: engine}

	got := proposerForGroup(rt, map[uint64]*kv.ShardGroup{})
	if got != raftengine.Proposer(engine) {
		t.Errorf("proposerForGroup did not fall back to rt.engine for missing shardGroup; got %T, want raw engine", got)
	}
}

// TestProposerForGroup_FallsBackToEngineWhenShardGroupNil pins
// the same fallback for an explicit nil entry in the map (an
// edge case if a future construction site stages map entries
// before populating them). proposerForGroup must treat nil the
// same as "missing" so the partial-map invariant holds for both
// shapes.
func TestProposerForGroup_FallsBackToEngineWhenShardGroupNil(t *testing.T) {
	t.Parallel()
	engine := &stubEngineForProposerTest{}
	rt := &raftGroupRuntime{spec: groupSpec{id: 7}, engine: engine}

	got := proposerForGroup(rt, map[uint64]*kv.ShardGroup{7: nil})
	if got != raftengine.Proposer(engine) {
		t.Errorf("proposerForGroup did not fall back to rt.engine for nil shardGroup entry; got %T, want raw engine", got)
	}
}

func TestSplitPromotionTargetLeaderResolverUsesTargetGroup(t *testing.T) {
	t.Parallel()

	source := capabilityConfigEngine{leader: raftengine.LeaderInfo{Address: "source:50051"}}
	target := capabilityConfigEngine{leader: raftengine.LeaderInfo{Address: "target:50051"}}
	resolve := splitPromotionTargetLeaderResolver(map[uint64]*kv.ShardGroup{
		1: {Engine: source},
		2: {Engine: target},
	})

	addr, err := resolve(distribution.SplitJob{
		SplitKey:      []byte("m"),
		TargetGroupID: 2,
	})
	if err != nil {
		t.Fatalf("resolve returned error: %v", err)
	}
	if addr != "target:50051" {
		t.Fatalf("resolve returned %q, want target leader address", addr)
	}
}

func TestSplitPromotionTargetLeaderResolverRejectsMissingLeader(t *testing.T) {
	t.Parallel()

	resolve := splitPromotionTargetLeaderResolver(map[uint64]*kv.ShardGroup{
		2: {Engine: capabilityConfigEngine{}},
	})
	_, err := resolve(distribution.SplitJob{TargetGroupID: 2})
	if !errors.Is(err, kv.ErrLeaderNotFound) {
		t.Fatalf("resolve error = %v, want ErrLeaderNotFound", err)
	}
}

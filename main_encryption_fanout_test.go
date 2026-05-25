package main

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
)

func TestRouteGroupFromConfiguration_SplitsVotersAndLearners(t *testing.T) {
	t.Parallel()
	cfg := raftengine.Configuration{Servers: []raftengine.Server{
		{ID: "n1", Address: "127.0.0.1:5051", Suffrage: etcdraftengine.SuffrageVoter},
		{ID: "n2", Address: "127.0.0.1:5052", Suffrage: etcdraftengine.SuffrageLearner},
		{ID: "n3", Address: "127.0.0.1:5053", Suffrage: ""}, // empty == voter
	}}
	g := routeGroupFromConfiguration(7, cfg)
	if g.GroupID != 7 {
		t.Errorf("GroupID = %d, want 7", g.GroupID)
	}
	if len(g.Voters) != 2 {
		t.Fatalf("Voters = %d, want 2 (n1 + empty-suffrage n3)", len(g.Voters))
	}
	if len(g.Learners) != 1 {
		t.Fatalf("Learners = %d, want 1 (n2)", len(g.Learners))
	}
	// FullNodeID must match the canonical etcd derivation so the
	// fan-out dedup + verdict matching line up with the rest of the
	// encryption stack's node identity.
	if g.Voters[0].FullNodeID != etcdraftengine.DeriveNodeID("n1") {
		t.Errorf("Voters[0].FullNodeID = %d, want DeriveNodeID(n1)=%d",
			g.Voters[0].FullNodeID, etcdraftengine.DeriveNodeID("n1"))
	}
	if g.Voters[0].Address != "127.0.0.1:5051" {
		t.Errorf("Voters[0].Address = %q, want 127.0.0.1:5051", g.Voters[0].Address)
	}
	if g.Learners[0].FullNodeID != etcdraftengine.DeriveNodeID("n2") {
		t.Errorf("Learners[0].FullNodeID mismatch")
	}
}

func TestRouteGroupFromConfiguration_Empty(t *testing.T) {
	t.Parallel()
	g := routeGroupFromConfiguration(1, raftengine.Configuration{})
	if g.GroupID != 1 {
		t.Errorf("GroupID = %d, want 1", g.GroupID)
	}
	if len(g.Voters) != 0 || len(g.Learners) != 0 {
		t.Errorf("empty configuration should yield no members, got %d voters / %d learners",
			len(g.Voters), len(g.Learners))
	}
}

// TestBuildCapabilityFanoutFn_NonNil pins that the builder returns a
// usable closure (it is only wired when encryption mutators are
// enabled; the closure's fan-out behavior is exercised end-to-end in
// 6D-6c-3b).
func TestBuildCapabilityFanoutFn_NonNil(t *testing.T) {
	t.Parallel()
	fn := buildCapabilityFanoutFn(nil, nil, capabilityFanoutTimeout)
	if fn == nil {
		t.Fatal("buildCapabilityFanoutFn returned nil")
	}
}

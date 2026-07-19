package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"golang.org/x/sync/errgroup"
)

func TestStorageEnvelopeV2CapabilityMonitorActivatesOnce(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	eg, groupCtx := errgroup.WithContext(ctx)
	wiring := encryptionWriteWiring{storageEnvelopeV2Active: &atomic.Bool{}}
	var calls atomic.Int32
	startStorageEnvelopeV2CapabilityMonitor(groupCtx, eg, func(context.Context) (admin.CapabilityFanoutResult, error) {
		calls.Add(1)
		return admin.CapabilityFanoutResult{Verdicts: []admin.CapabilityVerdict{{
			Reachable: true, EncryptionCapable: true, StorageEnvelopeV2Capable: true,
		}}}, nil
	}, wiring)
	deadline := time.Now().Add(time.Second)
	for !wiring.storageEnvelopeV2WritesActive() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !wiring.storageEnvelopeV2WritesActive() {
		t.Fatal("capability monitor did not activate V2 writes")
	}
	cancel()
	if err := eg.Wait(); err != nil {
		t.Fatalf("capability monitor: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("capability monitor calls = %d, want one sticky activation", got)
	}
}

// stubConfigReader is a configReader for the snapshot-builder tests.
type stubConfigReader struct {
	cfg raftengine.Configuration
	err error
}

func (s stubConfigReader) Configuration(context.Context) (raftengine.Configuration, error) {
	return s.cfg, s.err
}

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

// TestBuildEncryptionCapabilityFanout_DisabledReturnsNil pins that the
// closure is not built when encryption mutators are off — the cutover
// RPC is then unreachable, so wiring a fan-out would be dead weight.
func TestBuildEncryptionCapabilityFanout_DisabledReturnsNil(t *testing.T) {
	t.Parallel()
	if fn := buildEncryptionCapabilityFanout(context.Background(), nil, nil, false); fn != nil {
		t.Error("buildEncryptionCapabilityFanout(enableMutators=false) = non-nil, want nil")
	}
}

func TestRouteSnapshotFromSources_MapsAllGroups(t *testing.T) {
	t.Parallel()
	sources := []groupConfigSource{
		{groupID: 1, engine: stubConfigReader{cfg: raftengine.Configuration{Servers: []raftengine.Server{
			{ID: "n1", Address: "a:1", Suffrage: etcdraftengine.SuffrageVoter},
		}}}},
		{groupID: 2, engine: stubConfigReader{cfg: raftengine.Configuration{Servers: []raftengine.Server{
			{ID: "n2", Address: "b:2", Suffrage: etcdraftengine.SuffrageLearner},
		}}}},
	}
	snap, err := routeSnapshotFromSources(context.Background(), sources)
	if err != nil {
		t.Fatalf("routeSnapshotFromSources: %v", err)
	}
	if len(snap.Groups) != 2 {
		t.Fatalf("Groups = %d, want 2", len(snap.Groups))
	}
	if snap.Groups[0].GroupID != 1 || len(snap.Groups[0].Voters) != 1 {
		t.Errorf("group 1 mapping wrong: %+v", snap.Groups[0])
	}
	if snap.Groups[1].GroupID != 2 || len(snap.Groups[1].Learners) != 1 {
		t.Errorf("group 2 mapping wrong: %+v", snap.Groups[1])
	}
}

// TestRouteSnapshotFromSources_ConfigurationErrorFailsClosed pins the
// load-bearing fail-closed invariant: if any group's Configuration
// errors, the whole snapshot is abandoned so the cutover refuses
// rather than proposing against a partially-enumerated membership.
func TestRouteSnapshotFromSources_ConfigurationErrorFailsClosed(t *testing.T) {
	t.Parallel()
	boom := errors.New("configuration unavailable")
	sources := []groupConfigSource{
		{groupID: 1, engine: stubConfigReader{cfg: raftengine.Configuration{Servers: []raftengine.Server{
			{ID: "n1", Address: "a:1", Suffrage: etcdraftengine.SuffrageVoter},
		}}}},
		{groupID: 2, engine: stubConfigReader{err: boom}},
	}
	_, err := routeSnapshotFromSources(context.Background(), sources)
	if err == nil {
		t.Fatal("expected error when a group's Configuration fails, got nil")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error does not wrap the Configuration failure: %v", err)
	}
}

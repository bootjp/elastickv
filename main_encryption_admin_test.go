package main

import (
	"context"
	"net"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// stubEncryptionAdminEngine satisfies the encryptionAdminEngine
// interface (Proposer + LeaderView) for tests that exercise the
// Stage 6B-2 mutator-wiring gate. The methods are nil-deref-safe
// stubs; the registration helper only stores them on the
// EncryptionAdminServer options. Tests that exercise the gate via
// a gRPC client never actually issue a Raft proposal because the
// mutator path returns ErrFailedPrecondition before reaching
// Propose() in the no-mutator case, and tests for the enabled
// case stop at status.Code inspection.
type stubEncryptionAdminEngine struct{}

func (stubEncryptionAdminEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{}, nil
}
func (s stubEncryptionAdminEngine) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return s.Propose(ctx, data)
}
func (stubEncryptionAdminEngine) State() raftengine.State       { return raftengine.StateLeader }
func (stubEncryptionAdminEngine) Leader() raftengine.LeaderInfo { return raftengine.LeaderInfo{} }
func (stubEncryptionAdminEngine) VerifyLeader(context.Context) error {
	return nil
}
func (stubEncryptionAdminEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

// TestEncryptionAdminFullNodeID_DistinctPerRaftId pins the
// PR760 r1 Codex P1 regression: full_node_id must be derived from
// --raftId (per-node-stable) and NOT from the Raft group id
// (shared across replicas). An earlier wiring passed rt.spec.id,
// which makes every replica return the same CapabilityReport
// value and breaks BootstrapEncryption's
// validateWriterBatchUniqueness check. The fix routes through
// etcd.DeriveNodeID(raftId) at the call site; this test confirms
// the canonical derivation is in fact distinct for the canonical
// 3-node config (n1 / n2 / n3) the demo + Jepsen runners use.
func TestEncryptionAdminFullNodeID_DistinctPerRaftId(t *testing.T) {
	seen := make(map[uint64]string)
	for _, id := range []string{"n1", "n2", "n3"} {
		got := etcdraftengine.DeriveNodeID(id)
		if got == 0 {
			t.Fatalf("DeriveNodeID(%q) returned 0; the §6.1 not-capable sentinel must never be used as a real node id", id)
		}
		if dup, exists := seen[got]; exists {
			t.Fatalf("DeriveNodeID(%q) = DeriveNodeID(%q) = %d; full_node_id must be distinct across raftIds — BootstrapEncryption uniqueness would reject", id, dup, got)
		}
		seen[got] = id
	}
}

// TestEncryptionAdmin_MutatingRPCRefusedWhenGateOff pins the
// Stage 6B-2 double-gate. With either condition of the gate false
// (enableMutators=false OR engine=nil), Proposer + LeaderView
// MUST stay unwired and EncryptionAdminServer.BootstrapEncryption
// returns FailedPrecondition at the gRPC boundary — the proposal
// never reaches Raft.
//
// The matrix below exercises all 4 corners of (enableMutators,
// engine-supplied). Only the (true, non-nil) combination is the
// production "gate ON" case, which is tested separately by
// TestEncryptionAdmin_MutatingRPCEnabledWhenGateOn.
func TestEncryptionAdmin_MutatingRPCRefusedWhenGateOff(t *testing.T) {
	for _, tc := range []struct {
		name           string
		enableMutators bool
		engine         encryptionAdminEngine
	}{
		{name: "flag_off_engine_nil", enableMutators: false, engine: nil},
		{name: "flag_off_engine_set", enableMutators: false, engine: stubEncryptionAdminEngine{}},
		{name: "flag_on_engine_nil", enableMutators: true, engine: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := callBootstrapAgainstNewServer(t, tc.enableMutators, tc.engine)
			if err == nil {
				t.Fatal("BootstrapEncryption succeeded with gate off; mutating wiring must be refused at the RPC boundary")
			}
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Errorf("BootstrapEncryption status=%v, want FailedPrecondition; err=%v", got, err)
			}
		})
	}
}

// TestEncryptionAdmin_MutatingRPCEnabledWhenGateOn pins the
// other side of the Stage 6B-2 gate: when enableMutators=true AND
// engine is non-nil, the mutator wiring is installed and
// BootstrapEncryption reaches the Raft propose path. The stub
// engine's Propose() returns success, so the only-RPC-layer
// portion of the path completes; deeper apply-layer behaviour is
// covered by the applier tests in internal/encryption.
//
// The expected status is NOT FailedPrecondition — that is the
// gate-off signature. The exact OK / OtherCode depends on the
// EncryptionAdminServer's downstream validation of the empty
// wrapped payload; the test asserts the FailedPrecondition gate
// is no longer firing, which is the property that distinguishes
// gate-on from gate-off.
func TestEncryptionAdmin_MutatingRPCEnabledWhenGateOn(t *testing.T) {
	err := callBootstrapAgainstNewServer(t, true, stubEncryptionAdminEngine{})
	// Whatever the deeper error, it must not be the FailedPrecondition
	// from the gate. The stub Propose() returns success on a malformed
	// payload, so the actual return may be a deeper InvalidArgument
	// or codes.OK — anything except FailedPrecondition / transport
	// statuses is acceptable.
	if err != nil {
		got := status.Code(err)
		if got == codes.FailedPrecondition {
			t.Errorf("BootstrapEncryption returned FailedPrecondition with gate ON; want a deeper status (gate should not refuse): err=%v", err)
		}
		// Bufconn transport failures or context-derived statuses
		// would defeat the test purpose — fail loud rather than
		// silently passing on infra noise (coderabbit PR #776
		// minor).
		if got == codes.Unavailable || got == codes.DeadlineExceeded || got == codes.Canceled {
			t.Fatalf("BootstrapEncryption failed on test transport/setup, not gate behavior: code=%v err=%v", got, err)
		}
	}
}

func callBootstrapAgainstNewServer(t *testing.T, enableMutators bool, engine encryptionAdminEngine) error {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	registerEncryptionAdminServer(gs, 1, "", enableMutators, engine, nil, nil)
	go func() { _ = gs.Serve(listener) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewEncryptionAdminClient(conn)
	_, err = client.BootstrapEncryption(context.Background(), &pb.BootstrapEncryptionRequest{
		StorageDekId:      1,
		RaftDekId:         2,
		WrappedStorageDek: []byte("w"),
		WrappedRaftDek:    []byte("w"),
	})
	return err
}

func TestRegisterEncryptionAdminServer_Registers(t *testing.T) {
	gs := grpc.NewServer()
	registerEncryptionAdminServer(gs, 1, "", false, nil, nil, nil)
	info := gs.GetServiceInfo()
	if _, ok := info["EncryptionAdmin"]; !ok {
		var registered []string
		for name := range info {
			registered = append(registered, name)
		}
		t.Fatalf("EncryptionAdmin not registered; got services=%v", registered)
	}
}

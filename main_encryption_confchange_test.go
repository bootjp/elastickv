package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// stubDeriveNodeID returns a sentinel uint64 for any input — lets the
// tests assert that whatever raftID lands at the adapter is hashed
// through the injected function value, not a hardcoded global.
const stubDerivedNodeID uint64 = 0xCAFEF00DDEADBEEF

func stubDeriveNodeID(string) uint64 { return stubDerivedNodeID }

func allowStorageEnvelopeV2Capability(context.Context, string) error { return nil }

// TestEncryptionPreRegister_PreBootstrapSkips pins design §5.2: when
// the StateCache reports (0, false) — no active storage DEK, either
// pre-bootstrap or encryption-disabled — PreAddMember returns nil
// without proposing or reading the registry.
func TestEncryptionPreRegister_PreBootstrapSkips(t *testing.T) {
	t.Parallel()
	cache := encryption.NewStateCache() // zero-value: ActiveStorageKeyID()=(0,false)
	st := newRegistrationTestStore(t)
	defaultGroup := &kv.ShardGroup{Store: st}
	pre := newEncryptionPreRegister(&kv.ShardedCoordinator{}, defaultGroup, cache, "", stubDeriveNodeID)
	if pre == nil {
		t.Fatal("newEncryptionPreRegister returned nil despite non-nil cache+group")
	}
	if err := pre.PreAddMember(context.Background(), "n1", "n1:50051"); err != nil {
		t.Errorf("PreAddMember should skip pre-bootstrap: got %v", err)
	}
}

// TestEncryptionPreRegister_NilCacheReturnsNilInterceptor pins the
// adapter constructor's defensive nil-check: missing wiring inputs
// produce a nil interceptor (no pre-step) rather than a panicking
// runtime hook.
func TestEncryptionPreRegister_NilCacheReturnsNilInterceptor(t *testing.T) {
	t.Parallel()
	if got := newEncryptionPreRegister(nil, nil, nil, "", nil); got != nil {
		t.Errorf("all-nil inputs: want nil interceptor, got %T", got)
	}
	if got := newEncryptionPreRegister(&kv.ShardedCoordinator{}, &kv.ShardGroup{}, nil, "", stubDeriveNodeID); got != nil {
		t.Error("nil cache: want nil interceptor")
	}
	if got := newEncryptionPreRegister(&kv.ShardedCoordinator{}, &kv.ShardGroup{}, encryption.NewStateCache(), "", nil); got != nil {
		t.Error("nil deriveNodeID: want nil interceptor")
	}
}

// TestEncryptionPreRegister_IdempotentWhenRowExists pins design §5.2
// + the gemini-CRITICAL/claude-round-2 fix: when a registry row
// already exists for (activeDEK, NodeID16(newNodeFullID)) with the
// SAME FullNodeID, PreAddMember returns nil without proposing
// (idempotent retry, e.g. after a leader flip). This pins the
// §3.1 read-before-propose guard against the §4.1 case-3
// ErrLocalEpochRollback regression that would otherwise fire on
// re-proposing epoch=0.
func TestEncryptionPreRegister_IdempotentWhenRowExists(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	// Pre-populate (testRegDEKID, stubDerivedNodeID, 0). Use the
	// registry handle directly (writeRegistryRow uses a different
	// FullNodeID derivation).
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}
	val := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID: stubDerivedNodeID, FirstSeenLocalEpoch: 0, LastSeenLocalEpoch: 0,
	})
	if err := reg.SetRegistryRow(encryption.RegistryKey(testRegDEKID, encryption.NodeID16(stubDerivedNodeID)), val); err != nil {
		t.Fatalf("SetRegistryRow: %v", err)
	}

	cache := encryption.NewStateCache()
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion, StorageEnvelopeActive: true}
	sc.Active.Storage = testRegDEKID
	cache.RefreshFromSidecar(sc)

	pre := newEncryptionPreRegister(&kv.ShardedCoordinator{}, &kv.ShardGroup{Store: st}, cache, "", stubDeriveNodeID, allowStorageEnvelopeV2Capability)
	if err := pre.PreAddMember(context.Background(), "raftN", "raftN:50051"); err != nil {
		t.Errorf("PreAddMember should skip when matching row exists: got %v", err)
	}
}

func TestEncryptionPreRegister_RejectsMemberWithoutV2Capability(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	cache := encryption.NewStateCache()
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion, StorageEnvelopeActive: true}
	sc.Active.Storage = testRegDEKID
	cache.RefreshFromSidecar(sc)
	sentinel := errors.New("old binary")
	var probedAddress string
	pre := newEncryptionPreRegister(
		&kv.ShardedCoordinator{},
		&kv.ShardGroup{Store: st},
		cache,
		"",
		stubDeriveNodeID,
		func(_ context.Context, address string) error {
			probedAddress = address
			return sentinel
		},
	)
	err := pre.PreAddMember(context.Background(), "old", "old:50051")
	if !errors.Is(err, sentinel) {
		t.Fatalf("PreAddMember error = %v, want capability error", err)
	}
	if probedAddress != "old:50051" {
		t.Fatalf("capability probe address = %q, want old:50051", probedAddress)
	}
	reg, regErr := store.WriterRegistryFor(st)
	if regErr != nil {
		t.Fatalf("WriterRegistryFor: %v", regErr)
	}
	if _, ok, regErr := reg.GetRegistryRow(encryption.RegistryKey(testRegDEKID, encryption.NodeID16(stubDerivedNodeID))); regErr != nil || ok {
		t.Fatalf("registry changed before capability acceptance: present=%t err=%v", ok, regErr)
	}
}

// TestEncryptionPreRegister_Uint16CollisionReturnsTypedError pins
// design §5.2 + §3.1's §6.1-collision branch: when a row exists at
// the same uint16 truncation with a DIFFERENT FullNodeID, the guard
// returns ErrWriterUint16Collision WITHOUT proposing. No §4.1
// case-4 halt-apply is triggered; the admin RPC layer surfaces a
// retryable client-facing error.
func TestEncryptionPreRegister_Uint16CollisionReturnsTypedError(t *testing.T) {
	t.Parallel()
	st := newRegistrationTestStore(t)
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}
	// Stored row's FullNodeID == stubDerivedNodeID; a colliding
	// FullNodeID is one that hits the same uint16 truncation but
	// differs in the upper 48 bits.
	val := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID: stubDerivedNodeID, FirstSeenLocalEpoch: 0, LastSeenLocalEpoch: 0,
	})
	if err := reg.SetRegistryRow(encryption.RegistryKey(testRegDEKID, encryption.NodeID16(stubDerivedNodeID)), val); err != nil {
		t.Fatalf("SetRegistryRow: %v", err)
	}

	cache := encryption.NewStateCache()
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion, StorageEnvelopeActive: true}
	sc.Active.Storage = testRegDEKID
	cache.RefreshFromSidecar(sc)

	// Adapter that derives a DIFFERENT FullNodeID with the same uint16
	// truncation as stubDerivedNodeID.
	collidingFullNodeID := (uint64(0x1111_2222_3333) << 16) | uint64(encryption.NodeID16(stubDerivedNodeID))
	if collidingFullNodeID == stubDerivedNodeID {
		t.Fatalf("test bug: colliding == stored; choose a different upper-16 mask")
	}
	collidingDerive := func(string) uint64 { return collidingFullNodeID }

	pre := newEncryptionPreRegister(&kv.ShardedCoordinator{}, &kv.ShardGroup{Store: st}, cache, "", collidingDerive, allowStorageEnvelopeV2Capability)
	err = pre.PreAddMember(context.Background(), "raftN", "raftN:50051")
	if !errors.Is(err, encryption.ErrWriterUint16Collision) {
		t.Errorf("PreAddMember on §6.1 collision: want ErrWriterUint16Collision, got %v", err)
	}
}

func TestEncryptionPreRegister_ActiveRaftDEKForPreRegister(t *testing.T) {
	t.Parallel()
	const activeRaftDEK uint32 = 22
	sidecarPath := writeRaftCutoverSidecarForStartup(t, activeRaftDEK, 0, 100)
	pre := &encryptionPreRegister{sidecarPath: sidecarPath}
	got, ok, err := pre.activeRaftDEKForPreRegister()
	if err != nil {
		t.Fatalf("activeRaftDEKForPreRegister: %v", err)
	}
	if !ok {
		t.Fatal("activeRaftDEKForPreRegister returned ok=false for active raft cutover")
	}
	if got != activeRaftDEK {
		t.Fatalf("activeRaftDEKForPreRegister = %d, want %d", got, activeRaftDEK)
	}
}

func TestEncryptionPreRegister_ActiveRaftDEKForPreRegisterBeforeCutover(t *testing.T) {
	t.Parallel()
	const activeRaftDEK uint32 = 22
	sidecarPath := writeRaftCutoverSidecarForStartup(t, activeRaftDEK, 0, 0)
	pre := &encryptionPreRegister{sidecarPath: sidecarPath}
	got, ok, err := pre.activeRaftDEKForPreRegister()
	if err != nil {
		t.Fatalf("activeRaftDEKForPreRegister: %v", err)
	}
	if !ok {
		t.Fatal("activeRaftDEKForPreRegister returned ok=false for active raft DEK before cutover")
	}
	if got != activeRaftDEK {
		t.Fatalf("activeRaftDEKForPreRegister = %d, want %d", got, activeRaftDEK)
	}
}

type recordingMembershipProposer struct {
	raftengine.Engine
	proposeCalls      int
	proposeAdminCalls int
}

func (p *recordingMembershipProposer) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	p.proposeCalls++
	return &raftengine.ProposalResult{CommitIndex: 1}, nil
}

func (p *recordingMembershipProposer) ProposeAdmin(context.Context, []byte) (*raftengine.ProposalResult, error) {
	p.proposeAdminCalls++
	return &raftengine.ProposalResult{CommitIndex: 1}, nil
}

func (p *recordingMembershipProposer) State() raftengine.State { return raftengine.StateLeader }
func (p *recordingMembershipProposer) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "self", Address: "127.0.0.1:0"}
}
func (p *recordingMembershipProposer) VerifyLeader(context.Context) error { return nil }
func (p *recordingMembershipProposer) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}
func (p *recordingMembershipProposer) Status() raftengine.Status {
	return raftengine.Status{State: raftengine.StateLeader}
}
func (p *recordingMembershipProposer) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}
func (p *recordingMembershipProposer) Close() error { return nil }

func TestProposeWriterRegistrationBlockingOnCutover_LeaderUsesPropose(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	proposer := &recordingMembershipProposer{}
	groups := map[uint64]*kv.ShardGroup{1: {Engine: proposer}}
	coord := kv.NewShardedCoordinator(engine, groups, 1, kv.NewHLC(), nil)
	req := &pb.RegisterEncryptionWriterRequest{DekId: 7, Writers: []*pb.WriterRegistryEntry{{
		FullNodeId: 0x1234,
		LocalEpoch: 0,
	}}}
	err := proposeWriterRegistrationBlockingOnCutover(
		context.Background(),
		coord,
		proposer,
		&kv.GRPCConnCache{},
		[]byte{0x01, 0x02},
		req,
	)
	if err != nil {
		t.Fatalf("proposeWriterRegistrationBlockingOnCutover: %v", err)
	}
	if proposer.proposeCalls != 1 {
		t.Fatalf("Propose calls = %d, want 1", proposer.proposeCalls)
	}
	if proposer.proposeAdminCalls != 0 {
		t.Fatalf("ProposeAdmin calls = %d, want 0", proposer.proposeAdminCalls)
	}
}

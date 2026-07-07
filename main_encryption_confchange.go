package main

import (
	"context"
	"log/slog"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// encryptionPreRegister is the Stage 7c §3.1 encryption-aware
// implementation of raftadmin.MembershipChangeInterceptor. It runs
// on the leader's AddVoter/AddLearner handler before the underlying
// Raft engine proposes the conf-change, proposing a writer-registry
// row for the new node so:
//
//   - Encrypted writes from the new node never fail closed on the
//     7a-2 storage gate after the conf-change commits (guarantee 1:
//     write-window elimination).
//   - A §6.1 uint16 NodeID collision is caught at the RPC layer via
//     ErrWriterUint16Collision before the conf-change is proposed —
//     no §4.1 case-4 halt apply on a durable conf-change entry
//     (guarantee 2: collision-safe membership change).
//
// See docs/design/2026_05_29_proposed_7c_confchange_time_registration.md.
type encryptionPreRegister struct {
	coordinate   *kv.ShardedCoordinator
	defaultGroup *kv.ShardGroup
	cache        *encryption.StateCache
	sidecarPath  string
	// deriveNodeID is injected so the adapter is engine-neutral —
	// main.go supplies etcdraftengine.DeriveNodeID today; a future
	// engine swap is a one-line wiring change here, not an adapter
	// refactor (gemini medium #1 on PR #868 / design §3.1).
	deriveNodeID func(raftID string) uint64
}

// newEncryptionPreRegister constructs the interceptor or returns nil
// when encryption is not wired (no shared cache or no default
// group). A nil interceptor causes raftadmin.Server to skip the
// pre-step, matching the pre-7c behavior on encryption-disabled
// clusters.
func newEncryptionPreRegister(
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	cache *encryption.StateCache,
	sidecarPath string,
	deriveNodeID func(raftID string) uint64,
) raftadmin.MembershipChangeInterceptor {
	if coordinate == nil || defaultGroup == nil || defaultGroup.Store == nil || cache == nil || deriveNodeID == nil {
		return nil
	}
	return &encryptionPreRegister{
		coordinate:   coordinate,
		defaultGroup: defaultGroup,
		cache:        cache,
		sidecarPath:  sidecarPath,
		deriveNodeID: deriveNodeID,
	}
}

// PreAddMember implements raftadmin.MembershipChangeInterceptor.
// Returns nil (skip) when the cluster is not bootstrapped or when a
// matching registry row already exists; returns
// encryption.ErrWriterUint16Collision on a §6.1 collision; otherwise
// proposes RegisterEncryptionWriter(activeDEK, NodeID(raftID), 0)
// and surfaces the propose result.
func (e *encryptionPreRegister) PreAddMember(ctx context.Context, raftID string) error {
	activeDEK, ok := e.cache.ActiveStorageKeyID()
	if !ok {
		// Pre-bootstrap cluster: there is no registry to gate on.
		// (Encryption-enabled but not yet bootstrapped, or
		// encryption-disabled with an empty cache.)
		return nil
	}
	newNodeFullID := e.deriveNodeID(raftID)
	if err := e.preRegisterDEK(ctx, activeDEK, newNodeFullID, "storage"); err != nil {
		return err
	}
	raftDEK, ok, err := e.activeRaftDEKForPreRegister()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return e.preRegisterDEK(ctx, raftDEK, newNodeFullID, "raft")
}

func (e *encryptionPreRegister) activeRaftDEKForPreRegister() (uint32, bool, error) {
	if e.sidecarPath == "" {
		return 0, false, nil
	}
	sc, err := encryption.ReadSidecar(e.sidecarPath)
	if err != nil {
		if encryption.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, errors.Wrap(err, "7c pre-register: read sidecar for raft DEK")
	}
	if sc.RaftEnvelopeCutoverIndex == 0 || sc.Active.Raft == 0 {
		return 0, false, nil
	}
	return sc.Active.Raft, true, nil
}

func (e *encryptionPreRegister) preRegisterDEK(ctx context.Context, activeDEK uint32, newNodeFullID uint64, purpose string) error {
	// store.WriterRegistryFor is a stateless wrapper (same idiom as
	// 7a §3.1 startup check); avoiding a cached field on the adapter
	// keeps construction simple and the dependency surface narrow.
	reg, err := store.WriterRegistryFor(e.defaultGroup.Store)
	if err != nil {
		return errors.Wrapf(err, "7c pre-register: %s registry handle", purpose)
	}
	// Read-before-propose guard (design §3.1). Two purposes:
	//
	//   1. LOAD-BEARING: catch §6.1 uint16 collisions (existing row
	//      under the same NodeID16 but a different FullNodeID) at
	//      the RPC layer and return the typed
	//      ErrWriterUint16Collision WITHOUT proposing. Without the
	//      guard, the propose would commit and the apply would hit
	//      §4.1 case-4 (different FullNodeID at same uint16) → halt
	//      apply on a durable conf-change entry. This is the
	//      irrecoverable scenario 7c exists to prevent.
	//
	//   2. OPTIMIZATION: skip a redundant Raft round-trip on a
	//      same-FullNodeID retry (e.g. leader-flip mid-step). The
	//      FSM would safely no-op via §4.1 case-2-idempotent
	//      (proposed_epoch=0 == last_seen_epoch=0 → return nil; see
	//      applier.go:526), so re-proposing is not unsafe — just
	//      wasteful. (An earlier comment claimed case-3
	//      ErrLocalEpochRollback fired on the retry path, which was
	//      wrong: case-3 requires strictly less than; corrected in
	//      claude round-3 on PR #872.)
	//
	// Branches: existing row + matching FullNodeID → nil (idempotent
	// skip); existing row + FullNodeID mismatch →
	// ErrWriterUint16Collision; no row → fall through to propose.
	existing, exists, err := reg.GetRegistryRow(encryption.RegistryKey(activeDEK, encryption.NodeID16(newNodeFullID)))
	if err != nil {
		return errors.Wrapf(err, "7c pre-register: %s read registry row", purpose)
	}
	if exists {
		val, derr := encryption.DecodeRegistryValue(existing)
		if derr != nil {
			return errors.Wrapf(derr, "7c pre-register: %s decode registry value", purpose)
		}
		if val.FullNodeID == newNodeFullID {
			return nil // already registered; idempotent skip
		}
		return encryption.ErrWriterUint16Collision
	}
	entry := registrationEntry(activeDEK, newNodeFullID, 0)
	req := registrationRequest(activeDEK, newNodeFullID, 0)
	// Always allocate connCache (NOT lazy-on-non-leader): a lazy
	// allocation guarded by `if !e.coordinate.IsLeader()` would race
	// with leadership change inside proposeWriterRegistration, which
	// performs its own IsLeader() check before consulting connCache.
	// A leader-flip between the two checks would reach
	// connCache.ConnFor on a nil pointer — segfault (claude round-2
	// MEDIUM on PR #872, reverting the round-1 gemini optimization).
	// The same goroutine-scoped pattern is what the 7a/7b/7b'
	// registration paths use.
	connCache := &kv.GRPCConnCache{}
	defer func() {
		if cerr := connCache.Close(); cerr != nil {
			slog.Warn("encryption: 7c pre-register failed to close gRPC connection cache",
				slog.String("error", cerr.Error()))
		}
	}()
	// TOCTOU residual for concurrent same-raftID AddVoter calls
	// (claude round-2 on PR #872 — corrected): two concurrent
	// PreAddMember calls for the SAME raftID can both read "no row"
	// before either proposal applies, then both propose epoch=0.
	// The FSM applies the first as §4.1 case-1 first-seen
	// (FirstSeen=LastSeen=0); the second hits §4.1 **case-2-
	// idempotent** because proposed_epoch=0 == last_seen_epoch=0 →
	// return nil (no error, no halt apply, safe no-op). Case-3
	// requires proposed_epoch STRICTLY LESS than last_seen_epoch,
	// which 0 == 0 does not satisfy. The concurrent same-raftID
	// scenario is therefore harmless — one Raft round-trip on the
	// duplicate, no operator recovery needed. (The §6.1 uint16-
	// collision TOCTOU — different FullNodeID at the same NodeID16
	// — is the genuine case-4 halt-apply residual; see §3.3 of the
	// design doc.)
	return proposeWriterRegistration(ctx, e.coordinate, e.defaultGroup.Proposer(), connCache, entry, req)
}

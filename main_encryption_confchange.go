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
	deriveNodeID func(raftID string) uint64,
) raftadmin.MembershipChangeInterceptor {
	if coordinate == nil || defaultGroup == nil || defaultGroup.Store == nil || cache == nil || deriveNodeID == nil {
		return nil
	}
	return &encryptionPreRegister{
		coordinate:   coordinate,
		defaultGroup: defaultGroup,
		cache:        cache,
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
	// store.WriterRegistryFor is a stateless wrapper (same idiom as
	// 7a §3.1 startup check); avoiding a cached field on the adapter
	// keeps construction simple and the dependency surface narrow.
	reg, err := store.WriterRegistryFor(e.defaultGroup.Store)
	if err != nil {
		return errors.Wrap(err, "7c pre-register: registry handle")
	}
	// Read-before-propose guard (design §3.1, claude round-2 BLOCKING
	// on PR #868). Required because §4.1 case-2 fires ONLY when
	// proposed_epoch > last_seen_epoch (strictly greater) — re-
	// proposing epoch=0 against an existing (epoch=0) row hits case-3
	// ErrLocalEpochRollback, not the idempotent case-2 path. The
	// guard reads the row first; if it already exists for this exact
	// FullNodeID at any epoch, skip the propose (idempotent retry).
	// FullNodeID mismatch at the same uint16 truncation is the §6.1
	// collision — return the typed error WITHOUT proposing so we do
	// not trigger a case-4 halt apply.
	existing, exists, err := reg.GetRegistryRow(encryption.RegistryKey(activeDEK, encryption.NodeID16(newNodeFullID)))
	if err != nil {
		return errors.Wrap(err, "7c pre-register: read registry row")
	}
	if exists {
		val, derr := encryption.DecodeRegistryValue(existing)
		if derr != nil {
			return errors.Wrap(derr, "7c pre-register: decode registry value")
		}
		if val.FullNodeID == newNodeFullID {
			return nil // already registered; idempotent skip
		}
		return encryption.ErrWriterUint16Collision
	}
	entry := registrationEntry(activeDEK, newNodeFullID, 0)
	req := registrationRequest(activeDEK, newNodeFullID, 0)
	// connCache is only consulted by proposeWriterRegistration when
	// the local node is NOT the leader (forward-to-leader path). On
	// the common leader path — AddVoter/AddLearner is leader-only —
	// the cache is never touched, so allocating it eagerly is wasted
	// work (gemini medium on PR #872). Lazy-allocate + defer-close
	// only on the non-leader branch.
	var connCache *kv.GRPCConnCache
	if !e.coordinate.IsLeader() {
		connCache = &kv.GRPCConnCache{}
		defer func() {
			if cerr := connCache.Close(); cerr != nil {
				slog.Warn("encryption: 7c pre-register failed to close gRPC connection cache",
					slog.String("error", cerr.Error()))
			}
		}()
	}
	// TOCTOU residual (claude review on PR #872): two concurrent
	// PreAddMember calls for the SAME raftID can both read "no row"
	// before either proposal applies, then both propose epoch=0. The
	// FSM applies the first as §4.1 case-1 first-seen; the second
	// hits §4.1 case-3 (proposed_epoch=0 ≤ last_seen_epoch=0) →
	// ErrLocalEpochRollback halt-apply. Recovery is process restart;
	// the entry replays cleanly. Concurrent AddVoter for the same
	// raftID is an unusual operator pattern, but the guard cannot
	// catch it because both reads precede both writes.
	return proposeWriterRegistration(ctx, e.coordinate, e.defaultGroup.Engine, connCache, entry, req)
}

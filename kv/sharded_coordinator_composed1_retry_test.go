package kv

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// composed1RetryingTransactional is a stub Transactional that fails the
// first Commit attempt with a Composed-1 sentinel and succeeds on the
// next.  Used to drive the M4 retry path without spinning up two real
// Raft groups for an integration test (a future follow-up under M5 will
// land the full Jepsen-shaped scenario).
type composed1RetryingTransactional struct {
	calls            atomic.Int32
	firstErr         error
	observedSeen     []uint64
	prevCommitTSSeen []uint64
}

func (s *composed1RetryingTransactional) Commit(_ context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	n := s.calls.Add(1)
	if len(reqs) > 0 {
		s.observedSeen = append(s.observedSeen, reqs[0].GetObservedRouteVersion())
		// PrevCommitTS rides inside the encoded TxnMeta on the first
		// mutation of a one-phase txn request (see
		// onePhaseTxnRequestWithPrevCommit in coordinator.go).  Decode
		// it so the M4 retry tests can assert the field is cleared on
		// the retry attempt (claude[bot] medium on PR #900: the M3
		// gate fires at apply time, so the original attempt never
		// committed; leaving PrevCommitTS non-zero on the retry causes
		// dispatchMultiShardTxn to refuse with
		// ErrTxnDedupRequiresSingleShard when groupMutations now spans
		// two groups).
		var prev uint64
		if muts := reqs[0].GetMutations(); len(muts) > 0 {
			if meta, err := DecodeTxnMeta(muts[0].GetValue()); err == nil {
				prev = meta.PrevCommitTS
			}
		}
		s.prevCommitTSSeen = append(s.prevCommitTSSeen, prev)
	}
	if n == 1 {
		return nil, s.firstErr
	}
	return &TransactionResponse{CommitIndex: uint64(n)}, nil //nolint:gosec // n is a small positive counter for test-stub responses
}

func (s *composed1RetryingTransactional) Abort(context.Context, []*pb.Request) (*TransactionResponse, error) {
	return &TransactionResponse{}, nil
}

// TestShardedCoordinator_RetriesOnComposed1Violation is the M4
// "Done when" criterion from the design doc: a Composed-1 sentinel
// surfacing from the underlying Commit triggers a single coordinator-
// side retry against the engine's now-current catalog version.  The
// caller's client-observable result must be the SECOND attempt's
// success, not the first attempt's ErrComposed1Violation.
func TestShardedCoordinator_RetriesOnComposed1Violation(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	txn := &composed1RetryingTransactional{
		firstErr: errors.WithStack(ErrComposed1Violation),
	}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: txn},
	}, 1, NewHLC(), nil)

	// In a real cluster the route catalog advance between the first
	// and second attempts is driven by a concurrent CatalogWatcher
	// fan-out.  This test does not model the advance directly — the
	// stub returns the sentinel unconditionally and the retry path's
	// re-read picks up whatever the engine reports.  observedSeen[1]
	// equality below asserts the RE-READ contract, not a particular
	// value (claude[bot] minor on PR #900: an empty goroutine here
	// looked like an unfinished concurrent-advance model).
	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 10,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.NoError(t, err, "second attempt must succeed; the first attempt's ErrComposed1Violation is what M4 absorbs")
	require.NotNil(t, resp)
	require.Equal(t, int32(2), txn.calls.Load(),
		"M4 retry budget is composed1RetryAttempts=1, so exactly two attempts must fire on a single Composed-1 sentinel")
	require.Len(t, txn.observedSeen, 2,
		"both attempts must record an ObservedRouteVersion (the first pinned at Dispatch entry, the second re-pinned on retry)")
	require.Equal(t, uint64(1), txn.observedSeen[0],
		"first attempt must carry the engine's catalog version at Dispatch entry (v=1)")
	require.Equal(t, uint64(1), txn.observedSeen[1],
		"second attempt must re-read and re-stamp the engine version; the engine did not advance between attempts in this test so the value happens to match — the contract is that we RE-READ, not that we always change")
}

// TestShardedCoordinator_RetriesOnComposed1VersionGCd verifies the
// second M4 sentinel (retention-miss → re-read catalog) drives the
// same retry path.  The two sentinels differ only in their cause;
// both call for the same "stamp a fresh ObservedRouteVersion and
// re-issue" recovery.
func TestShardedCoordinator_RetriesOnComposed1VersionGCd(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	txn := &composed1RetryingTransactional{
		firstErr: errors.WithStack(ErrComposed1VersionGCd),
	}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: txn},
	}, 1, NewHLC(), nil)

	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 10,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.NoError(t, err, "second attempt must succeed; the first attempt's ErrComposed1VersionGCd is what M4 absorbs")
	require.NotNil(t, resp)
	require.Equal(t, int32(2), txn.calls.Load(),
		"retention-miss sentinel must drive exactly the same single retry as ErrComposed1Violation")
}

// TestShardedCoordinator_PersistentComposed1ViolationSurfaces verifies
// the retry budget: a Composed-1 sentinel that survives the M4
// re-route must surface to the caller, not loop forever.  This is
// what lets a wrapping adapter retry harness (or the client) see
// the failure when the catalog churn outpaces the coordinator's
// re-read cadence.
func TestShardedCoordinator_PersistentComposed1ViolationSurfaces(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	// Persistent failure — every Commit returns the sentinel.
	persistent := &persistentComposed1Failer{err: errors.WithStack(ErrComposed1Violation)}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: persistent},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 10,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.ErrorIs(t, err, ErrComposed1Violation,
		"the sentinel must surface unchanged after the retry budget is exhausted")
	require.Equal(t, int32(2), persistent.calls.Load(),
		"composed1RetryAttempts=1 means initial + 1 retry = exactly 2 total Commit calls on persistent failure")
}

type persistentComposed1Failer struct {
	calls atomic.Int32
	err   error
}

func (p *persistentComposed1Failer) Commit(context.Context, []*pb.Request) (*TransactionResponse, error) {
	p.calls.Add(1)
	return nil, p.err
}

func (p *persistentComposed1Failer) Abort(context.Context, []*pb.Request) (*TransactionResponse, error) {
	return &TransactionResponse{}, nil
}

// TestShardedCoordinator_PinsObservedRouteVersionAtDispatchEntry
// documents the M4 pin: every txn that flows through Dispatch with
// ObservedRouteVersion=0 gets stamped with the engine's current
// catalog version.  This makes the M3 verifyComposed1 gate
// effective by default — without this pin every caller would have
// to discover and follow the §4.1 contract independently.
func TestShardedCoordinator_PinsObservedRouteVersionAtDispatchEntry(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 7,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	txn := &composed1RetryingTransactional{firstErr: nil} // succeeds on first attempt
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: txn},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 10,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.Len(t, txn.observedSeen, 1)
	require.Equal(t, uint64(7), txn.observedSeen[0],
		"Dispatch must pin reqs.ObservedRouteVersion to engine.Version() at entry when the caller leaves it at zero")
}

// TestShardedCoordinator_HonorsCallerPinnedObservedRouteVersion
// documents that an adapter that DOES set ObservedRouteVersion
// explicitly (a future migration target) wins over the
// Dispatch-entry pin.  This preserves the §4.1 contract for callers
// that want to manage the version themselves (e.g., a BeginTxn
// snapshot taken earlier than this Dispatch call would otherwise
// observe).
func TestShardedCoordinator_HonorsCallerPinnedObservedRouteVersion(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 7,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	txn := &composed1RetryingTransactional{firstErr: nil}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: txn},
	}, 1, NewHLC(), nil)

	const callerPinned = uint64(3)
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:                true,
		StartTS:              10,
		ObservedRouteVersion: callerPinned,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.Len(t, txn.observedSeen, 1)
	require.Equal(t, callerPinned, txn.observedSeen[0],
		"a caller-supplied ObservedRouteVersion must survive the Dispatch-entry pin so adapters can manage the version themselves (§4.1)")
}

// TestShardedCoordinator_DoesNotRetryReadWriteTxnOnComposed1 is the
// regression for the gemini CRITICAL / codex P1 finding on PR #900:
// transparently retrying a txn with a bumped StartTS is only safe
// when the txn is write-only.  When the txn has read keys
// (Redis MULTI/EXEC, DynamoDB TransactGetItems → TransactWrite, etc.),
// the client already performed those reads at the original StartTS.
// Bumping StartTS on retry would silently mask any concurrent writes
// that landed between the old and new timestamps — the FSM's OCC
// check rejects only versions with `latest > startTS`, so the retry
// could commit decisions based on stale reads, violating SSI.
//
// Expected behaviour: the sentinel surfaces immediately (no retry)
// when len(ReadKeys) > 0.  The caller / adapter is then responsible
// for re-executing the entire txn (including re-reading the keys
// at a fresh timestamp).
func TestShardedCoordinator_DoesNotRetryReadWriteTxnOnComposed1(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	txn := &composed1RetryingTransactional{
		firstErr: errors.WithStack(ErrComposed1Violation),
	}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: txn},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:    true,
		StartTS:  10,
		ReadKeys: [][]byte{[]byte("k_read")},
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k"), Value: []byte("v")},
		},
	})
	require.ErrorIs(t, err, ErrComposed1Violation,
		"read-write txns (ReadKeys non-empty) must surface the Composed-1 sentinel WITHOUT retry: the client read at the original StartTS, so silently bumping StartTS on retry would let concurrent writes between the old and new timestamps go undetected by OCC (SSI violation)")
	require.Equal(t, int32(1), txn.calls.Load(),
		"read-write txn must NOT retry: exactly 1 Commit attempt — the M4 retry is reserved for write-only txns where bumping StartTS is OCC-safe")
}

// TestShardedCoordinator_DoesNotAutoPinObservedRouteVersionForReadWriteTxn
// is the regression for the codex P1 finding on PR #900: when a caller
// already supplies a transaction StartTS/ReadKeys but leaves
// ObservedRouteVersion unset, this used to stamp the catalog version
// at EXEC/commit dispatch time rather than when the read set was
// captured.
//
// Concrete scenario: Redis txnContext.commit builds an OperationGroup
// with StartTS (allocated at MULTI) and ReadKeys (the GETs / WATCHes
// from the txn body) but no ObservedRouteVersion.  If a route moves
// between the MULTI-time reads and this Dispatch call, an auto-pin
// at Dispatch entry would stamp the POST-shift catalog version on
// the request and verifyComposed1 would NOT detect the shift the
// guard is meant to catch.
//
// Correct behaviour: do NOT auto-fill ObservedRouteVersion when the
// caller already has a read snapshot (len(ReadKeys) > 0).  Such
// callers must pin ObservedRouteVersion themselves at BeginTxn time
// per design doc §4.1; leaving it at zero surfaces as "unpinned"
// to the M3 gate (which short-circuits), which is the safe default
// for non-migrated callers — the gate cannot retroactively pin
// reads it was not present for.
//
// Write-only txns (len(ReadKeys) == 0) continue to get auto-pinned
// at Dispatch entry because there are no prior reads whose
// snapshot version could disagree with the Dispatch-time pin.
func TestShardedCoordinator_DoesNotAutoPinObservedRouteVersionForReadWriteTxn(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 5,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	txn := &composed1RetryingTransactional{firstErr: nil}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: txn},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:    true,
		StartTS:  10,
		ReadKeys: [][]byte{[]byte("k_read")},
		// ObservedRouteVersion intentionally left at zero — caller
		// did not migrate to pin at BeginTxn.
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k_write"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.Len(t, txn.observedSeen, 1)
	require.Equal(t, uint64(0), txn.observedSeen[0],
		"read-write txns with unset ObservedRouteVersion must NOT be auto-pinned at Dispatch entry — pinning at commit time disagrees with the reads' actual snapshot version, so verifyComposed1 must short-circuit (observedVer=0) until the caller migrates to pin at BeginTxn per §4.1")
}

// TestShardedCoordinator_ClearsPrevCommitTSOnComposed1Retry verifies
// that the M4 retry path zeros OperationGroup.PrevCommitTS before
// the second dispatchTxn call.  Rationale (claude[bot] medium on
// PR #900): the M3 gate fires at apply time, so the ORIGINAL attempt
// never committed — there is no prior landing for PrevCommitTS to
// dedup against on the retry.  If the retry kept the caller's
// PrevCommitTS non-zero AND the post-shift catalog reroutes the txn
// to span two groups, dispatchMultiShardTxn would short-circuit with
// ErrTxnDedupRequiresSingleShard (kv/sharded_coordinator.go:736) on a
// signal that no longer holds, surfacing a misleading error to the
// caller instead of a correct re-route.  The contract is: on
// Composed-1 retry, drop the dedup probe — the FSM's option-2
// idempotency check has nothing valid to compare against.
//
// This is a write-only txn (no ReadKeys); the M4 retry is allowed
// (per the gemini-CRITICAL gate already in place) and PrevCommitTS
// is the option-2 dedup probe the adapter set on the first attempt.
func TestShardedCoordinator_ClearsPrevCommitTSOnComposed1Retry(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	txn := &composed1RetryingTransactional{
		firstErr: errors.WithStack(ErrComposed1Violation),
	}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: txn},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:        true,
		StartTS:      10,
		CommitTS:     20,
		PrevCommitTS: 17, // adapter-supplied option-2 dedup probe
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k_write"), Value: []byte("v")},
		},
	})
	require.NoError(t, err, "M4 must retry write-only Composed-1 once and succeed on attempt 2")
	require.Len(t, txn.prevCommitTSSeen, 2,
		"both attempts must be observed for the PrevCommitTS comparison")
	require.Equal(t, uint64(17), txn.prevCommitTSSeen[0],
		"first attempt must carry the caller's PrevCommitTS unchanged — that is the option-2 dedup probe semantics")
	require.Equal(t, uint64(0), txn.prevCommitTSSeen[1],
		"second attempt must drop PrevCommitTS — the M3 gate fired at apply time so the original attempt never committed; leaving the probe set would let dispatchMultiShardTxn return ErrTxnDedupRequiresSingleShard on a stale signal if the retry's groupMutations now spans two groups")
}

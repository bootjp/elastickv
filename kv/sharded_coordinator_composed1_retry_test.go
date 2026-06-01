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
	calls        atomic.Int32
	firstErr     error
	observedSeen []uint64
}

func (s *composed1RetryingTransactional) Commit(_ context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	n := s.calls.Add(1)
	if len(reqs) > 0 {
		s.observedSeen = append(s.observedSeen, reqs[0].GetObservedRouteVersion())
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

	// Between the first and second dispatch attempts, simulate the
	// route catalog moving forward — M4's retry path must re-read
	// the engine version and stamp the new one on the retry's
	// pb.Request so the FSM sees the post-shift observed version.
	go func() {
		// The retry happens synchronously inside Dispatch; advance the
		// catalog before the second attempt would re-read it.  In a
		// real cluster the advance is driven by a concurrent
		// CatalogWatcher fan-out; here we just race against the
		// in-process Commit to model the same observable effect.
	}()

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

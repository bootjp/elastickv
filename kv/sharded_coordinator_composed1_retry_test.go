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
		// it so the M4 retry tests can assert the field is PRESERVED
		// across retry attempts — the probe names the ADAPTER'S
		// prior attempt (e.g. adapter/redis.go:3209, :3615 send it
		// as the option-2 ambiguous-outcome probe), which may have
		// LANDED at the named commitTS before the route shift that
		// triggered our M3 sentinel.  Dropping the probe in the
		// retry would let the FSM re-apply against the post-shift
		// route and double-write if that prior attempt landed
		// (codex P1 on 43c55dfe, PR #900 — proved the earlier
		// claude[bot] "clear it" advice was wrong).
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
		IsTxn: true,
		// StartTS intentionally omitted — let the coordinator
		// allocate via nextStartTS so the M4 retry path is
		// eligible to fire.  Caller-supplied StartTS surfaces the
		// sentinel without retry (codex P1 on 57da8886, PR #900);
		// this test exercises the coordinator-allocated path.
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
		IsTxn: true,
		// StartTS omitted so the coordinator allocates — see
		// TestShardedCoordinator_RetriesOnComposed1Violation for
		// rationale (codex P1 on 57da8886, PR #900).
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
		IsTxn: true,
		// StartTS omitted so the coordinator allocates — see
		// TestShardedCoordinator_RetriesOnComposed1Violation
		// (codex P1 on 57da8886, PR #900).
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

// TestShardedCoordinator_PreservesPrevCommitTSOnComposed1Retry pins
// the at-most-once correctness contract: the M4 retry path MUST keep
// OperationGroup.PrevCommitTS intact across attempts.
//
// Earlier in this PR the retry path cleared PrevCommitTS in the
// retry block (taking claude[bot]'s reasoning at face value: "M3
// fired at apply time → original attempt never committed → probe is
// stale").  codex P1 on 43c55dfe correctly identified that this
// conflates two different "original attempts":
//
//   - Within M4: attempt 1's M3 rejection proves attempt 1 wrote
//     nothing (the FSM rejects before any state change).
//   - Across adapter retries: PrevCommitTS names the ADAPTER'S prior
//     attempt — see adapter/redis.go list-push reuse path that sends
//     PrevCommitTS = pending.commitTS as the option-2 ambiguous-outcome
//     probe.  That earlier attempt may have actually LANDED at the
//     named commitTS before the route shift that triggered our M3
//     sentinel.
//
// Dropping the probe in the retry would let the FSM re-apply the
// writes against the post-shift route, double-writing the caller's
// at-most-once-payload if the named prior attempt did land.
// Preserving the probe is the safe behaviour even at the cost of
// dispatchMultiShardTxn:749 surfacing ErrTxnDedupRequiresSingleShard
// in the post-shift-multi-shard case — that error IS the faithful
// signal that the adapter's at-most-once retry contract cannot be
// transparently honoured across a shard split.
//
// This test uses a single-shard fixture (one route, one ShardGroup)
// so the second attempt succeeds in the stub; the assertion is on
// the bytes that reach Commit, which is what the FSM actually
// reads.
func TestShardedCoordinator_PreservesPrevCommitTSOnComposed1Retry(t *testing.T) {
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
		IsTxn: true,
		// StartTS / CommitTS intentionally omitted — let the
		// coordinator allocate so the M4 retry path is eligible
		// (codex P1 on 57da8886, PR #900: caller-supplied
		// StartTS surfaces without retry).  PrevCommitTS is a
		// separate option-2 dedup probe and is set unchanged.
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
	require.Equal(t, uint64(17), txn.prevCommitTSSeen[1],
		"second attempt must ALSO carry PrevCommitTS=17 — the M3 gate only proves attempt 1 did not write; it tells us nothing about the ADAPTER's prior attempt (whose commitTS the probe names), so dropping the probe would double-write the caller's at-most-once payload if that prior attempt landed (codex P1 on 43c55dfe)")
}

// TestShardedCoordinator_SurfacesComposed1OnCallerSuppliedStartTS
// pins the OCC-safety contract for caller-supplied snapshots
// (codex P1 on 57da8886, PR #900): when the CALLER supplied a
// non-zero StartTS — even with ReadKeys empty — the M4 retry path
// must NOT bump StartTS and silently retry.  The caller's StartTS
// names a specific snapshot they read against, and the M4 retry's
// nextStartTS allocation would let any concurrent commit landed
// in (originalStartTS, newStartTS) bypass the FSM's
// `latest > startTS` OCC write-conflict check, since the bumped
// snapshot now dominates the intervening commit_ts.
//
// Real exposure: many adapter sites read metadata at readTS and
// then dispatch with `StartTS: readTS` and empty ReadKeys, e.g.
// adapter/s3.go:573 (createBucket), adapter/sqs_messages.go:541,
// adapter/dynamodb.go:4366, adapter/s3_admin_objects.go:144, etc.
// Their pre-Dispatch read is invisible to the coordinator (no
// ReadKeys), so the only signal that the snapshot is owned by the
// caller is reqs.StartTS being non-zero at Dispatch entry.
//
// Contract: caller-supplied StartTS → surface the sentinel
// unchanged.  Coordinator-allocated StartTS (when caller passed
// StartTS=0 at Dispatch entry) is the only case M4 may retry —
// the coordinator owns that timestamp and no one has read at it.
func TestShardedCoordinator_SurfacesComposed1OnCallerSuppliedStartTS(t *testing.T) {
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
		IsTxn:   true,
		StartTS: 42, // caller-supplied — adapter read metadata at this ts
		// ReadKeys intentionally empty — the read happened at the
		// adapter layer and is not surfaced as a ReadKeys entry
		// (typical for S3/SQS/DynamoDB metadata reads).
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("k_write"), Value: []byte("v")},
		},
	})
	require.ErrorIs(t, err, ErrComposed1Violation,
		"caller-supplied StartTS must surface the sentinel without retry — bumping StartTS would let any commit landed in (42, newStartTS) bypass the FSM's latest>startTS OCC check (codex P1 on 57da8886)")
	require.Equal(t, int32(1), txn.calls.Load(),
		"exactly ONE attempt must fire — M4 must NOT retry caller-supplied snapshots")
}

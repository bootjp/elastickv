package adapter

import (
	"context"
	stderrors "errors"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A renewal that fails part way through its fan-out releases every group pin
// and forgets the session. If another renewal has already succeeded in the
// meantime, that teardown leaves its caller holding a token whose pins are
// gone, with retention free to compact the versions underneath it -- the
// backup is silently invalid while the caller believes it was renewed.
//
// The concurrent success is injected from inside the failing renewal's own
// fan-out, after it captured the session generation: onPropose fires on the
// reserve that precedes the failing pin, and calls the real
// extendBackupSession, which is what a successful renewal ends with.
func TestRenewBackupKeepsAConcurrentlyRenewedSession(t *testing.T) {
	t.Parallel()

	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	tok, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)

	// onPropose runs on the goroutine proposeBackupAll spawns per group, so it
	// must not call require: t.FailNow is only defined on the test goroutine.
	// Record the outcome and assert it below.
	var extended atomic.Bool
	proposer.mu.Lock()
	proposer.failures[backupSubtypePin] = 8
	proposer.transportError[backupSubtypePin] = stderrors.New("leader unavailable")
	proposer.onPropose = func(subtype byte, _ uint64) {
		if subtype != backupSubtypeReserve {
			return
		}
		// Exactly once, and only after RenewBackup has read the generation it
		// will compare against.
		proposer.onPropose = nil
		extended.Store(srv.extendBackupSession(tok))
	}
	proposer.mu.Unlock()

	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.True(t, extended.Load(), "the concurrent renewal must have extended the session")

	// The session the concurrent renewal owns must survive, and no release or
	// unreserve may have been proposed on its behalf.
	_, err = srv.backupRouteSnapshotForToken(tok)
	require.NoError(t, err, "the concurrently renewed session must still be live")
	require.NoError(t, srv.requireLiveBackupSession(tok))
	require.NotContains(t, proposer.subtypes(), backupSubtypeRelease)
	require.NotContains(t, proposer.subtypes(), backupSubtypeUnreserve)
}

// With no concurrent renewal the failing attempt still owns the session, so it
// must tear the pin down exactly as before.
func TestRenewBackupStillReleasesWhenItOwnsTheSession(t *testing.T) {
	t.Parallel()

	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	tok, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)

	proposer.mu.Lock()
	proposer.failures[backupSubtypePin] = 8
	proposer.transportError[backupSubtypePin] = stderrors.New("leader unavailable")
	proposer.mu.Unlock()

	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, proposer.subtypes(), backupSubtypeRelease)
	require.Contains(t, proposer.subtypes(), backupSubtypeUnreserve)
	_, err = srv.backupRouteSnapshotForToken(tok)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// A reservation whose proposal fails for any reason other than a capacity
// rejection is ambiguous: it may have committed with only the response lost.
// Leaving it in place holds one of the few global active-backup slots until
// its TTL for a backup no caller ever received.
func TestBeginBackupUnreservesAmbiguousReservationFailures(t *testing.T) {
	t.Parallel()

	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	proposer.failures[backupSubtypeReserve] = 8
	proposer.transportError[backupSubtypeReserve] = stderrors.New("leader unavailable")
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, proposer.subtypes(), backupSubtypeUnreserve,
		"an ambiguous reservation must be compensated")
}

// A session that has *disappeared* is a different case from one a newer
// generation owns. EndBackup can remove it while this renewal is in flight,
// and a reserve or partial pin fan-out that commits behind that release stays
// active until the new TTL -- holding one of the few global backup slots and
// blocking compaction for a backup that has already ended. Only a still-live
// session at another generation proves someone else owns the pins.
func TestRenewBackupCompensatesWhenTheSessionDisappeared(t *testing.T) {
	t.Parallel()

	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	tok, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)

	proposer.mu.Lock()
	proposer.failures[backupSubtypePin] = 8
	proposer.transportError[backupSubtypePin] = stderrors.New("leader unavailable")
	proposer.onPropose = func(subtype byte, _ uint64) {
		if subtype != backupSubtypeReserve {
			return
		}
		// Exactly once, and only after RenewBackup captured the generation:
		// this is what EndBackup's deferred forgetBackupSession does.
		proposer.onPropose = nil
		srv.forgetBackupSession(tok.pinID)
	}
	proposer.mu.Unlock()

	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, proposer.subtypes(), backupSubtypeRelease,
		"a renewal whose session vanished must still release what it half-renewed")
	require.Contains(t, proposer.subtypes(), backupSubtypeUnreserve,
		"a renewal whose session vanished must still unreserve what it half-renewed")
}

// backupProposalGroupError re-stamps any ResourceExhausted status, so a
// forwarded or proxied failure is indistinguishable from a real capacity
// rejection by code alone. Only kv.ErrTooManyActiveBackups as a Go error
// proves nothing was reserved; a bare status is ambiguous and must still be
// compensated, while keeping the client-facing code the caller expects.
func TestBeginBackupUnreservesAmbiguousResourceExhausted(t *testing.T) {
	t.Parallel()

	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	proposer.failures[backupSubtypeReserve] = 8
	proposer.transportError[backupSubtypeReserve] = status.Error(codes.ResourceExhausted, "upstream quota exceeded")
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Contains(t, proposer.subtypes(), backupSubtypeUnreserve,
		"an ambiguous ResourceExhausted must be compensated, not assumed definitive")
}

// The definitive case must keep skipping compensation: kv.ErrTooManyActiveBackups
// on the apply response proves the reservation was refused, so an unreserve
// would be pure noise.
func TestBeginBackupSkipsUnreserveOnDefinitiveCapacityRejection(t *testing.T) {
	t.Parallel()

	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	proposer.responseError[backupSubtypeReserve] = kv.ErrTooManyActiveBackups
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.NotContains(t, proposer.subtypes(), backupSubtypeUnreserve,
		"a definitive capacity rejection reserved nothing")
}

// A newer generation only proves someone else owns the pins while the session
// is still live. If EndBackup has marked it closing, no renewal will ever own
// it again -- that is precisely what the closing flag exists to express -- so a
// failed renewal that raced both a successful renewal and EndBackup must still
// compensate. Skipping it leaves the reserve or partial pins this attempt
// committed active until their TTL, after EndBackup already released, blocking
// compaction and holding a global backup slot for an ended backup.
func TestRenewBackupCompensatesWhenTheSessionIsClosing(t *testing.T) {
	t.Parallel()

	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	tok, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)

	// See the note above about require in this callback.
	var extended atomic.Bool
	proposer.mu.Lock()
	proposer.failures[backupSubtypePin] = 8
	proposer.transportError[backupSubtypePin] = stderrors.New("leader unavailable")
	proposer.onPropose = func(subtype byte, _ uint64) {
		if subtype != backupSubtypeReserve {
			return
		}
		proposer.onPropose = nil
		// A concurrent renewal lands (advancing the generation) and EndBackup
		// then starts tearing the session down, both after this renewal read
		// the generation it will be compared against.
		extended.Store(srv.extendBackupSession(tok))
		srv.closeBackupSession(tok.pinID)
	}
	proposer.mu.Unlock()

	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.True(t, extended.Load(), "the concurrent renewal must have extended the session")

	require.Contains(t, proposer.subtypes(), backupSubtypeRelease,
		"a closing session has no renewal owner; the failed attempt must still release")
	require.Contains(t, proposer.subtypes(), backupSubtypeUnreserve,
		"a closing session has no renewal owner; the failed attempt must still unreserve")
}

// A renewal owns the generation it was ADMITTED with. Reading the generation
// later -- after the route snapshot and group lookups, as this originally did --
// let a renewal that completed in that window advance it first, so this attempt
// recorded the other renewal's generation as its own. Its failure path then saw
// an unchanged generation, concluded it still owned the session, and released
// pins the other caller had already been handed a valid renewed token for,
// letting compaction proceed underneath a live backup.
//
// admitBackupRenewal reads admission and ownership under one hold of
// backupStateMu, so the generation cannot move between them. This asserts the
// consequence: once another renewal completes, the admitted generation is stale
// and the failure path must classify the session as owned by someone else.
func TestBackupRenewalOwnsTheGenerationItWasAdmittedWith(t *testing.T) {
	t.Parallel()

	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	tok, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)

	// Renewal A is admitted and captures the generation it starts from.
	admitted, live, err := srv.admitBackupRenewal(tok)
	require.NoError(t, err)
	require.True(t, live)

	// Renewal B completes and is handed a valid renewed token.
	require.True(t, srv.extendBackupSession(tok))

	// A now fails. The session belongs to B, so A must leave B's pins alone.
	require.Equal(t, backupSessionOwnershipTaken,
		srv.forgetBackupSessionAtGeneration(tok.pinID, admitted),
		"a renewal that captured its generation late would see B's generation as its own and release B's pins")
	require.NotContains(t, proposer.subtypes(), backupSubtypeRelease)
	require.NotContains(t, proposer.subtypes(), backupSubtypeUnreserve)
}

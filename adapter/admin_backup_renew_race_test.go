package adapter

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
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
		require.True(t, srv.extendBackupSession(tok))
	}
	proposer.mu.Unlock()

	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.Unavailable, status.Code(err))

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

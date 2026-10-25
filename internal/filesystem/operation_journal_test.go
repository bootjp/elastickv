package filesystem

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func testOperationJournal(t *testing.T) (*OperationJournal, store.MVCCStore, *testCoordinator) {
	t.Helper()
	st := store.NewMVCCStore()
	coord := &testCoordinator{st: st}
	service, err := NewService(st, coord)
	require.NoError(t, err)
	journal, err := NewOperationJournal(service)
	require.NoError(t, err)
	return journal, st, coord
}

func testOperationID(t *testing.T, journal *OperationJournal, sequence uint64) OperationID {
	t.Helper()
	filesystemID, err := journal.EnsureFilesystem(context.Background())
	require.NoError(t, err)
	session, err := journal.RegisterSession(context.Background(), filesystemID, "mount-a")
	require.NoError(t, err)
	return OperationID{Filesystem: session.Filesystem, Session: session.Session, Sequence: sequence}
}

func TestOperationJournalPublishesResultWithEffectAndRetiresIdentity(t *testing.T) {
	ctx := context.Background()
	journal, st, coord := testOperationJournal(t)
	id := testOperationID(t, journal, 1)

	admitted, err := journal.Admit(ctx, id, []byte("write:7:0:abc"), 128, 1024)
	require.NoError(t, err)
	require.Equal(t, OperationPending, admitted.State)
	duplicate, err := journal.Admit(ctx, id, []byte("write:7:0:abc"), 128, 1024)
	require.NoError(t, err)
	require.Equal(t, admitted, duplicate)
	_, err = journal.Admit(ctx, id, []byte("write:7:0:xyz"), 128, 1024)
	require.ErrorIs(t, err, ErrOperationIDReused)

	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	require.EqualValues(t, 1, attempt.Generation)
	require.NotEqual(t, [16]byte{}, attempt.AttemptID)
	outcome, _, err := journal.ResolveAttempt(ctx, id, attempt.AttemptID)
	require.Equal(t, AttemptUnknown, outcome)
	require.ErrorIs(t, err, ErrOperationPending)
	_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent:  []byte("write:7:0:xyz"),
		Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("xyz")}},
	})
	require.ErrorIs(t, err, ErrOperationIDReused)

	committed, err := journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent:  []byte("write:7:0:abc"),
		Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("abc")}},
		Result:  []byte("3"),
	})
	require.NoError(t, err)
	require.Equal(t, OperationCommitted, committed.State)
	require.Equal(t, []byte("3"), committed.Result)
	require.NotZero(t, committed.CommitTS)
	beforeDuplicate := st.LastCommitTS()
	duplicateResult, err := journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent:  []byte("write:7:0:abc"),
		Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("abc")}},
		Result:  []byte("3"),
	})
	require.NoError(t, err)
	require.Equal(t, committed, duplicateResult)
	require.Equal(t, beforeDuplicate, st.LastCommitTS())
	outcome, _, err = journal.ResolveAttempt(ctx, id, attempt.AttemptID)
	require.NoError(t, err)
	require.Equal(t, AttemptCommitted, outcome)
	value, err := st.GetAt(ctx, []byte("data"), st.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), value)

	restartedService, err := NewService(st, coord)
	require.NoError(t, err)
	restarted, err := NewOperationJournal(restartedService)
	require.NoError(t, err)
	recovered, err := restarted.Resolve(ctx, id)
	require.NoError(t, err)
	require.Equal(t, committed, recovered)

	require.NoError(t, restarted.Acknowledge(ctx, id))
	require.NoError(t, restarted.Acknowledge(ctx, id))
	_, err = restarted.Resolve(ctx, id)
	require.ErrorIs(t, err, ErrOperationRetired)
	_, err = restarted.Admit(ctx, id, []byte("write:7:0:abc"), 128, 1024)
	require.ErrorIs(t, err, ErrOperationRetired)
	require.NoError(t, restarted.CloseSession(ctx, OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}))
}

type lostOperationCommitBackend struct {
	operationJournalBackend
	once sync.Once
}

type lostSessionRegistrationBackend struct {
	operationJournalBackend
	once     sync.Once
	ownerKey []byte
}

func (b *lostSessionRegistrationBackend) Commit(ctx context.Context, ts uint64, elems []*kv.Elem[kv.OP], readKeys [][]byte) (*kv.CoordinateResponse, error) {
	response, err := b.operationJournalBackend.Commit(ctx, ts, elems, readKeys)
	if err != nil {
		return response, err
	}
	for _, elem := range elems {
		if bytes.Equal(elem.Key, b.ownerKey) {
			lost := false
			b.once.Do(func() { lost = true })
			if lost {
				return nil, kv.ErrTransactionOutcomeUnknown
			}
		}
	}
	return response, nil
}

type deferredOperationProofBackend struct {
	operationJournalBackend
	ready bool
}

type partitionedOperationProofBackend struct{ operationJournalBackend }

type unavailableOperationRouteCoordinator struct{ *testCoordinator }

func (unavailableOperationRouteCoordinator) ResolveOperationGroupForKey([]byte) (uint64, error) {
	return 0, kv.ErrOperationGroupResolverUnavailable
}

func TestOperationProofsAllowUnshardedCoordinatorWithoutRouteCapability(t *testing.T) {
	st := store.NewMVCCStore()
	service, err := NewService(st, unavailableOperationRouteCoordinator{&testCoordinator{st: st}})
	require.NoError(t, err)
	journal, err := NewOperationJournal(service)
	require.NoError(t, err)
	groups, _, effects, err := journal.operationProofs(OperationID{Sequence: 1}, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: []byte("data"), Value: []byte("value")},
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, groups)
	require.True(t, effects[0].RequireGroupMatch)
	require.Zero(t, effects[0].ExpectedGroupID)
}

func (b partitionedOperationProofBackend) GroupIDForKey(key []byte) (uint64, error) {
	if bytes.Equal(key, []byte("data-a")) {
		return 1, nil
	}
	return 2, nil
}

func (b partitionedOperationProofBackend) CommittedVersionAtGroup(ctx context.Context, key []byte, _ uint64, ts uint64) (bool, error) {
	return b.CommittedVersionAt(ctx, key, ts)
}

func TestOperationProofsBindRoutedEffectsToReceiptParticipants(t *testing.T) {
	journal, _, _ := testOperationJournal(t)
	journal = newOperationJournalWithBackend(partitionedOperationProofBackend{journal.backend})
	input := []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: []byte("data-a"), Value: []byte("a")},
		{Op: kv.Put, Key: []byte("data-b"), Value: []byte("b")},
	}
	groups, proofs, effects, err := journal.operationProofs(OperationID{Sequence: 1}, input)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, groups)
	require.Len(t, proofs, 2)
	require.Equal(t, uint64(1), proofs[0].GroupID)
	require.Equal(t, uint64(2), proofs[1].GroupID)
	for index, group := range groups {
		require.Zero(t, effects[index].GroupID)
		require.True(t, effects[index].RequireGroupMatch)
		require.Equal(t, group, effects[index].ExpectedGroupID)
		require.False(t, input[index].RequireGroupMatch)
	}
}

func (b *deferredOperationProofBackend) CommittedVersionAtGroup(ctx context.Context, key []byte, groupID, ts uint64) (bool, error) {
	if fskeys.IsOperationProofKey(key) && !b.ready {
		return false, nil
	}
	return b.operationJournalBackend.CommittedVersionAtGroup(ctx, key, groupID, ts)
}

type delayedOperationCommitBackend struct {
	operationJournalBackend
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *delayedOperationCommitBackend) Commit(ctx context.Context, ts uint64, elems []*kv.Elem[kv.OP], readKeys [][]byte) (*kv.CoordinateResponse, error) {
	for _, elem := range elems {
		if bytes.Equal(elem.Key, []byte("data")) {
			b.once.Do(func() {
				close(b.entered)
				<-b.release
			})
		}
	}
	return b.operationJournalBackend.Commit(ctx, ts, elems, readKeys)
}

func (b *lostOperationCommitBackend) Commit(ctx context.Context, ts uint64, elems []*kv.Elem[kv.OP], readKeys [][]byte) (*kv.CoordinateResponse, error) {
	response, err := b.operationJournalBackend.Commit(ctx, ts, elems, readKeys)
	if err != nil {
		return response, err
	}
	lost := false
	for _, elem := range elems {
		if elem.CommitTSValueOffset != 0 {
			b.once.Do(func() { lost = true })
		}
	}
	if lost {
		return nil, kv.ErrTransactionOutcomeUnknown
	}
	return response, nil
}

func TestOperationJournalResolvesLostCommitResponseWithoutReplayingEffect(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("write:7:0:abc"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	journal = newOperationJournalWithBackend(&lostOperationCommitBackend{operationJournalBackend: journal.backend})

	_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent:  []byte("write:7:0:abc"),
		Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("abc")}},
		Result:  []byte("3"),
	})
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	record, err := journal.Resolve(ctx, id)
	require.NoError(t, err)
	require.Equal(t, []byte("3"), record.Result)
	duplicate, err := journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{Intent: []byte("write:7:0:abc")})
	require.NoError(t, err)
	require.Equal(t, record, duplicate)
	value, err := st.GetAt(ctx, []byte("data"), st.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), value)
	_, err = journal.BeginAttempt(ctx, id)
	require.ErrorIs(t, err, ErrOperationGeneration)
}

func TestOperationJournalRetriesLostSessionRegistrationWithSameIdentity(t *testing.T) {
	ctx := context.Background()
	journal, _, _ := testOperationJournal(t)
	filesystemID, err := journal.EnsureFilesystem(ctx)
	require.NoError(t, err)
	token, err := randomOperationToken()
	require.NoError(t, err)
	id := OperationSessionID{Filesystem: filesystemID, Session: token}
	backend := &lostSessionRegistrationBackend{
		operationJournalBackend: journal.backend,
		ownerKey:                fskeys.OperationOwnerKey(filesystemID, []byte("mount-a")),
	}
	journal = newOperationJournalWithBackend(backend)
	err = journal.RegisterSessionWithID(ctx, id, "mount-a")
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.NoError(t, journal.RegisterSessionWithID(ctx, id, "mount-a"))
	_, err = journal.RegisterSession(ctx, filesystemID, "mount-a")
	require.ErrorIs(t, err, ErrOperationPending)
	_, err = journal.Admit(ctx, OperationID{Filesystem: filesystemID, Session: token, Sequence: 1}, []byte("write"), 128, 1024)
	require.NoError(t, err)
}

func TestOperationJournalReturnsSessionIdentityAfterLostRegistrationReply(t *testing.T) {
	ctx := context.Background()
	journal, _, _ := testOperationJournal(t)
	filesystemID, err := journal.EnsureFilesystem(ctx)
	require.NoError(t, err)
	backend := &lostSessionRegistrationBackend{
		operationJournalBackend: journal.backend,
		ownerKey:                fskeys.OperationOwnerKey(filesystemID, []byte("mount-a")),
	}
	journal = newOperationJournalWithBackend(backend)
	id, err := journal.RegisterSession(ctx, filesystemID, "mount-a")
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, filesystemID, id.Filesystem)
	require.NotEqual(t, [16]byte{}, id.Session)
	require.NoError(t, journal.RegisterSessionWithID(ctx, id, "mount-a"))
}

func TestOperationJournalWaitsForEveryEffectAfterDecision(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	backend := &deferredOperationProofBackend{operationJournalBackend: journal.backend}
	journal = newOperationJournalWithBackend(backend)
	_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent: []byte("write"), Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("value")}},
		Result: []byte("5"),
	})
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	version := st.LastCommitTS()
	_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{Intent: []byte("write")})
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, version, st.LastCommitTS(), "unknown result must not replay the effect")
	backend.ready = true
	record, err := journal.Resolve(ctx, id)
	require.NoError(t, err)
	require.Equal(t, []byte("5"), record.Result)
	require.Equal(t, version, st.LastCommitTS())
}

func TestOperationJournalKeepsProofAfterEffectVersionCompaction(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	committed, err := journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent: []byte("write"), Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("old")}},
		Result: []byte("3"),
	})
	require.NoError(t, err)
	later := st.LastCommitTS() + 1
	require.NoError(t, st.PutAt(ctx, []byte("data"), []byte("new"), later, 0))
	require.NoError(t, st.Compact(ctx, later))
	effectPresent, err := st.CommittedVersionAt(ctx, []byte("data"), committed.CommitTS)
	require.NoError(t, err)
	require.False(t, effectPresent)
	recovered, err := journal.Resolve(ctx, id)
	require.NoError(t, err)
	require.Equal(t, committed, recovered)
	duplicate, err := journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{Intent: []byte("write")})
	require.NoError(t, err)
	require.Equal(t, committed, duplicate)
	require.NoError(t, journal.Acknowledge(ctx, id))
}

func TestOperationJournalWritesOneReceiptPerParticipant(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("rename"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	journal = newOperationJournalWithBackend(partitionedOperationProofBackend{journal.backend})
	record, err := journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent: []byte("rename"), Effects: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: []byte("data-a"), Value: []byte("a")},
			{Op: kv.Put, Key: []byte("data-b"), Value: []byte("b")},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, record.ProofGroups)
	for _, groupID := range record.ProofGroups {
		present, proofErr := st.CommittedVersionAt(ctx,
			fskeys.OperationProofKey(id.Filesystem, id.Session, id.Sequence, groupID), record.CommitTS)
		require.NoError(t, proofErr)
		require.True(t, present)
	}
	require.NoError(t, journal.Acknowledge(ctx, id))
}

func TestOperationJournalCancellationFencesAttempt(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("write:7:0:abc"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	cancelled, err := journal.Cancel(ctx, id)
	require.NoError(t, err)
	require.Equal(t, OperationCancelled, cancelled.State)
	duplicateCancel, err := journal.Cancel(ctx, id)
	require.NoError(t, err)
	require.Equal(t, cancelled, duplicateCancel)
	outcome, _, err := journal.ResolveAttempt(ctx, id, attempt.AttemptID)
	require.NoError(t, err)
	require.Equal(t, AttemptAborted, outcome)
	_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent:  []byte("write:7:0:abc"),
		Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("abc")}},
	})
	require.ErrorIs(t, err, ErrOperationGeneration)
	_, err = st.GetAt(ctx, []byte("data"), st.LastCommitTS())
	require.True(t, errors.Is(err, store.ErrKeyNotFound))
}

func TestOperationJournalCancellationReturnsWinningCommit(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	committed, err := journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent: []byte("write"), Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("value")}},
		Result: []byte("5"),
	})
	require.NoError(t, err)
	beforeCancel := st.LastCommitTS()
	winner, err := journal.Cancel(ctx, id)
	require.NoError(t, err)
	require.Equal(t, committed, winner)
	require.Equal(t, beforeCancel, st.LastCommitTS())
}

func TestOperationJournalDuplicateRejectionReturnsOriginalResult(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("create:absent"), 128, 0)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	readTS, err := journal.PlanSnapshot(ctx, id, attempt.AttemptID)
	require.NoError(t, err)
	first, err := journal.Reject(ctx, id, attempt.AttemptID, readTS, []byte("create:absent"), []byte("EEXIST"), nil)
	require.NoError(t, err)
	require.Equal(t, OperationRejected, first.State)
	beforeDuplicate := st.LastCommitTS()
	duplicate, err := journal.Reject(ctx, id, attempt.AttemptID, readTS, []byte("create:absent"), []byte("ENOENT"), nil)
	require.NoError(t, err)
	require.Equal(t, first, duplicate)
	require.Equal(t, beforeDuplicate, st.LastCommitTS())
	_, err = journal.Reject(ctx, id, attempt.AttemptID, readTS, []byte("create:changed"), []byte("EEXIST"), nil)
	require.ErrorIs(t, err, ErrOperationIDReused)
}

func TestOperationJournalPublishUsesPlanningSnapshotForReadValidation(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("increment"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	readTS, err := journal.PlanSnapshot(ctx, id, attempt.AttemptID)
	require.NoError(t, err)
	key := []byte("counter")
	require.NoError(t, st.PutAt(ctx, key, []byte("1"), readTS+1, 0))
	_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
		Intent: []byte("increment"), ReadTS: readTS, ReadKeys: [][]byte{key},
		Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: []byte("1")}},
	})
	require.ErrorIs(t, err, store.ErrWriteConflict)
	outcome, _, err := journal.ResolveAttempt(ctx, id, attempt.AttemptID)
	require.Equal(t, AttemptUnknown, outcome)
	require.ErrorIs(t, err, ErrOperationPending)
}

func TestOperationJournalRejectUsesPlanningSnapshotForReadValidation(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("create"), 128, 0)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	readTS, err := journal.PlanSnapshot(ctx, id, attempt.AttemptID)
	require.NoError(t, err)
	key := []byte("directory-entry")
	require.NoError(t, st.PutAt(ctx, key, []byte("new"), readTS+1, 0))
	_, err = journal.Reject(ctx, id, attempt.AttemptID, readTS, []byte("create"), []byte("EEXIST"), [][]byte{key})
	require.ErrorIs(t, err, store.ErrWriteConflict)
	outcome, _, err := journal.ResolveAttempt(ctx, id, attempt.AttemptID)
	require.Equal(t, AttemptUnknown, outcome)
	require.ErrorIs(t, err, ErrOperationPending)
}

func TestOperationJournalRejectsJournalKeysAsEffects(t *testing.T) {
	ctx := context.Background()
	journal, _, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	for _, key := range [][]byte{id.key(), OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}.key(),
		fskeys.OperationFilesystemKey(), fskeys.OperationOwnerKey(id.Filesystem, []byte("mount-a"))} {
		_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
			Intent: []byte("write"), Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: []byte("corrupt")}},
		})
		require.ErrorIs(t, err, ErrInvalid)
	}
}

func TestOperationJournalOwnerFenceAndRecovery(t *testing.T) {
	ctx := context.Background()
	journal, _, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.RegisterSession(ctx, id.Filesystem, "mount-a")
	require.ErrorIs(t, err, ErrOperationPending)
	_, err = journal.Admit(ctx, id, []byte("write"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	sessionID := OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}
	require.NoError(t, journal.FenceSession(ctx, sessionID))
	_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{Intent: []byte("write")})
	require.ErrorIs(t, err, ErrOperationSessionClosed)
	_, err = journal.RegisterSession(ctx, id.Filesystem, "mount-a")
	require.ErrorIs(t, err, ErrOperationPending)
	cancelled, err := journal.RecoverClosedSession(ctx, sessionID, 10)
	require.NoError(t, err)
	require.Equal(t, 1, cancelled)
	record, err := journal.Resolve(ctx, id)
	require.NoError(t, err)
	require.Equal(t, OperationCancelled, record.State)
	require.NoError(t, journal.CloseSession(ctx, sessionID))
	replacement, err := journal.RegisterSession(ctx, id.Filesystem, "mount-a")
	require.NoError(t, err)
	require.NotEqual(t, id.Session, replacement.Session)
	_, err = journal.Admit(ctx, OperationID{Filesystem: id.Filesystem, Session: id.Session, Sequence: 2}, []byte("late"), 128, 1024)
	require.ErrorIs(t, err, ErrOperationSessionClosed)
}

func TestOperationJournalAcknowledgementGapsRetireWithoutReuse(t *testing.T) {
	ctx := context.Background()
	journal, _, _ := testOperationJournal(t)
	first := testOperationID(t, journal, 1)
	ids := []OperationID{first, {Filesystem: first.Filesystem, Session: first.Session, Sequence: 2},
		{Filesystem: first.Filesystem, Session: first.Session, Sequence: 3}}
	for _, id := range ids {
		_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
		require.NoError(t, err)
		attempt, err := journal.BeginAttempt(ctx, id)
		require.NoError(t, err)
		_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{Intent: []byte("write"), Result: []byte("done")})
		require.NoError(t, err)
	}
	for _, index := range []int{2, 0, 1} {
		require.NoError(t, journal.Acknowledge(ctx, ids[index]))
	}
	for _, id := range ids {
		_, err := journal.Resolve(ctx, id)
		require.ErrorIs(t, err, ErrOperationRetired)
		_, err = journal.Admit(ctx, id, []byte("write"), 128, 1024)
		require.ErrorIs(t, err, ErrOperationRetired)
	}
}

func TestOperationJournalDoesNotAdvanceWithoutAbortDecision(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
	require.NoError(t, err)
	old, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	_, err = journal.AdvanceGeneration(ctx, id, old.AttemptID)
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	outcome, _, err := journal.ResolveAttempt(ctx, id, old.AttemptID)
	require.ErrorIs(t, err, ErrOperationPending)
	require.Equal(t, AttemptUnknown, outcome)
	current, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	require.Equal(t, old.AttemptID, current.AttemptID)
	require.Equal(t, old.Generation, current.Generation)
	_, err = journal.Publish(ctx, id, current.AttemptID, operationAttemptPlan{
		Intent:  []byte("write"),
		Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("old")}},
	})
	require.NoError(t, err)
	value, err := st.GetAt(ctx, []byte("data"), st.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("old"), value)
}

func TestOperationJournalDelayedCommitCannotBeSupersededWithoutDecision(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
	require.NoError(t, err)
	old, err := journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	backend := &delayedOperationCommitBackend{
		operationJournalBackend: journal.backend,
		entered:                 make(chan struct{}),
		release:                 make(chan struct{}),
	}
	journal = newOperationJournalWithBackend(backend)
	result := make(chan error, 1)
	go func() {
		_, publishErr := journal.Publish(ctx, id, old.AttemptID, operationAttemptPlan{
			Intent: []byte("write"), Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("old")}},
		})
		result <- publishErr
	}()
	<-backend.entered
	_, err = journal.AdvanceGeneration(ctx, id, old.AttemptID)
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	close(backend.release)
	require.NoError(t, <-result)
	current, err := journal.BeginAttempt(ctx, id)
	require.ErrorIs(t, err, ErrOperationGeneration)
	require.Equal(t, operationRecord{}, current)
	value, err := st.GetAt(ctx, []byte("data"), st.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("old"), value)
}

func TestOperationJournalSnapshotRetainsTerminalAndPendingRecords(t *testing.T) {
	ctx := context.Background()
	journal, st, _ := testOperationJournal(t)
	committedID := testOperationID(t, journal, 1)
	pendingID := OperationID{Filesystem: committedID.Filesystem, Session: committedID.Session, Sequence: 2}
	_, err := journal.Admit(ctx, committedID, []byte("write"), 128, 1024)
	require.NoError(t, err)
	attempt, err := journal.BeginAttempt(ctx, committedID)
	require.NoError(t, err)
	_, err = journal.Publish(ctx, committedID, attempt.AttemptID, operationAttemptPlan{
		Intent:  []byte("write"),
		Effects: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("data"), Value: []byte("value")}},
		Result:  []byte("5"),
	})
	require.NoError(t, err)
	_, err = journal.Admit(ctx, pendingID, []byte("create"), 128, 1024)
	require.NoError(t, err)
	pendingAttempt, err := journal.BeginAttempt(ctx, pendingID)
	require.NoError(t, err)

	snapshot, err := st.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, snapshot.Close()) })
	var image bytes.Buffer
	_, err = snapshot.WriteTo(&image)
	require.NoError(t, err)
	restoredStore := store.NewMVCCStore()
	require.NoError(t, restoredStore.Restore(bytes.NewReader(image.Bytes())))
	restoredService, err := NewService(restoredStore, &testCoordinator{st: restoredStore})
	require.NoError(t, err)
	restored, err := NewOperationJournal(restoredService)
	require.NoError(t, err)
	restoredID, err := restored.EnsureFilesystem(ctx)
	require.NoError(t, err)
	require.Equal(t, committedID.Filesystem, restoredID)
	record, err := restored.Resolve(ctx, committedID)
	require.NoError(t, err)
	require.Equal(t, []byte("5"), record.Result)
	outcome, _, err := restored.ResolveAttempt(ctx, pendingID, pendingAttempt.AttemptID)
	require.Equal(t, AttemptUnknown, outcome)
	require.ErrorIs(t, err, ErrOperationPending)
}

func TestOperationRecordCodecRejectsInvalidEnvelopeAndUnstampedTerminal(t *testing.T) {
	journal, _, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	record := newOperationRecord(id, []byte("write"), 128, 1024)
	raw, err := encodeOperationRecord(record)
	require.NoError(t, err)
	decoded, err := decodeOperationRecord(raw)
	require.NoError(t, err)
	require.Equal(t, record, decoded)

	for _, change := range []func([]byte) []byte{
		func(b []byte) []byte { return b[:operationHeaderBytes-1] },
		func(b []byte) []byte { b[0] = 'X'; return b },
		func(b []byte) []byte { b[4]++; return b },
		func(b []byte) []byte { b[5] = byte(OperationCommitted); return b },
		func(b []byte) []byte { return append(b, []byte("{}")...) },
	} {
		corrupted := change(bytes.Clone(raw))
		_, err = decodeOperationRecord(corrupted)
		require.ErrorIs(t, err, ErrOperationRecordCorrupt)
	}
	record.State = OperationCancelled
	terminal, err := encodeOperationRecord(record)
	require.NoError(t, err)
	_, err = decodeOperationRecord(terminal)
	require.ErrorIs(t, err, ErrOperationRecordCorrupt)
}

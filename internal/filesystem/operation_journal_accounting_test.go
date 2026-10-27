package filesystem

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestOperationJournalRetainedAccountingAcrossSequenceBoundary(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		journal, _, _ := testOperationJournal(t)
		assertOperationJournalRetainedAccountingAcrossSequenceBoundary(t, journal)
	})
	t.Run("pebble", func(t *testing.T) {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		service, err := NewService(st, &testCoordinator{st: st})
		require.NoError(t, err)
		journal, err := NewOperationJournal(service)
		require.NoError(t, err)
		assertOperationJournalRetainedAccountingAcrossSequenceBoundary(t, journal)
	})
}

func TestOperationJournalConcurrentAcknowledgementsPreserveAccounting(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		journal, _, _ := testOperationJournal(t)
		assertConcurrentAcknowledgementsPreserveAccounting(t, journal)
	})
	t.Run("pebble", func(t *testing.T) {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		service, err := NewService(st, &testCoordinator{st: st})
		require.NoError(t, err)
		journal, err := NewOperationJournal(service)
		require.NoError(t, err)
		assertConcurrentAcknowledgementsPreserveAccounting(t, journal)
	})
}

func assertConcurrentAcknowledgementsPreserveAccounting(t *testing.T, journal *OperationJournal) {
	t.Helper()
	ctx := context.Background()
	first := testOperationID(t, journal, 1)
	ids := []OperationID{first, {Filesystem: first.Filesystem, Session: first.Session, Sequence: 2}}
	for _, id := range ids {
		_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
		require.NoError(t, err)
		attempt, err := journal.BeginAttempt(ctx, id)
		require.NoError(t, err)
		_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
			Intent: []byte("write"), Result: []byte("done"),
		})
		require.NoError(t, err)
	}

	sessionID := OperationSessionID{Filesystem: first.Filesystem, Session: first.Session}
	backend := &synchronizeSessionReadsBackend{
		operationJournalBackend: journal.backend,
		sessionKey:              sessionID.key(),
		both:                    make(chan struct{}),
	}
	concurrent := newOperationJournalWithBackend(backend)
	type outcome struct {
		id  OperationID
		err error
	}
	results := make(chan outcome, len(ids))
	var workers sync.WaitGroup
	for _, id := range ids {
		workers.Add(1)
		go func() {
			defer workers.Done()
			results <- outcome{id: id, err: concurrent.acknowledgeOnce(ctx, id)}
		}()
	}
	workers.Wait()
	close(results)

	succeeded, conflicted := 0, 0
	for result := range results {
		if result.err == nil {
			succeeded++
			continue
		}
		require.True(t, IsWriteConflict(result.err), "ack sequence %d: %v", result.id.Sequence, result.err)
		conflicted++
		require.NoError(t, journal.Acknowledge(ctx, result.id))
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, conflicted)

	ts, err := journal.backend.Snapshot(ctx)
	require.NoError(t, err)
	session, found, err := journal.loadSessionAt(ctx, sessionID, ts)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(2), session.RejectBefore)
	require.Zero(t, session.Pending)
	require.Zero(t, session.RetainedCount)
	require.Zero(t, session.RetainedBytes)
}

func assertOperationJournalRetainedAccountingAcrossSequenceBoundary(t *testing.T, journal *OperationJournal) {
	t.Helper()
	ctx := context.Background()
	first := testOperationID(t, journal, 1)
	const batchSize = 16
	const operations = 1280

	for start := uint64(1); start <= operations; start += batchSize {
		for sequence := start; sequence < start+batchSize; sequence++ {
			id := OperationID{Filesystem: first.Filesystem, Session: first.Session, Sequence: sequence}
			_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
			require.NoError(t, err, "admit sequence %d", sequence)
			attempt, err := journal.BeginAttempt(ctx, id)
			require.NoError(t, err, "begin sequence %d", sequence)
			if sequence%3 == 0 {
				attempt, err = journal.AdvanceGeneration(ctx, id, attempt.AttemptID)
				require.NoError(t, err, "advance sequence %d", sequence)
			}
			_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
				Intent: []byte("write"), Result: []byte("done"),
			})
			require.NoError(t, err, "publish sequence %d", sequence)
		}
		for sequence := start + batchSize - 1; sequence >= start; sequence-- {
			id := OperationID{Filesystem: first.Filesystem, Session: first.Session, Sequence: sequence}
			require.NoError(t, journal.Acknowledge(ctx, id), "ack sequence %d", sequence)
		}
	}

	ts, err := journal.backend.Snapshot(ctx)
	require.NoError(t, err)
	session, found, err := journal.loadSessionAt(ctx, OperationSessionID{
		Filesystem: first.Filesystem, Session: first.Session,
	}, ts)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(operations), session.RejectBefore)
	require.Zero(t, session.Pending)
	require.Zero(t, session.RetainedCount)
	require.Zero(t, session.RetainedBytes)
}

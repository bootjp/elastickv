package filesystem

import (
	"context"
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

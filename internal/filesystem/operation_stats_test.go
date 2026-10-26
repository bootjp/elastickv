package filesystem

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestOperationJournalStatsTrackRetainedWorkAcrossSessions(t *testing.T) {
	st := store.NewMVCCStore()
	svc, err := NewService(st, &testCoordinator{st: st})
	require.NoError(t, err)
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	filesystemID, err := journal.EnsureFilesystem(t.Context())
	require.NoError(t, err)
	first, err := journal.RegisterSession(t.Context(), filesystemID, "first")
	require.NoError(t, err)
	_, err = journal.RegisterSession(t.Context(), filesystemID, "second")
	require.NoError(t, err)
	id := OperationID{Filesystem: first.Filesystem, Session: first.Session, Sequence: 1}
	_, err = journal.Admit(t.Context(), id, []byte("write"), 128, 1024)
	require.NoError(t, err)

	stats, err := svc.ListOperationJournalStats(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.Sessions)
	require.EqualValues(t, 1, stats.Pending)
	require.EqualValues(t, 1, stats.RetainedCount)
	require.Positive(t, stats.RetainedBytes)
	require.EqualValues(t, 2*defaultOperationQuota, stats.MaxBytes)

	_, err = journal.Cancel(t.Context(), id)
	require.NoError(t, err)
	require.NoError(t, journal.Acknowledge(t.Context(), id))
	require.NoError(t, journal.CloseSession(t.Context(), first))
	stats, err = svc.ListOperationJournalStats(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.ClosedSessions)
	require.Zero(t, stats.Pending)
	require.Zero(t, stats.RetainedCount)
	require.Zero(t, stats.RetainedBytes)
}

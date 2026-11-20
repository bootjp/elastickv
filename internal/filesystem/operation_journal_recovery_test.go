package filesystem

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type recoveryConflictBackend struct {
	operationJournalBackend
	err           error
	remaining     int
	attempts      int
	ids           []OperationID
	cancel        context.CancelFunc
	beforeFailure func()
}

func (b *recoveryConflictBackend) Commit(ctx context.Context, ts uint64, elems []*kv.Elem[kv.OP], reads [][]byte) (*kv.CoordinateResponse, error) {
	for _, elem := range elems {
		// Terminal rows receive their commit timestamp only when committed.
		raw := append([]byte(nil), elem.Value...)
		if len(raw) < operationHeaderBytes {
			continue
		}
		binary.BigEndian.PutUint64(raw[operationCommitTSOffset:], ts+1)
		record, err := decodeOperationRecord(raw)
		if err != nil || record.State != OperationCancelled {
			continue
		}
		b.attempts++
		b.ids = append(b.ids, record.ID)
		if b.cancel != nil {
			b.cancel()
		}
		if b.remaining > 0 {
			b.remaining--
			if b.beforeFailure != nil {
				b.beforeFailure()
			}
			return nil, b.err
		}
	}
	return b.operationJournalBackend.Commit(ctx, ts, elems, reads)
}

func TestRecoverClosedSessionRetriesOnlyDefinitiveCancellationConflicts(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		failure  error
		failures int
		want     error
		attempts int
	}{
		{name: "conflict", failure: store.ErrWriteConflict, failures: 1, attempts: 2},
		{name: "unknown", failure: errors.Join(store.ErrWriteConflict, kv.ErrTransactionOutcomeUnknown), failures: 1, want: kv.ErrTransactionOutcomeUnknown, attempts: 1},
		{name: "unknown-retired", failure: errors.Join(ErrOperationRetired, kv.ErrTransactionOutcomeUnknown), failures: 1, want: kv.ErrTransactionOutcomeUnknown, attempts: 1},
		{name: "corrupt", failure: ErrOperationRecordCorrupt, failures: 1, want: ErrOperationRecordCorrupt, attempts: 1},
		{name: "bounded", failure: store.ErrWriteConflict, failures: operationCASRetries + 1, want: ErrContentionLimit, attempts: operationCASRetries},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			journal, _, _ := testOperationJournal(t)
			id := testOperationID(t, journal, 1)
			_, err := journal.Admit(t.Context(), id, []byte("rename"), 128, 1024)
			require.NoError(t, err)
			_, err = journal.BeginAttempt(t.Context(), id)
			require.NoError(t, err)
			sessionID := OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}
			require.NoError(t, journal.FenceSession(t.Context(), sessionID))
			backend := &recoveryConflictBackend{operationJournalBackend: journal.backend, err: tc.failure, remaining: tc.failures}
			recovering := newOperationJournalWithBackend(backend)
			count, err := recovering.RecoverClosedSession(t.Context(), sessionID, 10)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Zero(t, count)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, count)
			}
			require.Equal(t, tc.attempts, backend.attempts)
			for _, got := range backend.ids {
				require.Equal(t, id, got)
			}
			ts, err := journal.backend.Snapshot(t.Context())
			require.NoError(t, err)
			session, found, err := journal.loadSessionAt(t.Context(), sessionID, ts)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, session.Closed)
			if tc.want == nil {
				require.Zero(t, session.Pending)
				record, err := journal.Resolve(t.Context(), id)
				require.NoError(t, err)
				require.Equal(t, OperationCancelled, record.State)
				count, err = recovering.RecoverClosedSession(t.Context(), sessionID, 10)
				require.NoError(t, err)
				require.Zero(t, count)
				require.Equal(t, tc.attempts, backend.attempts)
			} else {
				require.EqualValues(t, 1, session.Pending)
			}
		})
	}
}

func TestRecoverClosedSessionCancellationInterruptsConflictBackoff(t *testing.T) {
	t.Parallel()
	journal, _, _ := testOperationJournal(t)
	id := testOperationID(t, journal, 1)
	_, err := journal.Admit(t.Context(), id, []byte("rename"), 128, 1024)
	require.NoError(t, err)
	sessionID := OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}
	require.NoError(t, journal.FenceSession(t.Context(), sessionID))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend := &recoveryConflictBackend{operationJournalBackend: journal.backend, err: store.ErrWriteConflict, remaining: 10, cancel: cancel}
	_, err = newOperationJournalWithBackend(backend).RecoverClosedSession(ctx, sessionID, 10)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, backend.attempts)
}

func TestRecoverClosedSessionDoesNotFenceReplacementAfterConflict(t *testing.T) {
	t.Parallel()
	for _, ack := range []bool{false, true} {
		t.Run(map[bool]string{false: "terminal-record", true: "acknowledged"}[ack], func(t *testing.T) {
			t.Parallel()
			journal, _, _ := testOperationJournal(t)
			id := testOperationID(t, journal, 1)
			_, err := journal.Admit(t.Context(), id, []byte("rename"), 128, 1024)
			require.NoError(t, err)
			old := OperationSessionID{Filesystem: id.Filesystem, Session: id.Session}
			require.NoError(t, journal.FenceSession(t.Context(), old))
			var replacement OperationSessionID
			backend := &recoveryConflictBackend{operationJournalBackend: journal.backend, err: store.ErrWriteConflict, remaining: 1}
			backend.beforeFailure = func() {
				// Another recoverer wins, then this owner registers its replacement.
				_, err = journal.Cancel(t.Context(), id)
				require.NoError(t, err)
				if ack {
					require.NoError(t, journal.Acknowledge(t.Context(), id))
				}
				replacement, err = journal.RegisterSession(t.Context(), id.Filesystem, "mount-a")
				require.NoError(t, err)
			}
			count, err := newOperationJournalWithBackend(backend).RecoverClosedSession(t.Context(), old, 10)
			require.NoError(t, err)
			if ack {
				require.Zero(t, count)
			} else {
				require.Equal(t, 1, count)
			}
			require.Equal(t, []OperationID{id}, backend.ids)
			ts, err := journal.backend.Snapshot(t.Context())
			require.NoError(t, err)
			oldSession, found, err := journal.loadSessionAt(t.Context(), old, ts)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, oldSession.Closed)
			require.Zero(t, oldSession.Pending)
			// A retry against the old operation must not fence the new incarnation.
			_, err = journal.Admit(t.Context(), OperationID{Filesystem: replacement.Filesystem, Session: replacement.Session, Sequence: 1}, []byte("new"), 128, 1024)
			require.NoError(t, err)
		})
	}
}

package filesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationJournalAttemptIdentityParity(t *testing.T) {
	for _, tc := range []struct {
		name      string
		attempt   [16]byte
		match     bool
		unstarted bool
	}{
		{name: "zero matches unstarted record", unstarted: true},
		{name: "zero"},
		{name: "mismatch", attempt: [16]byte{1}},
		{name: "match", match: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			journal, _, _ := testOperationJournal(t)
			id := testOperationID(t, journal, 1)
			record, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
			require.NoError(t, err)
			if !tc.unstarted {
				record, err = journal.BeginAttempt(ctx, id)
				require.NoError(t, err)
			}
			validReadTS, _, _, err := journal.pendingSnapshot(ctx, id)
			require.NoError(t, err)
			attempt := tc.attempt
			if tc.match {
				attempt = record.AttemptID
			} else if !tc.unstarted {
				require.NotEqual(t, record.AttemptID, attempt)
			}
			readTS, planErr := journal.PlanSnapshot(ctx, id, attempt)
			_, publishErr := journal.Publish(ctx, id, attempt, operationAttemptPlan{Intent: []byte("wrong intent")})
			_, rejectErr := journal.Reject(ctx, id, attempt, validReadTS, []byte("wrong intent"), nil, nil)
			if tc.match {
				require.NoError(t, planErr)
				require.NotZero(t, readTS)
				require.ErrorIs(t, publishErr, ErrOperationIDReused)
				require.ErrorIs(t, rejectErr, ErrOperationIDReused)
				return
			}
			require.Zero(t, readTS)
			require.ErrorIs(t, planErr, ErrOperationGeneration)
			require.ErrorIs(t, publishErr, ErrOperationGeneration)
			require.ErrorIs(t, rejectErr, ErrOperationGeneration)
		})
	}
}

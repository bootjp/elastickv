package adapter

import (
	"context"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestQuorumGuardRejectionProvesAbsentBeforeReattempt(t *testing.T) {
	var events []string
	attempts := 0
	err := retryQuorumGuardRejection(t.Context(), func() error {
		events = append(events, "attempt")
		attempts++
		if attempts == 1 {
			return errors.Join(kv.ErrTransactionOutcomeUnknown, raftengine.ErrLeadershipLost)
		}
		return kv.ErrMigrationTransferTooLarge
	}, func() error {
		events = append(events, "proof")
		return nil
	})
	require.ErrorIs(t, err, kv.ErrMigrationTransferTooLarge)
	require.Equal(t, []string{"attempt", "proof", "attempt", "proof"}, events)
}

func TestQuorumGuardRejectionStopsOnFailedProof(t *testing.T) {
	failed := errors.New("persisted version or missing guard")
	attempts := 0
	err := retryQuorumGuardRejection(t.Context(), func() error {
		attempts++
		return errors.Join(kv.ErrTransactionOutcomeUnknown, raftengine.ErrLeadershipLost)
	}, func() error { return failed })
	require.ErrorIs(t, err, failed)
	require.Equal(t, 1, attempts)
}

func TestQuorumGuardRejectionDoesNotRetryOtherOutcomes(t *testing.T) {
	for index, outcome := range []error{nil, kv.ErrRouteCutoverPending, errors.Join(kv.ErrTransactionOutcomeUnknown, context.DeadlineExceeded)} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			attempts := 0
			err := retryQuorumGuardRejection(t.Context(), func() error {
				attempts++
				return outcome
			}, func() error {
				t.Fatal("must not reinterpret another outcome as a guarded rejection")
				return nil
			})
			require.Equal(t, outcome, err)
			require.Equal(t, 1, attempts)
		})
	}
}

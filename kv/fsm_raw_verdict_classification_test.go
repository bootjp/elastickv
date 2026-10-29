package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRawApplyVerdictClassification(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"invalid request", ErrInvalidRequest},
		{"equivalent invalid request", errors.New(ErrInvalidRequest.Error())},
		{"migration transfer too large", ErrMigrationTransferTooLarge},
		{"route cutover pending", ErrRouteCutoverPending},
		{"unknown request type", ErrUnknownRequestType},
		{"TTL inline capability required", ErrTTLInlineCapabilityRequired},
		{"backup timestamp fenced", ErrBackupTimestampFenced},
		{"prepare creation sealed", ErrPrepareCreationSealed},
		{"route write fenced", ErrRouteWriteFenced},
		{"route write timestamp too low", ErrRouteWriteTimestampTooLow},
		{"composed version GCd", ErrComposed1VersionGCd},
		{"transaction locked", ErrTxnLocked},
		{"store write conflict", store.ErrWriteConflict},
		{"store unknown op", store.ErrUnknownOp},
		{"store value too large", store.ErrValueTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, variant := range []struct {
				name string
				err  error
				want bool
			}{
				{"sentinel", tc.err, true},
				{"wrapped", errors.Wrap(tc.err, "apply"), true},
				{"joined verdicts", errors.Join(tc.err, store.ErrWriteConflict), true},
				{"joined operational error", errors.Join(tc.err, errors.New("local read failed")), true},
				{"exclusion first", errors.Join(errTargetReadinessUnproven, tc.err), false},
				{"exclusion last", errors.Join(tc.err, errTargetReadinessUnproven), false},
				{"wrapped exclusion", errors.Wrap(errors.Join(tc.err, errTargetReadinessUnproven), "apply"), false},
				{"marked exclusion", errors.Mark(tc.err, errTargetReadinessUnproven), false},
			} {
				t.Run(variant.name, func(t *testing.T) {
					require.Equal(t, variant.want, isRawApplyVerdict(variant.err))
				})
			}
		})
	}
}

func TestRawApplyVerdictRejectsOperationalErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"unknown", errors.New("local read failed")},
		{"canceled", context.Canceled},
		{"store key not found", store.ErrKeyNotFound},
		{"store invalid checksum", store.ErrInvalidChecksum},
		{"entry index mismatch", store.ErrRaftEntryIndexMismatch},
		{"entry store swapped", store.ErrRaftEntryStoreSwapped},
		{"readiness unproven", errTargetReadinessUnproven},
		{"readiness apply", ErrTargetReadinessApply},
		{"migration tracker persistence", errMigrationTrackerPersistence},
		{"composed ownership shifted", ErrComposed1Violation},
		{"transaction outcome unknown", ErrTransactionOutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, isRawApplyVerdict(tc.err))
			require.False(t, isRawApplyVerdict(errors.Wrap(tc.err, "apply")))
		})
	}
}

func TestFirstOperationalRequestFailureClassification(t *testing.T) {
	t.Parallel()
	first := errors.New("first local failure")
	later := errors.New("later local failure")
	unproven := errors.Join(ErrRouteCutoverPending, errTargetReadinessUnproven)
	for _, tc := range []struct {
		name    string
		results []error
		want    error
		index   string
	}{
		{name: "empty"},
		{name: "successes", results: []error{nil, nil}},
		{name: "only verdicts", results: []error{ErrInvalidRequest, nil, errors.Wrap(store.ErrWriteConflict, "apply"), ErrRouteCutoverPending}},
		{name: "first operational failure", results: []error{nil, ErrInvalidRequest, first, later}, want: first, index: "request 2 failed operationally"},
		{name: "wrapped operational failure", results: []error{errors.Wrap(first, "apply"), later}, want: first, index: "request 0 failed operationally"},
		{name: "readiness exclusion", results: []error{store.ErrWriteConflict, unproven, later}, want: unproven, index: "request 1 failed operationally"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := firstOperationalRequestFailure(tc.results)
			if tc.want == nil {
				require.NoError(t, got)
				return
			}
			require.ErrorIs(t, got, tc.want)
			require.ErrorContains(t, got, tc.index)
			require.NotErrorIs(t, got, later)
		})
	}
}

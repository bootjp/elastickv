package kv

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestMigrationFrontierSelectionMatchesConservativeIntervals(t *testing.T) {
	f, _, original := frontierAdmissionSplitFixture(t)
	reader, ok := f.store.(store.MigrationOwnershipStore)
	require.True(t, ok)
	history, err := reader.MigrationServingState(t.Context())
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		start, end string
		truncate   bool
		overlap    bool
	}{
		{name: "point", start: "n", end: "o"},
		{name: "boundary", start: "t", end: "u"},
		{name: "full union", start: "m", end: "z"},
		{name: "uncovered prefix", start: "l", end: "z"},
		{name: "uncovered suffix", start: "m", end: "zz"},
		{name: "unbounded suffix", start: "m"},
		{name: "unaffected legacy", start: "a", end: "b"},
		{name: "missing sibling", start: "m", end: "z", truncate: true},
		{name: "overlapping old ledger", start: "m", end: "z", overlap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := original
			if tc.truncate {
				view.Partitions.Partitions = view.Partitions.Partitions[:1]
			}
			if tc.overlap {
				view.Ownership = history.Ownership
			}
			r := migrationApplyRange{[]byte(tc.start), []byte(tc.end)}
			want, wantErr := migrationAdmissionOverlappingRangeProofs(view, migrationAdmissionProofs(view), r)
			got, gotErr := migrationAdmissionRangeProofs(view, r)
			if wantErr != nil {
				require.ErrorIs(t, gotErr, ErrRouteCutoverPending)
				return
			}
			require.NoError(t, gotErr)
			require.Equal(t, want, got)
		})
	}
}

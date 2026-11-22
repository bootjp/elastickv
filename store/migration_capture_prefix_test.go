package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnlineCapturePrefixGuardIsAtomic(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, exclude string
		reject, success       bool
	}{
		{name: "outside", prefix: "a/", success: true},
		{name: "same-entry captured key", prefix: "n"},
		{name: "empty prefix"},
		{name: "excluded captured key", exclude: "n", success: true},
		{name: "false rejected verdict", prefix: "a/", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openSSTSnapshotTestStore(t, t.TempDir())
			require.NoError(t, s.PutAt(t.Context(), []byte("a/old"), []byte("old"), 10, 0))
			session := startOnlineCaptureTest(t, s)
			entry, err := CaptureOnlineMigrationEntry(t.Context(), s, session.Identity.JobID, 11, sha256.Sum256([]byte(tc.name)), func(key []byte) bool {
				return bytes.Equal(key, []byte("n"))
			}, func(ctx context.Context, writer *MigrationCaptureWriter) error {
				if e := writer.Request(func() (MigrationCaptureOutcome, error) {
					return MigrationCaptureApplied, s.ApplyMutationsRaftAt(ctx, raftEntryPut("n", "point"), nil, 20, 20, 11)
				}); e != nil {
					return e
				}
				return writer.Request(func() (MigrationCaptureOutcome, error) {
					e := s.DeletePrefixesAtRaftAt(ctx, []PrefixDelete{{Prefix: []byte(tc.prefix), ExcludePrefix: []byte(tc.exclude)}}, 21, 11)
					if tc.reject {
						return MigrationCaptureRejected, e
					}
					return MigrationCaptureApplied, e
				})
			})
			saved, readErr := ReadOnlineMigrationSession(t.Context(), s, session.Identity.JobID)
			require.NoError(t, readErr)
			if tc.success {
				require.NoError(t, err)
				require.Equal(t, uint64(1), saved.Sequence)
				require.Len(t, entry.Effects, 1)
				value, e := s.GetAt(t.Context(), []byte("n"), 30)
				require.NoError(t, e)
				require.Equal(t, []byte("point"), value)
				_, e = s.GetAt(t.Context(), []byte("a/old"), 30)
				require.ErrorIs(t, e, ErrKeyNotFound)
			} else {
				require.Error(t, err)
				require.Zero(t, saved.Sequence)
				index, present, e := s.LastAppliedIndex()
				require.NoError(t, e)
				require.True(t, present)
				require.Equal(t, uint64(10), index)
				_, e = s.GetAt(t.Context(), []byte("n"), 30)
				require.ErrorIs(t, e, ErrKeyNotFound)
				value, e := s.GetAt(t.Context(), []byte("a/old"), 30)
				require.NoError(t, e)
				require.Equal(t, []byte("old"), value)
			}
		})
	}
}

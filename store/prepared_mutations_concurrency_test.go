package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPreparedMutationsWaitForEntryAndComparePublishedOverwrite(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			api := preparedMutationAPI(t, s)
			muts, expected := preparedMutationFixture()
			seedPreparedMutationFixture(t, s, expected)
			ready, release := make(chan struct{}), make(chan struct{})
			entryDone, applyDone := make(chan struct{}), make(chan struct{})
			entryResult, applyResult := make(chan error, 1), make(chan error, 1)
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() {
				unblock()
				testutil.Closed(t, entryDone, time.Second, "overwrite entry did not join")
				testutil.Closed(t, applyDone, time.Second, "concurrent resolution did not join")
			})
			go func() {
				defer close(entryDone)
				entryResult <- raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
					if err := api.ApplyPreparedMutationsRaftAt(ctx, raftEntryPut(string(expected[0].Key), "published-overwrite"), nil, 30, 30, nil, 7); err != nil {
						return err
					}
					close(ready)
					<-release
					return nil
				})
			}()
			started := make(chan struct{})
			go func() {
				defer close(applyDone)
				<-ready
				close(started)
				applyResult <- api.ApplyPreparedMutationsRaftAt(t.Context(), muts, nil, 10, 30, expected, 8)
			}()
			testutil.Closed(t, started, time.Second, "real concurrent resolution did not start")
			select {
			case err := <-applyResult:
				t.Fatalf("resolution escaped the entry's writer lock: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			unblock()
			require.NoError(t, testutil.Recv(t, entryResult, time.Second, "overwrite entry did not commit"))
			require.ErrorIs(t, testutil.Recv(t, applyResult, time.Second, "resolution did not compare after overwrite"), ErrWriteConflict)
			require.False(t, raftEntryExists(t, s, "user"))
			got, err := s.GetAt(t.Context(), expected[0].Key, 30)
			require.NoError(t, err)
			require.Equal(t, []byte("published-overwrite"), got)
		})
	}
}

func TestPreparedMutationsAuthenticatedReadFailureAbortsEntry(t *testing.T) {
	for _, mode := range []string{"tag", "tombstone-header", "expiry-header"} {
		t.Run(mode, func(t *testing.T) {
			f := newEncryptedStoreFixture(t, 23)
			muts, expected := preparedMutationFixture()
			seedPreparedMutationFixture(t, f.mvcc, expected)
			f.tamperPebbleValue(t, expected[0].Key, 30, func(raw []byte) []byte {
				switch mode {
				case "tag":
					raw[len(raw)-1] ^= 1
				case "tombstone-header":
					raw[0] ^= tombstoneMask
				case "expiry-header":
					raw[1] ^= 1
				}
				return raw
			})
			s := f.mvcc
			err := raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
				require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("earlier", "must-abort"), nil, 0, 5, 7))
				applyErr := preparedMutationAPI(t, s).ApplyPreparedMutationsRaftAt(ctx, muts, nil, 10, 30, expected, 7)
				require.True(t, errors.Is(applyErr, ErrEncryptedReadIntegrity), "%v", applyErr)
				require.False(t, IsApplyVerdict(applyErr))
				return nil
			})
			require.True(t, errors.Is(err, ErrEncryptedReadIntegrity), "%v", err)
			require.False(t, raftEntryExists(t, s, "earlier"))
			require.False(t, raftEntryExists(t, s, "user"))
			index, present := raftEntryAppliedIndex(t, s)
			require.True(t, present)
			require.Equal(t, uint64(1), index)
		})
	}
}

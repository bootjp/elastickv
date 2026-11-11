package store

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPreparedMutationsSameEntryPrepareAndResolve(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			api := preparedMutationAPI(t, s)
			muts, expected := preparedMutationFixture()
			require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
				for _, version := range expected {
					require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut(string(version.Key), string(version.Value)), nil, 10, 30, 7))
				}
				require.NoError(t, api.ApplyPreparedMutationsRaftAt(ctx, muts, nil, 10, 30, expected, 7))
				got, err := s.GetAt(ctx, []byte("user"), 30)
				require.NoError(t, err)
				require.Equal(t, []byte("committed"), got)
				require.False(t, raftEntryExists(t, s, "user"), "entry changes must remain unpublished until the callback completes")
				return nil
			}))
			require.True(t, raftEntryExists(t, s, "user"))
			require.False(t, raftEntryExists(t, s, string(expected[0].Key)))
		})
	}
}

func TestPreparedMutationsEntryVerdictsAndOperationalRollback(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, mode := range []string{"write-conflict", "read-conflict", "altered-prepared", "altered-expiry", "callback-error", "ignored-cancel", "index-mismatch"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				testPreparedEntryMode(t, factory(t), mode)
			})
		}
	}
}

func testPreparedEntryMode(t *testing.T, s MVCCStore, mode string) {
	t.Helper()
	api := preparedMutationAPI(t, s)
	muts, expected := preparedMutationFixture()
	seedPreparedMutationFixture(t, s, expected)
	boom := errors.New("abort prepared resolution entry")
	operational := mode == "callback-error" || mode == "ignored-cancel" || mode == "index-mismatch"
	err := raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
		require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("earlier", "staged"), nil, 0, 5, 7))
		reads := stagePreparedTestConflict(t, s, api, ctx, mode, expected)
		callCtx, index := ctx, uint64(7)
		if mode == "ignored-cancel" {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithCancel(ctx)
			cancel()
		}
		if mode == "index-mismatch" {
			index = 8
		}
		applyErr := api.ApplyPreparedMutationsRaftAt(callCtx, muts, reads, 10, 30, expected, index)
		if mode == "callback-error" {
			require.NoError(t, applyErr)
			return boom
		}
		if operational {
			require.Error(t, applyErr)
			require.False(t, IsApplyVerdict(applyErr))
			return nil // A swallowed operational error must still abort everything.
		}
		require.ErrorIs(t, applyErr, ErrWriteConflict)
		require.True(t, IsApplyVerdict(applyErr))
		return s.ApplyMutationsRaftAt(ctx, raftEntryPut("later", "survives-verdict"), nil, 0, 40, 7)
	})
	if operational {
		require.Error(t, err)
		require.False(t, raftEntryExists(t, s, "earlier"))
		require.False(t, raftEntryExists(t, s, "user"))
	} else {
		require.NoError(t, err)
		require.True(t, raftEntryExists(t, s, "earlier"))
		require.True(t, raftEntryExists(t, s, "later"))
	}
	require.True(t, raftEntryExists(t, s, string(expected[1].Key)), "a failed resolution must not partially clean its prepared records")
	if _, native := s.(*pebbleStore); native {
		index, present := raftEntryAppliedIndex(t, s)
		require.True(t, present)
		require.Equal(t, map[bool]uint64{true: 1, false: 7}[operational], index)
	}
}

func stagePreparedTestConflict(t *testing.T, s MVCCStore, api preparedMutationTestApplier, ctx context.Context, mode string, expected []MVCCVersion) [][]byte {
	t.Helper()
	switch mode {
	case "write-conflict":
		require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("user", "concurrent"), nil, 0, 20, 7))
	case "read-conflict":
		require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("dependency", "concurrent"), nil, 0, 20, 7))
		return [][]byte{[]byte("dependency")}
	case "altered-prepared", "altered-expiry":
		value, expiry := []byte("different-proof"), uint64(0)
		if mode == "altered-expiry" {
			value, expiry = expected[0].Value, 100
		}
		mutations := []*KVPairMutation{{Op: OpTypePut, Key: expected[0].Key, Value: value, ExpireAt: expiry}}
		require.NoError(t, api.ApplyPreparedMutationsRaftAt(ctx, mutations, nil, 30, 30, nil, 7))
		actual, err := s.GetAt(ctx, expected[0].Key, 30)
		require.NoError(t, err)
		require.Equal(t, value, actual)
	}
	return nil
}

func TestPreparedMutationsEncryptedComparisonAndReopen(t *testing.T) {
	f := newEncryptedStoreFixture(t, 17)
	s := f.mvcc
	muts, expected := preparedMutationFixture()
	seedPreparedMutationFixture(t, s, expected)
	require.NoError(t, preparedMutationAPI(t, s).ApplyPreparedMutationsRaftAt(t.Context(), muts, nil, 10, 30, expected, 2))
	f.closeIfOpen(t)
	f.reopen(t)
	got, err := f.mvcc.GetAt(t.Context(), []byte("user"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("committed"), got)
	index, present := raftEntryAppliedIndex(t, f.mvcc)
	require.True(t, present)
	require.Equal(t, uint64(2), index)
	require.False(t, raftEntryExists(t, f.mvcc, string(expected[0].Key)))
}

func TestPreparedMutationsEncryptionFailureAbortsEarlierWrites(t *testing.T) {
	f := newEncryptedStoreFixture(t, 19)
	s := f.mvcc
	muts, expected := preparedMutationFixture()
	seedPreparedMutationFixture(t, s, expected)
	ps, ok := s.(*pebbleStore)
	require.True(t, ok)
	err := raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
		require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("earlier", "must-abort"), nil, 0, 5, 7))
		original := ps.activeStorageKeyID
		ps.activeStorageKeyID = func() (uint32, bool) { return 999, true }
		applyErr := preparedMutationAPI(t, s).ApplyPreparedMutationsRaftAt(ctx, muts, nil, 10, 30, expected, 7)
		ps.activeStorageKeyID = original
		require.True(t, errors.Is(applyErr, encryption.ErrUnknownKeyID), "%v", applyErr)
		require.False(t, IsApplyVerdict(applyErr))
		return nil
	})
	require.True(t, errors.Is(err, encryption.ErrUnknownKeyID), "%v", err)
	require.False(t, raftEntryExists(t, s, "earlier"))
	require.False(t, raftEntryExists(t, s, "user"))
	index, present := raftEntryAppliedIndex(t, s)
	require.True(t, present)
	require.Equal(t, uint64(1), index)
}

func TestPreparedMutationsRejectRegressingDurableIndex(t *testing.T) {
	s := absenceTestStores(t)["pebble"](t)
	muts, expected := preparedMutationFixture()
	seedPreparedMutationFixture(t, s, expected)
	require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), raftEntryPut("later-entry", "retained"), nil, 0, 5, 10))
	err := preparedMutationAPI(t, s).ApplyPreparedMutationsRaftAt(t.Context(), muts, nil, 10, 30, expected, 2)
	require.ErrorIs(t, err, ErrRaftEntryIndexMismatch)
	require.False(t, raftEntryExists(t, s, "user"))
	index, present := raftEntryAppliedIndex(t, s)
	require.True(t, present)
	require.Equal(t, uint64(10), index)
}

package store

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type preparedMutationTestApplier interface {
	ApplyPreparedMutationsRaftAt(context.Context, []*KVPairMutation, [][]byte, uint64, uint64, []MVCCVersion, uint64) error
}

func preparedMutationAPI(t *testing.T, s MVCCStore) preparedMutationTestApplier {
	t.Helper()
	api, ok := s.(preparedMutationTestApplier)
	require.True(t, ok, "%T lacks atomic prepared-claim mutation capability", s)
	return api
}

func preparedMutationFixture() ([]*KVPairMutation, []MVCCVersion) {
	expected := []MVCCVersion{
		{Key: []byte("!txn|lock|user"), Value: []byte("claimed-lock-with-durable-proof"), CommitTS: 30},
		{Key: []byte("!txn|int|user"), Value: []byte("prepared-intent"), CommitTS: 30},
	}
	muts := []*KVPairMutation{{Op: OpTypePut, Key: []byte("user"), Value: []byte("committed")}}
	for _, version := range expected {
		muts = append(muts, &KVPairMutation{Op: OpTypeDelete, Key: bytes.Clone(version.Key)})
	}
	return muts, expected
}

func seedPreparedMutationFixture(t *testing.T, s MVCCStore, expected []MVCCVersion) {
	t.Helper()
	for _, version := range expected {
		require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), []*KVPairMutation{{Op: OpTypePut,
			Key: version.Key, Value: version.Value, ExpireAt: version.ExpireAt}}, nil, 0, version.CommitTS, 1))
	}
}

func TestPreparedMutationsCapabilityAndCleanup(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, scoped := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/direct", true: "/entry"}[scoped], func(t *testing.T) {
				s := factory(t)
				api := preparedMutationAPI(t, s)
				muts, expected := preparedMutationFixture()
				seedPreparedMutationFixture(t, s, expected)
				apply := func(ctx context.Context) error {
					return api.ApplyPreparedMutationsRaftAt(ctx, muts, nil, 10, 30, expected, 2)
				}
				if scoped {
					require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 2, apply))
				} else {
					require.NoError(t, apply(t.Context()))
				}
				got, err := s.GetAt(t.Context(), []byte("user"), 30)
				require.NoError(t, err)
				require.Equal(t, []byte("committed"), got)
				for _, version := range expected {
					_, err = s.GetAt(t.Context(), version.Key, 30)
					require.ErrorIs(t, err, ErrKeyNotFound)
				}
				if _, native := s.(*pebbleStore); native {
					index, present := raftEntryAppliedIndex(t, s)
					require.True(t, present)
					require.Equal(t, uint64(2), index)
				}
			})
		}
	}
}

func TestPreparedMutationsKeepOriginalUserAndReadOCC(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, readConflict := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/write", true: "/read"}[readConflict], func(t *testing.T) {
				s := factory(t)
				api := preparedMutationAPI(t, s)
				muts, expected := preparedMutationFixture()
				seedPreparedMutationFixture(t, s, expected)
				key := "user"
				var reads [][]byte
				if readConflict {
					key, reads = "read-dependency", [][]byte{[]byte("read-dependency")}
				}
				require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), raftEntryPut(key, "concurrent"), nil, 0, 20, 1))
				err := api.ApplyPreparedMutationsRaftAt(t.Context(), muts, reads, 10, 30, expected, 2)
				require.ErrorIs(t, err, ErrWriteConflict, "prepared cleanup must never raise the user/read snapshot to commitTS")
				for _, version := range expected {
					got, err := s.GetAt(t.Context(), version.Key, 30)
					require.NoError(t, err)
					require.Equal(t, version.Value, got)
				}
			})
		}
	}
}

func TestPreparedMutationsRejectUnsafeExpectations(t *testing.T) {
	for name, factory := range absenceTestStores(t) {
		for _, mode := range []string{"user-key", "missing", "changed-value", "changed-ts", "tombstone", "expected-tombstone", "expected-ts", "missing-expected", "expiry", "family", "extra", "duplicate-expected", "duplicate-cleanup", "not-delete", "empty-suffix", "commit-key", "rollback-key", "meta-key", "cleanup-value", "cleanup-expiry", "cleanup-absence", "read-not-exempt"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				s := factory(t)
				api := preparedMutationAPI(t, s)
				muts, expected := preparedMutationFixture()
				seedPreparedMutationFixture(t, s, expected)
				c := &preparedUnsafeCase{mutations: muts, expected: expected}
				changePreparedUnsafeKey(t, s, mode, c)
				changePreparedUnsafeVersion(t, s, mode, c)
				changePreparedUnsafeCleanup(mode, c)
				err := api.ApplyPreparedMutationsRaftAt(t.Context(), c.mutations, c.reads, 10, 30, c.expected, 2)
				require.ErrorIs(t, err, ErrWriteConflict)
				require.False(t, raftEntryExists(t, s, "other-user"))
				if mode != "user-key" {
					require.False(t, raftEntryExists(t, s, "user"))
				}
			})
		}
	}
}

type preparedUnsafeCase struct {
	mutations []*KVPairMutation
	expected  []MVCCVersion
	reads     [][]byte
}

func changePreparedUnsafeKey(t *testing.T, s MVCCStore, mode string, c *preparedUnsafeCase) {
	t.Helper()
	key, found := map[string]string{"user-key": "user", "empty-suffix": "!txn|lock|", "commit-key": "!txn|cmt|user", "rollback-key": "!txn|rb|user", "meta-key": "!txn|meta|user"}[mode]
	if !found {
		return
	}
	c.expected[0].Key, c.mutations[1].Key = []byte(key), []byte(key)
	if mode == "user-key" {
		c.mutations[0].Key = []byte("other-user")
	}
	seedPreparedMutationFixture(t, s, c.expected[:1])
}

func changePreparedUnsafeVersion(t *testing.T, s MVCCStore, mode string, c *preparedUnsafeCase) {
	t.Helper()
	switch mode {
	case "missing":
		c.expected[0].Key, c.mutations[1].Key = []byte("!txn|lock|absent"), []byte("!txn|lock|absent")
	case "changed-value":
		c.expected[0].Value = []byte("different-proof")
	case "changed-ts":
		require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), raftEntryPut(string(c.expected[0].Key), string(c.expected[0].Value)), nil, 30, 31, 1))
	case "tombstone":
		require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), []*KVPairMutation{{Op: OpTypeDelete, Key: c.expected[0].Key}}, nil, 30, 30, 0))
	case "expected-tombstone":
		c.expected[0].Tombstone = true
	case "expected-ts":
		c.expected[0].CommitTS = 29
	case "missing-expected":
		c.expected = c.expected[1:]
	case "expiry":
		c.expected[0].ExpireAt = 100
	case "family":
		c.expected[0].KeyFamily = 1
	}
}

func changePreparedUnsafeCleanup(mode string, c *preparedUnsafeCase) {
	switch mode {
	case "extra":
		c.expected = append(c.expected, MVCCVersion{Key: []byte("!txn|lock|extra"), CommitTS: 30})
	case "duplicate-expected":
		c.expected = append(c.expected, c.expected[0])
	case "duplicate-cleanup":
		c.mutations = append(c.mutations, c.mutations[1])
	case "not-delete":
		c.mutations[1].Op = OpTypePut
	case "cleanup-value":
		c.mutations[1].Value = []byte("not-cleanup")
	case "cleanup-expiry":
		c.mutations[1].ExpireAt = 100
	case "cleanup-absence":
		c.mutations[1].RequireAbsent = true
	case "read-not-exempt":
		c.reads = [][]byte{c.expected[0].Key}
	}
}

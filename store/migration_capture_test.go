package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func captureTestIdentity(index uint64) MigrationCaptureIdentity {
	return MigrationCaptureIdentity{SourceGroupID: 1, JobID: 9, Generation: 1, Index: index, Lineage: [32]byte{1}}
}

func captureTestRequest(key, value string, ts uint64) MigrationCaptureRequest {
	return MigrationCaptureRequest{Mutations: raftEntryPut(key, value), StartTS: ts, CommitTS: ts}
}

func TestMigrationCaptureAtomicDataAndRecord(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	id := captureTestIdentity(11)
	record, err := ApplyMigrationCaptureEntry(t.Context(), s, id, []MigrationCaptureRequest{
		captureTestRequest("a", "first", 100),
		captureTestRequest("b", "second", 101),
	}, MigrationCaptureLimits{})
	require.NoError(t, err)
	require.True(t, record.Complete)
	require.Len(t, record.Effects, 2)
	require.Equal(t, uint64(100), record.Effects[0].CommitTS)
	require.Equal(t, uint64(101), record.Effects[1].CommitTS)
	stored, err := ReadMigrationCaptureEntry(t.Context(), s, id)
	require.NoError(t, err)
	require.Equal(t, record, stored)
	index, present := raftEntryAppliedIndex(t, s)
	require.True(t, present)
	require.Equal(t, uint64(11), index)
	value, err := s.GetAt(t.Context(), []byte("b"), 102)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), value)
}

func TestMigrationCaptureReplayAndStaleEntry(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	id := captureTestIdentity(11)
	requests := []MigrationCaptureRequest{captureTestRequest("a", "first", 100)}
	first, err := ApplyMigrationCaptureEntry(t.Context(), s, id, requests, MigrationCaptureLimits{})
	require.NoError(t, err)
	require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), raftEntryPut("a", "newer"), nil, 101, 101, 12))
	duplicate, err := ApplyMigrationCaptureEntry(t.Context(), s, id, requests, MigrationCaptureLimits{})
	require.NoError(t, err)
	require.Equal(t, first, duplicate)
	_, err = ApplyMigrationCaptureEntry(t.Context(), s, id, []MigrationCaptureRequest{captureTestRequest("a", "different", 100)}, MigrationCaptureLimits{})
	require.ErrorIs(t, err, ErrMigrationCaptureConflict)
	_, err = ApplyMigrationCaptureEntry(t.Context(), s, captureTestIdentity(12), requests, MigrationCaptureLimits{})
	require.ErrorIs(t, err, ErrMigrationCaptureStaleIndex)
	value, err := s.GetAt(t.Context(), []byte("a"), 102)
	require.NoError(t, err)
	require.Equal(t, []byte("newer"), value)
	index, _ := raftEntryAppliedIndex(t, s)
	require.Equal(t, uint64(12), index)
}

func TestMigrationCaptureRejectsPartialCommit(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	id := captureTestIdentity(11)
	boom := errors.New("injected journal failure")
	broken := &captureFailingMetadata{MVCCStore: s, RaftEntryApplier: s, RaftControlMetadataStore: s, applied: s, fail: boom}
	_, err := ApplyMigrationCaptureEntry(t.Context(), broken, id, []MigrationCaptureRequest{captureTestRequest("a", "not-committed", 100)}, MigrationCaptureLimits{})
	require.ErrorIs(t, err, boom)
	_, err = s.GetAt(t.Context(), []byte("a"), 101)
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = ReadMigrationCaptureEntry(t.Context(), s, id)
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, present := raftEntryAppliedIndex(t, s)
	require.False(t, present)
}

type captureFailingMetadata struct {
	MVCCStore
	RaftEntryApplier
	RaftControlMetadataStore
	applied interface{ LastAppliedIndex() (uint64, bool, error) }
	fail    error
}

func (s *captureFailingMetadata) LastAppliedIndex() (uint64, bool, error) {
	return s.applied.LastAppliedIndex()
}

func (s *captureFailingMetadata) StageRaftControlMetadata(context.Context, []byte, []byte) error {
	return s.fail
}

func TestMigrationCaptureBoundDoesNotRejectWrite(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	id := captureTestIdentity(11)
	value := bytes.Repeat([]byte("x"), 2048)
	req := captureTestRequest("a", string(value), 100)
	record, err := ApplyMigrationCaptureEntry(t.Context(), s, id, []MigrationCaptureRequest{req}, MigrationCaptureLimits{MaxBytes: 1024})
	require.NoError(t, err)
	require.False(t, record.Complete)
	require.Equal(t, MigrationCaptureByteLimit, record.Invalidation)
	require.Empty(t, record.Effects)
	got, err := s.GetAt(t.Context(), []byte("a"), 101)
	require.NoError(t, err)
	require.Equal(t, value, got)
	stored, err := ReadMigrationCaptureEntry(t.Context(), s, id)
	require.NoError(t, err)
	require.Equal(t, record, stored)
}

func TestMigrationCaptureRecordsPostImageRatherThanInput(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	id := captureTestIdentity(11)
	record, err := ApplyMigrationCaptureEntry(t.Context(), s, id, []MigrationCaptureRequest{
		captureTestRequest("a", "first", 100),
		captureTestRequest("a", "different", 100),
	}, MigrationCaptureLimits{MaxEffects: 1})
	require.NoError(t, err)
	require.True(t, record.Complete)
	require.Len(t, record.Effects, 1, "a skipped replay is not another staged effect")
	value, err := s.GetAt(t.Context(), []byte("a"), 101)
	require.NoError(t, err)
	require.Equal(t, value, record.Effects[len(record.Effects)-1].Value)
}

func TestMigrationCaptureConflictTTLAndTombstone(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), raftEntryPut("existing", "old"), nil, 100, 100, 10))
	conflict := captureTestRequest("existing", "rejected", 101)
	conflict.StartTS = 99
	ttl := captureTestRequest("ttl", "expires", 110)
	ttl.Mutations[0].ExpireAt = 200
	deleted := MigrationCaptureRequest{CommitTS: 111, Mutations: []*KVPairMutation{{Op: OpTypeDelete, Key: []byte("deleted"), ExpireAt: 900}}}
	record, err := ApplyMigrationCaptureEntry(t.Context(), s, captureTestIdentity(11), []MigrationCaptureRequest{conflict, ttl, deleted}, MigrationCaptureLimits{})
	require.NoError(t, err)
	require.True(t, record.Complete)
	require.Equal(t, []MigrationCaptureOutcome{MigrationCaptureConflict, MigrationCaptureApplied, MigrationCaptureApplied}, record.Outcomes)
	require.Len(t, record.Effects, 2)
	require.Equal(t, uint64(1), record.Effects[0].Request)
	require.Equal(t, uint64(200), record.Effects[0].ExpireAt)
	require.True(t, record.Effects[1].Tombstone)
	require.Zero(t, record.Effects[1].ExpireAt)
	require.Nil(t, record.Effects[1].Value)
	low := captureTestRequest("late", "low-timestamp", 90)
	low.StartTS = 120
	record, err = ApplyMigrationCaptureEntry(t.Context(), s, captureTestIdentity(12), []MigrationCaptureRequest{low}, MigrationCaptureLimits{})
	require.NoError(t, err)
	require.Equal(t, uint64(90), record.Effects[0].CommitTS)
	require.Equal(t, uint64(12), record.Identity.Index)
}

func TestMigrationCaptureReceiptRequiresIndexedScope(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	id := captureTestIdentity(11)
	record, err := ApplyMigrationCaptureEntry(t.Context(), s, id, nil, MigrationCaptureLimits{})
	require.NoError(t, err)
	for _, remove := range []bool{false, true} {
		err = s.ApplyRaftControlMetadata(t.Context(), func(ctx context.Context) error {
			if remove {
				return s.DeleteRaftControlMetadata(ctx, migrationCaptureKey(id))
			}
			return s.StageRaftControlMetadata(ctx, migrationCaptureKey(id), []byte("overwrite"))
		})
		require.ErrorIs(t, err, ErrRaftControlScopeRequired)
	}
	got, err := ReadMigrationCaptureEntry(t.Context(), s, id)
	require.NoError(t, err)
	require.Equal(t, record, got)
}

func TestMigrationCaptureMixedFailureAbortsEntry(t *testing.T) {
	t.Parallel()
	for _, mixed := range []error{errors.Join(ErrWriteConflict, io.EOF), errors.WithSecondaryError(ErrWriteConflict, io.EOF)} {
		s := openSSTSnapshotTestStore(t, t.TempDir())
		wrapped := &captureFailingMutation{captureFailingMetadata: &captureFailingMetadata{MVCCStore: s, RaftEntryApplier: s, RaftControlMetadataStore: s, applied: s}, mutationError: mixed}
		_, err := ApplyMigrationCaptureEntry(t.Context(), wrapped, captureTestIdentity(11), []MigrationCaptureRequest{captureTestRequest("a", "first", 100), captureTestRequest("bad", "lost", 101)}, MigrationCaptureLimits{})
		require.ErrorIs(t, err, ErrWriteConflict)
		_, err = s.GetAt(t.Context(), []byte("a"), 102)
		require.ErrorIs(t, err, ErrKeyNotFound)
		_, err = ReadMigrationCaptureEntry(t.Context(), s, captureTestIdentity(11))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}
}

type captureFailingMutation struct {
	*captureFailingMetadata
	mutationError error
}

func (s *captureFailingMutation) ApplyMutationsRaftAt(ctx context.Context, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS, index uint64) error {
	if bytes.Equal(mutations[0].Key, []byte("bad")) {
		return s.mutationError
	}
	return s.MVCCStore.ApplyMutationsRaftAt(ctx, mutations, readKeys, startTS, commitTS, index)
}

func TestMigrationCaptureInputAndScope(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	for _, request := range []MigrationCaptureRequest{
		{CommitTS: 0, Mutations: raftEntryPut("a", "bad")},
		{CommitTS: 100, Mutations: []*KVPairMutation{nil}},
		{CommitTS: 100, Mutations: raftEntryPut("", "unsupported-empty-key")},
	} {
		_, err := ApplyMigrationCaptureEntry(t.Context(), s, captureTestIdentity(11), []MigrationCaptureRequest{request}, MigrationCaptureLimits{})
		require.ErrorIs(t, err, ErrMigrationCaptureInput)
	}
	require.NoError(t, s.ApplyRaftEntry(t.Context(), 11, func(ctx context.Context) error {
		_, err := ApplyMigrationCaptureEntry(ctx, s, captureTestIdentity(11), nil, MigrationCaptureLimits{})
		require.ErrorIs(t, err, ErrMigrationCaptureInput)
		return nil
	}))
	_, present := raftEntryAppliedIndex(t, s)
	require.False(t, present)
}

func TestMigrationCaptureWriterCannotIgnoreFailure(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	boom := errors.New("callback failed after staging")
	_, err := CaptureMigrationRaftEntry(t.Context(), s, captureTestIdentity(11), sha256.Sum256([]byte("command")), MigrationCaptureLimits{}, func(ctx context.Context, writer *MigrationCaptureWriter) error {
		requestErr := writer.Request(func() (MigrationCaptureOutcome, error) {
			require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("a", "not-committed"), nil, 100, 100, 11))
			return MigrationCaptureApplied, boom
		})
		require.ErrorIs(t, requestErr, boom)
		return nil
	})
	require.ErrorIs(t, err, boom)
	_, err = s.GetAt(t.Context(), []byte("a"), 101)
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = ReadMigrationCaptureEntry(t.Context(), s, captureTestIdentity(11))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMigrationCaptureRejectsUnscopedAndPrefixWrites(t *testing.T) {
	t.Parallel()
	for _, prefix := range []bool{false, true} {
		s := openSSTSnapshotTestStore(t, t.TempDir())
		_, err := CaptureMigrationRaftEntry(t.Context(), s, captureTestIdentity(11), sha256.Sum256([]byte("command")), MigrationCaptureLimits{}, func(ctx context.Context, writer *MigrationCaptureWriter) error {
			if !prefix {
				return s.ApplyMutationsRaftAt(ctx, raftEntryPut("a", "outside-request"), nil, 100, 100, 11)
			}
			return writer.Request(func() (MigrationCaptureOutcome, error) {
				err := s.DeletePrefixesAtRaftAt(ctx, []PrefixDelete{{Prefix: []byte("a")}}, 100, 11)
				return MigrationCaptureApplied, err
			})
		})
		require.Error(t, err)
		_, err = ReadMigrationCaptureEntry(t.Context(), s, captureTestIdentity(11))
		require.ErrorIs(t, err, ErrKeyNotFound)
		_, present := raftEntryAppliedIndex(t, s)
		require.False(t, present)
	}
}

func TestMigrationCaptureQuotaCannotHideRejectedWrites(t *testing.T) {
	t.Parallel()
	s := openSSTSnapshotTestStore(t, t.TempDir())
	_, err := CaptureMigrationRaftEntry(t.Context(), s, captureTestIdentity(11), sha256.Sum256([]byte("command")), MigrationCaptureLimits{MaxBytes: 1024}, func(ctx context.Context, writer *MigrationCaptureWriter) error {
		return writer.Request(func() (MigrationCaptureOutcome, error) {
			err := s.ApplyMutationsRaftAt(ctx, raftEntryPut("a", string(bytes.Repeat([]byte("x"), 2048))), nil, 100, 100, 11)
			require.NoError(t, err)
			return MigrationCaptureRejected, nil
		})
	})
	require.ErrorIs(t, err, ErrMigrationCaptureCorrupt)
	_, err = s.GetAt(t.Context(), []byte("a"), 101)
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = ReadMigrationCaptureEntry(t.Context(), s, captureTestIdentity(11))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

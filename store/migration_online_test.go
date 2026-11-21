package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func startOnlineCaptureTest(t *testing.T, s *pebbleStore) OnlineMigrationSession {
	t.Helper()
	session := OnlineMigrationSession{Version: OnlineMigrationVersion, Identity: captureTestIdentity(10), StartIndex: 10, LastIndex: 10, ConfigurationIndex: 3, RouteStart: []byte("m"), RouteEnd: []byte("z")}
	require.NoError(t, s.ApplyRaftEntry(t.Context(), 10, func(ctx context.Context) error { return StartOnlineMigrationSession(ctx, s, session) }))
	return session
}

func captureOnlineTestWrite(t *testing.T, s *pebbleStore, index, ts uint64, key, value string) MigrationCaptureEntry {
	t.Helper()
	entry, err := CaptureOnlineMigrationEntry(t.Context(), s, 9, index, sha256.Sum256([]byte(key+value)), func(key []byte) bool {
		return bytes.Compare(key, []byte("m")) >= 0 && bytes.Compare(key, []byte("z")) < 0
	}, func(ctx context.Context, writer *MigrationCaptureWriter) error {
		return writer.Request(func() (MigrationCaptureOutcome, error) {
			err := s.ApplyMutationsRaftAt(ctx, raftEntryPut(key, value), nil, ts, ts, index)
			return MigrationCaptureApplied, err
		})
	})
	require.NoError(t, err)
	return entry
}

func TestOnlineMigrationCaptureSequenceAndReplay(t *testing.T) {
	t.Parallel()
	source := openSSTSnapshotTestStore(t, t.TempDir())
	session := startOnlineCaptureTest(t, source)
	first := captureOnlineTestWrite(t, source, 11, 100, "n", "first")
	second := captureOnlineTestWrite(t, source, 15, 20, "p", "late-low-timestamp")
	require.Len(t, second.Effects, 1)
	require.Equal(t, uint64(20), second.Effects[0].CommitTS)
	replay := captureOnlineTestWrite(t, source, 15, 20, "p", "late-low-timestamp")
	require.Equal(t, second, replay)
	state, err := ReadOnlineMigrationSession(t.Context(), source, 9)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.Sequence)
	require.Equal(t, uint64(15), state.LastIndex)
	frame, err := ReadOnlineMigrationFrame(t.Context(), source, state, 1)
	require.NoError(t, err)
	require.Equal(t, first, frame)
	require.NoError(t, source.ApplyRaftEntry(t.Context(), 16, func(ctx context.Context) error { return StartOnlineMigrationSession(ctx, source, session) }))
	state, err = ReadOnlineMigrationSession(t.Context(), source, 9)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.Sequence)
	require.NoError(t, source.ApplyRaftEntry(t.Context(), 17, func(ctx context.Context) error { _, err := SealOnlineMigrationSession(ctx, source, 9, 3); return err }))
	_, err = CaptureOnlineMigrationEntry(t.Context(), source, 9, 18, sha256.Sum256([]byte("sealed")), nil, func(context.Context, *MigrationCaptureWriter) error {
		t.Fatal("sealed capture must not execute mutation")
		return nil
	})
	require.ErrorIs(t, err, ErrOnlineMigrationSealed)
}

func TestOnlineMigrationInvalidationDoesNotAdvanceIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var session OnlineMigrationSession
	if !t.Run("persist invalidation and write", func(t *testing.T) {
		source := openSSTSnapshotTestStore(t, dir)
		session = startOnlineCaptureTest(t, source)
		require.NoError(t, InvalidateOnlineMigration(t.Context(), source, session, "unsupported prefix delete"))
		index, _ := raftEntryAppliedIndex(t, source)
		require.Equal(t, uint64(10), index)
		require.NoError(t, source.ApplyMutationsRaftAt(t.Context(), raftEntryPut("n", "ordinary write"), nil, 20, 20, 11))
	}) {
		return
	}
	source := openSSTSnapshotTestStore(t, dir)
	value, err := source.GetAt(t.Context(), []byte("n"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("ordinary write"), value)
	state, err := ReadOnlineMigrationSession(t.Context(), source, 9)
	require.NoError(t, err)
	require.Equal(t, "unsupported prefix delete", state.Invalid)
	require.ErrorIs(t, source.ApplyRaftEntry(t.Context(), 12, func(ctx context.Context) error { return StartOnlineMigrationSession(ctx, source, session) }), ErrOnlineMigrationInvalid)
	require.ErrorIs(t, source.ApplyRaftEntry(t.Context(), 12, func(ctx context.Context) error { _, err := SealOnlineMigrationSession(ctx, source, 9, 3); return err }), ErrOnlineMigrationInvalid)
}

func TestOnlineMigrationTargetReceiptsAndClosure(t *testing.T) {
	t.Parallel()
	source := openSSTSnapshotTestStore(t, t.TempDir())
	session := startOnlineCaptureTest(t, source)
	target := openSSTSnapshotTestStore(t, t.TempDir())
	require.NoError(t, target.ApplyRaftEntry(t.Context(), 10, func(ctx context.Context) error {
		return PrepareOnlineMigrationTarget(ctx, target, session, 3, func([]byte) bool { return true })
	}))
	frame := captureOnlineTestWrite(t, source, 11, 20, "n", "new")
	state, err := ReadOnlineMigrationSession(t.Context(), source, 9)
	require.NoError(t, err)
	encoded, err := EncodeOnlineMigrationFrame(1, frame)
	require.NoError(t, err)
	cursor := binary.BigEndian.AppendUint64(nil, 1)
	opts := ImportVersionsOptions{JobID: 9, AppliedIndex: 11, BracketID: OnlineMigrationBracketID, BatchSeq: 1, Cursor: cursor, Versions: []MVCCVersion{{Key: []byte("staged/n"), Value: []byte("new"), CommitTS: 20}}, OnlineSequence: 1, OnlineDigest: sha256.Sum256(encoded), OnlineSourceIdentity: session.Identity, OnlineSourceIndex: 11}
	result, err := target.ImportVersionsRaft(t.Context(), opts)
	require.NoError(t, err)
	require.False(t, result.Duplicate)
	opts.AppliedIndex++
	result, err = target.ImportVersionsRaft(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, result.Duplicate)
	changed := opts
	changed.OnlineDigest = sha256.Sum256([]byte("changed"))
	_, err = target.ImportVersionsRaft(t.Context(), changed)
	require.ErrorIs(t, err, ErrMigrationCaptureConflict)
	gap := opts
	gap.OnlineSequence = 3
	gap.BatchSeq = 3
	_, err = target.ImportVersionsRaft(t.Context(), gap)
	require.ErrorIs(t, err, ErrImportBatchGap)
	state.Sealed = true
	require.NoError(t, target.ApplyRaftEntry(t.Context(), 13, func(ctx context.Context) error { return ReadyOnlineMigrationTarget(ctx, target, state, 3, 1) }))
	opts.AppliedIndex = 14
	_, err = target.ImportVersionsRaft(t.Context(), opts)
	require.ErrorIs(t, err, ErrOnlineMigrationTargetClosed)
	_, err = target.ImportVersionsRaft(t.Context(), ImportVersionsOptions{JobID: 9, AppliedIndex: 14, BracketID: 1, BatchSeq: 1})
	require.ErrorIs(t, err, ErrOnlineMigrationTargetClosed)
}

func TestOnlineMigrationTargetRejectsHistory(t *testing.T) {
	t.Parallel()
	for _, tombstone := range []bool{false, true} {
		t.Run(map[bool]string{false: "value", true: "tombstone"}[tombstone], func(t *testing.T) {
			source := openSSTSnapshotTestStore(t, t.TempDir())
			session := startOnlineCaptureTest(t, source)
			target := openSSTSnapshotTestStore(t, t.TempDir())
			if tombstone {
				require.NoError(t, target.DeleteAt(t.Context(), []byte("old"), 1))
			} else {
				require.NoError(t, target.PutAt(t.Context(), []byte("old"), []byte("v"), 1, 0))
			}
			err := target.ApplyRaftEntry(t.Context(), 10, func(ctx context.Context) error {
				return PrepareOnlineMigrationTarget(ctx, target, session, 3, func([]byte) bool { return true })
			})
			require.ErrorIs(t, err, ErrOnlineMigrationTargetNotEmpty)
		})
	}
}

func TestOnlineMigrationReadyReattestsCurrentConfiguration(t *testing.T) {
	t.Parallel()
	source := openSSTSnapshotTestStore(t, t.TempDir())
	session := startOnlineCaptureTest(t, source)
	target := openSSTSnapshotTestStore(t, t.TempDir())
	require.NoError(t, target.ApplyRaftEntry(t.Context(), 10, func(ctx context.Context) error {
		return PrepareOnlineMigrationTarget(ctx, target, session, 3, func([]byte) bool { return true })
	}))
	session.Sealed = true
	require.NoError(t, target.ApplyRaftEntry(t.Context(), 11, func(ctx context.Context) error { return ReadyOnlineMigrationTarget(ctx, target, session, 3, 0) }))
	require.NoError(t, target.ApplyRaftEntry(t.Context(), 12, func(ctx context.Context) error { return ReadyOnlineMigrationTarget(ctx, target, session, 4, 0) }))
	state, err := ReadOnlineMigrationTarget(t.Context(), target, 9)
	require.NoError(t, err)
	require.Equal(t, uint64(4), state.ConfigurationIndex)
	require.True(t, state.Ready)
	require.Zero(t, state.Sequence)
	require.Equal(t, session.Identity, state.Source.Identity)
}

func TestOnlineMigrationCaptureQuota(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		sequence uint64
		bytes    uint64
	}{
		{"last empty frame", onlineMigrationMaxFrames - 1, 1 << 20},
		{"one byte remaining", 1, onlineMigrationMaxBytes - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := openSSTSnapshotTestStore(t, t.TempDir())
			session := startOnlineCaptureTest(t, source)
			session.Sequence = tc.sequence
			session.LastIndex = session.StartIndex + tc.sequence
			session.Bytes = tc.bytes
			job := session.Identity.JobID
			index := session.LastIndex
			require.NoError(t, source.ApplyRaftEntry(t.Context(), index, func(tx context.Context) error {
				return stageOnlineJSON(tx, source, onlineMigrationKey("source", job, 0), session)
			}))

			if tc.sequence == onlineMigrationMaxFrames-1 {
				index++
				empty, err := CaptureOnlineMigrationEntry(t.Context(), source, job, index, sha256.Sum256([]byte("rejected")), nil, func(_ context.Context, writer *MigrationCaptureWriter) error {
					return writer.Request(func() (MigrationCaptureOutcome, error) { return MigrationCaptureRejected, nil })
				})
				require.NoError(t, err)
				require.True(t, empty.Complete)
				require.Empty(t, empty.Effects)
				session, err = ReadOnlineMigrationSession(t.Context(), source, job)
				require.NoError(t, err)
				require.Empty(t, session.Invalid)
				require.Equal(t, uint64(onlineMigrationMaxFrames), session.Sequence)
				last, err := ReadOnlineMigrationFrame(t.Context(), source, session, session.Sequence)
				require.NoError(t, err)
				require.Equal(t, empty, last)
			}
			index++
			entry := captureOnlineTestWrite(t, source, index, 20, "n", "acknowledged")
			require.Equal(t, []MigrationCaptureOutcome{MigrationCaptureApplied}, entry.Outcomes)
			invalid, err := ReadOnlineMigrationSession(t.Context(), source, job)
			require.NoError(t, err)
			require.Equal(t, "capture budget exhausted", invalid.Invalid)
			require.Equal(t, session.Sequence, invalid.Sequence)
			require.Equal(t, session.Bytes, invalid.Bytes)
			require.Equal(t, session.LastIndex, invalid.LastIndex)
			_, err = source.ReadRaftControlMetadata(t.Context(), onlineMigrationKey("frame", job, invalid.Sequence+1))
			require.ErrorIs(t, err, ErrKeyNotFound)
			applied, ok := raftEntryAppliedIndex(t, source)
			require.True(t, ok)
			require.Equal(t, index, applied)
			value, err := source.GetAt(t.Context(), []byte("n"), 20)
			require.NoError(t, err)
			require.Equal(t, []byte("acknowledged"), value)
		})
	}
}

func TestOnlineMigrationReplaysActualSameTimestampPostimage(t *testing.T) {
	t.Parallel()
	source := openSSTSnapshotTestStore(t, t.TempDir())
	session := startOnlineCaptureTest(t, source)
	digest := sha256.Sum256([]byte("two writes to n at 100"))
	frame, err := CaptureOnlineMigrationEntry(t.Context(), source, session.Identity.JobID, 11, digest, nil, func(tx context.Context, writer *MigrationCaptureWriter) error {
		for _, value := range []string{"first", "different"} {
			if err := writer.Request(func() (MigrationCaptureOutcome, error) {
				return MigrationCaptureApplied, source.ApplyMutationsRaftAt(tx, raftEntryPut("n", value), nil, 100, 100, 11)
			}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	value, err := source.GetAt(t.Context(), []byte("n"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), value)
	require.Len(t, frame.Effects, 1)
	require.Equal(t, value, frame.Effects[0].Value)
	require.Equal(t, uint64(100), frame.Effects[0].CommitTS)
	require.Equal(t, []MigrationCaptureOutcome{MigrationCaptureApplied, MigrationCaptureApplied}, frame.Outcomes)
	replayed, err := CaptureOnlineMigrationEntry(t.Context(), source, session.Identity.JobID, 11, digest, nil, func(context.Context, *MigrationCaptureWriter) error {
		t.Fatal("a replay must return its durable frame")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, frame, replayed)
	state, err := ReadOnlineMigrationSession(t.Context(), source, session.Identity.JobID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.Sequence)
}

func TestOnlineOwnershipGenerationLength(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		length int
		valid  bool
	}{{0, false}, {15, false}, {16, true}, {128, true}, {129, false}} {
		gen := string(bytes.Repeat([]byte("x"), tc.length))
		require.Equal(t, tc.valid, ValidOnlineOwnershipGeneration(gen, OnlineMigrationLineage(gen)), "length %d", tc.length)
	}
}

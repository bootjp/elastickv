package kv

import (
	"context"
	"crypto/sha256"
	"io"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func shadowCaptureConfig() MigrationCaptureShadowConfig {
	return MigrationCaptureShadowConfig{SourceGroupID: 1, JobID: 9, Generation: 1, Lineage: [32]byte{1}}
}

func shadowCaptureID(index uint64) store.MigrationCaptureIdentity {
	cfg := shadowCaptureConfig()
	return store.MigrationCaptureIdentity{SourceGroupID: cfg.SourceGroupID, JobID: cfg.JobID, Generation: cfg.Generation, Lineage: cfg.Lineage, Index: index}
}

func TestFSMShadowCaptureRealRawEntries(t *testing.T) {
	t.Parallel()
	for _, many := range []bool{false, true} {
		st := g17OpenStore(t, t.TempDir())
		observer := &shadowCaptureObserver{t: t, st: st}
		fsm := g17FSM(t, st, WithMigrationCaptureShadow(shadowCaptureConfig()), WithApplyObserver(observer))
		fsm.SetApplyIndex(11)
		requests := []*pb.Request{g17RawPut(100, "a", "first")}
		if many {
			requests = append(requests, g17RawPut(101, "b", "second"))
		}
		data := g17Entry(t, requests...)
		require.Nil(t, fsm.Apply(data))
		record, err := store.ReadMigrationCaptureEntry(t.Context(), st, shadowCaptureID(11))
		require.NoError(t, err)
		require.True(t, record.Complete)
		require.Len(t, record.Effects, len(requests))
		require.Equal(t, len(requests), observer.calls)
		fsm.SetApplyIndex(11)
		require.Nil(t, fsm.Apply(data), "a replay uses the stored result without another apply notice")
		require.Equal(t, len(requests), observer.calls)
	}
}

type shadowCaptureObserver struct {
	t     *testing.T
	st    store.MVCCStore
	calls int
}

func (o *shadowCaptureObserver) OnApply(_ pb.Op, key []byte) {
	record, err := store.ReadMigrationCaptureEntry(o.t.Context(), o.st, shadowCaptureID(11))
	require.NoError(o.t, err, "the journal must be committed before notifying observers")
	require.True(o.t, record.Complete)
	value, err := o.st.GetAt(o.t.Context(), key, 200)
	require.NoError(o.t, err)
	require.NotEmpty(o.t, value)
	o.calls++
}

func TestFSMShadowCaptureRetainsAdmissionGuard(t *testing.T) {
	t.Parallel()
	st := g17OpenStore(t, t.TempDir())
	writer, ok := st.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(t.Context(), store.TargetStagedReadinessState{
		JobID: 9, MigrationJobID: 9, Armed: true, SourceWriteFence: true,
		RouteStart: []byte("a"), RouteEnd: []byte("z"), MinWriteTSExclusive: 100, RetentionPinTS: 1,
	}))
	fsm := g17FSM(t, st, WithMigrationCaptureShadow(shadowCaptureConfig()))
	fsm.SetApplyIndex(11)
	data := g17Entry(t, g17RawPut(200, "a", "fenced"))
	response, ok := fsm.Apply(data).(error)
	require.True(t, ok)
	require.ErrorIs(t, response, ErrRouteCutoverPending)
	record, err := store.ReadMigrationCaptureEntry(t.Context(), st, shadowCaptureID(11))
	require.NoError(t, err)
	require.Empty(t, record.Effects)
	require.Equal(t, []store.MigrationCaptureOutcome{store.MigrationCaptureRejected}, record.Outcomes)
	fsm.SetApplyIndex(11)
	replayed, ok := fsm.Apply(data).(error)
	require.True(t, ok)
	require.ErrorIs(t, replayed, ErrTransactionOutcomeUnknown, "a coarse rejection receipt must never become a success")
}

func TestFSMShadowCaptureDisabledAndUnsupportedShape(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		st := g17OpenStore(t, t.TempDir())
		var options []FSMOption
		if enabled {
			options = append(options, WithMigrationCaptureShadow(shadowCaptureConfig()))
		}
		fsm := g17FSM(t, st, options...)
		fsm.SetApplyIndex(11)
		request := g17RawPut(100, "a", "ordinary")
		if enabled {
			request = g17RawDelPrefix(100, "prefix/")
		}
		require.Nil(t, fsm.Apply(g17Entry(t, request)))
		_, err := store.ReadMigrationCaptureEntry(t.Context(), st, shadowCaptureID(11))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

type shadowJournalFailure struct {
	store.MVCCStore
	store.RaftEntryApplier
	store.RaftControlMetadataStore
	applied interface{ LastAppliedIndex() (uint64, bool, error) }
}

func (s *shadowJournalFailure) LastAppliedIndex() (uint64, bool, error) {
	return s.applied.LastAppliedIndex()
}

func (s *shadowJournalFailure) StageRaftControlMetadata(context.Context, []byte, []byte) error {
	return io.ErrClosedPipe
}

func TestFSMShadowCaptureJournalFailureHaltsWithoutData(t *testing.T) {
	t.Parallel()
	st := g17OpenStore(t, t.TempDir())
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	index, ok := st.(interface{ LastAppliedIndex() (uint64, bool, error) })
	require.True(t, ok)
	wrapped := &shadowJournalFailure{MVCCStore: st, RaftEntryApplier: applier, RaftControlMetadataStore: metadata, applied: index}
	observer := &shadowCaptureObserver{t: t, st: st}
	fsm := g17FSM(t, wrapped, WithMigrationCaptureShadow(shadowCaptureConfig()), WithApplyObserver(observer))
	fsm.SetApplyIndex(11)
	response, ok := fsm.Apply(g17Entry(t, g17RawPut(100, "a", "not-committed"))).(*haltApplyResponse)
	require.True(t, ok)
	require.True(t, errors.Is(response.HaltApply(), io.ErrClosedPipe))
	require.Zero(t, observer.calls)
	_, err := st.GetAt(t.Context(), []byte("a"), 101)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = store.ReadMigrationCaptureEntry(t.Context(), st, shadowCaptureID(11))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestFSMShadowCaptureZeroTimestampKeepsNormalSemantics(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		st := g17OpenStore(t, t.TempDir())
		var options []FSMOption
		if enabled {
			options = append(options, WithMigrationCaptureShadow(shadowCaptureConfig()))
		}
		fsm := g17FSM(t, st, options...)
		fsm.SetApplyIndex(11)
		require.Nil(t, fsm.Apply(g17Entry(t, g17RawPut(0, "a", "zero-timestamp"))))
		value, err := st.GetAt(t.Context(), []byte("a"), 100)
		require.NoError(t, err)
		require.Equal(t, []byte("zero-timestamp"), value)
		_, err = store.ReadMigrationCaptureEntry(t.Context(), st, shadowCaptureID(11))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

func TestFSMShadowCaptureReplayRequiresAllOutcomes(t *testing.T) {
	t.Parallel()
	for _, many := range []bool{false, true} {
		st := g17OpenStore(t, t.TempDir())
		requests := []*pb.Request{g17RawPut(100, "a", "not-confirmed")}
		if many {
			requests = append(requests, g17RawPut(101, "b", "also-not-confirmed"))
		}
		data := g17Entry(t, requests...)
		_, err := store.CaptureMigrationRaftEntry(t.Context(), st, shadowCaptureID(11), sha256.Sum256(data), store.MigrationCaptureLimits{}, func(_ context.Context, writer *store.MigrationCaptureWriter) error {
			if many {
				return writer.Request(func() (store.MigrationCaptureOutcome, error) { return store.MigrationCaptureApplied, nil })
			}
			return nil
		})
		require.NoError(t, err)
		fsm := g17FSM(t, st, WithMigrationCaptureShadow(shadowCaptureConfig()))
		fsm.SetApplyIndex(11)
		halted, ok := fsm.Apply(data).(*haltApplyResponse)
		require.True(t, ok)
		require.ErrorIs(t, halted.HaltApply(), store.ErrMigrationCaptureCorrupt)
		_, err = st.GetAt(t.Context(), []byte("a"), 102)
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

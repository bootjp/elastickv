package distribution

import (
	"context"
	"math"
	"reflect"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

func TestCatalogNextSplitJobIDCodecRoundTrip(t *testing.T) {
	raw := EncodeCatalogNextSplitJobID(42)
	got, err := DecodeCatalogNextSplitJobID(raw)
	if err != nil {
		t.Fatalf("decode next split job id: %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestCatalogNextSplitJobIDCodecRejectsInvalidPayload(t *testing.T) {
	if _, err := DecodeCatalogNextSplitJobID(nil); !errors.Is(err, ErrCatalogInvalidNextSplitJobID) {
		t.Fatalf("expected ErrCatalogInvalidNextSplitJobID, got %v", err)
	}
	if _, err := DecodeCatalogNextSplitJobID(EncodeCatalogVersion(0)); !errors.Is(err, ErrCatalogInvalidNextSplitJobID) {
		t.Fatalf("expected ErrCatalogInvalidNextSplitJobID, got %v", err)
	}
}

func TestCatalogSplitJobKeyHelpers(t *testing.T) {
	key := CatalogSplitJobKey(11)
	if !IsCatalogSplitJobKey(key) {
		t.Fatal("expected split job key prefix match")
	}
	id, ok := CatalogSplitJobIDFromKey(key)
	if !ok {
		t.Fatal("expected split job id parse to succeed")
	}
	if id != 11 {
		t.Fatalf("expected split job id 11, got %d", id)
	}
	if _, ok := CatalogSplitJobIDFromKey([]byte("not-a-job-key")); ok {
		t.Fatal("expected parse failure for non-job key")
	}
	if _, ok := CatalogSplitJobIDFromKey([]byte(catalogSplitJobPrefix + "short")); ok {
		t.Fatal("expected parse failure for short job key")
	}
}

func TestCatalogSplitJobHistoryKeyHelpers(t *testing.T) {
	key := CatalogSplitJobHistoryKey(12345, 99)
	if !IsCatalogSplitJobHistoryKey(key) {
		t.Fatal("expected split job history key prefix match")
	}
	terminalAtMs, jobID, ok := CatalogSplitJobHistoryKeyParts(key)
	if !ok {
		t.Fatal("expected split job history key parse to succeed")
	}
	if terminalAtMs != 12345 || jobID != 99 {
		t.Fatalf("expected terminal/job (12345,99), got (%d,%d)", terminalAtMs, jobID)
	}
	if _, _, ok := CatalogSplitJobHistoryKeyParts([]byte("not-a-history-key")); ok {
		t.Fatal("expected parse failure for non-history key")
	}
}

func TestSplitJobCodecRoundTrip(t *testing.T) {
	want := sampleSplitJob(7)
	raw, err := EncodeSplitJob(want)
	if err != nil {
		t.Fatalf("encode split job: %v", err)
	}
	got, err := DecodeSplitJob(raw)
	if err != nil {
		t.Fatalf("decode split job: %v", err)
	}
	assertSplitJobEqual(t, want, got)
}

func TestSplitJobCodecRejectsInvalidVersion(t *testing.T) {
	job := sampleSplitJob(1)
	raw, err := EncodeSplitJob(job)
	if err != nil {
		t.Fatalf("encode split job: %v", err)
	}
	raw[0] = catalogJobCodecVersion + 1
	if _, err := DecodeSplitJob(raw); !errors.Is(err, ErrCatalogInvalidSplitJobRecord) {
		t.Fatalf("expected ErrCatalogInvalidSplitJobRecord, got %v", err)
	}
}

func TestSplitJobCodecRejectsInvalidPhase(t *testing.T) {
	job := sampleSplitJob(1)
	job.Phase = SplitJobPhaseNone
	if _, err := EncodeSplitJob(job); !errors.Is(err, ErrCatalogInvalidSplitJobPhase) {
		t.Fatalf("expected ErrCatalogInvalidSplitJobPhase, got %v", err)
	}
}

func TestSplitJobCodecValidatesRestartPhases(t *testing.T) {
	for _, phase := range []SplitJobPhase{
		SplitJobPhaseBackfill,
		SplitJobPhaseFence,
		SplitJobPhaseDeltaCopy,
		SplitJobPhaseCutover,
		SplitJobPhaseCleanup,
	} {
		job := sampleSplitJob(uint64(phase) + 10)
		job.Phase = SplitJobPhaseFailed
		job.RetryPhase = phase
		if _, err := EncodeSplitJob(job); err != nil {
			t.Fatalf("expected failed job with retry phase %v to encode: %v", phase, err)
		}
	}

	for _, phase := range []SplitJobPhase{
		SplitJobPhaseBackfill,
		SplitJobPhaseFence,
		SplitJobPhaseDeltaCopy,
	} {
		job := sampleSplitJob(uint64(phase) + 20)
		job.Phase = SplitJobPhaseAbandoning
		job.AbandonFromPhase = phase
		if _, err := EncodeSplitJob(job); err != nil {
			t.Fatalf("expected abandoning job from phase %v to encode: %v", phase, err)
		}
	}
}

func TestSplitJobCodecRejectsInvalidRestartPhases(t *testing.T) {
	for _, tc := range []struct {
		name             string
		phase            SplitJobPhase
		retryPhase       SplitJobPhase
		abandonFromPhase SplitJobPhase
	}{
		{
			name:       "retry phase outside failed",
			phase:      SplitJobPhaseDeltaCopy,
			retryPhase: SplitJobPhaseBackfill,
		},
		{
			name:       "failed without retry phase",
			phase:      SplitJobPhaseFailed,
			retryPhase: SplitJobPhaseNone,
		},
		{
			name:       "failed retry to planned",
			phase:      SplitJobPhaseFailed,
			retryPhase: SplitJobPhasePlanned,
		},
		{
			name:       "failed retry to terminal",
			phase:      SplitJobPhaseFailed,
			retryPhase: SplitJobPhaseDone,
		},
		{
			name:             "abandon phase outside abandoning",
			phase:            SplitJobPhaseDeltaCopy,
			abandonFromPhase: SplitJobPhaseFence,
		},
		{
			name:             "abandoning without source phase",
			phase:            SplitJobPhaseAbandoning,
			abandonFromPhase: SplitJobPhaseNone,
		},
		{
			name:             "abandoning from cutover",
			phase:            SplitJobPhaseAbandoning,
			abandonFromPhase: SplitJobPhaseCutover,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := sampleSplitJob(2)
			job.Phase = tc.phase
			job.RetryPhase = tc.retryPhase
			job.AbandonFromPhase = tc.abandonFromPhase
			if _, err := EncodeSplitJob(job); !errors.Is(err, ErrCatalogInvalidSplitJobPhase) {
				t.Fatalf("expected ErrCatalogInvalidSplitJobPhase, got %v", err)
			}
		})
	}
}

func TestSplitJobCodecRejectsDuplicateBracketIDs(t *testing.T) {
	job := sampleSplitJob(1)
	dupe := job.BracketProgress[0]
	dupe.Family++
	dupe.Cursor = []byte("other-cursor")
	job.BracketProgress = append(job.BracketProgress, dupe)
	if _, err := EncodeSplitJob(job); !errors.Is(err, ErrCatalogInvalidSplitJobRecord) {
		t.Fatalf("expected ErrCatalogInvalidSplitJobRecord, got %v", err)
	}
}

func TestSplitJobCodecRejectsUnknownProtoEnumValues(t *testing.T) {
	body, err := gproto.MarshalOptions{Deterministic: true}.Marshal(&pb.SplitJob{
		JobId:         1,
		SourceRouteId: 2,
		TargetGroupId: 3,
		Phase:         pb.SplitJobPhase(300),
	})
	if err != nil {
		t.Fatalf("marshal invalid split job proto: %v", err)
	}
	raw := append([]byte{catalogJobCodecVersion}, body...)
	if _, err := DecodeSplitJob(raw); !errors.Is(err, ErrCatalogInvalidSplitJobPhase) {
		t.Fatalf("expected ErrCatalogInvalidSplitJobPhase, got %v", err)
	}

	body, err = gproto.MarshalOptions{Deterministic: true}.Marshal(&pb.SplitJob{
		JobId:                 1,
		SourceRouteId:         2,
		TargetGroupId:         3,
		Phase:                 pb.SplitJobPhase_SPLIT_JOB_PHASE_PLANNED,
		CutoverReadFenceState: pb.SplitJobBarrierState(300),
		BracketProgress:       []*pb.SplitJobBracketProgress{{ExportPhase: pb.SplitJobExportPhase_SPLIT_JOB_EXPORT_PHASE_BACKFILL}},
	})
	if err != nil {
		t.Fatalf("marshal invalid barrier proto: %v", err)
	}
	raw = append([]byte{catalogJobCodecVersion}, body...)
	if _, err := DecodeSplitJob(raw); !errors.Is(err, ErrCatalogInvalidSplitJobBarrierState) {
		t.Fatalf("expected ErrCatalogInvalidSplitJobBarrierState, got %v", err)
	}

	body, err = gproto.MarshalOptions{Deterministic: true}.Marshal(&pb.SplitJob{
		JobId:         1,
		SourceRouteId: 2,
		TargetGroupId: 3,
		Phase:         pb.SplitJobPhase_SPLIT_JOB_PHASE_PLANNED,
		BracketProgress: []*pb.SplitJobBracketProgress{{
			ExportPhase: pb.SplitJobExportPhase(300),
		}},
	})
	if err != nil {
		t.Fatalf("marshal invalid export phase proto: %v", err)
	}
	raw = append([]byte{catalogJobCodecVersion}, body...)
	if _, err := DecodeSplitJob(raw); !errors.Is(err, ErrCatalogInvalidSplitJobExportPhase) {
		t.Fatalf("expected ErrCatalogInvalidSplitJobExportPhase, got %v", err)
	}
}

func TestCatalogStoreNextSplitJobID_DefaultsToOne(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	next, err := cs.NextSplitJobID(context.Background())
	if err != nil {
		t.Fatalf("next split job id: %v", err)
	}
	if next != 1 {
		t.Fatalf("expected next split job id 1, got %d", next)
	}
}

func TestCatalogStoreSaveAndLoadSplitJob(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(7)

	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	got, found, err := cs.SplitJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("load split job: %v", err)
	}
	if !found {
		t.Fatal("expected split job to be found")
	}
	assertSplitJobEqual(t, job, got)

	next, err := cs.NextSplitJobID(ctx)
	if err != nil {
		t.Fatalf("next split job id: %v", err)
	}
	if next != 8 {
		t.Fatalf("expected next split job id 8, got %d", next)
	}
}

func TestCatalogStoreCreateSplitJobRejectsExistingJob(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(7)

	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	if err := cs.CreateSplitJob(ctx, job); !errors.Is(err, ErrCatalogSplitJobConflict) {
		t.Fatalf("expected ErrCatalogSplitJobConflict, got %v", err)
	}
}

func TestCatalogStoreSaveSplitJobRejectsStaleExpectedJob(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(8)

	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	expected, found, err := cs.SplitJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("load split job: %v", err)
	}
	if !found {
		t.Fatal("expected split job to be found")
	}

	advanced := expected
	advanced.Phase = SplitJobPhaseCutover
	advanced.Cursor = []byte("advanced")
	advanced.UpdatedAtMs++
	if err := cs.SaveSplitJob(ctx, expected, advanced); err != nil {
		t.Fatalf("save advanced split job: %v", err)
	}

	stale := expected
	stale.Phase = SplitJobPhaseBackfill
	stale.Cursor = []byte("stale")
	stale.UpdatedAtMs++
	if err := cs.SaveSplitJob(ctx, expected, stale); !errors.Is(err, ErrCatalogSplitJobConflict) {
		t.Fatalf("expected ErrCatalogSplitJobConflict, got %v", err)
	}
	got, found, err := cs.SplitJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("reload split job: %v", err)
	}
	if !found {
		t.Fatal("expected split job to remain found")
	}
	assertSplitJobEqual(t, advanced, got)
}

func TestCatalogStoreRetrySplitJobUsesDurableRetryPhase(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(18)
	job.Phase = SplitJobPhaseFailed
	job.RetryPhase = SplitJobPhaseDeltaCopy
	job.LastError = "transient"

	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	retried, err := cs.RetrySplitJob(ctx, job.JobID, 1200)
	if err != nil {
		t.Fatalf("retry split job: %v", err)
	}
	if retried.Phase != SplitJobPhaseDeltaCopy || retried.RetryPhase != SplitJobPhaseNone {
		t.Fatalf("unexpected retry transition: %+v", retried)
	}
	if retried.AbandonFromPhase != SplitJobPhaseNone || retried.LastError != "" || retried.UpdatedAtMs != 1200 {
		t.Fatalf("unexpected retry witness cleanup: %+v", retried)
	}
	got, found, err := cs.SplitJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("load retried split job: %v", err)
	}
	if !found {
		t.Fatal("expected retried split job")
	}
	assertSplitJobEqual(t, retried, got)
}

func TestCatalogStoreRetrySplitJobRejectsMissingRetryWitness(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(19)
	job.Phase = SplitJobPhaseFailed
	job.RetryPhase = SplitJobPhaseNone

	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	if _, err := cs.RetrySplitJob(ctx, job.JobID, 1200); !errors.Is(err, ErrCatalogSplitJobCannotRetry) {
		t.Fatalf("expected ErrCatalogSplitJobCannotRetry, got %v", err)
	}
}

func TestCatalogStoreBeginSplitJobAbandonRecordsPreCutoverPhase(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(20)
	job.Phase = SplitJobPhaseFailed
	job.RetryPhase = SplitJobPhaseFence
	job.AbandonFromPhase = SplitJobPhaseNone

	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	abandoning, err := cs.BeginSplitJobAbandon(ctx, job.JobID, 1300)
	if err != nil {
		t.Fatalf("begin split job abandon: %v", err)
	}
	if abandoning.Phase != SplitJobPhaseAbandoning ||
		abandoning.RetryPhase != SplitJobPhaseNone ||
		abandoning.AbandonFromPhase != SplitJobPhaseFence ||
		abandoning.UpdatedAtMs != 1300 {
		t.Fatalf("unexpected abandon transition: %+v", abandoning)
	}
	got, found, err := cs.SplitJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("load abandoning split job: %v", err)
	}
	if !found {
		t.Fatal("expected abandoning split job")
	}
	assertSplitJobEqual(t, abandoning, got)
}

func TestCatalogStoreBeginSplitJobAbandonRejectsPostCutoverPhases(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(21)
	job.Phase = SplitJobPhaseCleanup
	job.RetryPhase = SplitJobPhaseNone

	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	if _, err := cs.BeginSplitJobAbandon(ctx, job.JobID, 1300); !errors.Is(err, ErrCatalogSplitJobCannotAbandon) {
		t.Fatalf("expected ErrCatalogSplitJobCannotAbandon, got %v", err)
	}
}

func TestCatalogStoreListSplitJobsIncludesLiveAndHistory(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	live := sampleSplitJob(2)
	history := sampleSplitJob(1)
	history.Phase = SplitJobPhaseDone
	history.TerminalAtMs = 1000

	if err := cs.CreateSplitJob(ctx, live); err != nil {
		t.Fatalf("create live split job: %v", err)
	}
	if err := cs.CreateSplitJob(ctx, history); err != nil {
		t.Fatalf("create terminal split job: %v", err)
	}
	if err := cs.MoveSplitJobToHistory(ctx, history, history); err != nil {
		t.Fatalf("move split job to history: %v", err)
	}

	jobs, err := cs.ListSplitJobs(ctx)
	if err != nil {
		t.Fatalf("list split jobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	assertSplitJobEqual(t, live, jobs[0])
	assertSplitJobEqual(t, history, jobs[1])

	got, found, err := cs.SplitJob(ctx, history.JobID)
	if err != nil {
		t.Fatalf("load history split job: %v", err)
	}
	if !found {
		t.Fatal("expected history split job to be found by id")
	}
	assertSplitJobEqual(t, history, got)
}

func TestCatalogStoreMoveSplitJobToHistoryIsIdempotentByJobID(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(12)

	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	terminal := job
	terminal.Phase = SplitJobPhaseDone
	terminal.TerminalAtMs = 1000
	if err := cs.MoveSplitJobToHistory(ctx, job, terminal); err != nil {
		t.Fatalf("move split job to history: %v", err)
	}

	retryTerminal := terminal
	retryTerminal.TerminalAtMs = 2000
	retryTerminal.LastError = "rebuilt terminal record"
	if err := cs.MoveSplitJobToHistory(ctx, job, retryTerminal); err != nil {
		t.Fatalf("retry move split job to history: %v", err)
	}

	jobs, err := cs.ListSplitJobs(ctx)
	if err != nil {
		t.Fatalf("list split jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one history job, got %d", len(jobs))
	}
	assertSplitJobEqual(t, terminal, jobs[0])
}

func TestCatalogStoreMoveSplitJobToHistoryRejectsStaleExpectedJob(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(13)

	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	advanced := job
	advanced.Phase = SplitJobPhaseFence
	advanced.Cursor = []byte("advanced")
	advanced.UpdatedAtMs++
	if err := cs.SaveSplitJob(ctx, job, advanced); err != nil {
		t.Fatalf("save advanced split job: %v", err)
	}

	terminal := job
	terminal.Phase = SplitJobPhaseDone
	terminal.TerminalAtMs = 1000
	if err := cs.MoveSplitJobToHistory(ctx, job, terminal); !errors.Is(err, ErrCatalogSplitJobConflict) {
		t.Fatalf("expected ErrCatalogSplitJobConflict, got %v", err)
	}
	got, found, err := cs.SplitJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("reload split job: %v", err)
	}
	if !found {
		t.Fatal("expected split job to remain live")
	}
	assertSplitJobEqual(t, advanced, got)
}

func TestCatalogStoreMoveSplitJobToHistoryRejectsNonTerminal(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	job := sampleSplitJob(1)
	if err := cs.MoveSplitJobToHistory(context.Background(), job, job); !errors.Is(err, ErrCatalogSplitJobTerminalRequired) {
		t.Fatalf("expected ErrCatalogSplitJobTerminalRequired, got %v", err)
	}
}

func TestCatalogStoreDeleteSplitJob(t *testing.T) {
	cs := NewCatalogStore(store.NewMVCCStore())
	ctx := context.Background()
	job := sampleSplitJob(3)
	if err := cs.CreateSplitJob(ctx, job); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	if err := cs.DeleteSplitJob(ctx, job.JobID); err != nil {
		t.Fatalf("delete split job: %v", err)
	}
	if _, found, err := cs.SplitJob(ctx, job.JobID); err != nil {
		t.Fatalf("load deleted split job: %v", err)
	} else if found {
		t.Fatal("expected deleted split job to be absent")
	}
	next, err := cs.NextSplitJobID(ctx)
	if err != nil {
		t.Fatalf("next split job id: %v", err)
	}
	if next != 4 {
		t.Fatalf("expected next split job id to remain 4, got %d", next)
	}
}

func TestCatalogStoreSaveSeedsNextSplitJobIDFromExistingJobsWhenMetaMissing(t *testing.T) {
	st := store.NewMVCCStore()
	cs := NewCatalogStore(st)
	ctx := context.Background()

	live := sampleSplitJob(50)
	liveRaw, err := EncodeSplitJob(live)
	if err != nil {
		t.Fatalf("encode live split job: %v", err)
	}
	history := sampleSplitJob(80)
	history.Phase = SplitJobPhaseAbandoned
	history.TerminalAtMs = 5000
	historyRaw, err := EncodeSplitJob(history)
	if err != nil {
		t.Fatalf("encode history split job: %v", err)
	}
	if err := st.PutAt(ctx, CatalogSplitJobKey(live.JobID), liveRaw, 7, 0); err != nil {
		t.Fatalf("put live split job: %v", err)
	}
	if err := st.PutAt(ctx, CatalogSplitJobHistoryKey(history.TerminalAtMs, history.JobID), historyRaw, 8, 0); err != nil {
		t.Fatalf("put history split job: %v", err)
	}

	if err := cs.CreateSplitJob(ctx, sampleSplitJob(10)); err != nil {
		t.Fatalf("create split job: %v", err)
	}
	next, err := cs.NextSplitJobID(ctx)
	if err != nil {
		t.Fatalf("next split job id: %v", err)
	}
	if next != 81 {
		t.Fatalf("expected next split job id 81, got %d", next)
	}
}

func TestCatalogStoreBuildSplitJobPutMutationsSkipsCurrentNextSplitJobID(t *testing.T) {
	st := store.NewMVCCStore()
	cs := NewCatalogStore(st)
	ctx := context.Background()

	for _, jobID := range []uint64{1, 2} {
		if err := cs.CreateSplitJob(ctx, sampleSplitJob(jobID)); err != nil {
			t.Fatalf("create split job %d: %v", jobID, err)
		}
	}
	updated := sampleSplitJob(1)
	updated.Phase = SplitJobPhaseBackfill
	updated.UpdatedAtMs++
	raw, err := EncodeSplitJob(updated)
	if err != nil {
		t.Fatalf("encode updated split job: %v", err)
	}

	mutations, err := cs.buildSplitJobPutMutations(ctx, st.LastCommitTS(), CatalogSplitJobKey(updated.JobID), raw, updated.JobID)
	if err != nil {
		t.Fatalf("build split job mutations: %v", err)
	}
	if len(mutations) != 1 {
		t.Fatalf("expected only job mutation, got %d", len(mutations))
	}
	if string(mutations[0].Key) != string(CatalogSplitJobKey(updated.JobID)) {
		t.Fatalf("expected job mutation, got key %q", mutations[0].Key)
	}
}

func TestCatalogStoreNextSplitJobIDAt_FallsBackWhenMetaMissing(t *testing.T) {
	st := store.NewMVCCStore()
	cs := NewCatalogStore(st)
	ctx := context.Background()

	live := sampleSplitJob(10)
	liveRaw, err := EncodeSplitJob(live)
	if err != nil {
		t.Fatalf("encode live split job: %v", err)
	}
	history := sampleSplitJob(20)
	history.Phase = SplitJobPhaseAbandoned
	history.TerminalAtMs = 5000
	historyRaw, err := EncodeSplitJob(history)
	if err != nil {
		t.Fatalf("encode history split job: %v", err)
	}

	const ts = uint64(7)
	if err := st.PutAt(ctx, CatalogSplitJobKey(live.JobID), liveRaw, ts, 0); err != nil {
		t.Fatalf("put live split job: %v", err)
	}
	if err := st.PutAt(ctx, CatalogSplitJobHistoryKey(history.TerminalAtMs, history.JobID), historyRaw, ts, 0); err != nil {
		t.Fatalf("put history split job: %v", err)
	}

	nextAt, err := cs.NextSplitJobIDAt(ctx, ts)
	if err != nil {
		t.Fatalf("next split job id at ts: %v", err)
	}
	if nextAt != 21 {
		t.Fatalf("expected next split job id 21, got %d", nextAt)
	}
}

func TestNextSplitJobIDFloorRejectsOverflow(t *testing.T) {
	_, err := NextSplitJobIDFloor([]SplitJob{{JobID: math.MaxUint64}})
	if !errors.Is(err, ErrCatalogSplitJobIDOverflow) {
		t.Fatalf("expected ErrCatalogSplitJobIDOverflow, got %v", err)
	}
}

func sampleSplitJob(jobID uint64) SplitJob {
	return SplitJob{
		JobID:                            jobID,
		SourceRouteID:                    11,
		SplitKey:                         []byte("split-key"),
		TargetGroupID:                    22,
		Phase:                            SplitJobPhaseDeltaCopy,
		RetryPhase:                       SplitJobPhaseNone,
		AbandonFromPhase:                 SplitJobPhaseNone,
		SnapshotTS:                       101,
		SnapshotMinAdmittedTS:            102,
		WriteTrackerArmed:                true,
		DeltaFloor:                       103,
		PostFenceDrainCompleted:          true,
		FenceTS:                          104,
		CutoverVersion:                   105,
		CutoverReadFenceState:            SplitJobBarrierArmed,
		TargetStagedReadinessState:       SplitJobBarrierArming,
		SourceCutoverReadFenceAckCursor:  []byte("source-read-fence"),
		TargetStagedReadinessAckCursor:   []byte("target-ready"),
		Cursor:                           []byte("cursor"),
		MaxImportedTS:                    106,
		TargetPromotionDone:              true,
		PromotionCompletedTS:             107,
		FenceCatalogVersion:              108,
		FenceAckCursor:                   []byte("fence-ack"),
		SourceCutoverAckCursor:           []byte("source-cutover"),
		SourceReadDrainCursor:            []byte("source-drain"),
		TargetClearedDescriptorAckCursor: []byte("target-clear"),
		BracketProgress: []SplitJobBracketProgress{
			{
				BracketID:         31,
				Family:            2,
				ExportPhase:       SplitJobExportPhaseDeltaCopy,
				Cursor:            []byte("bracket-cursor"),
				Done:              true,
				ScannedBytes:      4096,
				AcceptedRows:      128,
				LastAckedBatchSeq: 9,
			},
		},
		SourceRetentionPinTS: 109,
		LastError:            "last error",
		StartedAtMs:          10000,
		UpdatedAtMs:          20000,
	}
}

func assertSplitJobEqual(t *testing.T, want, got SplitJob) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("split job mismatch:\nwant=%+v\ngot=%+v", want, got)
	}
}

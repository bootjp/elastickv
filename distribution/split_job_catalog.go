package distribution

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"sort"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

const (
	catalogNextSplitJobIDKey     = catalogMetaPrefix + "next_job_id"
	catalogSplitJobPrefix        = "!dist|job|"
	catalogSplitJobHistoryPrefix = "!dist|jobhist|"

	catalogJobCodecVersion byte = 1
)

var (
	ErrCatalogSplitJobIDRequired          = errors.New("catalog split job id is required")
	ErrCatalogSplitJobSourceRouteRequired = errors.New("catalog split job source route id is required")
	ErrCatalogSplitJobTargetGroupRequired = errors.New("catalog split job target group id is required")
	ErrCatalogInvalidSplitJobRecord       = errors.New("catalog split job record is invalid")
	ErrCatalogInvalidSplitJobPhase        = errors.New("catalog split job phase is invalid")
	ErrCatalogInvalidSplitJobBarrierState = errors.New("catalog split job barrier state is invalid")
	ErrCatalogInvalidSplitJobExportPhase  = errors.New("catalog split job export phase is invalid")
	ErrCatalogInvalidSplitJobKey          = errors.New("catalog split job key is invalid")
	ErrCatalogInvalidNextSplitJobID       = errors.New("catalog next split job id record is invalid")
	ErrCatalogSplitJobIDOverflow          = errors.New("catalog split job id overflow")
	ErrCatalogSplitJobKeyIDMismatch       = errors.New("catalog split job key and record job id mismatch")
	ErrCatalogSplitJobConflict            = errors.New("catalog split job conflict")
	ErrCatalogSplitJobTerminalRequired    = errors.New("catalog split job terminal state is required")
	ErrSplitJobOverlap                    = errors.New("split job overlaps requested route")
	ErrTooManyInFlightSplitJobs           = errors.New("too many in-flight split jobs")
)

// SplitJobPhase is the durable phase of a split migration job.
type SplitJobPhase byte

const (
	SplitJobPhaseNone SplitJobPhase = iota
	SplitJobPhasePlanned
	SplitJobPhaseBackfill
	SplitJobPhaseFence
	SplitJobPhaseDeltaCopy
	SplitJobPhaseCutover
	SplitJobPhaseCleanup
	SplitJobPhaseDone
	SplitJobPhaseFailed
	SplitJobPhaseAbandoning
	SplitJobPhaseAbandoned
)

func (p SplitJobPhase) valid() bool {
	switch p {
	case SplitJobPhaseNone,
		SplitJobPhasePlanned,
		SplitJobPhaseBackfill,
		SplitJobPhaseFence,
		SplitJobPhaseDeltaCopy,
		SplitJobPhaseCutover,
		SplitJobPhaseCleanup,
		SplitJobPhaseDone,
		SplitJobPhaseFailed,
		SplitJobPhaseAbandoning,
		SplitJobPhaseAbandoned:
		return true
	default:
		return false
	}
}

func (p SplitJobPhase) terminal() bool {
	return p == SplitJobPhaseDone || p == SplitJobPhaseAbandoned
}

// SplitJobBarrierState is a restart witness for per-voter barrier progress.
type SplitJobBarrierState byte

const (
	SplitJobBarrierNone SplitJobBarrierState = iota
	SplitJobBarrierArming
	SplitJobBarrierArmed
	SplitJobBarrierClearing
)

func (s SplitJobBarrierState) valid() bool {
	switch s {
	case SplitJobBarrierNone,
		SplitJobBarrierArming,
		SplitJobBarrierArmed,
		SplitJobBarrierClearing:
		return true
	default:
		return false
	}
}

// SplitJobExportPhase records which export window a bracket is scanning.
type SplitJobExportPhase byte

const (
	SplitJobExportPhaseNone SplitJobExportPhase = iota
	SplitJobExportPhaseBackfill
	SplitJobExportPhaseDeltaCopy
)

func (p SplitJobExportPhase) valid() bool {
	switch p {
	case SplitJobExportPhaseNone,
		SplitJobExportPhaseBackfill,
		SplitJobExportPhaseDeltaCopy:
		return true
	default:
		return false
	}
}

// SplitJobBracketProgress is per-family export-bracket resume state.
type SplitJobBracketProgress struct {
	BracketID         uint64
	Family            uint32
	ExportPhase       SplitJobExportPhase
	Cursor            []byte
	Done              bool
	ScannedBytes      uint64
	AcceptedRows      uint64
	LastAckedBatchSeq uint64
}

// SplitJob is the durable default-group state for a range split migration.
type SplitJob struct {
	JobID                            uint64
	SourceRouteID                    uint64
	SplitKey                         []byte
	TargetGroupID                    uint64
	Phase                            SplitJobPhase
	RetryPhase                       SplitJobPhase
	AbandonFromPhase                 SplitJobPhase
	SnapshotTS                       uint64
	SnapshotMinAdmittedTS            uint64
	WriteTrackerArmed                bool
	DeltaFloor                       uint64
	PostFenceDrainCompleted          bool
	FenceTS                          uint64
	CutoverVersion                   uint64
	CutoverReadFenceState            SplitJobBarrierState
	TargetStagedReadinessState       SplitJobBarrierState
	SourceCutoverReadFenceAckCursor  []byte
	TargetStagedReadinessAckCursor   []byte
	Cursor                           []byte
	MaxImportedTS                    uint64
	TargetPromotionDone              bool
	PromotionCompletedTS             uint64
	FenceCatalogVersion              uint64
	FenceAckCursor                   []byte
	SourceCutoverAckCursor           []byte
	SourceReadDrainCursor            []byte
	TargetClearedDescriptorAckCursor []byte
	BracketProgress                  []SplitJobBracketProgress
	SourceRetentionPinTS             uint64
	LastError                        string
	StartedAtMs                      int64
	UpdatedAtMs                      int64
	TerminalAtMs                     int64
	CapabilityRegressed              bool
}

// CatalogNextSplitJobIDKey returns the reserved key used for split-job ID allocation.
func CatalogNextSplitJobIDKey() []byte {
	return []byte(catalogNextSplitJobIDKey)
}

// CatalogSplitJobKey returns the reserved live-job key for a split job.
func CatalogSplitJobKey(jobID uint64) []byte {
	key := make([]byte, len(catalogSplitJobPrefix)+catalogUint64Bytes)
	copy(key, []byte(catalogSplitJobPrefix))
	binary.BigEndian.PutUint64(key[len(catalogSplitJobPrefix):], jobID)
	return key
}

// IsCatalogSplitJobKey reports whether key belongs to the live split-job namespace.
func IsCatalogSplitJobKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(catalogSplitJobPrefix))
}

// CatalogSplitJobIDFromKey parses the job ID from a live split-job key.
func CatalogSplitJobIDFromKey(key []byte) (uint64, bool) {
	if !IsCatalogSplitJobKey(key) {
		return 0, false
	}
	suffix := key[len(catalogSplitJobPrefix):]
	if len(suffix) != catalogUint64Bytes {
		return 0, false
	}
	return binary.BigEndian.Uint64(suffix), true
}

// CatalogSplitJobHistoryKey returns the reserved history key for a terminal split job.
func CatalogSplitJobHistoryKey(terminalAtMs int64, jobID uint64) []byte {
	key := make([]byte, len(catalogSplitJobHistoryPrefix)+catalogUint64Bytes+catalogUint64Bytes)
	copy(key, []byte(catalogSplitJobHistoryPrefix))
	if terminalAtMs < 0 {
		terminalAtMs = 0
	}
	off := len(catalogSplitJobHistoryPrefix)
	binary.BigEndian.PutUint64(key[off:], uint64(terminalAtMs)) //nolint:gosec // terminalAtMs is clamped non-negative above.
	binary.BigEndian.PutUint64(key[off+catalogUint64Bytes:], jobID)
	return key
}

// IsCatalogSplitJobHistoryKey reports whether key belongs to the split-job history namespace.
func IsCatalogSplitJobHistoryKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(catalogSplitJobHistoryPrefix))
}

// CatalogSplitJobHistoryKeyParts parses terminal time and job ID from a history key.
func CatalogSplitJobHistoryKeyParts(key []byte) (int64, uint64, bool) {
	if !IsCatalogSplitJobHistoryKey(key) {
		return 0, 0, false
	}
	suffix := key[len(catalogSplitJobHistoryPrefix):]
	if len(suffix) != 2*catalogUint64Bytes {
		return 0, 0, false
	}
	terminalAtRaw := binary.BigEndian.Uint64(suffix[:catalogUint64Bytes])
	if terminalAtRaw > uint64(math.MaxInt64) {
		return 0, 0, false
	}
	terminalAtMs := int64(terminalAtRaw)
	jobID := binary.BigEndian.Uint64(suffix[catalogUint64Bytes:])
	return terminalAtMs, jobID, true
}

// EncodeCatalogNextSplitJobID serializes a next split-job ID record.
func EncodeCatalogNextSplitJobID(nextJobID uint64) []byte {
	return EncodeCatalogVersion(nextJobID)
}

// DecodeCatalogNextSplitJobID deserializes a next split-job ID record.
func DecodeCatalogNextSplitJobID(raw []byte) (uint64, error) {
	nextJobID, err := DecodeCatalogVersion(raw)
	if err != nil {
		return 0, errors.Wrapf(ErrCatalogInvalidNextSplitJobID, "decode next split job id: %v", err)
	}
	if nextJobID == 0 {
		return 0, errors.WithStack(ErrCatalogInvalidNextSplitJobID)
	}
	return nextJobID, nil
}

// EncodeSplitJob serializes a SplitJob record as version byte + protobuf body.
func EncodeSplitJob(job SplitJob) ([]byte, error) {
	if err := validateSplitJob(job); err != nil {
		return nil, err
	}
	body, err := gproto.MarshalOptions{Deterministic: true}.Marshal(splitJobToProto(job))
	if err != nil {
		return nil, errors.Wrapf(ErrCatalogInvalidSplitJobRecord, "marshal split job: %v", err)
	}
	if len(body) > math.MaxInt-1 {
		return nil, errors.Wrap(ErrCatalogInvalidSplitJobRecord, "encoded split job exceeds maximum slice length")
	}
	out := make([]byte, len(body)+1)
	out[0] = catalogJobCodecVersion
	copy(out[1:], body)
	return out, nil
}

// DecodeSplitJob deserializes a SplitJob record.
func DecodeSplitJob(raw []byte) (SplitJob, error) {
	if len(raw) < 1 {
		return SplitJob{}, errors.WithStack(ErrCatalogInvalidSplitJobRecord)
	}
	if raw[0] != catalogJobCodecVersion {
		return SplitJob{}, errors.Wrapf(ErrCatalogInvalidSplitJobRecord, "unsupported version %d", raw[0])
	}
	var msg pb.SplitJob
	if err := gproto.Unmarshal(raw[1:], &msg); err != nil {
		return SplitJob{}, errors.Wrapf(ErrCatalogInvalidSplitJobRecord, "unmarshal split job: %v", err)
	}
	job, err := splitJobFromProto(&msg)
	if err != nil {
		return SplitJob{}, err
	}
	if err := validateSplitJob(job); err != nil {
		return SplitJob{}, err
	}
	return job, nil
}

// NextSplitJobID reads the next split-job ID counter from catalog metadata.
func (s *CatalogStore) NextSplitJobID(ctx context.Context) (uint64, error) {
	if err := ensureCatalogStore(s); err != nil {
		return 0, err
	}
	ctx = contextOrBackground(ctx)
	return s.NextSplitJobIDAt(ctx, s.store.LastCommitTS())
}

// NextSplitJobIDAt reads the next split-job ID counter at a given snapshot timestamp.
func (s *CatalogStore) NextSplitJobIDAt(ctx context.Context, ts uint64) (uint64, error) {
	if err := ensureCatalogStore(s); err != nil {
		return 0, err
	}
	ctx = contextOrBackground(ctx)
	nextJobID, err := s.nextSplitJobIDAt(ctx, ts)
	if err != nil {
		return 0, err
	}
	if nextJobID != 0 {
		return nextJobID, nil
	}
	jobs, err := s.ListSplitJobsAt(ctx, ts)
	if err != nil {
		return 0, err
	}
	return NextSplitJobIDFloor(jobs)
}

// SplitJob reads a live or history split job by ID at the latest timestamp.
func (s *CatalogStore) SplitJob(ctx context.Context, jobID uint64) (SplitJob, bool, error) {
	if err := ensureCatalogStore(s); err != nil {
		return SplitJob{}, false, err
	}
	ctx = contextOrBackground(ctx)
	return s.SplitJobAt(ctx, jobID, s.store.LastCommitTS())
}

// SplitJobAt reads a live or history split job by ID at a snapshot timestamp.
func (s *CatalogStore) SplitJobAt(ctx context.Context, jobID uint64, ts uint64) (SplitJob, bool, error) {
	if err := ensureCatalogStore(s); err != nil {
		return SplitJob{}, false, err
	}
	if jobID == 0 {
		return SplitJob{}, false, errors.WithStack(ErrCatalogSplitJobIDRequired)
	}
	ctx = contextOrBackground(ctx)
	job, found, err := s.liveSplitJobAt(ctx, jobID, ts)
	if err != nil || found {
		return job, found, err
	}
	return s.historySplitJobAt(ctx, jobID, ts)
}

// ListSplitJobs returns live and history split jobs at the latest timestamp.
func (s *CatalogStore) ListSplitJobs(ctx context.Context) ([]SplitJob, error) {
	if err := ensureCatalogStore(s); err != nil {
		return nil, err
	}
	ctx = contextOrBackground(ctx)
	return s.ListSplitJobsAt(ctx, s.store.LastCommitTS())
}

// ListSplitJobsAt returns live and history split jobs at a snapshot timestamp.
func (s *CatalogStore) ListSplitJobsAt(ctx context.Context, ts uint64) ([]SplitJob, error) {
	if err := ensureCatalogStore(s); err != nil {
		return nil, err
	}
	ctx = contextOrBackground(ctx)
	liveJobs, err := s.liveSplitJobsAt(ctx, ts)
	if err != nil {
		return nil, err
	}
	historyJobs, err := s.historySplitJobsAt(ctx, ts)
	if err != nil {
		return nil, err
	}
	out := make([]SplitJob, 0, len(liveJobs)+len(historyJobs))
	out = append(out, liveJobs...)
	out = append(out, historyJobs...)
	sortSplitJobs(out)
	return out, nil
}

// CreateSplitJob creates a live split job and advances next_job_id when needed.
func (s *CatalogStore) CreateSplitJob(ctx context.Context, job SplitJob) error {
	if err := ensureCatalogStore(s); err != nil {
		return err
	}
	ctx = contextOrBackground(ctx)
	encoded, err := EncodeSplitJob(job)
	if err != nil {
		return err
	}
	readTS := s.store.LastCommitTS()
	if err := s.expectNoSplitJobAt(ctx, job.JobID, readTS); err != nil {
		return err
	}
	mutations, err := s.buildSplitJobPutMutations(ctx, readTS, CatalogSplitJobKey(job.JobID), encoded, job.JobID)
	if err != nil {
		return err
	}
	return s.applySplitJobMutations(ctx, readTS, [][]byte{CatalogSplitJobKey(job.JobID)}, mutations)
}

// SaveSplitJob updates a live split job if it still matches expected.
func (s *CatalogStore) SaveSplitJob(ctx context.Context, expected SplitJob, job SplitJob) error {
	if err := ensureCatalogStore(s); err != nil {
		return err
	}
	if expected.JobID != job.JobID {
		return errors.WithStack(ErrCatalogSplitJobKeyIDMismatch)
	}
	ctx = contextOrBackground(ctx)
	expectedRaw, err := EncodeSplitJob(expected)
	if err != nil {
		return err
	}
	encoded, err := EncodeSplitJob(job)
	if err != nil {
		return err
	}
	readTS := s.store.LastCommitTS()
	if err := s.expectLiveSplitJobAt(ctx, job.JobID, expectedRaw, readTS); err != nil {
		return err
	}
	if _, found, err := s.historySplitJobAt(ctx, job.JobID, readTS); err != nil {
		return err
	} else if found {
		return errors.WithStack(ErrCatalogSplitJobConflict)
	}
	mutations, err := s.buildSplitJobPutMutations(ctx, readTS, CatalogSplitJobKey(job.JobID), encoded, job.JobID)
	if err != nil {
		return err
	}
	return s.applySplitJobMutations(ctx, readTS, [][]byte{CatalogSplitJobKey(job.JobID)}, mutations)
}

// MoveSplitJobToHistory moves a terminal split job from live state to history.
func (s *CatalogStore) MoveSplitJobToHistory(ctx context.Context, expected SplitJob, job SplitJob) error {
	if err := ensureCatalogStore(s); err != nil {
		return err
	}
	if err := validateSplitJobHistoryMove(expected, job); err != nil {
		return err
	}
	ctx = contextOrBackground(ctx)
	expectedRaw, err := EncodeSplitJob(expected)
	if err != nil {
		return err
	}
	encoded, err := EncodeSplitJob(job)
	if err != nil {
		return err
	}
	readTS := s.store.LastCommitTS()
	if applied, err := s.splitJobHistoryMoveApplied(ctx, job.JobID, readTS); err != nil || applied {
		return err
	}
	if err := s.expectLiveSplitJobAt(ctx, job.JobID, expectedRaw, readTS); err != nil {
		return err
	}
	historyKey := CatalogSplitJobHistoryKey(job.TerminalAtMs, job.JobID)
	mutations, err := s.buildSplitJobPutMutations(ctx, readTS, historyKey, encoded, job.JobID)
	if err != nil {
		return err
	}
	mutations = append(mutations, &store.KVPairMutation{
		Op:  store.OpTypeDelete,
		Key: CatalogSplitJobKey(job.JobID),
	})
	return s.applySplitJobMutations(ctx, readTS, [][]byte{CatalogSplitJobKey(job.JobID)}, mutations)
}

func validateSplitJobHistoryMove(expected SplitJob, job SplitJob) error {
	if expected.JobID != job.JobID {
		return errors.WithStack(ErrCatalogSplitJobKeyIDMismatch)
	}
	if !job.Phase.terminal() || job.TerminalAtMs <= 0 {
		return errors.WithStack(ErrCatalogSplitJobTerminalRequired)
	}
	return nil
}

func (s *CatalogStore) splitJobHistoryMoveApplied(ctx context.Context, jobID uint64, ts uint64) (bool, error) {
	if _, found, err := s.historySplitJobAt(ctx, jobID, ts); err != nil {
		return false, err
	} else if !found {
		return false, nil
	}
	if _, liveFound, err := s.liveSplitJobAt(ctx, jobID, ts); err != nil {
		return false, err
	} else if liveFound {
		return false, errors.WithStack(ErrCatalogSplitJobConflict)
	}
	return true, nil
}

// DeleteSplitJob deletes a live split job.
func (s *CatalogStore) DeleteSplitJob(ctx context.Context, jobID uint64) error {
	if err := ensureCatalogStore(s); err != nil {
		return err
	}
	if jobID == 0 {
		return errors.WithStack(ErrCatalogSplitJobIDRequired)
	}
	ctx = contextOrBackground(ctx)
	readTS := s.store.LastCommitTS()
	return s.applySplitJobMutations(ctx, readTS, [][]byte{CatalogSplitJobKey(jobID)}, []*store.KVPairMutation{{
		Op:  store.OpTypeDelete,
		Key: CatalogSplitJobKey(jobID),
	}})
}

// NextSplitJobIDFloor returns the minimum valid next split-job ID for jobs.
func NextSplitJobIDFloor(jobs []SplitJob) (uint64, error) {
	maxJobID := uint64(0)
	for _, job := range jobs {
		if job.JobID > maxJobID {
			maxJobID = job.JobID
		}
	}
	if maxJobID == math.MaxUint64 {
		return 0, errors.WithStack(ErrCatalogSplitJobIDOverflow)
	}
	return maxJobID + 1, nil
}

func validateSplitJob(job SplitJob) error {
	if job.JobID == 0 {
		return errors.WithStack(ErrCatalogSplitJobIDRequired)
	}
	if job.SourceRouteID == 0 {
		return errors.WithStack(ErrCatalogSplitJobSourceRouteRequired)
	}
	if job.TargetGroupID == 0 {
		return errors.WithStack(ErrCatalogSplitJobTargetGroupRequired)
	}
	if err := validateSplitJobPhases(job); err != nil {
		return err
	}
	if err := validateSplitJobBarriers(job); err != nil {
		return err
	}
	return validateSplitJobBracketProgress(job.BracketProgress)
}

func validateSplitJobPhases(job SplitJob) error {
	if job.Phase == SplitJobPhaseNone || !job.Phase.valid() {
		return errors.WithStack(ErrCatalogInvalidSplitJobPhase)
	}
	if !job.RetryPhase.valid() || !job.AbandonFromPhase.valid() {
		return errors.WithStack(ErrCatalogInvalidSplitJobPhase)
	}
	if !job.retryPhaseValidForPhase() || !job.abandonFromPhaseValidForPhase() {
		return errors.WithStack(ErrCatalogInvalidSplitJobPhase)
	}
	return nil
}

func (job SplitJob) retryPhaseValidForPhase() bool {
	if job.Phase == SplitJobPhaseFailed {
		return job.RetryPhase.retryable()
	}
	return job.RetryPhase == SplitJobPhaseNone
}

func (job SplitJob) abandonFromPhaseValidForPhase() bool {
	if job.Phase == SplitJobPhaseAbandoning {
		return job.AbandonFromPhase.abandonable()
	}
	return job.AbandonFromPhase == SplitJobPhaseNone
}

func (p SplitJobPhase) retryable() bool {
	switch p {
	case SplitJobPhaseBackfill,
		SplitJobPhaseFence,
		SplitJobPhaseDeltaCopy,
		SplitJobPhaseCutover,
		SplitJobPhaseCleanup:
		return true
	case SplitJobPhaseNone,
		SplitJobPhasePlanned,
		SplitJobPhaseDone,
		SplitJobPhaseFailed,
		SplitJobPhaseAbandoning,
		SplitJobPhaseAbandoned:
		return false
	default:
		return false
	}
}

func (p SplitJobPhase) abandonable() bool {
	switch p {
	case SplitJobPhasePlanned,
		SplitJobPhaseBackfill,
		SplitJobPhaseFence,
		SplitJobPhaseDeltaCopy:
		return true
	case SplitJobPhaseNone,
		SplitJobPhaseCutover,
		SplitJobPhaseCleanup,
		SplitJobPhaseDone,
		SplitJobPhaseFailed,
		SplitJobPhaseAbandoning,
		SplitJobPhaseAbandoned:
		return false
	default:
		return false
	}
}

func validateSplitJobBarriers(job SplitJob) error {
	if !job.CutoverReadFenceState.valid() || !job.TargetStagedReadinessState.valid() {
		return errors.WithStack(ErrCatalogInvalidSplitJobBarrierState)
	}
	return nil
}

func validateSplitJobBracketProgress(progress []SplitJobBracketProgress) error {
	seen := make(map[uint64]struct{}, len(progress))
	for _, p := range progress {
		if !p.ExportPhase.valid() {
			return errors.WithStack(ErrCatalogInvalidSplitJobExportPhase)
		}
		if _, ok := seen[p.BracketID]; ok {
			return errors.WithStack(ErrCatalogInvalidSplitJobRecord)
		}
		seen[p.BracketID] = struct{}{}
	}
	return nil
}

func (s *CatalogStore) nextSplitJobIDAt(ctx context.Context, ts uint64) (uint64, error) {
	raw, err := s.store.GetAt(ctx, CatalogNextSplitJobIDKey(), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	nextJobID, err := DecodeCatalogNextSplitJobID(raw)
	if err != nil {
		return 0, err
	}
	return nextJobID, nil
}

func (s *CatalogStore) liveSplitJobAt(ctx context.Context, jobID uint64, ts uint64) (SplitJob, bool, error) {
	raw, err := s.store.GetAt(ctx, CatalogSplitJobKey(jobID), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return SplitJob{}, false, nil
		}
		return SplitJob{}, false, errors.WithStack(err)
	}
	job, err := DecodeSplitJob(raw)
	if err != nil {
		return SplitJob{}, false, err
	}
	if job.JobID != jobID {
		return SplitJob{}, false, errors.WithStack(ErrCatalogSplitJobKeyIDMismatch)
	}
	return job, true, nil
}

func (s *CatalogStore) historySplitJobAt(ctx context.Context, jobID uint64, ts uint64) (SplitJob, bool, error) {
	jobs, err := s.historySplitJobsAt(ctx, ts)
	if err != nil {
		return SplitJob{}, false, err
	}
	for _, job := range jobs {
		if job.JobID == jobID {
			return job, true, nil
		}
	}
	return SplitJob{}, false, nil
}

func (s *CatalogStore) liveSplitJobsAt(ctx context.Context, ts uint64) ([]SplitJob, error) {
	entries, err := s.scanCatalogEntriesAt(ctx, []byte(catalogSplitJobPrefix), ts)
	if err != nil {
		return nil, err
	}
	out := make([]SplitJob, 0, len(entries))
	seen := make(map[uint64]struct{}, len(entries))
	for _, kvp := range entries {
		jobID, ok := CatalogSplitJobIDFromKey(kvp.Key)
		if !ok {
			return nil, errors.WithStack(ErrCatalogInvalidSplitJobKey)
		}
		job, err := DecodeSplitJob(kvp.Value)
		if err != nil {
			return nil, err
		}
		if job.JobID != jobID {
			return nil, errors.WithStack(ErrCatalogSplitJobKeyIDMismatch)
		}
		if _, exists := seen[job.JobID]; exists {
			return nil, errors.WithStack(ErrCatalogSplitJobKeyIDMismatch)
		}
		seen[job.JobID] = struct{}{}
		out = append(out, job)
	}
	sortSplitJobs(out)
	return out, nil
}

func (s *CatalogStore) historySplitJobsAt(ctx context.Context, ts uint64) ([]SplitJob, error) {
	entries, err := s.scanCatalogEntriesAt(ctx, []byte(catalogSplitJobHistoryPrefix), ts)
	if err != nil {
		return nil, err
	}
	out := make([]SplitJob, 0, len(entries))
	for _, kvp := range entries {
		terminalAtMs, jobID, ok := CatalogSplitJobHistoryKeyParts(kvp.Key)
		if !ok {
			return nil, errors.WithStack(ErrCatalogInvalidSplitJobKey)
		}
		job, err := DecodeSplitJob(kvp.Value)
		if err != nil {
			return nil, err
		}
		if job.JobID != jobID || job.TerminalAtMs != terminalAtMs {
			return nil, errors.WithStack(ErrCatalogSplitJobKeyIDMismatch)
		}
		out = append(out, job)
	}
	sortSplitJobs(out)
	return out, nil
}

func (s *CatalogStore) scanCatalogEntriesAt(ctx context.Context, prefix []byte, ts uint64) ([]*store.KVPair, error) {
	upper := prefixScanEnd(prefix)
	cursor := CloneBytes(prefix)
	out := make([]*store.KVPair, 0)

	for {
		page, err := s.store.ScanAt(ctx, cursor, upper, catalogScanPageSize, ts)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(page) == 0 {
			break
		}

		var (
			lastKey []byte
			done    bool
		)
		out, lastKey, done = appendRoutePage(out, page, prefix)
		if done {
			return out, nil
		}
		if lastKey == nil || len(page) < catalogScanPageSize {
			break
		}
		nextCursor, inRange := nextCursorWithinUpper(lastKey, upper)
		if !inRange {
			break
		}
		cursor = nextCursor
	}
	return out, nil
}

func (s *CatalogStore) expectNoSplitJobAt(ctx context.Context, jobID uint64, ts uint64) error {
	if _, found, err := s.liveSplitJobAt(ctx, jobID, ts); err != nil {
		return err
	} else if found {
		return errors.WithStack(ErrCatalogSplitJobConflict)
	}
	if _, found, err := s.historySplitJobAt(ctx, jobID, ts); err != nil {
		return err
	} else if found {
		return errors.WithStack(ErrCatalogSplitJobConflict)
	}
	return nil
}

func (s *CatalogStore) expectLiveSplitJobAt(ctx context.Context, jobID uint64, expectedRaw []byte, ts uint64) error {
	raw, err := s.store.GetAt(ctx, CatalogSplitJobKey(jobID), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return errors.WithStack(ErrCatalogSplitJobConflict)
		}
		return errors.WithStack(err)
	}
	if !bytes.Equal(raw, expectedRaw) {
		return errors.WithStack(ErrCatalogSplitJobConflict)
	}
	return nil
}

func (s *CatalogStore) buildSplitJobPutMutations(ctx context.Context, readTS uint64, key []byte, encoded []byte, jobID uint64) ([]*store.KVPairMutation, error) {
	if jobID == math.MaxUint64 {
		return nil, errors.WithStack(ErrCatalogSplitJobIDOverflow)
	}
	nextJobID, err := s.splitJobNextIDAdvanceAt(ctx, readTS, jobID)
	if err != nil {
		return nil, err
	}

	mutations := []*store.KVPairMutation{{
		Op:    store.OpTypePut,
		Key:   CloneBytes(key),
		Value: CloneBytes(encoded),
	}}
	if nextJobID != 0 {
		mutations = append(mutations, &store.KVPairMutation{
			Op:    store.OpTypePut,
			Key:   CatalogNextSplitJobIDKey(),
			Value: EncodeCatalogNextSplitJobID(nextJobID),
		})
	}
	return mutations, nil
}

func (s *CatalogStore) splitJobNextIDAdvanceAt(ctx context.Context, ts uint64, jobID uint64) (uint64, error) {
	nextJobID, err := s.nextSplitJobIDAt(ctx, ts)
	if err != nil {
		return 0, err
	}
	jobs, err := s.ListSplitJobsAt(ctx, ts)
	if err != nil {
		return 0, err
	}
	floor, err := NextSplitJobIDFloor(jobs)
	if err != nil {
		return 0, err
	}
	if jobID+1 > floor {
		floor = jobID + 1
	}
	if nextJobID == 0 || nextJobID < floor {
		return floor, nil
	}
	return 0, nil
}

func (s *CatalogStore) applySplitJobMutations(ctx context.Context, readTS uint64, readKeys [][]byte, mutations []*store.KVPairMutation) error {
	minCommitTS := readTS + 1
	if minCommitTS == 0 {
		return errors.WithStack(ErrCatalogVersionOverflow)
	}
	commitTS, err := s.commitTSForApply(minCommitTS)
	if err != nil {
		return err
	}
	if err := s.store.ApplyMutations(ctx, mutations, readKeys, readTS, commitTS); err != nil {
		if errors.Is(err, store.ErrWriteConflict) {
			return errors.WithStack(ErrCatalogSplitJobConflict)
		}
		return errors.WithStack(err)
	}
	return nil
}

func sortSplitJobs(jobs []SplitJob) {
	sort.Slice(jobs, func(i, j int) bool {
		return splitJobLess(jobs[i], jobs[j])
	})
}

func splitJobLess(left, right SplitJob) bool {
	leftTerminal := left.TerminalAtMs > 0
	rightTerminal := right.TerminalAtMs > 0
	if leftTerminal != rightTerminal {
		return !leftTerminal
	}
	if left.TerminalAtMs != right.TerminalAtMs {
		return left.TerminalAtMs < right.TerminalAtMs
	}
	return left.JobID < right.JobID
}

func CloneSplitJob(job SplitJob) SplitJob {
	return SplitJob{
		JobID:                            job.JobID,
		SourceRouteID:                    job.SourceRouteID,
		SplitKey:                         CloneBytes(job.SplitKey),
		TargetGroupID:                    job.TargetGroupID,
		Phase:                            job.Phase,
		RetryPhase:                       job.RetryPhase,
		AbandonFromPhase:                 job.AbandonFromPhase,
		SnapshotTS:                       job.SnapshotTS,
		SnapshotMinAdmittedTS:            job.SnapshotMinAdmittedTS,
		WriteTrackerArmed:                job.WriteTrackerArmed,
		DeltaFloor:                       job.DeltaFloor,
		PostFenceDrainCompleted:          job.PostFenceDrainCompleted,
		FenceTS:                          job.FenceTS,
		CutoverVersion:                   job.CutoverVersion,
		CutoverReadFenceState:            job.CutoverReadFenceState,
		TargetStagedReadinessState:       job.TargetStagedReadinessState,
		SourceCutoverReadFenceAckCursor:  CloneBytes(job.SourceCutoverReadFenceAckCursor),
		TargetStagedReadinessAckCursor:   CloneBytes(job.TargetStagedReadinessAckCursor),
		Cursor:                           CloneBytes(job.Cursor),
		MaxImportedTS:                    job.MaxImportedTS,
		TargetPromotionDone:              job.TargetPromotionDone,
		PromotionCompletedTS:             job.PromotionCompletedTS,
		FenceCatalogVersion:              job.FenceCatalogVersion,
		FenceAckCursor:                   CloneBytes(job.FenceAckCursor),
		SourceCutoverAckCursor:           CloneBytes(job.SourceCutoverAckCursor),
		SourceReadDrainCursor:            CloneBytes(job.SourceReadDrainCursor),
		TargetClearedDescriptorAckCursor: CloneBytes(job.TargetClearedDescriptorAckCursor),
		BracketProgress:                  cloneSplitJobBracketProgress(job.BracketProgress),
		SourceRetentionPinTS:             job.SourceRetentionPinTS,
		LastError:                        job.LastError,
		StartedAtMs:                      job.StartedAtMs,
		UpdatedAtMs:                      job.UpdatedAtMs,
		TerminalAtMs:                     job.TerminalAtMs,
		CapabilityRegressed:              job.CapabilityRegressed,
	}
}

// SplitJobToProto converts a catalog SplitJob into its wire representation.
func SplitJobToProto(job SplitJob) *pb.SplitJob {
	return splitJobToProto(job)
}

func splitJobToProto(job SplitJob) *pb.SplitJob {
	job = CloneSplitJob(job)
	return &pb.SplitJob{
		JobId:                            job.JobID,
		SourceRouteId:                    job.SourceRouteID,
		SplitKey:                         job.SplitKey,
		TargetGroupId:                    job.TargetGroupID,
		Phase:                            pb.SplitJobPhase(job.Phase),
		RetryPhase:                       pb.SplitJobPhase(job.RetryPhase),
		AbandonFromPhase:                 pb.SplitJobPhase(job.AbandonFromPhase),
		SnapshotTs:                       job.SnapshotTS,
		SnapshotMinAdmittedTs:            job.SnapshotMinAdmittedTS,
		WriteTrackerArmed:                job.WriteTrackerArmed,
		DeltaFloor:                       job.DeltaFloor,
		PostFenceDrainCompleted:          job.PostFenceDrainCompleted,
		FenceTs:                          job.FenceTS,
		CutoverVersion:                   job.CutoverVersion,
		CutoverReadFenceState:            pb.SplitJobBarrierState(job.CutoverReadFenceState),
		TargetStagedReadinessState:       pb.SplitJobBarrierState(job.TargetStagedReadinessState),
		SourceCutoverReadFenceAckCursor:  job.SourceCutoverReadFenceAckCursor,
		TargetStagedReadinessAckCursor:   job.TargetStagedReadinessAckCursor,
		Cursor:                           job.Cursor,
		MaxImportedTs:                    job.MaxImportedTS,
		TargetPromotionDone:              job.TargetPromotionDone,
		PromotionCompletedTs:             job.PromotionCompletedTS,
		FenceCatalogVersion:              job.FenceCatalogVersion,
		FenceAckCursor:                   job.FenceAckCursor,
		SourceCutoverAckCursor:           job.SourceCutoverAckCursor,
		SourceReadDrainCursor:            job.SourceReadDrainCursor,
		TargetClearedDescriptorAckCursor: job.TargetClearedDescriptorAckCursor,
		BracketProgress:                  splitJobBracketProgressToProto(job.BracketProgress),
		SourceRetentionPinTs:             job.SourceRetentionPinTS,
		LastError:                        job.LastError,
		StartedAtMs:                      job.StartedAtMs,
		UpdatedAtMs:                      job.UpdatedAtMs,
		TerminalAtMs:                     job.TerminalAtMs,
		CapabilityRegressed:              job.CapabilityRegressed,
	}
}

func splitJobFromProto(msg *pb.SplitJob) (SplitJob, error) {
	if msg == nil {
		return SplitJob{}, nil
	}
	phase, err := splitJobPhaseFromProto(msg.GetPhase())
	if err != nil {
		return SplitJob{}, err
	}
	retryPhase, err := splitJobPhaseFromProto(msg.GetRetryPhase())
	if err != nil {
		return SplitJob{}, err
	}
	abandonFromPhase, err := splitJobPhaseFromProto(msg.GetAbandonFromPhase())
	if err != nil {
		return SplitJob{}, err
	}
	cutoverReadFenceState, err := splitJobBarrierStateFromProto(msg.GetCutoverReadFenceState())
	if err != nil {
		return SplitJob{}, err
	}
	targetStagedReadinessState, err := splitJobBarrierStateFromProto(msg.GetTargetStagedReadinessState())
	if err != nil {
		return SplitJob{}, err
	}
	bracketProgress, err := splitJobBracketProgressFromProto(msg.GetBracketProgress())
	if err != nil {
		return SplitJob{}, err
	}
	return SplitJob{
		JobID:                            msg.GetJobId(),
		SourceRouteID:                    msg.GetSourceRouteId(),
		SplitKey:                         CloneBytes(msg.GetSplitKey()),
		TargetGroupID:                    msg.GetTargetGroupId(),
		Phase:                            phase,
		RetryPhase:                       retryPhase,
		AbandonFromPhase:                 abandonFromPhase,
		SnapshotTS:                       msg.GetSnapshotTs(),
		SnapshotMinAdmittedTS:            msg.GetSnapshotMinAdmittedTs(),
		WriteTrackerArmed:                msg.GetWriteTrackerArmed(),
		DeltaFloor:                       msg.GetDeltaFloor(),
		PostFenceDrainCompleted:          msg.GetPostFenceDrainCompleted(),
		FenceTS:                          msg.GetFenceTs(),
		CutoverVersion:                   msg.GetCutoverVersion(),
		CutoverReadFenceState:            cutoverReadFenceState,
		TargetStagedReadinessState:       targetStagedReadinessState,
		SourceCutoverReadFenceAckCursor:  CloneBytes(msg.GetSourceCutoverReadFenceAckCursor()),
		TargetStagedReadinessAckCursor:   CloneBytes(msg.GetTargetStagedReadinessAckCursor()),
		Cursor:                           CloneBytes(msg.GetCursor()),
		MaxImportedTS:                    msg.GetMaxImportedTs(),
		TargetPromotionDone:              msg.GetTargetPromotionDone(),
		PromotionCompletedTS:             msg.GetPromotionCompletedTs(),
		FenceCatalogVersion:              msg.GetFenceCatalogVersion(),
		FenceAckCursor:                   CloneBytes(msg.GetFenceAckCursor()),
		SourceCutoverAckCursor:           CloneBytes(msg.GetSourceCutoverAckCursor()),
		SourceReadDrainCursor:            CloneBytes(msg.GetSourceReadDrainCursor()),
		TargetClearedDescriptorAckCursor: CloneBytes(msg.GetTargetClearedDescriptorAckCursor()),
		BracketProgress:                  bracketProgress,
		SourceRetentionPinTS:             msg.GetSourceRetentionPinTs(),
		LastError:                        msg.GetLastError(),
		StartedAtMs:                      msg.GetStartedAtMs(),
		UpdatedAtMs:                      msg.GetUpdatedAtMs(),
		TerminalAtMs:                     msg.GetTerminalAtMs(),
		CapabilityRegressed:              msg.GetCapabilityRegressed(),
	}, nil
}

func splitJobBracketProgressToProto(progress []SplitJobBracketProgress) []*pb.SplitJobBracketProgress {
	if len(progress) == 0 {
		return nil
	}
	out := make([]*pb.SplitJobBracketProgress, 0, len(progress))
	for _, p := range progress {
		out = append(out, &pb.SplitJobBracketProgress{
			BracketId:         p.BracketID,
			Family:            p.Family,
			ExportPhase:       pb.SplitJobExportPhase(p.ExportPhase),
			Cursor:            CloneBytes(p.Cursor),
			Done:              p.Done,
			ScannedBytes:      p.ScannedBytes,
			AcceptedRows:      p.AcceptedRows,
			LastAckedBatchSeq: p.LastAckedBatchSeq,
		})
	}
	return out
}

func splitJobBracketProgressFromProto(progress []*pb.SplitJobBracketProgress) ([]SplitJobBracketProgress, error) {
	if len(progress) == 0 {
		return nil, nil
	}
	out := make([]SplitJobBracketProgress, 0, len(progress))
	for _, p := range progress {
		if p == nil {
			continue
		}
		exportPhase, err := splitJobExportPhaseFromProto(p.GetExportPhase())
		if err != nil {
			return nil, err
		}
		out = append(out, SplitJobBracketProgress{
			BracketID:         p.GetBracketId(),
			Family:            p.GetFamily(),
			ExportPhase:       exportPhase,
			Cursor:            CloneBytes(p.GetCursor()),
			Done:              p.GetDone(),
			ScannedBytes:      p.GetScannedBytes(),
			AcceptedRows:      p.GetAcceptedRows(),
			LastAckedBatchSeq: p.GetLastAckedBatchSeq(),
		})
	}
	return out, nil
}

func splitJobPhaseFromProto(p pb.SplitJobPhase) (SplitJobPhase, error) {
	switch p {
	case pb.SplitJobPhase_SPLIT_JOB_PHASE_NONE,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_PLANNED,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_BACKFILL,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_FENCE,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_DELTA_COPY,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_CUTOVER,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_CLEANUP,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_DONE,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_FAILED,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_ABANDONING,
		pb.SplitJobPhase_SPLIT_JOB_PHASE_ABANDONED:
		return SplitJobPhase(p), nil
	default:
		return 0, errors.WithStack(ErrCatalogInvalidSplitJobPhase)
	}
}

func splitJobBarrierStateFromProto(s pb.SplitJobBarrierState) (SplitJobBarrierState, error) {
	switch s {
	case pb.SplitJobBarrierState_SPLIT_JOB_BARRIER_STATE_NONE,
		pb.SplitJobBarrierState_SPLIT_JOB_BARRIER_STATE_ARMING,
		pb.SplitJobBarrierState_SPLIT_JOB_BARRIER_STATE_ARMED,
		pb.SplitJobBarrierState_SPLIT_JOB_BARRIER_STATE_CLEARING:
		return SplitJobBarrierState(s), nil
	default:
		return 0, errors.WithStack(ErrCatalogInvalidSplitJobBarrierState)
	}
}

func splitJobExportPhaseFromProto(p pb.SplitJobExportPhase) (SplitJobExportPhase, error) {
	switch p {
	case pb.SplitJobExportPhase_SPLIT_JOB_EXPORT_PHASE_NONE,
		pb.SplitJobExportPhase_SPLIT_JOB_EXPORT_PHASE_BACKFILL,
		pb.SplitJobExportPhase_SPLIT_JOB_EXPORT_PHASE_DELTA_COPY:
		return SplitJobExportPhase(p), nil
	default:
		return 0, errors.WithStack(ErrCatalogInvalidSplitJobExportPhase)
	}
}

func cloneSplitJobBracketProgress(progress []SplitJobBracketProgress) []SplitJobBracketProgress {
	if len(progress) == 0 {
		return nil
	}
	out := make([]SplitJobBracketProgress, len(progress))
	for i := range progress {
		out[i] = progress[i]
		out[i].Cursor = CloneBytes(progress[i].Cursor)
	}
	return out
}

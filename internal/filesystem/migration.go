package filesystem

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	movePageSize                  = 128
	moveProgressExtraElemCount    = 2
	moveProgressExtraReadKeyCount = 3
)

type MovePhase string

const (
	MovePhaseTargetCleanup MovePhase = "target_cleanup"
	MovePhaseCopy          MovePhase = "copy"
	MovePhaseSwitch        MovePhase = "switch"
	MovePhaseSourceCleanup MovePhase = "source_cleanup"
	MovePhaseCompleted     MovePhase = "completed"
)

type IntentKind string

const (
	IntentKindCreate IntentKind = "create"
	IntentKindDelete IntentKind = "delete"
	IntentKindMove   IntentKind = "move"
)

type IntentState struct {
	ID        []byte     `json:"id"`
	Kind      IntentKind `json:"kind"`
	Phase     string     `json:"phase"`
	Inode     uint64     `json:"inode,omitempty"`
	Parent    uint64     `json:"parent,omitempty"`
	Name      []byte     `json:"name,omitempty"`
	JobID     []byte     `json:"job_id,omitempty"`
	CreatedAt int64      `json:"created_at_nsec"`
	UpdatedAt int64      `json:"updated_at_nsec"`
}

type MoveJob struct {
	ID             []byte    `json:"id"`
	Inode          uint64    `json:"inode"`
	SourceHome     uint64    `json:"source_home"`
	TargetHome     uint64    `json:"target_home"`
	TargetGroup    uint64    `json:"target_group"`
	MigrationEpoch uint64    `json:"migration_epoch"`
	SwitchEpoch    uint64    `json:"switch_epoch,omitempty"`
	Phase          MovePhase `json:"phase"`
	Cursor         []byte    `json:"cursor,omitempty"`
	CopiedChunks   uint64    `json:"copied_chunks,omitempty"`
	CleanedChunks  uint64    `json:"cleaned_chunks,omitempty"`
	CreatedAt      int64     `json:"created_at_nsec"`
	UpdatedAt      int64     `json:"updated_at_nsec"`
}

type filePlacementResolver interface {
	FilesystemGroupForHome(homeSlot uint64, inode uint64) (uint64, bool)
	ResolveFilesystemHomeSlot(targetGroup uint64, inode uint64) (uint64, error)
}

// GetFileHome returns the durable placement state for an inode.
func (s *Service) GetFileHome(ctx context.Context, inode uint64) (Home, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return Home{}, err
	}
	return s.homeAt(ctx, inode, ts)
}

// MoveFile synchronously executes a resumable whole-file migration. The job is
// durable before any chunk copy starts, so RecoverIntents can resume it after a
// process or leader restart.
func (s *Service) MoveFile(ctx context.Context, inode uint64, targetGroup uint64) (MoveJob, error) {
	resolver, ok := s.store.(filePlacementResolver)
	if !ok {
		return MoveJob{}, ErrPlacementRequired
	}
	ts, meta, home, err := s.activeMoveSource(ctx, inode)
	if err != nil {
		return MoveJob{}, err
	}
	targetHome, err := resolver.ResolveFilesystemHomeSlot(targetGroup, inode)
	if err != nil {
		return MoveJob{}, errors.Wrap(err, "filesystem resolve move target")
	}
	if currentGroup, found := resolver.FilesystemGroupForHome(home.HomeSlot, inode); found && currentGroup == targetGroup {
		return completedMove(inode, home.HomeSlot, targetGroup), nil
	}
	if targetHome == home.HomeSlot || home.Epoch == math.MaxUint64 {
		return MoveJob{}, ErrInvalid
	}

	jobID, err := s.newRecoveryID()
	if err != nil {
		return MoveJob{}, err
	}
	job, intent := s.newMoveState(jobID, inode, targetGroup, targetHome, home)
	if err := s.persistMovePreparation(ctx, ts, meta, home, job, intent); err != nil {
		return MoveJob{}, err
	}
	return s.ResumeMoveFile(ctx, job.ID)
}

func (s *Service) activeMoveSource(ctx context.Context, inode uint64) (uint64, *InodeMeta, Home, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return 0, nil, Home{}, err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if err != nil {
		return 0, nil, Home{}, err
	}
	if meta.Type != TypeFile || meta.Orphaned || meta.Nlink == 0 {
		return 0, nil, Home{}, ErrInvalid
	}
	home, err := s.homeAt(ctx, inode, ts)
	if err != nil {
		return 0, nil, Home{}, err
	}
	if home.State != HomeStateActive || home.HomeSlot != meta.HomeSlot || home.Epoch != meta.Epoch {
		return 0, nil, Home{}, ErrStaleHome
	}
	return ts, meta, home, nil
}

func completedMove(inode uint64, homeSlot uint64, targetGroup uint64) MoveJob {
	return MoveJob{
		Inode:       inode,
		SourceHome:  homeSlot,
		TargetHome:  homeSlot,
		TargetGroup: targetGroup,
		Phase:       MovePhaseCompleted,
	}
}

func (s *Service) newMoveState(
	jobID []byte,
	inode uint64,
	targetGroup uint64,
	targetHome uint64,
	home Home,
) (MoveJob, IntentState) {
	now := s.now().UnixNano()
	job := MoveJob{
		ID:             jobID,
		Inode:          inode,
		SourceHome:     home.HomeSlot,
		TargetHome:     targetHome,
		TargetGroup:    targetGroup,
		MigrationEpoch: home.Epoch + 1,
		Phase:          MovePhaseTargetCleanup,
		Cursor:         fskeys.ChunkPrefix(targetHome, inode),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	return job, IntentState{
		ID:        append([]byte("move/"), jobID...),
		Kind:      IntentKindMove,
		Phase:     string(job.Phase),
		Inode:     inode,
		JobID:     append([]byte(nil), jobID...),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (s *Service) persistMovePreparation(
	ctx context.Context,
	ts uint64,
	meta *InodeMeta,
	home Home,
	job MoveJob,
	intent IntentState,
) error {
	home.State = HomeStateMigrating
	home.TargetHomeSlot = job.TargetHome
	home.Epoch = job.MigrationEpoch
	meta.Epoch = job.MigrationEpoch
	elems, err := putElems(
		fskeys.HomeKey(job.Inode), home,
		fskeys.InodeKey(job.Inode), meta,
		fskeys.MoveJobKey(job.ID), job,
		fskeys.IntentKey(intent.ID), intent,
	)
	if err != nil {
		return err
	}
	return s.dispatchTxn(ctx, ts, elems, [][]byte{
		fskeys.HomeKey(job.Inode), fskeys.InodeKey(job.Inode),
		fskeys.MoveJobKey(job.ID), fskeys.IntentKey(intent.ID),
	})
}

// ResumeMoveFile advances a durable migration state machine until completion.
func (s *Service) ResumeMoveFile(ctx context.Context, jobID []byte) (MoveJob, error) {
	if s.observer != nil {
		s.observer.ObserveMoveJob(jobID, true)
	}
	for {
		if err := ctx.Err(); err != nil {
			return MoveJob{}, errors.WithStack(err)
		}
		job, err := s.moveJob(ctx, jobID)
		if err != nil {
			return MoveJob{}, err
		}
		completed, stepErr := s.advanceMoveStep(ctx, &job)
		if completed {
			if s.observer != nil {
				s.observer.ObserveMoveJob(jobID, false)
			}
			return job, nil
		}
		if stepErr != nil && !errors.Is(stepErr, store.ErrWriteConflict) {
			return MoveJob{}, stepErr
		}
	}
}

func (s *Service) advanceMoveStep(ctx context.Context, job *MoveJob) (bool, error) {
	switch job.Phase {
	case MovePhaseTargetCleanup:
		return false, s.cleanupMoveTargetPage(ctx, job)
	case MovePhaseCopy:
		return false, s.copyMovePage(ctx, job)
	case MovePhaseSwitch:
		return false, s.switchMoveHome(ctx, job)
	case MovePhaseSourceCleanup:
		return false, s.cleanupMoveSourcePage(ctx, job)
	case MovePhaseCompleted:
		return true, nil
	default:
		return false, errors.Wrapf(ErrInvalid, "unknown move phase %q", job.Phase)
	}
}

func (s *Service) cleanupMoveTargetPage(ctx context.Context, job *MoveJob) error {
	return s.cleanupMoveChunkPage(ctx, job, job.TargetHome, MovePhaseCopy, false)
}

func (s *Service) cleanupMoveSourcePage(ctx context.Context, job *MoveJob) error {
	return s.cleanupMoveChunkPage(ctx, job, job.SourceHome, MovePhaseCompleted, true)
}

//nolint:cyclop // One paged transition must keep scan, fence, cursor, and completion decisions together.
func (s *Service) cleanupMoveChunkPage(
	ctx context.Context,
	job *MoveJob,
	homeSlot uint64,
	nextPhase MovePhase,
	complete bool,
) error {
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	current, err := s.moveJobAt(ctx, job.ID, ts)
	if err != nil {
		return err
	}
	if current.Phase != job.Phase {
		return nil
	}
	if err := s.validateMoveState(ctx, &current, ts); err != nil {
		return err
	}
	prefix := fskeys.ChunkPrefix(homeSlot, current.Inode)
	start := current.Cursor
	if !bytes.HasPrefix(start, prefix) {
		start = prefix
	}
	end := prefixEnd(prefix)
	page, err := s.store.ScanAt(ctx, start, end, movePageSize, ts)
	if err != nil {
		return errors.Wrap(err, "filesystem scan move cleanup chunks")
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(page)+moveProgressExtraElemCount)
	readKeys := make([][]byte, 0, len(page)+moveProgressExtraReadKeyCount)
	readKeys = append(readKeys, fskeys.MoveJobKey(current.ID), fskeys.HomeKey(current.Inode), fskeys.InodeKey(current.Inode))
	for _, pair := range page {
		if pair == nil || !bytes.HasPrefix(pair.Key, prefix) {
			continue
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: append([]byte(nil), pair.Key...)})
		readKeys = append(readKeys, append([]byte(nil), pair.Key...))
	}
	current.UpdatedAt = s.now().UnixNano()
	if len(page) == 0 {
		current.Phase = nextPhase
		current.Cursor = nil
	} else {
		current.Cursor = nextScanCursorForMove(page[len(page)-1].Key)
		current.CleanedChunks += uint64(len(elems))
	}
	elems, readKeys, err = s.appendMoveCleanupIntent(ctx, ts, &current, complete, elems, readKeys)
	if err != nil {
		return err
	}
	jobElem, err := putElem(fskeys.MoveJobKey(current.ID), current)
	if err != nil {
		return err
	}
	elems = append(elems, jobElem)
	return s.dispatchTxn(ctx, ts, elems, readKeys)
}

func (s *Service) appendMoveCleanupIntent(
	ctx context.Context,
	ts uint64,
	job *MoveJob,
	complete bool,
	elems []*kv.Elem[kv.OP],
	readKeys [][]byte,
) ([]*kv.Elem[kv.OP], [][]byte, error) {
	intentKey := fskeys.IntentKey(moveIntentID(job.ID))
	if complete && job.Phase == MovePhaseCompleted {
		return append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: intentKey}), append(readKeys, intentKey), nil
	}
	intent, err := s.moveIntentAt(ctx, job.ID, ts)
	if errors.Is(err, ErrNotFound) {
		return elems, readKeys, nil
	}
	if err != nil {
		return nil, nil, err
	}
	intent.Phase = string(job.Phase)
	intent.UpdatedAt = job.UpdatedAt
	intentElem, err := putElem(fskeys.IntentKey(intent.ID), intent)
	if err != nil {
		return nil, nil, err
	}
	return append(elems, intentElem), append(readKeys, intentKey), nil
}

//nolint:cyclop // Copy pagination validates every source row and checkpoints the matching cursor atomically.
func (s *Service) copyMovePage(ctx context.Context, job *MoveJob) error {
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	current, err := s.moveJobAt(ctx, job.ID, ts)
	if err != nil {
		return err
	}
	if current.Phase != MovePhaseCopy {
		return nil
	}
	if err := s.validateMoveState(ctx, &current, ts); err != nil {
		return err
	}
	prefix := fskeys.ChunkPrefix(current.SourceHome, current.Inode)
	start := current.Cursor
	if !bytes.HasPrefix(start, prefix) {
		start = prefix
	}
	page, err := s.store.ScanAt(ctx, start, prefixEnd(prefix), movePageSize, ts)
	if err != nil {
		return errors.Wrap(err, "filesystem scan move source chunks")
	}
	current.UpdatedAt = s.now().UnixNano()
	if len(page) == 0 {
		current.Phase = MovePhaseSwitch
		current.Cursor = nil
		return s.persistMoveProgress(ctx, ts, &current, nil, nil)
	}

	elems := make([]*kv.Elem[kv.OP], 0, len(page))
	readKeys := make([][]byte, 0, len(page))
	for _, pair := range page {
		if pair == nil {
			continue
		}
		index, ok := fskeys.ChunkIndexFromKey(current.SourceHome, current.Inode, pair.Key)
		if !ok {
			return errors.Wrap(ErrMoveFenceLost, "source scan returned a foreign chunk")
		}
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   fskeys.ChunkKey(current.TargetHome, current.Inode, index),
			Value: append([]byte(nil), pair.Value...),
		})
		readKeys = append(readKeys, append([]byte(nil), pair.Key...))
	}
	current.Cursor = nextScanCursorForMove(page[len(page)-1].Key)
	current.CopiedChunks += uint64(len(elems))
	return s.persistMoveProgress(ctx, ts, &current, elems, readKeys)
}

func (s *Service) persistMoveProgress(
	ctx context.Context,
	ts uint64,
	job *MoveJob,
	elems []*kv.Elem[kv.OP],
	readKeys [][]byte,
) error {
	jobElem, err := putElem(fskeys.MoveJobKey(job.ID), job)
	if err != nil {
		return err
	}
	intent, err := s.moveIntentAt(ctx, job.ID, ts)
	if err != nil {
		return err
	}
	intent.Phase = string(job.Phase)
	intent.UpdatedAt = job.UpdatedAt
	intentElem, err := putElem(fskeys.IntentKey(intent.ID), intent)
	if err != nil {
		return err
	}
	elems = append(elems, jobElem, intentElem)
	readKeys = append(readKeys, fskeys.MoveJobKey(job.ID), fskeys.IntentKey(intent.ID), fskeys.HomeKey(job.Inode), fskeys.InodeKey(job.Inode))
	return s.dispatchTxn(ctx, ts, elems, readKeys)
}

func (s *Service) switchMoveHome(ctx context.Context, job *MoveJob) error {
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	current, err := s.moveJobAt(ctx, job.ID, ts)
	if err != nil {
		return err
	}
	if current.Phase != MovePhaseSwitch {
		return nil
	}
	meta, err := s.inodeAt(ctx, current.Inode, ts)
	if err != nil {
		return err
	}
	home, err := s.homeAt(ctx, current.Inode, ts)
	if err != nil {
		return err
	}
	if !moveFenceMatches(meta, home, &current) || home.Epoch == math.MaxUint64 {
		return ErrMoveFenceLost
	}
	home.HomeSlot = current.TargetHome
	home.TargetHomeSlot = 0
	home.State = HomeStateActive
	home.Epoch++
	meta.HomeSlot = current.TargetHome
	meta.Epoch = home.Epoch
	current.SwitchEpoch = home.Epoch
	current.Phase = MovePhaseSourceCleanup
	current.Cursor = fskeys.ChunkPrefix(current.SourceHome, current.Inode)
	current.UpdatedAt = s.now().UnixNano()
	intent, err := s.moveIntentAt(ctx, current.ID, ts)
	if err != nil {
		return err
	}
	intent.Phase = string(current.Phase)
	intent.UpdatedAt = current.UpdatedAt
	elems, err := putElems(
		fskeys.HomeKey(current.Inode), home,
		fskeys.InodeKey(current.Inode), meta,
		fskeys.MoveJobKey(current.ID), current,
		fskeys.IntentKey(intent.ID), intent,
	)
	if err != nil {
		return err
	}
	return s.dispatchTxn(ctx, ts, elems, [][]byte{
		fskeys.HomeKey(current.Inode), fskeys.InodeKey(current.Inode),
		fskeys.MoveJobKey(current.ID), fskeys.IntentKey(intent.ID),
	})
}

func (s *Service) validateMoveState(ctx context.Context, job *MoveJob, ts uint64) error {
	meta, metaErr := s.inodeAt(ctx, job.Inode, ts)
	home, homeErr := s.homeAt(ctx, job.Inode, ts)
	if job.Phase == MovePhaseSourceCleanup || job.Phase == MovePhaseCompleted {
		return validateSwitchedMoveState(meta, metaErr, home, homeErr, job)
	}
	if metaErr != nil {
		return metaErr
	}
	if homeErr != nil {
		return homeErr
	}
	if !moveFenceMatches(meta, home, job) {
		return ErrMoveFenceLost
	}
	return nil
}

func validateSwitchedMoveState(meta *InodeMeta, metaErr error, home Home, homeErr error, job *MoveJob) error {
	// The namespace may be unlinked after the home switch. Both records being
	// absent proves no live file state remains to fence; the durable job still
	// identifies the old physical prefix that must be removed.
	if errors.Is(metaErr, ErrNotFound) && errors.Is(homeErr, ErrNotFound) {
		return nil
	}
	if metaErr != nil {
		return metaErr
	}
	if homeErr != nil {
		return homeErr
	}
	if !switchedMoveFenceMatches(meta, home, job) {
		return ErrMoveFenceLost
	}
	return nil
}

func switchedMoveFenceMatches(meta *InodeMeta, home Home, job *MoveJob) bool {
	return home.State == HomeStateActive &&
		home.HomeSlot == job.TargetHome && meta.HomeSlot == job.TargetHome &&
		home.Epoch == job.SwitchEpoch && meta.Epoch == job.SwitchEpoch
}

func moveFenceMatches(meta *InodeMeta, home Home, job *MoveJob) bool {
	return meta != nil && !meta.Orphaned && meta.Nlink > 0 &&
		home.State == HomeStateMigrating &&
		home.HomeSlot == job.SourceHome && home.TargetHomeSlot == job.TargetHome &&
		home.Epoch == job.MigrationEpoch &&
		meta.HomeSlot == job.SourceHome && meta.Epoch == job.MigrationEpoch
}

func (s *Service) moveJob(ctx context.Context, jobID []byte) (MoveJob, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return MoveJob{}, err
	}
	return s.moveJobAt(ctx, jobID, ts)
}

func (s *Service) moveJobAt(ctx context.Context, jobID []byte, ts uint64) (MoveJob, error) {
	raw, err := s.store.GetAt(ctx, fskeys.MoveJobKey(jobID), ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return MoveJob{}, ErrMoveJobNotFound
	}
	if err != nil {
		return MoveJob{}, errors.Wrap(err, "filesystem read move job")
	}
	return decodeJSON[MoveJob](raw)
}

func (s *Service) moveIntentAt(ctx context.Context, jobID []byte, ts uint64) (IntentState, error) {
	raw, err := s.store.GetAt(ctx, fskeys.IntentKey(moveIntentID(jobID)), ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return IntentState{}, ErrNotFound
	}
	if err != nil {
		return IntentState{}, errors.Wrap(err, "filesystem read move intent")
	}
	return decodeJSON[IntentState](raw)
}

func (s *Service) homeAt(ctx context.Context, inode uint64, ts uint64) (Home, error) {
	raw, err := s.store.GetAt(ctx, fskeys.HomeKey(inode), ts)
	if errors.Is(err, store.ErrKeyNotFound) {
		return Home{}, ErrNotFound
	}
	if err != nil {
		return Home{}, errors.Wrap(err, "filesystem read home")
	}
	return decodeJSON[Home](raw)
}

func (s *Service) ensureActiveHomeAt(ctx context.Context, meta *InodeMeta, ts uint64) error {
	if meta == nil {
		return ErrNotFound
	}
	home, err := s.homeAt(ctx, meta.Inode, ts)
	if err != nil {
		return err
	}
	if home.State != HomeStateActive || home.HomeSlot != meta.HomeSlot || home.Epoch != meta.Epoch {
		if s.observer != nil {
			s.observer.ObserveHomeEpochConflict()
		}
		return ErrStaleHome
	}
	return nil
}

func (s *Service) translateHomeConflict(ctx context.Context, expected *InodeMeta, dispatchErr error) error {
	if !errors.Is(dispatchErr, store.ErrWriteConflict) || expected == nil {
		return dispatchErr
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return dispatchErr
	}
	current, err := s.homeAt(ctx, expected.Inode, ts)
	if err != nil {
		return dispatchErr
	}
	if current.HomeSlot != expected.HomeSlot || current.Epoch != expected.Epoch || current.State != HomeStateActive {
		if s.observer != nil {
			s.observer.ObserveHomeEpochConflict()
		}
		return errors.Wrap(ErrStaleHome, "filesystem home changed during mutation")
	}
	return dispatchErr
}

func (s *Service) newRecoveryID() ([]byte, error) {
	id, err := s.allocID()
	if err != nil {
		return nil, errors.Wrap(err, "filesystem allocate recovery id")
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], id)
	return raw[:], nil
}

func moveIntentID(jobID []byte) []byte {
	return append([]byte("move/"), jobID...)
}

func nextScanCursorForMove(key []byte) []byte {
	return append(append([]byte(nil), key...), 0)
}

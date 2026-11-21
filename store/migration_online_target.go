package store

import (
	"bytes"
	"context"
	"encoding/json"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

const OnlineMigrationBracketID = math.MaxUint64

var ErrOnlineMigrationTargetNotEmpty = errors.New("online migration: target contains live history")
var ErrOnlineMigrationTargetClosed = errors.New("online migration: target imports closed")

type OnlineMigrationTarget struct {
	Source             OnlineMigrationSession
	ConfigurationIndex uint64
	Sequence           uint64
	LastSourceIndex    uint64
	Ready              bool
	Closed             bool
	CleanupSequence    uint64
}

func ReadOnlineMigrationTarget(ctx context.Context, st MVCCStore, job uint64) (OnlineMigrationTarget, error) {
	var target OnlineMigrationTarget
	metadata, ok := st.(RaftControlMetadataStore)
	if !ok {
		return target, errors.WithStack(ErrNotSupported)
	}
	if err := readOnlineJSON(ctx, metadata, onlineMigrationKey("target", job, 0), &target); err != nil {
		return target, err
	}
	if !validOnlineSessionIdentity(target.Source, job) || target.ConfigurationIndex == 0 || target.Sequence > onlineMigrationMaxFrames || target.LastSourceIndex < target.Source.StartIndex {
		return target, errors.WithStack(ErrMigrationCaptureCorrupt)
	}
	return target, nil
}

// PrepareOnlineMigrationTarget requires the caller's indexed transaction and
// validated nonowner interval. fresh excludes only explicitly local metadata.
func PrepareOnlineMigrationTarget(ctx context.Context, st MVCCStore, source OnlineMigrationSession, configuration uint64, fresh func([]byte) bool) error {
	metadata, ok := st.(RaftControlMetadataStore)
	if !ok || !validOnlineTargetPreparation(st, source, configuration, fresh) {
		return errors.WithStack(ErrOnlineMigrationIdentity)
	}
	current, err := ReadOnlineMigrationTarget(ctx, st, source.Identity.JobID)
	if err == nil {
		return validateOnlineTargetReactivation(current, source, configuration)
	}
	if !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if err := verifyOnlineTargetFresh(ctx, st, fresh); err != nil {
		return err
	}
	target := OnlineMigrationTarget{Source: source, ConfigurationIndex: configuration, LastSourceIndex: source.StartIndex}
	return stageOnlineJSON(ctx, metadata, onlineMigrationKey("target", source.Identity.JobID, 0), target)
}

func validOnlineTargetPreparation(st MVCCStore, source OnlineMigrationSession, configuration uint64, fresh func([]byte) bool) bool {
	return OnlineMigrationSupported(st) && configuration != 0 && fresh != nil && source.Invalid == "" && !source.Closed && !source.Sealed
}

func validateOnlineTargetReactivation(current OnlineMigrationTarget, source OnlineMigrationSession, configuration uint64) error {
	if !onlineTargetMatchesSource(current, source, configuration) {
		return errors.WithStack(ErrOnlineMigrationIdentity)
	}
	if current.Ready || current.Closed {
		return errors.WithStack(ErrOnlineMigrationTargetClosed)
	}
	return nil
}

func onlineTargetMatchesSource(target OnlineMigrationTarget, source OnlineMigrationSession, configuration uint64) bool {
	return sameOnlineMigrationSource(target.Source, source) && target.Source.StartIndex == source.StartIndex && target.ConfigurationIndex == configuration
}

func verifyOnlineTargetFresh(ctx context.Context, st MVCCStore, fresh func([]byte) bool) error {
	opts := ExportVersionsOptions{MaxVersions: 1, MaxBytes: migrationCaptureDefaultBytes, MaxScannedBytes: migrationCaptureDefaultBytes, AcceptKey: fresh}
	for {
		result, err := st.ExportVersions(ctx, opts)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(result.Versions) != 0 {
			return errors.WithStack(ErrOnlineMigrationTargetNotEmpty)
		}
		if result.Done {
			return nil
		}
		if len(result.NextCursor) == 0 || bytes.Equal(opts.Cursor, result.NextCursor) {
			return errors.WithStack(ErrMigrationCaptureCorrupt)
		}
		opts.Cursor = result.NextCursor
	}
}

// ReattestOnlineMigrationTarget binds an existing, importing target to a new
// membership before the sealed tail is complete. Only the configuration moves.
func ReattestOnlineMigrationTarget(ctx context.Context, st MVCCStore, seal OnlineMigrationSession, configuration uint64) error {
	target, err := ReadOnlineMigrationTarget(ctx, st, seal.Identity.JobID)
	if err != nil {
		return err
	}
	if target.Closed || target.Ready {
		return errors.WithStack(ErrOnlineMigrationTargetClosed)
	}
	if configuration < target.ConfigurationIndex || !onlineTargetCanFinishTail(target, seal) {
		return errors.WithStack(ErrOnlineMigrationIdentity)
	}
	if configuration == target.ConfigurationIndex {
		return nil
	}
	metadata, ok := st.(RaftControlMetadataStore)
	if !ok {
		return errors.WithStack(ErrNotSupported)
	}
	target.ConfigurationIndex = configuration
	return stageOnlineJSON(ctx, metadata, onlineMigrationKey("target", seal.Identity.JobID, 0), target)
}

func onlineTargetCanFinishTail(target OnlineMigrationTarget, seal OnlineMigrationSession) bool {
	return validOnlineSourceSeal(seal, seal.Sequence) && onlineTargetMatchesSource(target, seal, target.ConfigurationIndex) &&
		target.Sequence <= seal.Sequence && target.LastSourceIndex <= seal.LastIndex
}

func ReadyOnlineMigrationTarget(ctx context.Context, st MVCCStore, source OnlineMigrationSession, configuration, seq uint64) error {
	target, err := ReadOnlineMigrationTarget(ctx, st, source.Identity.JobID)
	if err != nil {
		return err
	}
	if target.Closed {
		return errors.WithStack(ErrOnlineMigrationTargetClosed)
	}
	if !validOnlineTargetReady(target, source, configuration, seq) {
		return errors.WithStack(ErrOnlineMigrationIdentity)
	}
	metadata, ok := st.(RaftControlMetadataStore)
	if !ok {
		return errors.WithStack(ErrNotSupported)
	}
	target.Source = source
	target.ConfigurationIndex = configuration
	target.Ready = true
	return stageOnlineJSON(ctx, metadata, onlineMigrationKey("target", source.Identity.JobID, 0), target)
}

type onlineMigrationImportReceipt struct {
	Digest      [32]byte
	Cursor      []byte
	SourceIndex uint64
}

func (s *pebbleStore) readOnlineMetadataLocked(key []byte, value any) error {
	physical, err := raftControlKey(key)
	if err != nil {
		return err
	}
	encoded, closer, err := s.db.Get(physical)
	if errors.Is(err, pebble.ErrNotFound) {
		return errors.WithStack(ErrKeyNotFound)
	}
	if err != nil {
		return errors.WithStack(err)
	}
	decoded, err := s.decodeRaftControlValue(physical, encoded)
	err = errors.Join(err, errors.WithStack(closer.Close()))
	if err != nil {
		return err
	}
	return decodeOnlineTransfer(decoded, value)
}

func (s *pebbleStore) stageOnlineMetadataBatch(batch *pebble.Batch, key []byte, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return errors.WithStack(err)
	}
	physical, err := raftControlKey(key)
	if err != nil {
		return err
	}
	body, flags, err := s.encryptForKey(physical, encoded, 0, false)
	if err != nil {
		return err
	}
	return errors.WithStack(batch.Set(physical, encodeValue(body, false, 0, flags), nil))
}

// The import lock is held. This gate applies to ordinary BASE imports too.
func (s *pebbleStore) validateOnlineImport(opts ImportVersionsOptions) (*OnlineMigrationTarget, *onlineMigrationImportReceipt, error) {
	target, err := s.onlineImportTarget(opts)
	if err != nil || target == nil {
		return target, nil, err
	}
	if opts.OnlineSequence == 0 {
		if opts.BracketID == OnlineMigrationBracketID {
			return nil, nil, errors.WithStack(ErrOnlineMigrationIdentity)
		}
		return target, nil, nil
	}
	if !validOnlineImportIdentity(opts, *target) {
		return nil, nil, errors.WithStack(ErrOnlineMigrationIdentity)
	}
	receipt, err := s.onlineImportReceipt(opts, *target)
	return target, receipt, err
}

func (s *pebbleStore) onlineImportTarget(opts ImportVersionsOptions) (*OnlineMigrationTarget, error) {
	var target OnlineMigrationTarget
	err := s.readOnlineMetadataLocked(onlineMigrationKey("target", opts.JobID, 0), &target)
	if errors.Is(err, ErrKeyNotFound) {
		if opts.OnlineSequence != 0 {
			return nil, errors.WithStack(ErrOnlineMigrationIdentity)
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if target.Ready || target.Closed {
		return nil, errors.WithStack(ErrOnlineMigrationTargetClosed)
	}
	return &target, nil
}

func validOnlineImportIdentity(opts ImportVersionsOptions, target OnlineMigrationTarget) bool {
	return opts.BracketID == OnlineMigrationBracketID && opts.BatchSeq == opts.OnlineSequence && opts.OnlineDigest != [32]byte{} && opts.OnlineSourceIdentity == target.Source.Identity
}

func (s *pebbleStore) onlineImportReceipt(opts ImportVersionsOptions, target OnlineMigrationTarget) (*onlineMigrationImportReceipt, error) {
	var receipt onlineMigrationImportReceipt
	err := s.readOnlineMetadataLocked(onlineMigrationKey("receipt", opts.JobID, opts.OnlineSequence), &receipt)
	if err == nil {
		if !matchingOnlineImportReceipt(opts, target, receipt) {
			return nil, errors.WithStack(ErrMigrationCaptureConflict)
		}
		return &receipt, nil
	}
	if !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}
	if opts.OnlineSequence != target.Sequence+1 || opts.OnlineSequence > onlineMigrationMaxFrames || opts.OnlineSourceIndex <= target.LastSourceIndex {
		return nil, errors.WithStack(ErrImportBatchGap)
	}
	return nil, nil
}

func matchingOnlineImportReceipt(opts ImportVersionsOptions, target OnlineMigrationTarget, receipt onlineMigrationImportReceipt) bool {
	return receipt.Digest == opts.OnlineDigest && bytes.Equal(receipt.Cursor, opts.Cursor) && receipt.SourceIndex == opts.OnlineSourceIndex && opts.OnlineSequence <= target.Sequence && opts.OnlineSourceIndex <= target.LastSourceIndex
}

func (s *pebbleStore) stageOnlineImport(batch *pebble.Batch, opts ImportVersionsOptions) error {
	if opts.OnlineSequence == 0 {
		return nil
	}
	target, receipt, err := s.validateOnlineImport(opts)
	if err != nil {
		return err
	}
	if target == nil || receipt != nil {
		return errors.WithStack(ErrOnlineMigrationIdentity)
	}
	target.Sequence = opts.OnlineSequence
	target.LastSourceIndex = opts.OnlineSourceIndex
	if err := s.stageOnlineMetadataBatch(batch, onlineMigrationKey("receipt", opts.JobID, opts.OnlineSequence), onlineMigrationImportReceipt{opts.OnlineDigest, bytes.Clone(opts.Cursor), opts.OnlineSourceIndex}); err != nil {
		return err
	}
	return s.stageOnlineMetadataBatch(batch, onlineMigrationKey("target", opts.JobID, 0), target)
}

// Retirement uses two range tombstones, independent of frame count. The
// terminal manifests remain to reject late activation/import for the old job.
func (s *pebbleStore) stageOnlineMigrationCleanup(batch *pebble.Batch, job, index uint64) (bool, error) {
	source, err := s.closeOnlineSource(batch, job, index)
	if err != nil {
		return false, err
	}
	target, err := s.closeOnlineTarget(batch, job, index)
	if err != nil {
		return false, err
	}
	if !source && !target {
		return false, nil
	}
	for _, kind := range []string{"frame", "receipt"} {
		key := onlineMigrationKey(kind, job, 0)
		prefix, err := raftControlKey(key[:len(key)-onlineSequenceKeyBytes])
		if err != nil {
			return false, err
		}
		if err := batch.DeleteRange(prefix, PrefixScanEnd(prefix), nil); err != nil {
			return false, errors.WithStack(err)
		}
	}
	return true, nil
}

func (s *pebbleStore) closeOnlineSource(batch *pebble.Batch, job, index uint64) (bool, error) {
	var source OnlineMigrationSession
	err := s.readOnlineMetadataLocked(onlineMigrationKey("source", job, 0), &source)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if index == 0 {
		return false, errors.WithStack(ErrRaftControlScopeRequired)
	}
	source.Closed = true
	source.CleanupSequence = source.Sequence
	return true, s.stageOnlineMetadataBatch(batch, onlineMigrationKey("source", job, 0), source)
}

func (s *pebbleStore) closeOnlineTarget(batch *pebble.Batch, job, index uint64) (bool, error) {
	var target OnlineMigrationTarget
	err := s.readOnlineMetadataLocked(onlineMigrationKey("target", job, 0), &target)
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if index == 0 {
		return false, errors.WithStack(ErrRaftControlScopeRequired)
	}
	target.Closed = true
	target.CleanupSequence = target.Sequence
	return true, s.stageOnlineMetadataBatch(batch, onlineMigrationKey("target", job, 0), target)
}

const onlineSequenceKeyBytes = 8

func validOnlineTargetReady(target OnlineMigrationTarget, source OnlineMigrationSession, configuration, seq uint64) bool {
	return validOnlineSourceSeal(source, seq) && target.Sequence == seq && configuration >= target.ConfigurationIndex && onlineTargetMatchesSource(target, source, target.ConfigurationIndex) && target.LastSourceIndex == source.LastIndex
}

func validOnlineSourceSeal(source OnlineMigrationSession, seq uint64) bool {
	return source.Sealed && source.Invalid == "" && !source.Closed && source.Sequence == seq
}

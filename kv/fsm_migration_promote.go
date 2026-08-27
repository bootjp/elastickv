package kv

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMigrationPromoteMaxVersions     = 1024
	defaultMigrationPromoteMaxBytes        = 4 << 20
	defaultMigrationPromoteMaxScannedBytes = defaultMigrationPromoteMaxBytes * 4

	// Hard server-side ceilings. The defaults above only apply to a request
	// that leaves a bound unset, so without these an operator or migrator could
	// ask one apply to load, re-encrypt, and commit an unbounded amount of
	// staged data in a single Pebble batch -- synchronously, in every voter's
	// apply loop. Clamping keeps the incremental, bounded promotion this API
	// promises: the caller simply gets more rounds through the cursor, which
	// PromoteVersionsResult already returns.
	//
	// The clamp is a pure function of the request and these constants, so every
	// replica derives the same bounds from the same command and apply stays
	// deterministic.
	maxMigrationPromoteMaxVersions     = 8192
	maxMigrationPromoteMaxBytes        = 32 << 20
	maxMigrationPromoteMaxScannedBytes = maxMigrationPromoteMaxBytes * 4
)

var ErrMigrationPromoteApply = errors.New("migration promote: FSM apply failed; halting apply")

// MarshalMigrationPromoteCommand encodes a target-group staged-data promotion
// chunk as a Raft FSM command.
func MarshalMigrationPromoteCommand(req *pb.PromoteStagedVersionsRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(b) >= maxMarshaledCommandSize {
		return nil, errors.New("marshaled migration promote request too large")
	}
	return prependByte(raftEncodeMigrationPromote, b), nil
}

func (f *kvFSM) applyMigrationPromote(ctx context.Context, data []byte) any {
	req := &pb.PromoteStagedVersionsRequest{}
	if err := proto.Unmarshal(data, req); err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationPromoteApply), "kv/fsm: decode migration promote"))
	}
	promoter, ok := f.store.(store.MigrationPromoter)
	if !ok {
		return haltErr(errors.Wrap(errors.Mark(store.ErrNotSupported, ErrMigrationPromoteApply), "kv/fsm: migration promote store"))
	}
	result, err := promoter.PromoteVersions(ctx, migrationPromoteOptionsFromProto(req, f.pendingApplyIdx))
	if err != nil {
		if isMigrationPromoteOrdinaryApplyError(err) {
			return errors.Wrap(err, "kv/fsm: apply migration promote")
		}
		return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationPromoteApply), "kv/fsm: apply migration promote"))
	}
	if f.hlc != nil && result.MaxPromotedTS > 0 {
		f.hlc.Observe(result.MaxPromotedTS)
	}
	return result
}

func migrationPromoteOptionsFromProto(req *pb.PromoteStagedVersionsRequest, appliedIndex uint64) store.PromoteVersionsOptions {
	// Clamped in the int domain the request already decodes into, so no
	// widening conversion is introduced here.
	maxVersions := int(req.GetMaxVersions())
	switch {
	case maxVersions <= 0:
		maxVersions = defaultMigrationPromoteMaxVersions
	case maxVersions > maxMigrationPromoteMaxVersions:
		maxVersions = maxMigrationPromoteMaxVersions
	}
	maxBytes := clampMigrationPromoteBound(
		req.GetMaxBytes(), defaultMigrationPromoteMaxBytes, maxMigrationPromoteMaxBytes)
	maxScannedBytes := clampMigrationPromoteBound(
		req.GetMaxScannedBytes(), defaultMigrationPromoteMaxScannedBytes, maxMigrationPromoteMaxScannedBytes)
	prefix := distribution.MigrationStagedDataKeyPrefix(req.GetJobId())
	return store.PromoteVersionsOptions{
		JobID:           req.GetJobId(),
		AppliedIndex:    appliedIndex,
		StartKey:        prefix,
		EndKey:          store.PrefixScanEnd(prefix),
		Cursor:          req.GetCursor(),
		MaxVersions:     maxVersions,
		MaxBytes:        maxBytes,
		MaxScannedBytes: maxScannedBytes,
		TargetKey:       migrationPromoteTargetKey(req.GetJobId()),
	}
}

// clampMigrationPromoteBound resolves one promotion bound: unset takes the
// default, anything above the hard ceiling is clamped down to it.
func clampMigrationPromoteBound(requested, fallback, ceiling uint64) uint64 {
	if requested == 0 {
		return fallback
	}
	return min(requested, ceiling)
}

func isMigrationPromoteOrdinaryApplyError(err error) bool {
	return errors.Is(err, store.ErrInvalidExportCursor)
}

func migrationPromoteTargetKey(jobID uint64) func([]byte) ([]byte, bool) {
	return func(stagedKey []byte) ([]byte, bool) {
		gotJobID, rawKey, ok := distribution.MigrationStagedDataKeyParts(stagedKey)
		if !ok || gotJobID != jobID {
			return nil, false
		}
		return rawKey, true
	}
}

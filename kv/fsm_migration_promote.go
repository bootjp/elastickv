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
		return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationPromoteApply), "kv/fsm: apply migration promote"))
	}
	if f.hlc != nil && result.MaxPromotedTS > 0 {
		f.hlc.Observe(result.MaxPromotedTS)
	}
	return result
}

func migrationPromoteOptionsFromProto(req *pb.PromoteStagedVersionsRequest, appliedIndex uint64) store.PromoteVersionsOptions {
	maxVersions := int(req.GetMaxVersions())
	if maxVersions <= 0 {
		maxVersions = defaultMigrationPromoteMaxVersions
	}
	maxBytes := req.GetMaxBytes()
	if maxBytes == 0 {
		maxBytes = defaultMigrationPromoteMaxBytes
	}
	maxScannedBytes := req.GetMaxScannedBytes()
	if maxScannedBytes == 0 {
		maxScannedBytes = defaultMigrationPromoteMaxScannedBytes
	}
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

func migrationPromoteTargetKey(jobID uint64) func([]byte) ([]byte, bool) {
	return func(stagedKey []byte) ([]byte, bool) {
		gotJobID, rawKey, ok := distribution.MigrationStagedDataKeyParts(stagedKey)
		if !ok || gotJobID != jobID {
			return nil, false
		}
		return rawKey, true
	}
}

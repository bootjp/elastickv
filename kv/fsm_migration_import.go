package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

var ErrMigrationImportApply = errors.New("migration import: FSM apply failed; halting apply")

// MarshalMigrationImportCommand encodes a target-group migration import batch
// as a Raft FSM command. The target Internal RPC handler uses this instead of
// mutating its local store directly so an acknowledged batch has been applied
// by the target group's voters.
func MarshalMigrationImportCommand(req *pb.ImportRangeVersionsRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(b) >= maxMarshaledCommandSize {
		return nil, errors.New("marshaled migration import request too large")
	}
	return prependByte(raftEncodeMigrationImport, b), nil
}

func (f *kvFSM) applyMigrationImport(ctx context.Context, data []byte) any {
	req := &pb.ImportRangeVersionsRequest{}
	if err := proto.Unmarshal(data, req); err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationImportApply), "kv/fsm: decode migration import"))
	}
	result, err := f.store.ImportVersionsRaft(ctx, store.ImportVersionsOptions{
		JobID:        req.GetJobId(),
		AppliedIndex: f.pendingApplyIdx,
		BracketID:    req.GetBracketId(),
		BatchSeq:     req.GetBatchSeq(),
		Cursor:       req.GetCursor(),
		Versions:     migrationStoreVersionsFromProto(req.GetJobId(), req.GetVersions()),
	})
	if err != nil {
		if isMigrationImportOrdinaryApplyError(err) {
			return errors.Wrap(err, "kv/fsm: apply migration import")
		}
		return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationImportApply), "kv/fsm: apply migration import"))
	}
	result.MaxImportedTS, err = f.migrationHLCFloorForApply(ctx, req, result)
	if err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationImportApply), "kv/fsm: migration import hlc floor"))
	}
	if f.hlc != nil && result.MaxImportedTS > 0 {
		f.hlc.Observe(result.MaxImportedTS)
	}
	return result
}

// isMigrationImportOrdinaryApplyError reports whether err is a verdict on the
// request bytes rather than a failure of this replica's store. Only those may
// be returned as an ordinary apply error: the engine advances setApplied past
// a response that does not implement HaltApply, and every replica applying the
// same entry decides an ordinary error identically, so the group stays in step.
//
// A store-side failure -- Pebble I/O, an encryption gate rejecting the write --
// is per-replica. Letting it advance setApplied would leave the leader acking
// the batch while the failed voter skips the imported versions for good, which
// surfaces as missing data after failover or promotion. Those halt instead,
// matching applyMigrationPromote.
func isMigrationImportOrdinaryApplyError(err error) bool {
	return errors.Is(err, store.ErrImportBatchGap) ||
		errors.Is(err, store.ErrInvalidImportVersion) ||
		errors.Is(err, store.ErrValueTooLarge)
}

func (f *kvFSM) migrationHLCFloorForApply(ctx context.Context, req *pb.ImportRangeVersionsRequest, result store.ImportVersionsResult) (uint64, error) {
	if result.MaxImportedTS > 0 || len(req.GetVersions()) == 0 {
		return result.MaxImportedTS, nil
	}
	floor, err := f.store.MigrationHLCFloor(ctx, req.GetJobId())
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return floor, nil
}

func migrationStoreVersionsFromProto(jobID uint64, in []*pb.MVCCVersion) []store.MVCCVersion {
	out := make([]store.MVCCVersion, 0, len(in))
	for _, version := range in {
		if version == nil {
			continue
		}
		out = append(out, store.MVCCVersion{
			Key:       distribution.MigrationStagedDataKey(jobID, version.GetKey()),
			CommitTS:  version.GetCommitTs(),
			Tombstone: version.GetTombstone(),
			Value:     bytes.Clone(version.GetValue()),
			KeyFamily: version.GetKeyFamily(),
			ExpireAt:  version.GetExpireAt(),
		})
	}
	return out
}

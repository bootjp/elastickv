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

// MarshalMigrationCleanupCommand encodes a bounded cleanup operation for Raft.
func MarshalMigrationCleanupCommand(req *pb.CleanupMigrationRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(b) >= maxMarshaledCommandSize {
		return nil, errors.New("marshaled migration cleanup request too large")
	}
	return prependByte(raftEncodeMigrationCleanup, b), nil
}

func (f *kvFSM) applyMigrationCleanup(ctx context.Context, data []byte) any {
	req := &pb.CleanupMigrationRequest{}
	if err := proto.Unmarshal(data, req); err != nil {
		return errors.WithStack(err)
	}
	cleaner, ok := f.store.(store.MigrationCleaner)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	if req.GetMode() == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA {
		return errors.WithStack(cleaner.ClearMigrationState(ctx, req.GetJobId(), f.pendingApplyIdx))
	}
	result, err := cleaner.CleanupVersions(ctx, migrationCleanupOptionsFromProto(req, f.pendingApplyIdx))
	if err != nil {
		return errors.WithStack(err)
	}
	return result
}

func migrationCleanupOptionsFromProto(req *pb.CleanupMigrationRequest, appliedIndex uint64) store.CleanupVersionsOptions {
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
	bracket := distribution.MigrationBracket{
		Family:                req.GetKeyFamily(),
		Start:                 bytes.Clone(req.GetRangeStart()),
		End:                   bytes.Clone(req.GetRangeEnd()),
		ExcludePrefixes:       cloneMigrationByteSlices(req.GetExcludePrefixes()),
		ExcludeKnownInternal:  req.GetExcludeKnownInternal(),
		RequiresRouteKeyCheck: req.GetRequiresRouteKeyCheck(),
		RequiresDecodedS3:     req.GetRequiresDecodedS3(),
	}
	return store.CleanupVersionsOptions{
		JobID:           req.GetJobId(),
		AppliedIndex:    appliedIndex,
		StartKey:        bytes.Clone(req.GetRangeStart()),
		EndKey:          bytes.Clone(req.GetRangeEnd()),
		Cursor:          bytes.Clone(req.GetCursor()),
		MaxCommitTS:     req.GetMaxCommitTs(),
		MaxVersions:     maxVersions,
		MaxBytes:        maxBytes,
		MaxScannedBytes: maxScannedBytes,
		KeyFamily:       req.GetKeyFamily(),
		AcceptVersion: func(key, value []byte) bool {
			return bracket.ContainsRoutedVersion(key, value, req.GetRouteStart(), req.GetRouteEnd(), routeKey)
		},
	}
}

func cloneMigrationByteSlices(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = bytes.Clone(in[i])
	}
	return out
}

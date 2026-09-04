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

// ErrMigrationCleanupApply marks a replica-local cleanup failure. Import,
// promote and retire already halt on their equivalents; cleanup returning an
// ordinary error let the Raft engine advance the applied index past a committed
// entry that this voter never performed, so it would silently keep versions the
// rest of the cluster deleted.
var ErrMigrationCleanupApply = errors.New("migration cleanup: FSM apply failed; halting apply")

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
		return haltErr(errors.Wrap(errors.Mark(store.ErrNotSupported, ErrMigrationCleanupApply), "kv/fsm: migration cleanup store"))
	}
	if req.GetMode() == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA {
		if err := cleaner.ClearMigrationState(ctx, req.GetJobId(), f.pendingApplyIdx); err != nil {
			return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationCleanupApply), "kv/fsm: clear migration state"))
		}
		return nil
	}
	result, err := cleaner.CleanupVersions(ctx, migrationCleanupOptionsFromProto(req, f.pendingApplyIdx))
	if err != nil {
		if isMigrationCleanupOrdinaryApplyError(err) {
			return errors.Wrap(err, "kv/fsm: apply migration cleanup")
		}
		return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationCleanupApply), "kv/fsm: apply migration cleanup"))
	}
	return result
}

// isMigrationCleanupOrdinaryApplyError separates deterministic verdicts on the
// request itself -- every replica reaches them for the same entry, so they are
// safe to return as an ordinary response -- from replica-local store failures
// (Pebble read, decrypt, batch, commit), which must halt rather than let this
// voter skip a committed cleanup.
func isMigrationCleanupOrdinaryApplyError(err error) bool {
	return errors.Is(err, ErrInvalidRequest) ||
		errors.Is(err, store.ErrInvalidImportVersion) ||
		errors.Is(err, store.ErrValueTooLarge) ||
		errors.Is(err, store.ErrSnapshotKeyTooLarge)
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
			if IsPartitionedSQSKey(key) {
				// routeKey collapses every partitioned SQS row to the single
				// global SQS route key, so a migrated interval covering that
				// key would look like it owns every partition -- including the
				// ones the resolver still routes to this group, whose rows the
				// export never claimed. Deleting those loses messages that are
				// still being served from here. Apply cannot ask the resolver
				// which partitions moved: it is process-local config
				// (--sqsFifoPartitionMap), not replicated, so a per-node answer
				// would make apply diverge across replicas. Catalog route
				// cleanup therefore deletes only catalog-routed data and leaves
				// the resolver-owned keyspace alone; retiring a partition's
				// rows belongs to the resolver's own migration path.
				return false
			}
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

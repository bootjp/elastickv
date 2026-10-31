package store

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/emirpasic/gods/maps/treemap"
)

// ExternalSnapshotRestorer rebases a snapshot without importing migration
// evidence from a different Raft lineage. Restore remains the same-lineage API.
type ExternalSnapshotRestorer interface {
	RestoreExternal(context.Context, io.Reader) error
}

var ErrExternalSnapshotMigrationState = errors.New("external snapshot contains migration state")

func (s *mvccStore) RestoreExternal(ctx context.Context, r io.Reader) error {
	return s.restoreSnapshot(&externalSnapshotReader{ctx: ctx, r: r}, func(tree *treemap.Map, meta mvccSnapshotMigrationMetadata) error {
		return validateExternalMVCCTree(ctx, tree, meta)
	})
}

func (s *pebbleStore) RestoreExternal(ctx context.Context, r io.Reader) error {
	return s.restoreSnapshot(&externalSnapshotReader{ctx: ctx, r: r}, func(dir string) error {
		return validateExternalPebbleDB(ctx, dir)
	})
}

type externalSnapshotReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *externalSnapshotReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, errors.WithStack(err)
	}
	n, err := r.r.Read(p)
	return n, errors.WithStack(err)
}

func validateRestoreTempDB(dir string, validate func(string) error) error {
	if validate == nil {
		return nil
	}
	if err := validate(dir); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	return nil
}

func validateExternalMVCCTree(ctx context.Context, tree *treemap.Map, meta mvccSnapshotMigrationMetadata) error {
	if len(meta.raftControl) != 0 || meta.controlAppliedIndex != 0 {
		return errors.WithStack(ErrExternalSnapshotControlState)
	}
	if len(meta.acks)+len(meta.floors)+len(meta.promotions)+len(meta.readiness)+len(meta.retired)+len(meta.ownership) != 0 || partitionHasEvidence(meta.partitions) {
		return errors.WithStack(ErrExternalSnapshotMigrationState)
	}
	iter := tree.Iterator()
	for iter.Next() {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		key, _ := iter.Key().([]byte)
		if err := validateExternalTypedMigrationKey(key); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	tree.Remove(MigrationConfigurationIndexKey())
	return nil
}

func validateExternalPebbleDB(ctx context.Context, dir string) (retErr error) {
	opts, cache := defaultPebbleOptionsWithCache(false)
	db, err := pebble.Open(dir, opts)
	if err != nil {
		cache.Unref()
		return errors.WithStack(err)
	}
	defer cache.Unref()
	defer func() {
		retErr = errors.Join(retErr, errors.WithStack(db.Close()), errors.WithStack(ctx.Err()))
	}()
	if err := validateExternalPebbleRows(ctx, db); err != nil {
		return err
	}
	return sanitizeExternalConfigurationEpoch(ctx, db)
}

func validateExternalPebbleRows(ctx context.Context, db *pebble.DB) error {
	iter, err := db.NewIter(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer iter.Close()
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		if err := validateExternalPebbleRow(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return errors.Join(errors.WithStack(iter.Error()), errors.WithStack(ctx.Err()))
}

func validateExternalPebbleRow(key, value []byte) error {
	if IsRaftControlMetadataKey(key) {
		return errors.WithStack(ErrExternalSnapshotControlState)
	}
	if isMigrationMetadataKey(key) {
		return validateExternalMigrationRow(key, value)
	}
	if isPebbleMetaKey(key) {
		return nil
	}
	userKey, _ := decodeKeyView(key)
	if userKey == nil {
		return errors.New("invalid external snapshot MVCC key")
	}
	// Decoded MVCC keys are logical keys, not the raw metadata namespace.
	// In particular, _migack/_mighlc/_migpromote remain ordinary user keys.
	return validateExternalTypedMigrationKey(userKey)
}

func validateExternalMigrationRow(key, value []byte) error {
	switch {
	case IsMigrationOwnershipPartitionMetadataKey(key):
		return validateExternalPartitionRow(value)
	case bytes.Equal(key, migrationOwnershipMetaKeyBytes):
		states, err := decodeMigrationOwnership(value)
		if err != nil {
			return err
		}
		return externalMigrationCollection(len(states), true)
	case bytes.Equal(key, migrationRetiredMetaKeyBytes):
		states, ok := decodeMigrationRetiredJobs(value)
		return externalMigrationCollection(len(states), ok)
	case bytes.Equal(key, migrationAckMetaKeyBytes):
		states, ok := decodeMigrationImportAcks(value)
		return externalMigrationCollection(len(states), ok)
	case bytes.Equal(key, migrationHLCFloorMetaKeyBytes):
		states, ok := decodeMigrationHLCFloors(value)
		return externalMigrationCollection(len(states), ok)
	case bytes.Equal(key, migrationPromoteMetaKeyBytes):
		states, ok := decodeMigrationPromotionStates(value)
		return externalMigrationCollection(len(states), ok)
	case isTargetReadinessRecordKey(key):
		_, ok := decodeTargetStagedReadinessState(value)
		return externalMigrationCollection(1, ok)
	default:
		return validateExternalTypedMigrationKey(key)
	}
}

func validateExternalPartitionRow(value []byte) error {
	state, err := decodeMigrationOwnershipPartitions(value)
	if err != nil {
		return err
	}
	return externalMigrationCollection(len(state.Partitions)+len(state.Receipts), true)
}

func externalMigrationCollection(size int, valid bool) error {
	if !valid {
		return errors.New("corrupt external snapshot migration metadata")
	}
	if size != 0 {
		return errors.WithStack(ErrExternalSnapshotMigrationState)
	}
	return nil
}

func validateExternalTypedMigrationKey(key []byte) error {
	if IsRaftControlMetadataKey(key) {
		return errors.WithStack(ErrExternalSnapshotControlState)
	}
	// Keep these logical-key guards aligned with distribution's migration-only
	// control namespaces. Raw store metadata is decoded separately above.
	for _, prefix := range []string{"!dist|migstage|", "!migstage|", "!migwrite|", "!migfence|", "!dist|meta|ownership_partition/"} {
		if bytes.HasPrefix(key, []byte(prefix)) {
			return errors.WithStack(ErrExternalSnapshotMigrationState)
		}
	}
	return nil
}

var _ ExternalSnapshotRestorer = (*mvccStore)(nil)
var _ ExternalSnapshotRestorer = (*pebbleStore)(nil)

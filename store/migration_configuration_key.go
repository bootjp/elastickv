package store

import (
	"bytes"
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// MigrationConfigurationIndexKey is the durable Raft configuration epoch, not
// the snapshot's data applied index. Return private bytes for each caller.
func MigrationConfigurationIndexKey() []byte {
	return []byte("!txn|meta|migration_configuration_index")
}

// Only the unpublished external-restore tempDB is writable here. An exact
// decoded-key comparison preserves adjacent logical keys sharing this prefix.
func sanitizeExternalConfigurationEpoch(ctx context.Context, db *pebble.DB) error {
	key := MigrationConfigurationIndexKey()
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: key, UpperBound: PrefixScanEnd(key)})
	if err != nil {
		return errors.WithStack(err)
	}
	defer iter.Close()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		logicalKey, _ := decodeKeyView(iter.Key())
		if !bytes.Equal(logicalKey, key) {
			continue
		}
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return errors.WithStack(err)
		}
		if snapshotBatchShouldFlush(batch) {
			if err := flushSnapshotBatch(db, &batch, pebble.Sync); err != nil {
				return err
			}
		}
	}
	if err := errors.Join(errors.WithStack(iter.Error()), errors.WithStack(ctx.Err())); err != nil {
		return err
	}
	return errors.WithStack(batch.Commit(pebble.Sync))
}

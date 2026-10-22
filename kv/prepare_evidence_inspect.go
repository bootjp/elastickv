package kv

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// InspectPrepareEvidence must follow an applied leader fence. The durable
// timestamp floor prevents creation of old locks during the paged scan.
func InspectPrepareEvidence(ctx context.Context, st store.MVCCStore, floor uint64) (enabled, unproven bool, fencedFloor uint64, err error) {
	enabled, err = PrepareEvidenceEnabled(ctx, st)
	if err != nil {
		return false, false, 0, err
	}
	fencedFloor, err = readPrepareEvidenceFenceFloor(ctx, st)
	if err != nil {
		return false, false, 0, err
	}
	if !enabled || fencedFloor < floor {
		return enabled, false, fencedFloor, nil
	}
	scanFloor := floor
	if floor == 0 {
		scanFloor = ^uint64(0)
	}
	for _, prefix := range [][]byte{txnLockPrefixBytes, distribution.MigrationStagedDataScanPrefix()} {
		unproven, err = unprovenPrepareEvidenceInPrefix(ctx, st, prefix, scanFloor)
		if err != nil || unproven {
			return enabled, unproven, fencedFloor, err
		}
	}
	return enabled, false, fencedFloor, nil
}

func readPrepareEvidenceFenceFloor(ctx context.Context, st store.MVCCStore) (uint64, error) {
	value, err := st.GetAt(ctx, prepareEvidenceTimestampFloorKey, ^uint64(0))
	if errors.Is(err, store.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if len(value) != backupTimestampFloorValueSize {
		return 0, errors.Wrap(ErrTxnInvalidMeta, "invalid prepare admission fence")
	}
	return binary.BigEndian.Uint64(value), nil
}

func unprovenPrepareEvidenceInPrefix(ctx context.Context, st store.MVCCStore, prefix []byte, floor uint64) (bool, error) {
	end := store.PrefixScanEnd(prefix)
	for cursor := bytes.Clone(prefix); ; {
		if err := ctx.Err(); err != nil {
			return false, errors.WithStack(err)
		}
		rows, err := st.ScanAt(ctx, cursor, end, txnLockBatchScanPageSize, ^uint64(0))
		if err != nil {
			return false, errors.WithStack(err)
		}
		if len(rows) == 0 {
			return false, nil
		}
		for _, row := range rows {
			blocked, err := unprovenPrepareEvidenceRow(row.Key, row.Value, floor)
			if err != nil || blocked {
				return blocked, err
			}
		}
		cursor = nextScanCursor(rows[len(rows)-1].Key)
	}
}

func unprovenPrepareEvidenceRow(key, value []byte, floor uint64) (bool, error) {
	if _, raw, staged := distribution.MigrationStagedDataKeyParts(key); staged {
		key = raw
	}
	if !bytes.HasPrefix(key, txnLockPrefixBytes) {
		return false, nil
	}
	lock, err := decodeTxnLock(value)
	if err != nil {
		return false, errors.Wrap(err, "inspect prepare evidence lock")
	}
	return lock.StartTS <= floor && lock.CommitTS == 0, nil
}

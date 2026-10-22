package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
)

func (c *ShardedCoordinator) prepareEvidenceMetas(ctx context.Context, primaryKey []byte, commitTS uint64, ids []uint64) (map[uint64]*pb.Mutation, error) {
	metas := make(map[uint64]*pb.Mutation, len(ids))
	needsReservation := false
	for _, id := range ids {
		meta, err := c.prepareEvidenceTxnMeta(ctx, primaryKey, commitTS, []uint64{id})
		if err != nil {
			return nil, err
		}
		needsReservation = needsReservation || meta.PrepareCommitTS != 0
		metas[id] = &pb.Mutation{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(meta)}
	}
	if needsReservation {
		if err := reservePrepareEvidenceTimestamp(ctx, c.tsAllocator, commitTS); err != nil {
			return nil, err
		}
	}
	return metas, nil
}

func (c *ShardedCoordinator) prepareEvidenceMeta(ctx context.Context, primaryKey []byte, commitTS uint64, groupIDs []uint64) (*pb.Mutation, error) {
	meta, err := c.prepareEvidenceTxnMeta(ctx, primaryKey, commitTS, groupIDs)
	if err != nil {
		return nil, err
	}
	if meta.PrepareCommitTS != 0 {
		if err := reservePrepareEvidenceTimestamp(ctx, c.tsAllocator, commitTS); err != nil {
			return nil, err
		}
	}
	return &pb.Mutation{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(meta)}, nil
}

func (c *ShardedCoordinator) prepareEvidenceTxnMeta(ctx context.Context, primaryKey []byte, commitTS uint64, groupIDs []uint64) (TxnMeta, error) {
	meta := TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms, PrepareCommitTS: commitTS}
	for _, id := range groupIDs {
		group, err := c.txnGroupForID(id)
		if err != nil {
			return TxnMeta{}, err
		}
		if group.Store == nil {
			meta.PrepareCommitTS = 0
			break
		}
		enabled, err := PrepareEvidenceEnabled(ctx, group.Store)
		if err != nil {
			return TxnMeta{}, err
		}
		if !enabled {
			meta.PrepareCommitTS = 0
			break
		}
	}
	return meta, nil
}

// Reservation advances the durable TSO range without changing values already
// stamped by the coordinator. It is needed before Phase D too: a proof must
// not bind a future timestamp beyond the activation barrier's allocation floor.
func reservePrepareEvidenceTimestamp(ctx context.Context, allocator TimestampAllocator, timestamp uint64) error {
	if allocator == nil {
		return ErrTSOAllocatorRequired
	}
	if _, ok := allocator.(DurableTimestampValidator); !ok {
		return ErrTSOProtocolUnsupported
	}
	_, _, required, err := phaseDTimestampValidator(allocator)
	if err != nil {
		return err
	}
	if required {
		return ValidateDurablePersistenceTimestamp(ctx, allocator, timestamp, "validate prepared commit timestamp")
	}
	_, err = nextTimestampAfterFromAllocator(ctx, allocator, timestamp, "reserve prepared commit timestamp bound")
	return err
}

// ReservePrepareEvidenceTimestamp protects the internal PREPARE ingress using
// the same durable reservation contract as the local coordinator.
func ReservePrepareEvidenceTimestamp(ctx context.Context, allocator TimestampAllocator, timestamp uint64) error {
	return reservePrepareEvidenceTimestamp(ctx, allocator, timestamp)
}

func (c *ShardedCoordinator) bindTxnLogPrepareEvidence(ctx context.Context, logs []*pb.Request, commitTS uint64, ids []uint64) ([]*pb.Request, error) {
	meta, _, err := extractTxnMeta(logs[0].Mutations)
	if err != nil {
		return nil, err
	}
	mutation, err := c.prepareEvidenceMeta(ctx, meta.PrimaryKey, commitTS, ids)
	if err != nil {
		return nil, err
	}
	logs[0].Mutations[0] = mutation
	return logs, nil
}

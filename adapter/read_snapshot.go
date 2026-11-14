package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// beginFencedReadSnapshot owns the ordering of the committed-log barrier and
// snapshot selection. Callers cannot supply a previously sampled timestamp.
// A failed distributed fence never falls back to a follower's local watermark.
// Explicit historical reads and DynamoDB eventual reads have separate paths.
func beginFencedReadSnapshot(ctx context.Context, coord kv.Coordinator, st store.MVCCStore, label string, keys ...[]byte) (kv.ReadTimestamp, error) {
	ts, err := fencedCommittedReadTS(ctx, coord, st, keys...)
	if err != nil {
		return kv.ReadTimestamp{}, errors.Wrap(err, label+": fence snapshot")
	}
	if coord != nil && coord.Clock() != nil {
		coord.Clock().Observe(ts)
	}
	stamp, err := kv.BeginReadTimestampThrough(ctx, coord, ts, label)
	return stamp, errors.WithStack(err)
}

func fencedCommittedReadTS(ctx context.Context, coord kv.Coordinator, st store.MVCCStore, keys ...[]byte) (uint64, error) {
	var ts uint64
	var err error
	if floor, ok := st.(keyedCommittedTimestampFloorProvider); ok && len(keys) > 0 {
		ts, err = floor.CommittedTimestampFloorForKeys(ctx, keys)
	} else if floor, ok := st.(committedTimestampFloorProvider); ok {
		ts, err = floor.GlobalCommittedTimestampFloor(ctx)
	} else {
		if coord == nil {
			return 0, errors.New("read snapshot: coordinator unavailable")
		}
		_, err = coord.LinearizableRead(ctx)
		if err == nil && st != nil {
			ts = st.LastCommitTS()
		}
	}
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return max(ts, 1), nil
}

type globalLastCommitTSProvider interface {
	GlobalLastCommitTS(ctx context.Context) uint64
}

type committedTimestampFloorProvider interface {
	GlobalCommittedTimestampFloor(ctx context.Context) (uint64, error)
}

type keyedCommittedTimestampFloorProvider interface {
	CommittedTimestampFloorForKeys(ctx context.Context, keys [][]byte) (uint64, error)
}

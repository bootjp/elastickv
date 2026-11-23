package kv

import (
	"context"
	"slices"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// A read index prevents an intervening apply from hiding beneath another
// group's larger watermark. The timestamp is the group's snapshot boundary.
type timedReadPosition struct {
	group, index, timestamp uint64
}

type timedPositionReader interface {
	timedReadPositions(context.Context) ([]timedReadPosition, error)
}

// TimedReadSnapshot pairs the transaction's MVCC boundary with the leader's
// wall time. A multi-group reader must Validate before publishing its result.
type TimedReadSnapshot struct {
	ReadTimestamp
	Instant   time.Time
	reader    timedPositionReader
	positions []timedReadPosition
}

// Validate catches a later apply in a group whose watermark was below the
// global snapshot: a newly applied version there can still sort below StartTS.
func (s TimedReadSnapshot) Validate(ctx context.Context) error {
	if s.reader == nil {
		return nil
	}
	positions, err := s.reader.timedReadPositions(ctx)
	if err != nil {
		return err
	}
	return unchangedTimedPositions(s.positions, positions)
}

func unchangedTimedPositions(before, after []timedReadPosition) error {
	if !slices.Equal(before, after) {
		return errors.Wrap(store.ErrWriteConflict, "data changed during timed snapshot")
	}
	return nil
}

// BeginTimedReadTimestamp selects an applied snapshot and the default data
// leader's wall time at a common instant. Collect each group's position before
// and after the clock sample. A change is retryable before any script dispatch.
func BeginTimedReadTimestamp(ctx context.Context, coord Coordinator, st store.MVCCStore, label string) (TimedReadSnapshot, error) {
	r, ok := ReadRequestFromContext(ctx)
	if !ok {
		r = NewReadRequest(ctx, coord, st, nil)
		ctx = r.Context()
		defer r.Close()
	}
	if !r.MatchesStore(st) {
		return TimedReadSnapshot{}, errors.WithStack(ErrReadSnapshotScope)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validate(ctx); err != nil {
		return TimedReadSnapshot{}, err
	}
	positions, instant, err := r.timedCommittedBoundary(ctx)
	if err != nil {
		return TimedReadSnapshot{}, errors.Wrap(err, label)
	}
	ts := uint64(1)
	for _, pos := range positions {
		ts = max(ts, pos.timestamp)
	}
	stamp, err := r.bindAppliedBoundary(ctx, label, ts)
	if err != nil {
		return TimedReadSnapshot{}, err
	}
	r.remember(r.register(stamp))
	snapshot := TimedReadSnapshot{ReadTimestamp: stamp, Instant: instant}
	if reader, sharded := r.st.(timedPositionReader); sharded && len(positions) > 1 {
		snapshot.reader = reader
		snapshot.positions = positions
	}
	return snapshot, nil
}

func (r *ReadRequest) timedCommittedBoundary(ctx context.Context) ([]timedReadPosition, time.Time, error) {
	before, err := r.timedReadPositions(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	instant, err := LeaseReadTimeThrough(r.coord, ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	after, err := r.timedReadPositions(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	if err := unchangedTimedPositions(before, after); err != nil {
		return nil, time.Time{}, err
	}
	return after, instant, nil
}

func (r *ReadRequest) timedReadPositions(ctx context.Context) ([]timedReadPosition, error) {
	if s, ok := r.st.(timedPositionReader); ok {
		return s.timedReadPositions(ctx)
	}
	if r.coord == nil {
		return nil, errors.WithStack(ErrReadSnapshotUnissued)
	}
	index, err := r.coord.LinearizableRead(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var ts uint64
	switch st := r.st.(type) {
	case *LeaderRoutedStore:
		ts, err = st.GlobalCommittedTimestampFloor(ctx)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	case interface {
		GlobalCommittedTimestampFloor(context.Context) (uint64, error)
	},
		interface {
			CommittedTimestampFloorForKeys(context.Context, [][]byte) (uint64, error)
		}:
		return nil, errors.Wrap(ErrReadSnapshotUnissued, "group positions unavailable")
	default:
		ts = st.LastCommitTS()
	}
	return []timedReadPosition{{index: index, timestamp: ts}}, nil
}

func (s *ShardStore) timedReadPositions(ctx context.Context) ([]timedReadPosition, error) {
	ids, err := s.tsoCommitFloorGroupIDs()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, errors.WithStack(ErrTSOCommitFloorUnavailable)
	}
	if len(ids) == 1 {
		pos, err := s.timedGroupPosition(ctx, ids[0])
		return []timedReadPosition{pos}, err
	}
	positions := make([]timedReadPosition, len(ids))
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(maxTSOCommitFloorConcurrency)
	for i, id := range ids {
		eg.Go(func() error {
			pos, err := s.timedGroupPosition(ctx, id)
			positions[i] = pos
			return err
		})
	}
	return positions, errors.WithStack(eg.Wait())
}

// Use the leader's ReadIndex rather than its cached lease position so the
// second collection covers every write acknowledged before the clock sample.
func (s *ShardStore) timedGroupPosition(ctx context.Context, id uint64) (timedReadPosition, error) {
	g := s.groups[id]
	engine := engineForGroup(g)
	if isLeaderEngine(engine) {
		index, err := linearizableReadEngineCtx(ctx, engine)
		if err != nil {
			return timedReadPosition{}, errors.Wrapf(err, "fence data group %d", id)
		}
		return timedReadPosition{group: id, index: index, timestamp: g.Store.LastCommitTS()}, nil
	}
	if g.leaderRead == nil {
		return timedReadPosition{}, errors.WithStack(ErrLeaderNotFound)
	}
	result, err := g.leaderRead(ctx)
	if err != nil {
		return timedReadPosition{}, errors.Wrapf(err, "fence data group %d", id)
	}
	return timedReadPosition{group: id, index: result.appliedIndex, timestamp: result.lastCommitTS}, nil
}

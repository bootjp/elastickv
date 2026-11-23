package kv

import (
	"context"
	"slices"
	"time"

	pb "github.com/bootjp/elastickv/proto"
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
	stamp     ReadTimestamp
	Instant   time.Time
	reader    timedPositionReader
	positions []timedReadPosition
	shards    *ShardStore
	version   uint64
}

func (s TimedReadSnapshot) Timestamp() uint64 { return s.stamp.Timestamp() }

// Dispatch validates the script's dependencies at Raft apply using a start
// timestamp already reached by each participant at the wall-clock sample.
// The read watermark of another group is not an OCC boundary for these keys.
func (s TimedReadSnapshot) Dispatch(ctx context.Context, coord Coordinator, req *OperationGroup[OP]) (*CoordinateResponse, error) {
	if req == nil || !req.IsTxn || req.StartTS != s.Timestamp() {
		return nil, errors.WithStack(ErrReadSnapshotScope)
	}
	if len(s.positions) == 0 {
		return DispatchWithReadTimestamp(s.stamp.WithDispatchVoucher(ctx), coord, req)
	}
	floor, version, err := s.mutationFloor(ctx, coord, req)
	if err != nil {
		return nil, err
	}
	stamp := s.stamp
	if floor != stamp.Timestamp() {
		stamp, err = BeginReadTimestampThrough(ctx, coord, floor, "timed mutation OCC boundary")
		if err != nil {
			return nil, err
		}
	}
	operation := *req
	operation.StartTS = floor
	operation.ObservedRouteVersion = version
	return DispatchWithReadTimestamp(stamp.WithDispatchVoucher(ctx), coord, &operation)
}

// A single participant can use its positive watermark directly. Multiple
// participants share a transaction identity and need the same OCC timestamp.
// Establish the read cut on lagging participants, then run Lua afresh so a
// change that landed before the barrier is read by the new attempt.
func (s TimedReadSnapshot) mutationFloor(ctx context.Context, coord Coordinator, req *OperationGroup[OP]) (uint64, uint64, error) {
	version := EncodeObservedRouteVersion(s.version)
	if req.ObservedRouteVersion != 0 && req.ObservedRouteVersion != version {
		return 0, 0, errors.WithStack(ErrReadSnapshotScope)
	}
	if s.shards.ReadFenceRouteVersion() != s.version {
		return 0, 0, errors.Wrap(store.ErrWriteConflict, "timed mutation route changed")
	}
	if !slices.ContainsFunc(s.positions, func(p timedReadPosition) bool { return p.timestamp < s.Timestamp() }) {
		return s.Timestamp(), version, nil
	}
	positions := s.positions
	if version != 0 {
		groups := s.pointParticipants(req)
		if len(groups) > 0 {
			positions = slices.DeleteFunc(slices.Clone(positions), func(pos timedReadPosition) bool { return !groups[pos.group] })
			if len(positions) != len(groups) {
				return 0, 0, errors.Wrap(store.ErrWriteConflict, "timed mutation participant was not sampled")
			}
		}
	}
	if len(positions) == 1 && positions[0].timestamp > 0 {
		return positions[0].timestamp, version, nil
	}
	return s.commonMutationFloor(ctx, coord, positions, version)
}

// If an operation can't be pinned to point routes, cover every sampled group.
// This also handles catalog version zero during a rolling upgrade.
func (s TimedReadSnapshot) pointParticipants(req *OperationGroup[OP]) map[uint64]bool {
	groups := make(map[uint64]bool)
	for _, elem := range req.Elems {
		if elem == nil || elem.GroupID != 0 || (elem.Op != Put && elem.Op != Del) {
			return nil
		}
		id, ok := s.pointParticipant(elem.Key)
		if !ok {
			return nil
		}
		groups[id] = true
	}
	for _, key := range req.ReadKeys {
		id, ok := s.pointParticipant(key)
		if !ok {
			return nil
		}
		groups[id] = true
	}
	return groups
}

func (s TimedReadSnapshot) commonMutationFloor(ctx context.Context, coord Coordinator, positions []timedReadPosition, version uint64) (uint64, uint64, error) {
	cut := s.Timestamp()
	lagging := slices.ContainsFunc(positions, func(p timedReadPosition) bool { return p.timestamp < cut })
	if !lagging {
		return cut, version, nil
	}
	alloc, _ := coordinatorTimestampAllocator(coord)
	// A previously applied cut can predate Phase D. Only new timestamp
	// allocations may create durable entries in the current epoch.
	if err := ValidateDurablePersistenceTimestamp(ctx, alloc, cut, "timed mutation barrier"); err != nil {
		if !errors.Is(err, ErrTSOTimestampPrePhaseD) {
			return 0, 0, err
		}
		cut, err = NextTimestampAfterThrough(ctx, coord, cut, "timed mutation barrier")
		if err != nil {
			return 0, 0, err
		}
	}
	if !ReadWatermarkAdvanceAdmissible(time.Now().UnixMilli(), cut) {
		return 0, 0, errors.WithStack(ErrInvalidWatermarkAdvance)
	}
	eg, groupCtx := errgroup.WithContext(ctx)
	eg.SetLimit(maxTSOCommitFloorConcurrency)
	for _, pos := range positions {
		if pos.timestamp < cut {
			eg.Go(func() error { return s.advanceMutationParticipant(groupCtx, alloc, pos.group, cut) })
		}
	}
	if err := eg.Wait(); err != nil {
		return 0, 0, errors.WithStack(err)
	}
	return 0, 0, errors.Wrap(store.ErrWriteConflict, "re-read after advancing timed mutation participants")
}

// An ordinary raw Raft request with no mutations advances LastCommitTS but
// writes no key. The legacy wire format, normal write admission, and the
// group's leader proxy also work before the read-watermark opcode is enabled.
func (s TimedReadSnapshot) advanceMutationParticipant(ctx context.Context, alloc TimestampAllocator, id, cut uint64) error {
	if s.shards == nil {
		return errors.WithStack(ErrReadSnapshotUnissued)
	}
	g := s.shards.groups[id]
	if g == nil || g.Txn == nil {
		return errors.WithStack(ErrLeaderNotFound)
	}
	reqs := []*pb.Request{{Ts: cut}}
	if err := StampTimestampAdmission(ctx, alloc, g.Store, reqs); err != nil {
		return err
	}
	resp, err := g.Txn.Commit(ctx, reqs)
	if err != nil {
		return errors.WithStack(err)
	}
	if resp == nil || resp.CommitIndex == 0 {
		return errors.WithStack(ErrWatermarkAdvanceReceiptUnavailable)
	}
	return nil
}

func (s TimedReadSnapshot) pointParticipant(key []byte) (uint64, bool) {
	if resolver := s.shards.partitionResolver; resolver != nil {
		if _, resolved := resolver.ResolveGroup(key); resolved || resolver.RecognisesPartitionedKey(key) {
			return 0, false
		}
	}
	route, _, version, found := s.shards.routeAndGroupForKeyWithVersion(key)
	return route.GroupID, found && version == s.version
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
	shards, _ := st.(*ShardStore)
	version := shards.ReadFenceRouteVersion()
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
	snapshot := TimedReadSnapshot{stamp: stamp, Instant: instant}
	if reader, sharded := r.st.(timedPositionReader); sharded && len(positions) > 1 {
		snapshot.reader = reader
		snapshot.positions = positions
		snapshot.shards = shards
		snapshot.version = version
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

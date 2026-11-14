package kv

import (
	"bytes"
	"context"
	"reflect"
	"sync"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrReadSnapshotScope = errors.New("read snapshot does not belong to this request")
var ErrReadRequestClosed = errors.New("read request is closed")
var ErrReadSnapshotKeyScope = errors.New("read key is outside the fenced snapshot scope")
var ErrReadSnapshotUnissued = errors.New("read snapshot was not issued by this request")

type readRequestKey struct{}

// ReadSnapshot is a request-local capability, not an integer timestamp. Its
// zero value is invalid. Copying a snapshot does not extend its owner's lifetime.
type ReadSnapshot struct {
	owner     *ReadRequest
	timestamp ReadTimestamp
	keys      [][]byte
	groups    map[uint64]struct{}
}

// Timestamp exposes the value for transaction metadata and diagnostics only.
// Get and Scan require the capability itself.
func (s ReadSnapshot) Timestamp() uint64                   { return s.timestamp.Timestamp() }
func (s ReadSnapshot) TransactionTimestamp() ReadTimestamp { return s.timestamp }

// ReadRequest owns snapshots and joins its reads before releasing their pins.
// Create one at ingress and defer Close; use Context for every derived context.
// A later request always has a different identity, even with the same parent.
type ReadRequest struct {
	mu         sync.RWMutex
	childrenMu sync.Mutex
	parent     *ReadRequest
	children   map[*ReadRequest]struct{}
	closeDone  chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	coord      Coordinator
	st         store.MVCCStore
	tracker    *ActiveTimestampTracker
	closed     bool
	issued     map[uint64]ReadSnapshot
	pins       []*ActiveTimestampToken
}

func NewReadRequest(ctx context.Context, coord Coordinator, st store.MVCCStore, tracker *ActiveTimestampTracker) *ReadRequest {
	parent, cancel := context.WithCancel(ctx)
	r := &ReadRequest{cancel: cancel, coord: coord, st: st, tracker: tracker, issued: make(map[uint64]ReadSnapshot), children: make(map[*ReadRequest]struct{}), closeDone: make(chan struct{})}
	r.ctx = context.WithValue(parent, readRequestKey{}, r)
	if enclosing, ok := ReadRequestFromContext(ctx); ok {
		enclosing.childrenMu.Lock()
		if enclosing.ctx.Err() == nil {
			enclosing.children[r] = struct{}{}
			r.parent = enclosing
		} else {
			cancel()
		}
		enclosing.childrenMu.Unlock()
	}
	return r
}

func (r *ReadRequest) Context() context.Context { return r.ctx }
func ReadRequestFromContext(ctx context.Context) (*ReadRequest, bool) {
	if ctx == nil {
		return nil, false
	}
	r, ok := ctx.Value(readRequestKey{}).(*ReadRequest)
	return r, ok
}

// MatchesStore prevents a request's snapshot from being resolved against a
// different adapter/store. Store implementations must have stable identities.
func (r *ReadRequest) MatchesStore(st any) bool {
	return st != nil && r.st != nil && reflect.TypeOf(st).Comparable() && reflect.TypeOf(r.st).Comparable() && st == r.st
}

func (r *ReadRequest) Close() {
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Lock()
	if r.closed {
		done := r.closeDone
		r.mu.Unlock()
		if done != nil {
			<-done
		}
		return
	}
	r.closed = true
	if r.closeDone == nil {
		r.closeDone = make(chan struct{})
	}
	pins := r.pins
	r.pins = nil
	r.issued = nil
	r.mu.Unlock()
	r.childrenMu.Lock()
	children := r.children
	r.children = nil
	r.childrenMu.Unlock()
	for child := range children {
		child.Close()
	}
	for _, pin := range pins {
		pin.Release()
	}
	if r.parent != nil {
		r.parent.childrenMu.Lock()
		delete(r.parent.children, r)
		r.parent.childrenMu.Unlock()
	}
	close(r.closeDone)
}

func (r *ReadRequest) validate(ctx context.Context) error {
	if r.closed {
		return errors.WithStack(ErrReadRequestClosed)
	}
	owner, ok := ReadRequestFromContext(ctx)
	if !ok || owner != r {
		return errors.WithStack(ErrReadSnapshotScope)
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	if err := r.ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Latest fences the affected groups before sampling their applied watermark.
// There is no timestamp or timestamp-selection callback in this interface.
func (r *ReadRequest) Latest(ctx context.Context, label string, keys ...[]byte) (ReadSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validate(ctx); err != nil {
		return ReadSnapshot{}, err
	}
	keys = requestFenceKeys(r.st, keys)
	ts, err := requestCommittedFloor(ctx, r.coord, r.st, keys)
	if err != nil {
		return ReadSnapshot{}, errors.Wrap(err, label+": read fence")
	}
	stamp, err := r.bindAppliedBoundary(ctx, label, ts)
	if err != nil {
		return ReadSnapshot{}, err
	}
	snapshot := r.scopedSnapshot(stamp, keys)
	r.remember(snapshot)
	return snapshot, nil
}

func requestCommittedFloor(ctx context.Context, coord Coordinator, st store.MVCCStore, keys [][]byte) (uint64, error) {
	var ts uint64
	var err error
	switch floor := st.(type) {
	case interface {
		CommittedTimestampFloorForKeys(context.Context, [][]byte) (uint64, error)
	}:
		if len(keys) > 0 {
			ts, err = floor.CommittedTimestampFloorForKeys(ctx, keys)
			break
		}
		fallthroughStore, ok := st.(interface {
			GlobalCommittedTimestampFloor(context.Context) (uint64, error)
		})
		if !ok {
			return 0, errors.WithStack(ErrReadSnapshotUnissued)
		}
		ts, err = fallthroughStore.GlobalCommittedTimestampFloor(ctx)
	case interface {
		GlobalCommittedTimestampFloor(context.Context) (uint64, error)
	}:
		ts, err = floor.GlobalCommittedTimestampFloor(ctx)
	default:
		return requestLocalCommittedFloor(ctx, coord, st)
	}
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return max(ts, 1), nil
}

func requestLocalCommittedFloor(ctx context.Context, coord Coordinator, st store.MVCCStore) (uint64, error) {
	if coord == nil || st == nil {
		return 0, errors.WithStack(ErrReadSnapshotUnissued)
	}
	if _, err := coord.LinearizableRead(ctx); err != nil {
		return 0, errors.WithStack(err)
	}
	if err := ctx.Err(); err != nil {
		return 0, errors.WithStack(err)
	}
	return max(st.LastCommitTS(), 1), nil
}

func (r *ReadRequest) register(stamp ReadTimestamp) ReadSnapshot {
	return ReadSnapshot{owner: r, timestamp: stamp}
}

func (r *ReadRequest) remember(snapshot ReadSnapshot) {
	old, exists := r.issued[snapshot.Timestamp()]
	if !exists {
		r.issued[snapshot.Timestamp()] = snapshot
		if r.tracker != nil {
			r.pins = append(r.pins, r.tracker.Pin(snapshot.Timestamp()))
		}
		return
	}
	if (len(old.keys) == 0 && len(old.groups) == 0) || (len(snapshot.keys) == 0 && len(snapshot.groups) == 0) {
		snapshot.keys = nil
		snapshot.groups = nil
		r.issued[snapshot.Timestamp()] = snapshot
		return
	}
	snapshot.keys = append(append([][]byte(nil), old.keys...), snapshot.keys...)
	merged := make(map[uint64]struct{}, len(old.groups)+len(snapshot.groups))
	for group := range old.groups {
		merged[group] = struct{}{}
	}
	for group := range snapshot.groups {
		merged[group] = struct{}{}
	}
	snapshot.groups = merged
	r.issued[snapshot.Timestamp()] = snapshot
}

// Historical is explicitly separate from Latest. It is for an existing MVCC
// version supplied by a historical RPC or transaction protocol, not freshness.
func (r *ReadRequest) Historical(ctx context.Context, ts uint64) (ReadSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validate(ctx); err != nil {
		return ReadSnapshot{}, err
	}
	snapshot := r.register(ReadTimestamp{timestamp: ts})
	r.remember(snapshot)
	return snapshot, nil
}

// Issued resolves legacy transaction metadata without granting authority to an
// arbitrary number. It never creates a snapshot, including when lookup fails.
func (r *ReadRequest) Issued(ctx context.Context, ts uint64) (ReadSnapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.validate(ctx); err != nil {
		return ReadSnapshot{}, err
	}
	s, ok := r.issued[ts]
	if !ok {
		return ReadSnapshot{}, errors.WithStack(ErrReadSnapshotUnissued)
	}
	return s, nil
}

func (r *ReadRequest) validateSnapshot(ctx context.Context, s ReadSnapshot) error {
	if err := r.validate(ctx); err != nil {
		return err
	}
	if s.owner != r {
		return errors.WithStack(ErrReadSnapshotScope)
	}
	return nil
}

func (r *ReadRequest) Get(ctx context.Context, s ReadSnapshot, key []byte) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.validateSnapshot(ctx, s); err != nil {
		return nil, err
	}
	if err := r.validateKeyScope(s, key); err != nil {
		return nil, err
	}
	value, err := r.st.GetAt(ctx, key, s.Timestamp())
	if scopeErr := r.validate(ctx); scopeErr != nil {
		return nil, scopeErr
	}
	return value, errors.WithStack(err)
}

func (r *ReadRequest) Scan(ctx context.Context, s ReadSnapshot, start, end []byte, limit int) ([]*store.KVPair, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.validateSnapshot(ctx, s); err != nil {
		return nil, err
	}
	if err := r.validateRangeScope(s, start, end, 0); err != nil {
		return nil, err
	}
	values, err := r.st.ScanAt(ctx, start, end, limit, s.Timestamp())
	if scopeErr := r.validate(ctx); scopeErr != nil {
		return nil, scopeErr
	}
	return values, errors.WithStack(err)
}

func (r *ReadRequest) validateKeyScope(s ReadSnapshot, key []byte) error {
	if len(s.keys) == 0 && len(s.groups) == 0 {
		return nil
	}
	if resolver, ok := r.coord.(interface{ EngineGroupIDForKey([]byte) uint64 }); ok && s.groups != nil {
		if _, allowed := s.groups[resolver.EngineGroupIDForKey(key)]; allowed {
			return nil
		}
	}
	for _, allowed := range s.keys {
		if bytes.Equal(allowed, key) {
			return nil
		}
	}
	return errors.WithStack(ErrReadSnapshotKeyScope)
}

// A keyed barrier without complete group identities only authorizes its exact
// keys. It must not silently expand into a global barrier or broader authority.
func requestFenceKeys(st store.MVCCStore, keys [][]byte) [][]byte {
	if _, keyed := st.(interface {
		CommittedTimestampFloorForKeys(context.Context, [][]byte) (uint64, error)
	}); !keyed {
		return nil
	}
	return keys
}

func (r *ReadRequest) validateGroupScope(s ReadSnapshot, groupID uint64, key []byte) error {
	if groupID == 0 {
		return r.validateKeyScope(s, key)
	}
	if len(s.keys) == 0 && len(s.groups) == 0 {
		return nil
	}
	if _, ok := s.groups[groupID]; ok {
		return nil
	}
	return errors.WithStack(ErrReadSnapshotKeyScope)
}

func (r *ReadRequest) validateRangeScope(s ReadSnapshot, start, end []byte, groupID uint64) error {
	if len(s.keys) == 0 && len(s.groups) == 0 {
		return nil
	}
	if groupID != 0 {
		if _, ok := s.groups[groupID]; ok {
			return nil
		}
		return errors.WithStack(ErrReadSnapshotKeyScope)
	}
	if resolver, ok := r.st.(interface {
		ReadFenceTargetsForRange([]byte, []byte) []ReadFenceTarget
	}); ok && s.groups != nil {
		targets := resolver.ReadFenceTargetsForRange(start, end)
		for _, target := range targets {
			if _, allowed := s.groups[target.GroupID]; !allowed {
				return errors.WithStack(ErrReadSnapshotKeyScope)
			}
		}
		if len(targets) > 0 {
			return nil
		}
	}
	return errors.WithStack(ErrReadSnapshotKeyScope)
}

// LatestTargets preserves physical group identity for legacy wide columns.
func (r *ReadRequest) LatestTargets(ctx context.Context, label string, targets []ReadFenceTarget) (ReadSnapshot, error) {
	reader, ok := r.st.(interface {
		GroupCommittedTimestampFloor(context.Context, uint64) (uint64, error)
	})
	if !ok || len(targets) == 0 {
		return r.Latest(ctx, label)
	}
	for _, target := range targets {
		if target.GroupID == 0 {
			return r.Latest(ctx, label)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.validate(ctx); err != nil {
		return ReadSnapshot{}, err
	}
	ts, groups, err := requestTargetFloor(ctx, reader, targets)
	if err != nil {
		return ReadSnapshot{}, err
	}
	stamp, err := r.bindAppliedBoundary(ctx, label, max(ts, 1))
	if err != nil {
		return ReadSnapshot{}, err
	}
	snapshot := r.register(stamp)
	snapshot.groups = groups
	r.remember(snapshot)
	return snapshot, nil
}

// CheckGet authorizes a legacy forwarding boundary before any remote read.
func (r *ReadRequest) CheckGet(ctx context.Context, snapshot ReadSnapshot, key []byte) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.validateSnapshot(ctx, snapshot); err != nil {
		return err
	}
	return r.validateKeyScope(snapshot, key)
}

func (r *ReadRequest) scopedSnapshot(stamp ReadTimestamp, keys [][]byte) ReadSnapshot {
	snapshot := r.register(stamp)
	if len(keys) > 0 {
		snapshot.keys = make([][]byte, len(keys))
		for i, key := range keys {
			snapshot.keys[i] = bytes.Clone(key)
		}
		if resolver, ok := r.coord.(interface{ EngineGroupIDForKey([]byte) uint64 }); ok {
			snapshot.groups = make(map[uint64]struct{}, len(keys))
			for _, key := range keys {
				groupID := resolver.EngineGroupIDForKey(key)
				if groupID == 0 {
					snapshot.groups = nil
					break
				}
				snapshot.groups[groupID] = struct{}{}
			}
		}
	}

	return snapshot
}

func requestTargetFloor(ctx context.Context, reader interface {
	GroupCommittedTimestampFloor(context.Context, uint64) (uint64, error)
}, targets []ReadFenceTarget) (uint64, map[uint64]struct{}, error) {
	groups := make(map[uint64]struct{}, len(targets))
	var ts uint64
	for _, target := range targets {
		if _, seen := groups[target.GroupID]; seen {
			continue
		}
		floor, err := reader.GroupCommittedTimestampFloor(ctx, target.GroupID)
		if err != nil {
			return 0, nil, errors.WithStack(err)
		}
		ts = max(ts, floor)
		groups[target.GroupID] = struct{}{}
	}
	return ts, groups, nil
}

// The caller holds mu and has already established the authoritative barrier.
// Only public Latest/LatestTargets obtain that proof; no caller-supplied time
// is accepted by either public constructor.
func (r *ReadRequest) bindAppliedBoundary(ctx context.Context, label string, ts uint64) (ReadTimestamp, error) {
	if err := r.validate(ctx); err != nil {
		return ReadTimestamp{}, err
	}
	if r.coord != nil && r.coord.Clock() != nil {
		r.coord.Clock().Observe(ts)
	}
	stamp, err := BeginReadTimestampThrough(ctx, r.coord, ts, label)
	if err != nil {
		return ReadTimestamp{}, errors.WithStack(err)
	}
	if err := r.validate(ctx); err != nil {
		return ReadTimestamp{}, err
	}
	return stamp, nil
}

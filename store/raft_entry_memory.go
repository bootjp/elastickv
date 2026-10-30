package store

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/emirpasic/gods/maps/treemap"
)

type memoryEntryKey struct{}

type memoryRaftEntry struct {
	owner       *mvccStore
	staged      *mvccStore
	index       uint64
	failed      error
	controlOnly bool
	touched     bool
}

func (s *mvccStore) memoryEntryScope(ctx context.Context) *memoryRaftEntry {
	if ctx == nil {
		return nil
	}
	scope, _ := ctx.Value(memoryEntryKey{}).(*memoryRaftEntry)
	if scope == nil || scope.owner != s {
		return nil
	}
	return scope
}

func (s *mvccStore) memoryEntryStore(ctx context.Context) *mvccStore {
	if scope := s.memoryEntryScope(ctx); scope != nil {
		return scope.staged
	}
	return nil
}

func (sc *memoryRaftEntry) fail(err error) error {
	if sc.failed == nil {
		sc.failed = err
	}
	return err
}

func (sc *memoryRaftEntry) result(err error) error {
	if scopeErr := sc.allowData(); scopeErr != nil {
		return scopeErr
	}
	if err != nil && !IsApplyVerdict(err) {
		return sc.fail(err)
	}
	if err == nil {
		sc.touched = true
	}
	return err
}

func (sc *memoryRaftEntry) finishIndex() {
	if sc.controlOnly || !sc.touched {
		return
	}
	if len(sc.staged.raftControl) != 0 || len(sc.staged.controlOverlay) != 0 || sc.staged.controlAppliedIndex != 0 {
		sc.staged.controlAppliedIndex = max(sc.staged.controlAppliedIndex, sc.index)
	}
}

func (sc *memoryRaftEntry) allowData() error {
	if sc.failed != nil {
		return sc.failed
	}
	if sc.controlOnly {
		return sc.fail(errors.Wrap(ErrNotSupported, "data mutation in journal-only scope"))
	}
	return nil
}

func (s *mvccStore) memoryEntryIndex(ctx context.Context, index uint64) error {
	if sc := s.memoryEntryScope(ctx); sc != nil {
		if sc.failed != nil {
			return sc.failed
		}
		if index != sc.index {
			return sc.fail(errors.WithStack(ErrRaftEntryIndexMismatch))
		}
	}
	return nil
}

// applyMu serializes writers, while mtx protects only published state. Callback
// reads with its context use the private overlay; unrelated readers see the entire
// pre-entry state until publication. LastCommitTS remains the committed value,
// as on Pebble. This is logical atomicity, not disk durability.
func (s *mvccStore) ApplyRaftEntry(ctx context.Context, index uint64, fn func(context.Context) error) error {
	return s.applyMemoryRaftEntry(ctx, index, false, fn)
}

func (s *mvccStore) applyMemoryRaftEntry(ctx context.Context, index uint64, controlOnly bool, fn func(context.Context) error) error {
	if sc := s.memoryEntryScope(ctx); sc != nil {
		if sc.controlOnly != controlOnly {
			return sc.fail(errors.WithStack(ErrRaftControlScopeRequired))
		}
		if err := s.memoryEntryIndex(ctx, index); err != nil {
			return err
		}
		if err := fn(ctx); err != nil {
			return sc.fail(err)
		}
		return sc.failed
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.mtx.RLock()
	staged := s.newMemoryEntryOverlayLocked()
	s.mtx.RUnlock()
	sc := &memoryRaftEntry{owner: s, staged: staged, index: index, controlOnly: controlOnly}
	if err := fn(context.WithValue(ctx, memoryEntryKey{}, sc)); err != nil {
		return err
	}
	if sc.failed != nil {
		return sc.failed
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	sc.finishIndex()
	s.publishMemoryEntryLocked(staged)
	return nil
}

func (s *mvccStore) newMemoryEntryOverlayLocked() *mvccStore {
	dst := newMVCCStore(WithLogger(s.log))
	dst.tree = s.tree
	dst.entryOverlay = make(map[string]memoryEntryValue)
	dst.controlOverlay = make(map[string]memoryControlValue)
	dst.lastCommitTS, dst.minRetainedTS = s.lastCommitTS, s.minRetainedTS
	dst.writeConflicts = s.writeConflicts
	// Published maps remain read-only during a scope. Readiness updates use
	// per-job deltas; the other typed migration writers reject the scope.
	dst.migrationAcks, dst.migrationHLCFloors = s.migrationAcks, s.migrationHLCFloors
	dst.migrationPromotions, dst.migrationReadiness = s.migrationPromotions, s.migrationReadiness
	dst.migrationRetired, dst.migrationOwnership = s.migrationRetired, s.migrationOwnership
	dst.migrationPartitions, dst.migrationAdmission = s.migrationPartitions, s.migrationAdmission
	dst.migrationReadinessCache = s.migrationReadinessCache
	dst.controlAppliedIndex = s.controlAppliedIndex
	dst.raftControl = s.raftControl
	return dst
}

func (s *mvccStore) publishMemoryEntryLocked(src *mvccStore) {
	for key, change := range src.entryOverlay {
		if change.removed {
			s.tree.Remove([]byte(key))
		} else {
			s.tree.Put([]byte(key), change.versions)
		}
	}
	for key, change := range src.controlOverlay {
		if change.removed {
			delete(s.raftControl, key)
		} else {
			s.raftControl[key] = change.value
		}
	}
	for job, state := range src.readinessOverlay {
		s.migrationReadiness[job] = state
		s.migrationReadinessCache = upsertTargetStagedReadinessStateCache(s.migrationReadinessCache, state)
	}
	for job, floor := range src.floorOverlay {
		s.migrationHLCFloors[job] = floor
	}
	s.lastCommitTS = src.lastCommitTS
	s.controlAppliedIndex = src.controlAppliedIndex
}

type memoryEntryValue struct {
	versions []VersionedValue
	removed  bool
}

type memoryControlValue struct {
	value   []byte
	removed bool
}

func (s *mvccStore) entryTreeGet(key []byte) (any, bool) {
	if value, exists := s.entryOverlay[string(key)]; exists {
		return value.versions, !value.removed
	}
	return s.tree.Get(key)
}

func (s *mvccStore) entryTreePut(key []byte, value []VersionedValue) {
	if s.entryOverlay != nil {
		s.entryOverlay[string(key)] = memoryEntryValue{versions: value}
		return
	}
	s.tree.Put(key, value)
}

func (s *mvccStore) entryTreeRemove(key []byte) {
	if s.entryOverlay != nil {
		s.entryOverlay[string(key)] = memoryEntryValue{removed: true}
		return
	}
	s.tree.Remove(key)
}

// A merged ordered key view is needed only for actual scans/prefix deletes.
// Values and existing payloads remain immutable shared references.
func (s *mvccStore) entryTreeIterator() treemap.Iterator {
	if len(s.entryOverlay) == 0 {
		return s.tree.Iterator()
	}
	view := treemap.NewWith(byteSliceComparator)
	it := s.tree.Iterator()
	for it.Next() {
		view.Put(it.Key(), it.Value())
	}
	for key, change := range s.entryOverlay {
		if change.removed {
			view.Remove([]byte(key))
		} else {
			view.Put([]byte(key), change.versions)
		}
	}
	return view.Iterator()
}

func (s *mvccStore) rejectMemoryEntry(ctx context.Context) error {
	if sc := s.memoryEntryScope(ctx); sc != nil {
		return sc.fail(errors.Wrap(ErrNotSupported, "typed migration operation inside indexed raft entry"))
	}
	return nil
}

func validateMemoryMutations(ctx context.Context, mutations []*KVPairMutation, commitTS uint64) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	for _, mut := range mutations {
		if err := validateControlVersionKey(mut.Key, commitTS); err != nil {
			return err
		}
		switch mut.Op {
		case OpTypePut:
			if err := validateValueSize(mut.Value); err != nil {
				return err
			}
		case OpTypeDelete:
		default:
			return errors.WithStack(ErrUnknownOp)
		}
	}
	return nil
}

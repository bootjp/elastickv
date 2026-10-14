package store

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// RaftEntryApplier is implemented by stores that can apply every Raft-apply
// write issued inside one callback as a single atomic batch.
//
// kvFSM.Apply uses it for a batched Raft entry (several requests in one
// RaftCommand). Without it each request commits its own Pebble batch, and
// the first one already persists the entry's applied index, so a crash
// between two requests leaves a replica whose durable applied index names
// an entry it only half applied: on restart the engine seeds its applied
// index from that value and drops the entry as a duplicate without calling
// the FSM, so the tail is never applied on that replica while every other
// replica applied all of it (serializable isolation audit, G17).
//
// Inside fn, ApplyMutationsRaftAt and DeletePrefixesAtRaftAt stage into one
// indexed batch and validate through it, so a later request observes an
// earlier request's writes exactly as it would after a per-request commit.
// The batch is committed once, with one applied-index write, after fn
// returns nil. A non-nil error from fn, or a failure while staging, discards
// the whole batch: nothing of the entry becomes durable. Per-request
// validation failures (a write conflict, an invalid mutation) are returned
// to the caller of the staging call and leave the batch untouched, so the
// per-request result semantics of a batched entry are unchanged.
type RaftEntryApplier interface {
	ApplyRaftEntry(ctx context.Context, appliedIndex uint64, fn func(ctx context.Context) error) error
}

// ErrRaftEntryStoreSwapped is returned when the Pebble DB was replaced (a
// snapshot restore) while a Raft entry scope was open. The engine applies
// entries and restores snapshots on one goroutine, so this cannot happen
// in a running node; the check keeps the scope from committing into a
// closed DB if that ever changes.
var ErrRaftEntryStoreSwapped = errors.New("store: pebble db replaced while a raft entry scope was open")

// ErrRaftEntryIndexMismatch is returned when a staging call inside a Raft
// entry scope carries a different applied index than the scope itself.
var ErrRaftEntryIndexMismatch = errors.New("store: raft entry scope applied index mismatch")

type raftEntryScopeKey struct{}

// raftEntryScope is the per-entry staging state carried through the
// context of ApplyRaftEntry's callback. It is owned by the goroutine that
// runs the callback and is never shared.
type raftEntryScope struct {
	store        *pebbleStore
	db           *pebble.DB
	batch        *pebble.Batch
	appliedIndex uint64
	// maxCommitTS is the highest commit timestamp staged so far; it becomes
	// the entry's lastCommitTS advance at commit.
	maxCommitTS uint64
	// advance records that at least one staged request advances
	// lastCommitTS, mirroring the advanceLastCommitTS argument of the
	// per-request path.
	advance bool
	// touched records that at least one request staged or observed
	// something, so an entry whose every request failed validation commits
	// nothing, as the per-request path does.
	touched bool
	// failed is the first staging error; once set, every later staging call
	// and the commit return it, so a partially staged request can never
	// land.
	failed error
}

func (s *pebbleStore) raftEntryScopeFrom(ctx context.Context) *raftEntryScope {
	if ctx == nil {
		return nil
	}
	scope, _ := ctx.Value(raftEntryScopeKey{}).(*raftEntryScope)
	if scope == nil || scope.store != s {
		return nil
	}
	return scope
}

func (sc *raftEntryScope) observe(commitTS uint64, advance bool) {
	sc.touched = true
	if !advance {
		return
	}
	sc.advance = true
	if commitTS > sc.maxCommitTS {
		sc.maxCommitTS = commitTS
	}
}

func (sc *raftEntryScope) fail(err error) error {
	if sc.failed == nil {
		sc.failed = err
	}
	return err
}

// hasCommitsAfter mirrors pebbleStore.hasCommitsAfter but also counts the
// commits staged earlier in this entry, which the in-memory lastCommitTS
// does not see until the entry commits; a request below an earlier
// request's timestamp is therefore still validated against it, exactly as
// it is after a per-request commit.
func (sc *raftEntryScope) hasCommitsAfter(startTS uint64) bool {
	return sc.store.hasCommitsAfter(startTS) || (sc.advance && sc.maxCommitTS > startTS)
}

func (sc *raftEntryScope) checkIndex(appliedIndex uint64) error {
	if appliedIndex != sc.appliedIndex {
		return sc.fail(errors.Wrapf(ErrRaftEntryIndexMismatch, "scope index=%d call index=%d", sc.appliedIndex, appliedIndex))
	}
	return nil
}

// ApplyRaftEntry implements RaftEntryApplier. No store lock is held across
// the callback: each staging call and the commit take dbMu.RLock and then
// applyMu, the order every other applier and newSSTIngestSnapshot use
// (dbMu before applyMu), and release both before returning. Holding
// applyMu across the callback and re-acquiring dbMu per staging call
// inverted that order and deadlocked against a snapshot taken between two
// requests, which holds dbMu exclusively while it waits for applyMu
// (found in review; TestApplyRaftEntry_SnapshotBetweenRequestsDoesNotDeadlock).
//
// A request's validation and the entry's commit are therefore two applyMu
// sections. That matches the per-request path in practice: on a running
// node every applier other than the Raft apply goroutine itself is the
// startup catalog bootstrap, so nothing can commit between a staged
// request and the entry's commit, and the indexed batch reads the DB's
// current state, so a staging call sees whatever did land before it.
func (s *pebbleStore) ApplyRaftEntry(ctx context.Context, appliedIndex uint64, fn func(ctx context.Context) error) error {
	if s.raftEntryScopeFrom(ctx) != nil {
		return fn(ctx)
	}
	scope := s.newRaftEntryScope(appliedIndex)
	defer func() { _ = scope.batch.Close() }()

	if err := fn(context.WithValue(ctx, raftEntryScopeKey{}, scope)); err != nil {
		return err
	}
	if scope.failed != nil {
		return scope.failed
	}
	return s.commitRaftEntryScope(scope)
}

func (s *pebbleStore) newRaftEntryScope(appliedIndex uint64) *raftEntryScope {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return &raftEntryScope{
		store:        s,
		db:           s.db,
		batch:        s.db.NewIndexedBatch(),
		appliedIndex: appliedIndex,
	}
}

// commitRaftEntryScope writes the entry's lastCommitTS and applied index
// into the staged batch and commits it once, under dbMu.RLock then
// applyMu like every other commit. The in-memory lastCommitTS advances
// only after the commit, as it does on the per-request path, so no reader
// sees a watermark above durable state.
func (s *pebbleStore) commitRaftEntryScope(scope *raftEntryScope) error {
	if !scope.touched {
		return nil
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db != scope.db {
		return errors.WithStack(ErrRaftEntryStoreSwapped)
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	s.mtx.Lock()
	defer s.mtx.Unlock()
	newLastTS := s.lastCommitTS
	if scope.advance {
		if scope.maxCommitTS > newLastTS {
			newLastTS = scope.maxCommitTS
		}
		if err := setPebbleUint64InBatch(scope.batch, metaLastCommitTSBytes, newLastTS); err != nil {
			return err
		}
	}
	if scope.appliedIndex > 0 {
		if err := setPebbleUint64InBatch(scope.batch, metaAppliedIndexBytes, scope.appliedIndex); err != nil {
			return err
		}
	}
	if err := scope.batch.Commit(s.raftApplyWriteOpts()); err != nil {
		return errors.WithStack(err)
	}
	if scope.advance {
		s.updateLastCommitTS(newLastTS)
	}
	return nil
}

// stageMutationsInRaftEntry is ApplyMutationsRaftAt inside a Raft entry
// scope: the replay probe and the conflict checks read through the scope's
// indexed batch, the mutations are encoded before anything is staged so a
// validation failure leaves the batch untouched, and nothing is committed.
func (s *pebbleStore) stageMutationsInRaftEntry(ctx context.Context, scope *raftEntryScope, mutations []*KVPairMutation, readKeys [][]byte, startTS, commitTS, appliedIndex uint64) error {
	if scope.failed != nil {
		return scope.failed
	}
	if err := scope.checkIndex(appliedIndex); err != nil {
		return err
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db != scope.db {
		return scope.fail(errors.WithStack(ErrRaftEntryStoreSwapped))
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	if appliedIndex > 0 {
		landed, err := s.raftApplyAlreadyLandedWith(scope.batch, mutations, commitTS)
		if err != nil {
			return err
		}
		if landed {
			scope.observe(commitTS, true)
			return nil
		}
	}
	if err := s.checkApplyConflictsWith(ctx, scope.batch, scope.hasCommitsAfter, mutations, readKeys, startTS, false); err != nil {
		return err
	}
	encoded, err := s.encodeMutationsForBatch(mutations, commitTS, false)
	if err != nil {
		return err
	}
	if err := stageEncodedMutations(scope.batch, encoded); err != nil {
		return scope.fail(err)
	}
	scope.observe(commitTS, true)
	return nil
}

// stagePrefixDeletesInRaftEntry is DeletePrefixesAtRaftAt inside a Raft
// entry scope. The scan iterates the scope's indexed batch, so a key an
// earlier request of the same entry wrote is tombstoned exactly as it is
// after a per-request commit; the iterator does not observe the tombstones
// this call stages, which matches the DB iterator of the per-request path.
func (s *pebbleStore) stagePrefixDeletesInRaftEntry(scope *raftEntryScope, deletes []PrefixDelete, commitTS, appliedIndex uint64) error {
	if len(deletes) == 0 {
		return nil
	}
	if scope.failed != nil {
		return scope.failed
	}
	if err := scope.checkIndex(appliedIndex); err != nil {
		return err
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db != scope.db {
		return scope.fail(errors.WithStack(ErrRaftEntryStoreSwapped))
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	iter, err := newDeletePrefixesIteratorWith(scope.batch, deletes)
	if err != nil {
		return errors.WithStack(err)
	}
	defer iter.Close()

	if err := s.stageDeletePrefixes(iter, scope.batch, deletes, commitTS); err != nil {
		return scope.fail(err)
	}
	scope.observe(commitTS, true)
	return nil
}

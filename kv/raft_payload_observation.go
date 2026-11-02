package kv

import "sync/atomic"

var raftPayloadPublicationID atomic.Uint64

// RaftPayloadWrapObservation is an opaque publication identity. It is comparable
// and retains its cell, so replacing even the same closure cannot produce ABA.
// It certifies installation only, not the closure's encryption semantics.
type RaftPayloadWrapObservation struct {
	cell     *RaftPayloadWrapper
	identity uint64
}

// Identity is process-local and changes on every installation, including
// republication of the same closure. It grants no payload or admission authority.
func (o RaftPayloadWrapObservation) Identity() uint64 { return o.identity }

// SetObservedRaftPayloadWrap publishes a closure and returns its exact cell.
// Only its trusted installer can associate this token with key/nonce evidence.
func (g *ShardGroup) SetObservedRaftPayloadWrap(wrap RaftPayloadWrapper) RaftPayloadWrapObservation {
	var begin func() func()
	if callback := g.raftPayloadWrapInvalidator.Load(); callback != nil {
		begin = *callback
	}
	mutation := BeginRaftPayloadWrapMutation(begin)
	defer mutation.Close()
	return g.SetObservedRaftPayloadWrapInMutation(wrap, mutation)
}

// RaftPayloadWrapMutation owns one paired authority fence. Its trusted owner
// must not close it until all publications and old installer state are retired.
// It does not wait for already-owned proposals; admission owns their drainage.
type RaftPayloadWrapMutation struct {
	active atomic.Bool
	end    func()
}

// BeginRaftPayloadWrapMutation calls begin outside any wrapper mutex. A nil
// callback preserves unprotected operation. A configured callback MUST return
// an end function; accepting nil would permit publication without a live fence.
func BeginRaftPayloadWrapMutation(begin func() func()) *RaftPayloadWrapMutation {
	m := &RaftPayloadWrapMutation{}
	if begin != nil {
		m.end = begin()
		if m.end == nil {
			panic("raft payload mutation callback returned no end fence")
		}
	}
	m.active.Store(true)
	return m
}

// Close settles the fence after the owner has released its wrapper mutexes.
func (m *RaftPayloadWrapMutation) Close() {
	if m.active.Swap(false) && m.end != nil {
		m.end()
	}
}

// SetObservedRaftPayloadWrapInMutation is for a trusted multi-group installer
// already owning a paired global fence. It never calls A under wrapper locks.
func (g *ShardGroup) SetObservedRaftPayloadWrapInMutation(wrap RaftPayloadWrapper, mutation *RaftPayloadWrapMutation) RaftPayloadWrapObservation {
	if mutation == nil || !mutation.active.Load() {
		panic("raft payload publication outside mutation fence")
	}
	if wrap == nil {
		g.raftPayloadWrap.Store(nil)
		return RaftPayloadWrapObservation{}
	}
	identity := nextRaftPayloadPublicationID()
	g.raftPayloadWrap.Store(&wrap)
	return RaftPayloadWrapObservation{cell: &wrap, identity: identity}
}

func nextRaftPayloadPublicationID() uint64 {
	for {
		previous := raftPayloadPublicationID.Load()
		if previous == ^uint64(0) {
			panic("raft payload publication identity exhausted")
		}
		if raftPayloadPublicationID.CompareAndSwap(previous, previous+1) {
			return previous + 1
		}
	}
}

// SetRaftPayloadWrapInvalidator installs an optional memory-only paired mutation
// callback. Wire it under startup exclusivity, before publishing admission.
// Callback installation itself is not a write-path mutation.
func (g *ShardGroup) SetRaftPayloadWrapInvalidator(invalidate func() func()) {
	if invalidate == nil {
		g.raftPayloadWrapInvalidator.Store(nil)
		return
	}
	g.raftPayloadWrapInvalidator.Store(&invalidate)
}

// HasRaftPayloadWrap observes the actual cell used by dynamicWrappedProposer.
func (g *ShardGroup) HasRaftPayloadWrap(installed RaftPayloadWrapObservation) bool {
	return g != nil && installed.cell != nil && g.raftPayloadWrap.Load() == installed.cell
}

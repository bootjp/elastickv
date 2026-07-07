package main

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const inertRaftEnvelopeCutoverIndex = ^uint64(0)

type raftEnvelopeRuntime struct {
	cipher       *encryption.Cipher
	nonceFactory store.NonceFactory

	mu         sync.Mutex
	groups     map[uint64]*kv.ShardGroup
	activeWrap kv.RaftPayloadWrapper

	cutoverIndex *atomic.Uint64
}

func newRaftEnvelopeRuntime(cipher *encryption.Cipher, nonceFactory store.NonceFactory, initialCutover uint64, activeRaftDEKID uint32, cell *atomic.Uint64) (*raftEnvelopeRuntime, error) {
	if cell == nil {
		return nil, errors.New("raft envelope runtime: nil cutover cell")
	}
	r := &raftEnvelopeRuntime{
		cipher:       cipher,
		nonceFactory: nonceFactory,
		groups:       map[uint64]*kv.ShardGroup{},
		cutoverIndex: cell,
	}
	if initialCutover == 0 {
		return r, nil
	}
	if activeRaftDEKID == 0 {
		return nil, errors.New("raft envelope runtime: active cutover requires active raft DEK")
	}
	wrap, err := r.wrapFor(activeRaftDEKID)
	if err != nil {
		return nil, err
	}
	r.activeWrap = wrap
	r.cutoverIndex.Store(initialCutover)
	return r, nil
}

func (r *raftEnvelopeRuntime) engineCutoverIndex() uint64 {
	if r == nil || r.cutoverIndex == nil {
		return inertRaftEnvelopeCutoverIndex
	}
	idx := r.cutoverIndex.Load()
	if idx == 0 {
		return inertRaftEnvelopeCutoverIndex
	}
	return idx
}

func (r *raftEnvelopeRuntime) attachGroup(groupID uint64, g *kv.ShardGroup) {
	if r == nil || g == nil {
		return
	}
	r.mu.Lock()
	r.groups[groupID] = g
	wrap := r.activeWrap
	r.mu.Unlock()
	if wrap != nil {
		g.SetRaftPayloadWrap(wrap)
	}
}

func (r *raftEnvelopeRuntime) installFromApply(cutoverIdx uint64, activeRaftDEKID uint32) error {
	if r == nil {
		return errors.New("raft envelope runtime: installer is not configured")
	}
	if cutoverIdx == 0 {
		return errors.New("raft envelope runtime: cutover index must be non-zero")
	}
	if activeRaftDEKID == 0 {
		return errors.New("raft envelope runtime: active raft DEK must be non-zero")
	}
	wrap, err := r.wrapFor(activeRaftDEKID)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.activeWrap = wrap
	for _, g := range r.groups {
		g.SetRaftPayloadWrap(wrap)
	}
	r.mu.Unlock()
	r.cutoverIndex.Store(cutoverIdx)
	return nil
}

func (r *raftEnvelopeRuntime) reinstallActiveWrap() {
	if r == nil {
		return
	}
	r.mu.Lock()
	wrap := r.activeWrap
	for _, g := range r.groups {
		if wrap != nil {
			g.SetRaftPayloadWrap(wrap)
		}
	}
	r.mu.Unlock()
}

func (r *raftEnvelopeRuntime) wrapFor(activeRaftDEKID uint32) (kv.RaftPayloadWrapper, error) {
	if r.cipher == nil {
		return nil, errors.New("raft envelope runtime: cipher is not configured")
	}
	if r.nonceFactory == nil {
		return nil, errors.New("raft envelope runtime: raft nonce factory is not configured")
	}
	return func(payload []byte) ([]byte, error) {
		nonce, err := r.nonceFactory.Next()
		if err != nil {
			return nil, errors.Wrap(err, "raft envelope runtime: next nonce")
		}
		return encryption.WrapRaftPayload(r.cipher, activeRaftDEKID, nonce[:], payload)
	}, nil
}

func (r *raftEnvelopeRuntime) barrier() adapter.CutoverBarrierController {
	if r == nil {
		return nil
	}
	return &raftEnvelopeCutoverBarrier{runtime: r}
}

func (r *raftEnvelopeRuntime) snapshotGroups() []*kv.ShardGroup {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	groups := make([]*kv.ShardGroup, 0, len(r.groups))
	for _, g := range r.groups {
		if g != nil {
			groups = append(groups, g)
		}
	}
	return groups
}

type raftEnvelopeCutoverBarrier struct {
	runtime *raftEnvelopeRuntime
}

func (b *raftEnvelopeCutoverBarrier) Begin() <-chan struct{} {
	groups := b.runtime.snapshotGroups()
	done := make(chan struct{})
	if len(groups) == 0 {
		close(done)
		return done
	}
	pending := len(groups)
	var wg sync.WaitGroup
	wg.Add(pending)
	for _, g := range groups {
		ch := g.BeginCutoverBarrier()
		go func(ch <-chan struct{}) {
			defer wg.Done()
			<-ch
		}(ch)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

func (b *raftEnvelopeCutoverBarrier) WaitDrained(ctx context.Context) error {
	for _, g := range b.runtime.snapshotGroups() {
		if err := g.WaitInflightDrained(ctx); err != nil {
			return errors.Wrap(err, "raft envelope barrier: wait drained")
		}
	}
	return nil
}

func (b *raftEnvelopeCutoverBarrier) InstallWrap() {
	b.runtime.reinstallActiveWrap()
}

func (b *raftEnvelopeCutoverBarrier) End() {
	for _, g := range b.runtime.snapshotGroups() {
		g.EndCutoverBarrier()
	}
}

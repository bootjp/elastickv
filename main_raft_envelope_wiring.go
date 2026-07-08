package main

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const inertRaftEnvelopeCutoverIndex = ^uint64(0)

type raftEnvelopeRuntime struct {
	cipher       *encryption.Cipher
	nonceFactory store.NonceFactory
	raftEpoch    uint16
	fullNodeID   uint64
	registration *raftRegistrationGate
	registered   func(dekID uint32, epoch uint16) bool

	mu              sync.Mutex
	groups          map[uint64]*kv.ShardGroup
	activeWrap      kv.RaftPayloadWrapper
	activeRaftDEKID uint32

	cutoverIndex *atomic.Uint64
}

type raftRegistrationGate struct {
	registered atomic.Uint64
}

func (g *raftRegistrationGate) MarkRegistered(dekID uint32, epoch uint16) {
	if g == nil || dekID == 0 {
		return
	}
	g.registered.Store(packRaftRegistration(dekID, epoch))
}

func (g *raftRegistrationGate) Registered(dekID uint32, epoch uint16) bool {
	if g == nil || dekID == 0 {
		return false
	}
	return g.registered.Load() == packRaftRegistration(dekID, epoch)
}

func packRaftRegistration(dekID uint32, epoch uint16) uint64 {
	return uint64(dekID)<<16 | uint64(epoch)
}

func newRaftEnvelopeRuntime(cipher *encryption.Cipher, nonceFactory store.NonceFactory, initialCutover uint64, activeRaftDEKID uint32, cell *atomic.Uint64, raftEpoch uint16, fullNodeID uint64, registration *raftRegistrationGate) (*raftEnvelopeRuntime, error) {
	if cell == nil {
		return nil, errors.New("raft envelope runtime: nil cutover cell")
	}
	r := &raftEnvelopeRuntime{
		cipher:       cipher,
		nonceFactory: nonceFactory,
		raftEpoch:    raftEpoch,
		fullNodeID:   fullNodeID,
		registration: registration,
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
	r.activeRaftDEKID = activeRaftDEKID
	r.cutoverIndex.Store(initialCutover)
	return r, nil
}

func (r *raftEnvelopeRuntime) setRegistrationVerifier(registered func(dekID uint32, epoch uint16) bool) {
	r.mu.Lock()
	r.registered = registered
	r.mu.Unlock()
}

func (r *raftEnvelopeRuntime) engineCutoverIndex() uint64 {
	idx := r.cutoverIndex.Load()
	if idx == 0 {
		return inertRaftEnvelopeCutoverIndex
	}
	return idx
}

func (r *raftEnvelopeRuntime) RaftEnvelopeCutoverIndex() uint64 {
	if r == nil || r.cutoverIndex == nil {
		return 0
	}
	return r.cutoverIndex.Load()
}

func (r *raftEnvelopeRuntime) attachGroup(groupID uint64, g *kv.ShardGroup) {
	r.mu.Lock()
	r.groups[groupID] = g
	wrap := r.activeWrap
	r.mu.Unlock()
	if wrap != nil {
		g.SetRaftPayloadWrap(wrap)
	}
}

func (r *raftEnvelopeRuntime) installFromApply(cutoverIdx uint64, activeRaftDEKID uint32) error {
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
	registered := r.registered
	r.mu.Unlock()
	if r.registration != nil && registered != nil && registered(activeRaftDEKID, r.raftEpoch) {
		r.registration.MarkRegistered(activeRaftDEKID, r.raftEpoch)
	}
	r.mu.Lock()
	r.activeWrap = wrap
	r.activeRaftDEKID = activeRaftDEKID
	for _, g := range r.groups {
		g.SetRaftPayloadWrap(wrap)
	}
	r.mu.Unlock()
	r.cutoverIndex.Store(cutoverIdx)
	return nil
}

func (r *raftEnvelopeRuntime) wrapInstalledFor(activeRaftDEKID uint32) bool {
	if r == nil || activeRaftDEKID == 0 || r.cutoverIndex == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.activeWrap != nil &&
		r.activeRaftDEKID == activeRaftDEKID &&
		r.cutoverIndex.Load() != 0
}

func (r *raftEnvelopeRuntime) reinstallActiveWrap() {
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
		if r.registration != nil && !r.registration.Registered(activeRaftDEKID, r.raftEpoch) && !isLocalRaftWriterRegistrationPayload(payload, activeRaftDEKID, r.raftEpoch, r.fullNodeID) {
			return nil, errors.Wrapf(store.ErrWriterNotRegistered,
				"raft envelope runtime: writer registration not committed for raft dek_id %d local_epoch %d",
				activeRaftDEKID, r.raftEpoch)
		}
		nonce, err := r.nonceFactory.Next()
		if err != nil {
			return nil, errors.Wrap(err, "raft envelope runtime: next nonce")
		}
		return encryption.WrapRaftPayload(r.cipher, activeRaftDEKID, nonce[:], payload)
	}, nil
}

func isLocalRaftWriterRegistrationPayload(payload []byte, activeRaftDEKID uint32, raftEpoch uint16, fullNodeID uint64) bool {
	if len(payload) == 0 || payload[0] != fsmwire.OpRegistration {
		return false
	}
	registration, err := fsmwire.DecodeRegistration(payload[1:])
	if err != nil {
		return false
	}
	return registration.DEKID == activeRaftDEKID &&
		registration.LocalEpoch == raftEpoch &&
		registration.FullNodeID == fullNodeID
}

func (r *raftEnvelopeRuntime) barrier() adapter.CutoverBarrierController {
	if r == nil {
		return nil
	}
	return &raftEnvelopeCutoverBarrier{runtime: r}
}

func (r *raftEnvelopeRuntime) snapshotGroups() []*kv.ShardGroup {
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
			if ch != nil {
				<-ch
			}
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

func (b *raftEnvelopeCutoverBarrier) ValidateCutoverScope() error {
	groups := b.runtime.snapshotGroups()
	if len(groups) == 1 {
		return nil
	}
	return errors.Errorf(
		"encryption: raft envelope cutover requires exactly one shard group until per-group raft cutover indexes are implemented (got %d)",
		len(groups))
}

func (b *raftEnvelopeCutoverBarrier) InstallWrap() {
	b.runtime.reinstallActiveWrap()
}

func (b *raftEnvelopeCutoverBarrier) End() {
	for _, g := range b.runtime.snapshotGroups() {
		g.EndCutoverBarrier()
	}
}

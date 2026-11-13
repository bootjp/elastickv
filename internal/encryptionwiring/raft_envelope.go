package encryptionwiring

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

type RaftEnvelopeRuntime struct {
	Cipher       *encryption.Cipher
	NonceFactory store.NonceFactory
	RaftEpoch    uint16
	FullNodeID   uint64
	Registration *RaftRegistrationGate
	Registered   func(dekID uint32, epoch uint16) bool

	Mu                     sync.Mutex
	Groups                 map[uint64]*kv.ShardGroup
	installedWraps         map[*kv.ShardGroup]kv.RaftPayloadWrapObservation
	beforeEncryptionChange atomic.Pointer[func() func()]
	activeWrap             kv.RaftPayloadWrapper
	ActiveRaftDEKID        uint32

	CutoverIndex *atomic.Uint64
}

type RaftRegistrationGate struct {
	registered             atomic.Uint64
	beforeEncryptionChange atomic.Pointer[func() func()]
}

func (g *RaftRegistrationGate) MarkRegistered(dekID uint32, epoch uint16) {
	if g == nil || dekID == 0 {
		return
	}
	if callback := g.beforeEncryptionChange.Load(); callback != nil {
		end := (*callback)()
		if end == nil {
			panic("raft registration mutation callback returned no end fence")
		}
		defer end()
	}
	g.registered.Store(packRaftRegistration(dekID, epoch))
}

func (g *RaftRegistrationGate) Registered(dekID uint32, epoch uint16) bool {
	if g == nil || dekID == 0 {
		return false
	}
	return g.registered.Load() == packRaftRegistration(dekID, epoch)
}

func packRaftRegistration(dekID uint32, epoch uint16) uint64 {
	return uint64(dekID)<<16 | uint64(epoch)
}

func NewRaftEnvelopeRuntime(cipher *encryption.Cipher, nonceFactory store.NonceFactory, initialCutover uint64, activeRaftDEKID uint32, cell *atomic.Uint64, raftEpoch uint16, fullNodeID uint64, registration *RaftRegistrationGate) (*RaftEnvelopeRuntime, error) {
	if cell == nil {
		return nil, errors.New("raft envelope runtime: nil cutover cell")
	}
	r := &RaftEnvelopeRuntime{
		Cipher:         cipher,
		NonceFactory:   nonceFactory,
		RaftEpoch:      raftEpoch,
		FullNodeID:     fullNodeID,
		Registration:   registration,
		Groups:         map[uint64]*kv.ShardGroup{},
		installedWraps: map[*kv.ShardGroup]kv.RaftPayloadWrapObservation{},
		CutoverIndex:   cell,
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
	r.ActiveRaftDEKID = activeRaftDEKID
	r.CutoverIndex.Store(initialCutover)
	return r, nil
}

func (r *RaftEnvelopeRuntime) setRegistrationVerifier(registered func(dekID uint32, epoch uint16) bool) {
	r.Mu.Lock()
	r.Registered = registered
	r.Mu.Unlock()
}

func (r *RaftEnvelopeRuntime) engineCutoverIndex() uint64 {
	idx := r.CutoverIndex.Load()
	if idx == 0 {
		return inertRaftEnvelopeCutoverIndex
	}
	return idx
}

func (r *RaftEnvelopeRuntime) RaftEnvelopeCutoverIndex() uint64 {
	if r == nil || r.CutoverIndex == nil {
		return 0
	}
	return r.CutoverIndex.Load()
}

func (r *RaftEnvelopeRuntime) AttachGroup(groupID uint64, g *kv.ShardGroup) {
	mutation := r.beginEncryptionChange()
	defer mutation.Close()
	r.Mu.Lock()
	defer r.Mu.Unlock()
	var begin func() func()
	if callback := r.beforeEncryptionChange.Load(); callback != nil {
		begin = *callback
	}
	g.SetRaftPayloadWrapInvalidator(begin)
	r.Groups[groupID] = g
	wrap := r.activeWrap
	if wrap != nil {
		r.installObservedWrap(g, wrap, mutation)
	}
}

func (r *RaftEnvelopeRuntime) installFromApply(cutoverIdx uint64, activeRaftDEKID uint32) error {
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
	mutation := r.beginEncryptionChange()
	defer mutation.Close()
	r.Mu.Lock()
	registered := r.Registered
	r.Mu.Unlock()
	if r.Registration != nil && registered != nil && registered(activeRaftDEKID, r.RaftEpoch) {
		r.Registration.MarkRegistered(activeRaftDEKID, r.RaftEpoch)
	}
	r.Mu.Lock()
	r.activeWrap = wrap
	defer r.Mu.Unlock()
	r.ActiveRaftDEKID = activeRaftDEKID
	for _, g := range r.Groups {
		r.installObservedWrap(g, wrap, mutation)
	}
	r.CutoverIndex.Store(cutoverIdx)
	return nil
}

func (r *RaftEnvelopeRuntime) installRotatedRaftDEK(activeRaftDEKID uint32) error {
	if r == nil {
		return nil
	}
	if r.CutoverIndex == nil {
		return errors.New("raft envelope runtime: nil cutover cell")
	}
	if r.CutoverIndex.Load() == 0 {
		return nil
	}
	if activeRaftDEKID == 0 {
		return errors.New("raft envelope runtime: active raft DEK must be non-zero")
	}
	wrap, err := r.wrapFor(activeRaftDEKID)
	if err != nil {
		return err
	}
	mutation := r.beginEncryptionChange()
	defer mutation.Close()
	r.Mu.Lock()
	registered := r.Registered
	r.Mu.Unlock()
	if r.Registration != nil && registered != nil && registered(activeRaftDEKID, r.RaftEpoch) {
		r.Registration.MarkRegistered(activeRaftDEKID, r.RaftEpoch)
	}
	r.Mu.Lock()
	r.activeWrap = wrap
	defer r.Mu.Unlock()
	r.ActiveRaftDEKID = activeRaftDEKID
	for _, g := range r.Groups {
		r.installObservedWrap(g, wrap, mutation)
	}
	return nil
}

func (r *RaftEnvelopeRuntime) wrapInstalledFor(activeRaftDEKID uint32) bool {
	if r == nil || activeRaftDEKID == 0 || r.CutoverIndex == nil {
		return false
	}
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.activeWrap != nil &&
		r.ActiveRaftDEKID == activeRaftDEKID &&
		r.CutoverIndex.Load() != 0
}

func (r *RaftEnvelopeRuntime) ReinstallActiveWrap() {
	if r == nil {
		return
	}
	mutation := r.beginEncryptionChange()
	defer mutation.Close()
	r.Mu.Lock()
	defer r.Mu.Unlock()
	wrap := r.activeWrap
	for _, g := range r.Groups {
		if wrap != nil {
			r.installObservedWrap(g, wrap, mutation)
		}
	}
}

func (r *RaftEnvelopeRuntime) wrapFor(activeRaftDEKID uint32) (kv.RaftPayloadWrapper, error) {
	if r.Cipher == nil {
		return nil, errors.New("raft envelope runtime: cipher is not configured")
	}
	if r.NonceFactory == nil {
		return nil, errors.New("raft envelope runtime: raft nonce factory is not configured")
	}
	return func(payload []byte) ([]byte, error) {
		if r.Registration != nil && !r.Registration.Registered(activeRaftDEKID, r.RaftEpoch) && !isLocalRaftWriterRegistrationPayload(payload, activeRaftDEKID, r.RaftEpoch, r.FullNodeID) {
			return nil, errors.Wrapf(store.ErrWriterNotRegistered,
				"raft envelope runtime: writer registration not committed for raft dek_id %d local_epoch %d",
				activeRaftDEKID, r.RaftEpoch)
		}
		nonce, err := r.NonceFactory.Next()
		if err != nil {
			return nil, errors.Wrap(err, "raft envelope runtime: next nonce")
		}
		return encryption.WrapRaftPayload(r.Cipher, activeRaftDEKID, nonce[:], payload)
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

func (r *RaftEnvelopeRuntime) Barrier() adapter.CutoverBarrierController {
	if r == nil {
		return nil
	}
	return &raftEnvelopeCutoverBarrier{runtime: r}
}

func (r *RaftEnvelopeRuntime) snapshotGroups() []*kv.ShardGroup {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	groups := make([]*kv.ShardGroup, 0, len(r.Groups))
	for _, g := range r.Groups {
		if g != nil {
			groups = append(groups, g)
		}
	}
	return groups
}

type raftEnvelopeCutoverBarrier struct {
	runtime *RaftEnvelopeRuntime
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
	b.runtime.ReinstallActiveWrap()
}

func (b *raftEnvelopeCutoverBarrier) End() {
	for _, g := range b.runtime.snapshotGroups() {
		g.EndCutoverBarrier()
	}
}

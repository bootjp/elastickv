package kv

import (
	"context"
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const tsoControlStateKey = "TSO/scalars/v1"
const tsoControlStateLen = tsoSnapshotV5Len + 16

var ErrTSODurableBacking = errors.New("tso fsm: durable control backing required or inconsistent")

// TSODurableBacking belongs exclusively to one dedicated TSO group. Pebble
// provides disk durability; memory stores provide only logical atomicity.
type TSODurableBacking interface {
	store.RaftEntryApplier
	store.RaftControlMetadataStore
	store.RaftControlSnapshotValidator
	Snapshot() (store.Snapshot, error)
}

// TSOControlStage stages opaque P, receipts, journals and configuration records
// in the callback context. Returning an error aborts both metadata and scalars.
// It must not retain ctx, publish mirrors or start a nested transaction.
type TSOControlStage func(context.Context, store.RaftControlMetadataStore) error

// NewDurableTSOStateMachine is the required constructor for protected groups.
// It loads local state, NOT proof of WAL/snapshot lineage or replay coverage.
// The native owner must reconcile those before opening admission. The legacy
// constructor intentionally remains unbacked for existing callers.
func NewDurableTSOStateMachine(hlc *HLC, backing TSODurableBacking, opts ...TSOStateMachineOption) (*TSOStateMachine, error) {
	if backing == nil {
		return nil, errors.WithStack(ErrTSODurableBacking)
	}
	f := NewTSOStateMachine(hlc, opts...)
	f.backing = backing
	raw, err := backing.ReadRaftControlMetadata(context.Background(), []byte(tsoControlStateKey))
	if errors.Is(err, store.ErrKeyNotFound) {
		return f, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "load tso control scalars")
	}
	state, index, configuration, err := decodeTSOControlState(raw)
	if err != nil {
		return nil, err
	}
	f.publishControlState(state, configuration)
	f.applyIndex = index
	return f, nil
}

func (f *TSOStateMachine) SetApplyIndex(index uint64) { f.applyIndex = index }

// ApplyWithControl uses the unchanged inner TSO codecs on an isolated candidate.
// An empty payload allows a metadata/configuration-only transition. Deterministic
// receipt-only rejections should use that form and stage their terminal receipt.
func (f *TSOStateMachine) ApplyWithControl(ctx context.Context, index uint64, payload []byte, stage TSOControlStage) any {
	if f == nil || f.backing == nil || index == 0 {
		return haltErr(errors.WithStack(ErrTSODurableBacking))
	}
	f.controlMu.Lock()
	defer f.controlMu.Unlock()
	candidate := f.controlCandidate()
	var response any
	if len(payload) != 0 {
		response = candidate.applyLegacy(payload)
		if _, halt := response.(*haltApplyResponse); halt {
			return response
		}
	}
	err := f.commitControlState(ctx, index, candidate, f.configurationIndex.Load(), stage)
	if err != nil {
		return haltErr(err)
	}
	return response
}

func (f *TSOStateMachine) commitControlState(ctx context.Context, index uint64, candidate *TSOStateMachine, configuration uint64, stage TSOControlStage) error {
	err := f.backing.ApplyRaftEntry(ctx, index, func(ctx context.Context) error {
		raw, err := f.backing.ReadRaftControlMetadata(ctx, []byte(tsoControlStateKey))
		if err == nil {
			_, previous, _, decodeErr := decodeTSOControlState(raw)
			if decodeErr != nil || previous >= index {
				return errors.Join(ErrTSODurableBacking, decodeErr)
			}
		} else if !errors.Is(err, store.ErrKeyNotFound) {
			return errors.Wrap(err, "read tso control scalars")
		}
		if stage != nil {
			if err := stage(ctx, f.backing); err != nil {
				return err
			}
		}
		return f.backing.StageRaftControlMetadata(ctx, []byte(tsoControlStateKey), encodeTSOControlState(candidate, index, configuration))
	})
	if err != nil {
		return errors.Wrap(err, "commit tso control state")
	}
	f.publishControlState(candidate, configuration)
	return nil
}

func (f *TSOStateMachine) controlCandidate() *TSOStateMachine {
	c := NewTSOStateMachine(nil)
	c.ceilingMs.Store(f.ceilingMs.Load())
	c.allocationFloor.Store(f.allocationFloor.Load())
	c.cutoverActive.Store(f.cutoverActive.Load())
	c.phaseDActive.Store(f.phaseDActive.Load())
	c.phaseDFloor.Store(f.phaseDFloor.Load())
	c.pendingPhaseDFloor.Store(f.pendingPhaseDFloor.Load())
	return c
}

func (f *TSOStateMachine) publishControlState(c *TSOStateMachine, configuration uint64) {
	f.ceilingMs.Store(c.ceilingMs.Load())
	f.allocationFloor.Store(c.allocationFloor.Load())
	f.cutoverActive.Store(c.cutoverActive.Load())
	f.phaseDFloor.Store(c.phaseDFloor.Load())
	f.pendingPhaseDFloor.Store(c.pendingPhaseDFloor.Load())
	f.phaseDActive.Store(c.phaseDActive.Load())
	f.configurationIndex.Store(configuration)
	if f.hlc != nil {
		f.hlc.SetPhysicalCeiling(c.ceilingMs.Load())
		f.hlc.Observe(c.allocationFloor.Load())
	}
	f.observeDurableState()
}

func encodeTSOControlState(c *TSOStateMachine, index, configuration uint64) []byte {
	b := make([]byte, tsoControlStateLen)
	binary.BigEndian.PutUint64(b, uint64(c.ceilingMs.Load())) // #nosec G115 -- inner codec bounds physical millis.
	binary.BigEndian.PutUint64(b[8:], c.allocationFloor.Load())
	if c.cutoverActive.Load() {
		b[tsoSnapshotV2Len] = 1
	}
	if c.phaseDActive.Load() {
		b[tsoSnapshotV3Len] = 1
	}
	binary.BigEndian.PutUint64(b[tsoSnapshotV3Len+1:], c.phaseDFloor.Load())
	binary.BigEndian.PutUint64(b[tsoSnapshotV4Len:], c.pendingPhaseDFloor.Load())
	binary.BigEndian.PutUint64(b[tsoSnapshotV5Len:], index)
	binary.BigEndian.PutUint64(b[tsoSnapshotV5Len+8:], configuration)
	return b
}

func decodeTSOControlState(b []byte) (*TSOStateMachine, uint64, uint64, error) {
	if len(b) != tsoControlStateLen {
		return nil, 0, 0, errors.WithStack(ErrTSODurableBacking)
	}
	ceiling, floor, cutover, active, phaseFloor, _, err := decodeTSOSnapshotV4(b[:tsoSnapshotV4Len])
	if err != nil {
		return nil, 0, 0, err
	}
	pending := binary.BigEndian.Uint64(b[tsoSnapshotV4Len:])
	if err := validatePendingPhaseDSnapshot(pending, floor, cutover, active, phaseFloor); err != nil {
		return nil, 0, 0, err
	}
	index := binary.BigEndian.Uint64(b[tsoSnapshotV5Len:])
	configuration := binary.BigEndian.Uint64(b[tsoSnapshotV5Len+8:])
	if index == 0 || configuration > index {
		return nil, 0, 0, errors.WithStack(ErrTSODurableBacking)
	}
	c := NewTSOStateMachine(nil)
	c.ceilingMs.Store(ceiling)
	c.allocationFloor.Store(floor)
	c.cutoverActive.Store(cutover)
	c.phaseDActive.Store(active)
	c.phaseDFloor.Store(phaseFloor)
	c.pendingPhaseDFloor.Store(pending)
	return c, index, configuration, nil
}

func (f *TSOStateMachine) AppliedConfigurationIndex() uint64 { return f.configurationIndex.Load() }

// ApplyConfigurationWithControl is the FSM_RECEIPTED boundary. The engine
// supplies the exact P/configuration/receipt recipe, then reconciles RawNode
// and publishes peers separately. This index alone is not ConfState proof.
func (f *TSOStateMachine) ApplyConfigurationWithControl(ctx context.Context, index uint64, stage TSOControlStage) error {
	if f == nil || f.backing == nil || index == 0 {
		return errors.WithStack(ErrTSODurableBacking)
	}
	f.controlMu.Lock()
	defer f.controlMu.Unlock()
	return f.commitControlState(ctx, index, f.controlCandidate(), index, stage)
}

// RecordAppliedConfiguration supports the native ordered hook for legacy
// configurations. Protected configurations must use ApplyConfigurationWithControl.
func (f *TSOStateMachine) RecordAppliedConfiguration(index uint64) error {
	if index <= f.AppliedConfigurationIndex() {
		return nil
	}
	if f.backing == nil {
		f.configurationIndex.Store(index)
		return nil
	}
	return f.ApplyConfigurationWithControl(context.Background(), index, nil)
}

// SetDurableAppliedIndex pins the complete scalar/control checkpoint at the
// native snapshot/WAL durability boundary. It never implies replay-skip proof.
func (f *TSOStateMachine) SetDurableAppliedIndex(index uint64) error {
	if f.backing == nil {
		return nil
	}
	if index == 0 {
		return errors.WithStack(ErrTSODurableBacking)
	}
	f.controlMu.Lock()
	defer f.controlMu.Unlock()
	raw, err := f.backing.ReadRaftControlMetadata(context.Background(), []byte(tsoControlStateKey))
	if err == nil {
		_, previous, _, err := decodeTSOControlState(raw)
		if err != nil {
			return err
		}
		if previous >= index {
			return nil
		}
	} else if !errors.Is(err, store.ErrKeyNotFound) {
		return errors.Wrap(err, "read tso checkpoint")
	}
	return f.commitControlState(context.Background(), index, f.controlCandidate(), f.configurationIndex.Load(), nil)
}

// Never advertise LastAppliedIndex/replay-skip without native selected-snapshot
// and exact receipt suffix verification. Durable scalar rows are not that proof.

var _ raftengine.ApplyIndexAware = (*TSOStateMachine)(nil)
var _ raftengine.AppliedConfigurationAware = (*TSOStateMachine)(nil)
var _ raftengine.AppliedIndexWriter = (*TSOStateMachine)(nil)

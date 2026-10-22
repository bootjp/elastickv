package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"math"
	"sync"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const replicatedPinStateVersion = 1

var replicatedPinStateKey = []byte(store.ReplicatedControlPrefix + "backup-pins")

type replicatedPinStateRecord struct {
	Version      uint32
	AppliedIndex uint64
	Pins         replicatedBackupPins
	// Only the activated control group's record may allocate identities.
	Allocator           bool
	AllocatedThrough    uint64
	LastAllocationIndex uint64
}

// The journal installs a new in-memory view only after its state and Raft
// applied index have committed together. Runtime activation must provide the
// initial epoch after the legacy drain; an absent record stays inactive.
type replicatedPinJournal struct {
	mu    sync.RWMutex
	store store.MVCCStore
	state replicatedPinStateRecord
}

func openReplicatedPinJournal(ctx context.Context, st store.MVCCStore) (*replicatedPinJournal, error) {
	j := &replicatedPinJournal{store: st}
	if err := j.reload(ctx); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *replicatedPinJournal) reload(ctx context.Context) error {
	value, err := j.store.GetAt(ctx, replicatedPinStateKey, math.MaxUint64)
	if errors.Is(err, store.ErrKeyNotFound) {
		j.state = replicatedPinStateRecord{}
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "read replicated backup pins")
	}
	record, err := decodeReplicatedPinState(value)
	if err != nil {
		return err
	}
	j.state = record
	return nil
}

func decodeReplicatedPinState(value []byte) (replicatedPinStateRecord, error) {
	var record replicatedPinStateRecord
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return record, errors.Wrap(err, "decode replicated backup pins")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return record, errors.New("trailing replicated backup pin state")
	}
	if err := record.validate(); err != nil {
		return record, err
	}
	return record, nil
}

func (r replicatedPinStateRecord) validate() error {
	if !r.validHeader() {
		return errors.New("invalid replicated backup pin state header")
	}
	if r.Pins.Capacity != 0 && uint64(len(r.Pins.Active)) > uint64(r.Pins.Capacity) {
		return errors.New("replicated backup pin state exceeds capacity")
	}
	if r.Pins.PinGeneration < uint64(len(r.Pins.Active)) {
		return errors.New("replicated backup pin generation is behind active set")
	}
	if err := r.validateAllocator(); err != nil {
		return err
	}
	for generation, pin := range r.Pins.Active {
		if generation == 0 || !pin.validAtFloor(r.Pins.ReadFloor) {
			return errors.New("invalid active replicated backup pin")
		}
	}
	return nil
}

func (r replicatedPinStateRecord) validateAllocator() error {
	if !r.Allocator {
		if r.AllocatedThrough != 0 || r.LastAllocationIndex != 0 {
			return errors.New("data-group backup pin state cannot allocate identities")
		}
		return nil
	}
	if (r.AllocatedThrough == 0) != (r.LastAllocationIndex == 0) || r.LastAllocationIndex > r.AppliedIndex {
		return errors.New("invalid backup identity allocation checkpoint")
	}
	if r.Pins.RetiredThrough > r.AllocatedThrough {
		return errors.New("control retirement exceeds allocated backup identities")
	}
	for generation := range r.Pins.Active {
		if generation > r.AllocatedThrough {
			return errors.New("control reservation has an unallocated backup identity")
		}
	}
	return nil
}

func (r replicatedPinStateRecord) validHeader() bool {
	return r.Version == replicatedPinStateVersion && r.AppliedIndex != 0 && r.Pins.Epoch != 0 && r.Pins.ReadFloor != math.MaxUint64
}

func (p replicatedBackupPin) validAtFloor(floor uint64) bool {
	return !p.PinID.IsZero() && p.ReadTS != 0 && p.ReadTS != math.MaxUint64 && p.Deadline > 0 && p.ReadTS >= floor
}

func (j *replicatedPinJournal) apply(ctx context.Context, index uint64, command replicatedPinCommand) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if index == 0 {
		return errors.Wrap(ErrReplicatedBackupPin, "missing Raft apply index")
	}
	if index <= j.state.AppliedIndex {
		return nil
	}
	if j.state.Allocator && command.Identity.Generation > j.state.AllocatedThrough {
		return errors.Wrap(ErrReplicatedBackupPin, "backup identity was not allocated")
	}
	pins, _, err := j.state.Pins.transition(command)
	if err != nil {
		return err
	}
	record := j.state
	record.Version = replicatedPinStateVersion
	record.AppliedIndex = index
	record.Pins = pins
	return j.persist(ctx, record)
}

func (j *replicatedPinJournal) allocate(ctx context.Context, index, epoch uint64) (BackupIdentity, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.state.Allocator || epoch == 0 || epoch != j.state.Pins.Epoch || index == 0 {
		return BackupIdentity{}, errors.Wrap(ErrReplicatedBackupPin, "identity allocator is inactive or epoch is stale")
	}
	if index <= j.state.AppliedIndex {
		return j.allocationAt(index)
	}
	if j.state.AllocatedThrough == math.MaxUint64 {
		return BackupIdentity{}, errors.Wrap(ErrReplicatedBackupPin, "backup identity generation exhausted")
	}
	record := j.state
	record.AppliedIndex = index
	record.AllocatedThrough++
	record.LastAllocationIndex = index
	if err := j.persist(ctx, record); err != nil {
		return BackupIdentity{}, err
	}
	// An already-applied disk index caused a reload. Never return the stale
	// computed identity unless that exact allocation is in the durable state.
	return j.allocationAt(index)
}

func (j *replicatedPinJournal) allocationAt(index uint64) (BackupIdentity, error) {
	if index != j.state.LastAllocationIndex || j.state.AllocatedThrough == 0 {
		// A lost old response may allocate again with a new Raft entry, but may
		// never guess an identity that a later allocation could already own.
		return BackupIdentity{}, errors.Wrap(ErrReplicatedBackupPin, "allocation result is no longer available; allocate a fresh identity")
	}
	return BackupIdentity{Epoch: j.state.Pins.Epoch, Generation: j.state.AllocatedThrough}, nil
}

func (j *replicatedPinJournal) persist(ctx context.Context, record replicatedPinStateRecord) error {
	value, err := json.Marshal(record)
	if err != nil {
		return errors.Wrap(err, "encode replicated backup pins")
	}
	writer, ok := j.store.(store.ReplicatedControlStore)
	if !ok {
		return errors.New("store does not support durable replicated control state")
	}
	applied, err := writer.ApplyReplicatedControlState(ctx, replicatedPinStateKey, value, record.AppliedIndex)
	if err != nil {
		return errors.Wrap(err, "persist replicated backup pins")
	}
	if !applied {
		return j.reload(ctx)
	}
	j.state = record
	return nil
}

func (j *replicatedPinJournal) snapshot() replicatedPinStateRecord {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.state
	record.Pins.Active = maps.Clone(j.state.Pins.Active)
	return record
}

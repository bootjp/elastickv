package etcd

import (
	"math"

	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Four Entry allowances per prospective member cover Changer's shallow
// Progress copy/maps, joint ConfState arrays/clones, inventory and plan records.
// Existing inflight buffers are shared by Changer.checkAndCopy, not cloned.
const pipelineConfigurationMemberBytes = pipelinePhysicalEntryBytes * 4

func (e *Engine) pipelineConfigurationPreflightBytes(entry *raftpb.Entry) (uint64, error) {
	size, err := pipelineConfigurationEntryBytes(entry)
	if err != nil {
		return 0, err
	}
	size = pipelineAddBytes(size, pipelineArrayBytes(len(e.peers), pipelineConfigurationMemberBytes))
	for _, peer := range e.peers {
		size = pipelineAddBytes(size, pipelineArrayBytes(len(peer.ID)+len(peer.Address), outboundCapacityFactor))
	}
	if size > pipelineControlBytes {
		return 0, errors.WithStack(errStorageWorkInvalidReservation)
	}
	return size, nil
}

func pipelineConfigurationEntryBytes(entry *raftpb.Entry) (uint64, error) {
	if entry == nil || (entry.GetType() != raftpb.EntryConfChange && entry.GetType() != raftpb.EntryConfChangeV2) {
		return 0, errors.WithStack(errInvalidStorageWork)
	}
	if len(entry.Data) > pipelineControlBytes/(outboundCapacityFactor*outboundCapacityFactor) {
		return 0, errors.WithStack(errStorageWorkInvalidReservation)
	}
	changes, err := pipelineConfigurationWireChanges(entry.GetData(), entry.GetType() == raftpb.EntryConfChangeV2)
	if err != nil {
		return 0, err
	}
	// Context/unknown decode, peer string conversion and allocator-rounded
	// arrays are reserved before Unmarshal or native ApplyConfChange executes.
	size := pipelineAddBytes(pipelineTaskOverhead, pipelineArrayBytes(len(entry.Data), outboundCapacityFactor*outboundCapacityFactor))
	size = pipelineAddBytes(size, pipelineArrayBytes(changes, pipelineConfigurationMemberBytes))
	if size > pipelineControlBytes {
		return 0, errors.WithStack(errStorageWorkInvalidReservation)
	}
	return size, nil
}

// RawNode.stepLeader also unmarshals proposed configuration entries. Its
// temporary decode must be reserved at ingress, not only at ordered apply.
// The loop's previous decode dies before the next, so reserve the peak, not a
// second persistent copy of every entry in the batch.
func (p *engineStoragePipeline) reservePipelineConfigurationDecode(message *raftpb.Message) (*storageWorkPermit, error) {
	var peak uint64
	for _, entry := range message.Entries {
		if entry.GetType() != raftpb.EntryConfChange && entry.GetType() != raftpb.EntryConfChangeV2 {
			continue
		}
		bytes, err := pipelineConfigurationEntryBytes(entry)
		if err != nil {
			return nil, err
		}
		if bytes > peak {
			peak = bytes
		}
	}
	if peak == 0 {
		return nil, nil
	}
	permit, err := p.controlOwnership.tryAcquire(peak)
	if err != nil {
		return nil, err
	}
	for _, entry := range message.Entries {
		if err := validatePipelineConfigurationGrammar(entry); err != nil {
			permit.Release()
			return nil, err
		}
	}
	return permit, nil
}

// The zero-copy wire scan bounds known-message expansion first. Only after its
// control reservation succeeds do we allocate the standard protobuf decoder's
// temporary result. This also validates nested ConfChangeSingle/unknown groups
// before native stepLeader can panic on invalid protobuf grammar.
func validatePipelineConfigurationGrammar(entry *raftpb.Entry) error {
	var message proto.Message
	switch entry.GetType() {
	case raftpb.EntryConfChange:
		message = &raftpb.ConfChange{}
	case raftpb.EntryConfChangeV2:
		message = &raftpb.ConfChangeV2{}
	case raftpb.EntryNormal:
		return nil
	default:
		return nil
	}
	if err := proto.Unmarshal(entry.Data, message); err != nil {
		return errors.Wrap(errors.WithSecondaryError(errInvalidStorageWork, err), "invalid configuration protobuf")
	}
	return nil
}

func pipelineConfigurationWireChanges(data []byte, v2 bool) (int, error) {
	changes := 1
	if v2 {
		changes = 0
	}
	for len(data) != 0 {
		number, kind, tag := protowire.ConsumeTag(data)
		if tag < 0 {
			return 0, errors.WithStack(protowire.ParseError(tag))
		}
		data = data[tag:]
		size := protowire.ConsumeFieldValue(number, kind, data)
		if size < 0 {
			return 0, errors.WithStack(protowire.ParseError(size))
		}
		if v2 && number == 2 && kind == protowire.BytesType {
			if changes >= pipelineControlBytes/pipelineConfigurationMemberBytes {
				return 0, errors.WithStack(errStorageWorkInvalidReservation)
			}
			changes++
		}
		data = data[size:]
	}
	return changes, nil
}

// This validates borrowed recovery input before NewRawNode can emit a first
// oversized committed entry or expand the initial configuration. It is NOT a
// bound on cumulative durable MemoryStorage, mixed-range fan-out, or Snapshot's
// opaque data (InitialState exposes ConfState without cloning, not snapshot Data).
func validatePipelineRecoveredStorage(storage *etcdraft.MemoryStorage) error {
	if storage == nil {
		return errors.WithStack(errStorageWorkInvalidReservation)
	}
	_, state, err := storage.InitialState()
	if err != nil {
		return errors.WithStack(err)
	}
	if err := validatePipelineConfigurationState(state); err != nil {
		return err
	}
	return validatePipelineRecoveredEntries(storage)
}

func validatePipelineConfigurationState(state *raftpb.ConfState) error {
	bytes := pipelineOwnedConfStateBytes(state)
	for _, ids := range [][]uint64{state.GetVoters(), state.GetVotersOutgoing(), state.GetLearners(), state.GetLearnersNext()} {
		bytes = pipelineAddBytes(bytes, pipelineArrayBytes(len(ids), pipelineConfigurationMemberBytes))
	}
	if bytes > pipelineControlBytes {
		return errors.WithStack(errStorageWorkInvalidReservation)
	}
	return nil
}

func validatePipelineRecoveredEntries(storage *etcdraft.MemoryStorage) error {
	first, err := storage.FirstIndex()
	if err != nil {
		return errors.WithStack(err)
	}
	last, err := storage.LastIndex()
	if err != nil {
		return errors.WithStack(err)
	}
	if last == math.MaxUint64 {
		return errors.WithStack(errStorageWorkInvalidReservation)
	}
	if first > last {
		return nil
	}
	entries, err := storage.Entries(first, last+1, math.MaxUint64)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, entry := range entries {
		if entry == nil || pipelineOwnedEntryBytes(entry) > pipelineMaxOwnedInputBytes {
			return errors.WithStack(errStorageWorkInvalidReservation)
		}
	}
	return nil
}

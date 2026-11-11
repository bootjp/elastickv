package etcd

import (
	"sort"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var errSnapshotConfigurationPrefix = errors.New("snapshot configuration prefix unavailable")

type indexedSnapshotConfiguration struct {
	index uint64
	state *raftpb.ConfState
}

// snapshotConfigurationHistory is owner-only. Its base comes from the selected
// Raft snapshot, never the latest peers file, which may lead the applied prefix.
// Captures own clones, so pruning unused history cannot alter in-flight work.
type snapshotConfigurationHistory struct {
	entries []indexedSnapshotConfiguration
}

func (h *snapshotConfigurationHistory) reset(index uint64, state *raftpb.ConfState) {
	h.entries = []indexedSnapshotConfiguration{{index: index, state: proto.CloneOf(state)}}
}

func (h *snapshotConfigurationHistory) record(index uint64, state *raftpb.ConfState) error {
	if len(h.entries) == 0 || state == nil {
		return errors.WithStack(errSnapshotConfigurationPrefix)
	}
	if index < h.entries[0].index {
		// Restart can replay configuration entries already represented by the
		// selected snapshot. They must not replace its more recent base.
		return nil
	}
	last := h.entries[len(h.entries)-1]
	if index == last.index && proto.Equal(state, last.state) {
		return nil
	}
	if index <= last.index {
		return errors.WithStack(errSnapshotConfigurationPrefix)
	}
	h.entries = append(h.entries, indexedSnapshotConfiguration{index: index, state: proto.CloneOf(state)})
	return nil
}

func (h *snapshotConfigurationHistory) at(index uint64) (*raftpb.ConfState, error) {
	i := sort.Search(len(h.entries), func(i int) bool { return h.entries[i].index > index })
	if i == 0 || h.entries[i-1].state == nil {
		return nil, errors.WithStack(errSnapshotConfigurationPrefix)
	}
	return proto.CloneOf(h.entries[i-1].state), nil
}

func (h *snapshotConfigurationHistory) pruneThrough(index uint64) {
	i := sort.Search(len(h.entries), func(i int) bool { return h.entries[i].index > index })
	if i <= 1 {
		return
	}
	// Allocate a new pointer slice rather than retaining a large old backing
	// array (and its superseded configuration objects) behind a small suffix.
	h.entries = append([]indexedSnapshotConfiguration(nil), h.entries[i-1:]...)
}

func (e *Engine) ensureSnapshotConfigurationHistory() error {
	if len(e.snapshotConfigurations.entries) != 0 {
		return nil
	}
	snapshot, err := e.storage.Snapshot()
	if err != nil {
		return errors.WithStack(err)
	}
	e.snapshotConfigurations.reset(snapshot.GetMetadata().GetIndex(), snapshot.GetMetadata().GetConfState())
	return nil
}

func (e *Engine) recordSnapshotConfiguration(plan configApplyPlan) error {
	if err := e.snapshotConfigurations.record(plan.index, &plan.confState); err != nil {
		return err
	}
	if e.storagePipeline == nil {
		// Synchronous capture always runs at the current applied prefix and
		// owns its full state before a later configuration can be applied.
		// No future capture can need an older version, even if FSM snapshots
		// defer forever. Async capture needs its own pins/accounting protocol.
		e.snapshotConfigurations.pruneThrough(plan.index)
	}
	if plan.index >= e.snapshotConfigurationReplayIndex {
		e.snapshotConfigurationReplayIndex = 0
	}
	return nil
}

func (e *Engine) snapshotConfigurationAt(index uint64) (*raftpb.ConfState, error) {
	if err := e.ensureSnapshotConfigurationHistory(); err != nil {
		return nil, err
	}
	current, err := e.storage.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	e.snapshotConfigurations.pruneThrough(current.GetMetadata().GetIndex())
	return e.snapshotConfigurations.at(index)
}

// Capture both halves on the owner at the same applied prefix. The disk worker
// must not consult RawNode, peer metadata, or this owner-only history later.
func (e *Engine) captureLocalSnapshotRequest(index uint64) (snapshotRequest, error) {
	if replay := e.snapshotConfigurationReplayIndex; replay != 0 && replay <= index {
		return snapshotRequest{}, errors.Wrap(raftengine.ErrSnapshotDeferred, "committed configuration replay has not reached the FSM prefix")
	}
	if pending := e.pendingConfChangeIndex.Load(); pending != 0 && pending <= index {
		return snapshotRequest{}, errors.Wrap(raftengine.ErrSnapshotDeferred, "configuration replay has not reached the FSM prefix")
	}
	state, err := e.snapshotConfigurationAt(index)
	if err != nil {
		return snapshotRequest{}, err
	}
	snapshot, err := e.fsm.Snapshot()
	if err != nil {
		return snapshotRequest{}, errors.WithStack(err)
	}
	return snapshotRequest{index: index, snapshot: snapshot, confState: state}, nil
}

package etcd

import (
	"io"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

// AdaptHashicorpFSM bridges a hashicorp/raft FSM into the etcd-backed engine.
func AdaptHashicorpFSM(fsm raft.FSM) StateMachine {
	if fsm == nil {
		return nil
	}
	return hashicorpFSMAdapter{fsm: fsm}
}

type hashicorpFSMAdapter struct {
	fsm raft.FSM
}

func (a hashicorpFSMAdapter) Apply(data []byte) any {
	return a.fsm.Apply(&raft.Log{Type: raft.LogCommand, Data: data})
}

func (a hashicorpFSMAdapter) Snapshot() (Snapshot, error) {
	snapshot, err := a.fsm.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return raftFSMSnapshotAdapter{snapshot: snapshot}, nil
}

func (a hashicorpFSMAdapter) Restore(r io.Reader) error {
	return errors.WithStack(a.fsm.Restore(io.NopCloser(r)))
}

type raftFSMSnapshotAdapter struct {
	snapshot raft.FSMSnapshot
}

func (a raftFSMSnapshotAdapter) WriteTo(w io.Writer) (int64, error) {
	sink := &raftSnapshotSinkAdapter{writer: w}
	if err := a.snapshot.Persist(sink); err != nil {
		return sink.written, errors.WithStack(err)
	}
	return sink.written, nil
}

func (a raftFSMSnapshotAdapter) Close() error {
	a.snapshot.Release()
	return nil
}

type raftSnapshotSinkAdapter struct {
	writer  io.Writer
	written int64
}

func (s *raftSnapshotSinkAdapter) ID() string {
	return "etcd-hashicorp-fsm-adapter"
}

func (s *raftSnapshotSinkAdapter) Cancel() error {
	return nil
}

func (s *raftSnapshotSinkAdapter) Close() error {
	return nil
}

func (s *raftSnapshotSinkAdapter) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	s.written += int64(n)
	return n, errors.WithStack(err)
}

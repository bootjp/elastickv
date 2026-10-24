package etcd

import (
	"slices"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func namespaceApplyMembers(state raftpb.ConfState) []uint64 {
	ids := append(slices.Clone(state.GetVoters()), state.GetVotersOutgoing()...)
	ids = append(ids, state.GetLearners()...)
	ids = append(ids, state.GetLearnersNext()...)
	slices.Sort(ids)
	return slices.Compact(ids)
}

func recordNamespaceConfiguration(fsm StateMachine, index uint64, state raftpb.ConfState) error {
	if aware, ok := fsm.(raftengine.NamespaceConfigurationAware); ok {
		return errors.WithStack(aware.RecordAppliedNamespaceConfiguration(index, namespaceApplyMembers(state)))
	}
	if aware, ok := fsm.(raftengine.AppliedConfigurationAware); ok {
		return errors.WithStack(aware.RecordAppliedConfiguration(index))
	}
	return nil
}

func recoverNamespaceConfiguration(fsm StateMachine, snapshot raftpb.Snapshot) error {
	if aware, ok := fsm.(raftengine.NamespaceConfigurationAware); ok {
		return errors.WithStack(aware.RecoverNamespaceConfiguration(snapshot.GetMetadata().GetIndex(), namespaceApplyMembers(confStateValue(snapshot.GetMetadata().GetConfState()))))
	}
	return nil
}

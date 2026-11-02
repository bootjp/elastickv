package etcd

import (
	"cmp"
	"slices"

	"github.com/bootjp/elastickv/internal/raftengine"
)

func (e *Engine) deliverApplyNativeConfiguration() func() {
	aware, ok := e.fsm.(raftengine.ApplyNativeConfigurationAware)
	if !ok {
		return func() {}
	}
	if e.rawNode == nil || e.recoveredIdentity == nil {
		aware.ClearApplyNativeConfiguration()
		return aware.ClearApplyNativeConfiguration
	}
	config := e.rawNode.Status().Config
	image := raftengine.NativeConfiguration{Index: e.nativeConfigurationIndex, AutoLeave: config.AutoLeave}
	for id := range config.Voters[0] {
		image.Voters = append(image.Voters, id)
	}
	for id := range config.Voters[1] {
		image.VotersOutgoing = append(image.VotersOutgoing, id)
	}
	for id := range config.Learners {
		image.Learners = append(image.Learners, id)
	}
	for id := range config.LearnersNext {
		image.LearnersNext = append(image.LearnersNext, id)
	}
	slices.Sort(image.Voters)
	slices.Sort(image.VotersOutgoing)
	slices.Sort(image.Learners)
	slices.Sort(image.LearnersNext)
	for id, peer := range e.peers {
		image.Members = append(image.Members, raftengine.NativeConfigurationMember{ID: id, Address: peer.Address})
	}
	slices.SortFunc(image.Members, func(a, b raftengine.NativeConfigurationMember) int {
		return cmp.Compare(a.ID, b.ID)
	})
	aware.SetApplyNativeConfiguration(image)
	return aware.ClearApplyNativeConfiguration
}

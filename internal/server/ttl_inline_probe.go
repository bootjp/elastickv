package server

import (
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
)

func ttlInlineInternalOptions(rt *clusterbootstrap.GroupRuntime) []adapter.InternalOption {
	if rt.BootstrapPeerConfig == nil {
		return nil
	}
	engine, _ := rt.Engine.(interface {
		NativeConfigurationSnapshot() (raftengine.NativeConfiguration, uint64, bool)
	})
	fsm, _ := rt.StateMachine.(interface{ TTLInlineFenceReady() bool })
	probe := clusterbootstrap.NewTTLInlineLiveProbe(rt.Spec.ID, rt.BootstrapPeerConfig.Local.Member, engine, fsm)
	return []adapter.InternalOption{adapter.WithInternalTTLInlineCapabilityProbe(probe)}
}

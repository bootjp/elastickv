package main

import (
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	"google.golang.org/grpc"
)

// raftGroupRuntime is a Raft group's runtime plus the protected-bootstrap
// state attached to it. The bootstrap fields stay in package main until the
// bootstrap area moves out (M6 of the root package decomposition).
type raftGroupRuntime struct {
	*noderuntime.GroupRuntime
	bootstrapDomain     *bootstrapDomain
	bootstrapPeerConfig *bootstrapPeerConfig
	bootstrapNetwork    *bootstrapNetwork
}

// snapshotEngine is GroupRuntime.SnapshotEngine, nil-safe on the wrapper.
func (r *raftGroupRuntime) snapshotEngine() raftengine.Engine {
	if r == nil {
		return nil
	}
	return r.SnapshotEngine()
}

// Close is GroupRuntime.Close, nil-safe on the wrapper.
func (r *raftGroupRuntime) Close() {
	if r == nil {
		return
	}
	r.GroupRuntime.Close()
}

// registerGRPC is GroupRuntime.RegisterGRPC, nil-safe on the wrapper.
func (r *raftGroupRuntime) registerGRPC(server grpc.ServiceRegistrar) {
	if r == nil {
		return
	}
	r.RegisterGRPC(server)
}

// runtimeCore returns rt's noderuntime core, or nil for a nil wrapper.
func runtimeCore(rt *raftGroupRuntime) *noderuntime.GroupRuntime {
	if rt == nil {
		return nil
	}
	return rt.GroupRuntime
}

// runtimeCores returns the noderuntime cores of runtimes, keeping nil entries
// as nil, for the areas that need only the core.
func runtimeCores(runtimes []*raftGroupRuntime) []*noderuntime.GroupRuntime {
	if runtimes == nil {
		return nil
	}
	out := make([]*noderuntime.GroupRuntime, len(runtimes))
	for i, rt := range runtimes {
		out[i] = runtimeCore(rt)
	}
	return out
}

// findDefaultGroupRuntime returns the runtime whose spec.id
// matches defaultGroup, or nil if no such runtime is present in
// the supplied slice. Used by the startup-guard phase to scope
// the §9.1 gap check to the group whose Raft log advances the
// sidecar's raft_applied_index.
func findDefaultGroupRuntime(runtimes []*raftGroupRuntime, defaultGroup uint64) *raftGroupRuntime {
	for _, rt := range runtimes {
		if rt != nil && rt.Spec.ID == defaultGroup {
			return rt
		}
	}
	return nil
}

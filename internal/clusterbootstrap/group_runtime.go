package clusterbootstrap

import (
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
)

// GroupRuntime is a Raft group's runtime plus the protected-bootstrap
// state attached to it. The bootstrap fields stay in package main until the
// bootstrap area moves out (M6 of the root package decomposition).
type GroupRuntime struct {
	*noderuntime.GroupRuntime
	BootstrapDomain     *BootstrapDomain
	BootstrapPeerConfig *BootstrapPeerConfig
	BootstrapNetwork    *BootstrapNetwork
}

// snapshotEngine is GroupRuntime.SnapshotEngine, nil-safe on the wrapper.
func (r *GroupRuntime) snapshotEngine() raftengine.Engine {
	if r == nil {
		return nil
	}
	return r.SnapshotEngine()
}

// Close is GroupRuntime.Close, nil-safe on the wrapper.
func (r *GroupRuntime) Close() {
	if r == nil {
		return
	}
	r.GroupRuntime.Close()
}

// RuntimeCore returns rt's noderuntime core, or nil for a nil wrapper.
func RuntimeCore(rt *GroupRuntime) *noderuntime.GroupRuntime {
	if rt == nil {
		return nil
	}
	return rt.GroupRuntime
}

// RuntimeCores returns the noderuntime cores of runtimes, keeping nil entries
// as nil, for the areas that need only the core.
func RuntimeCores(runtimes []*GroupRuntime) []*noderuntime.GroupRuntime {
	if runtimes == nil {
		return nil
	}
	out := make([]*noderuntime.GroupRuntime, len(runtimes))
	for i, rt := range runtimes {
		out[i] = RuntimeCore(rt)
	}
	return out
}

// FindDefaultGroupRuntime returns the runtime whose spec.id
// matches defaultGroup, or nil if no such runtime is present in
// the supplied slice. Used by the startup-guard phase to scope
// the §9.1 gap check to the group whose Raft log advances the
// sidecar's raft_applied_index.
func FindDefaultGroupRuntime(runtimes []*GroupRuntime, defaultGroup uint64) *GroupRuntime {
	for _, rt := range runtimes {
		if rt != nil && rt.Spec.ID == defaultGroup {
			return rt
		}
	}
	return nil
}

// GroupID is the Raft group this runtime serves.
func (r *GroupRuntime) GroupID() uint64 {
	return r.Spec.ID
}

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

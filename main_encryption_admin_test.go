package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"google.golang.org/grpc"
)

// stubEncryptionAdminEngine satisfies the encryptionAdminEngine
// interface (Proposer + LeaderView) with do-nothing impls. The
// registration helper only stores the engine and validates the
// option pairing, so the methods are unreachable from the
// registration path itself.
type stubEncryptionAdminEngine struct{}

func (stubEncryptionAdminEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{}, nil
}
func (stubEncryptionAdminEngine) State() raftengine.State       { return raftengine.StateLeader }
func (stubEncryptionAdminEngine) Leader() raftengine.LeaderInfo { return raftengine.LeaderInfo{} }
func (stubEncryptionAdminEngine) VerifyLeader(context.Context) error {
	return nil
}
func (stubEncryptionAdminEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

func TestRegisterEncryptionAdminServer_Registers(t *testing.T) {
	gs := grpc.NewServer()
	registerEncryptionAdminServer(gs, stubEncryptionAdminEngine{}, 1)
	// reflection-style check: the service descriptor must show up
	// in the registered service map. grpc.Server exposes this via
	// GetServiceInfo.
	info := gs.GetServiceInfo()
	// The .proto declares no package statement, so the service
	// name surfaces as the bare "EncryptionAdmin" in
	// grpc.Server.GetServiceInfo() (not "proto.EncryptionAdmin").
	if _, ok := info["EncryptionAdmin"]; !ok {
		var registered []string
		for name := range info {
			registered = append(registered, name)
		}
		t.Fatalf("EncryptionAdmin not registered; got services=%v", registered)
	}
}

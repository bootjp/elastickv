package server

import (
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/kv"
)

func TestEncryptionAdminDefaultRuntimeOptionsUsesDefaultGroupRuntime(t *testing.T) {
	t.Parallel()

	defaultEngine := &stubEngineForProposerTest{}
	otherEngine := &stubEngineForProposerTest{}
	defaultGroup := &kv.ShardGroup{Engine: defaultEngine}
	otherGroup := &kv.ShardGroup{Engine: otherEngine}
	_ = kv.NewLeaderProxyForShardGroup(defaultGroup)
	_ = kv.NewLeaderProxyForShardGroup(otherGroup)

	got, err := encryptionwiring.AdminDefaultRuntimeOptions(
		clusterbootstrap.RuntimeCores([]*clusterbootstrap.GroupRuntime{
			{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 1}, Engine: defaultEngine}},
			{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 2}, Engine: otherEngine}},
		}),
		map[uint64]*kv.ShardGroup{
			1: defaultGroup,
			2: otherGroup,
		},
		1,
		true,
	)
	if err != nil {
		t.Fatalf("encryptionwiring.AdminDefaultRuntimeOptions: %v", err)
	}
	if got.Engine != encryptionwiring.AdminEngine(defaultEngine) {
		t.Fatalf("engine=%T, want default group engine", got.Engine)
	}
	if got.PostCutoverProposer != defaultGroup.Proposer() {
		t.Error("postCutoverProposer did not use the default group's wrap-aware proposer")
	}
	if got.PostCutoverProposer == otherGroup.Proposer() {
		t.Error("postCutoverProposer unexpectedly used the non-default group's proposer")
	}
}

package hashicorp_test

import (
	"testing"
	"time"

	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
)

func TestConformance(t *testing.T) {
	factory := hashicorpraftengine.NewFactory(hashicorpraftengine.FactoryConfig{
		CommitTimeout:       50 * time.Millisecond,
		HeartbeatTimeout:    200 * time.Millisecond,
		ElectionTimeout:     2000 * time.Millisecond,
		LeaderLeaseTimeout:  100 * time.Millisecond,
		SnapshotRetainCount: 3,
	})
	raftenginetest.RunConformanceSuite(t, factory)
}

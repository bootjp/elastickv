package etcd_test

import (
	"testing"
	"time"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
)

func TestConformance(t *testing.T) {
	factory := etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval:   10 * time.Millisecond,
		HeartbeatTick:  1,
		ElectionTick:   10,
		MaxSizePerMsg:  1 << 20,
		MaxInflightMsg: 256,
	})
	raftenginetest.RunConformanceSuite(t, factory)
}

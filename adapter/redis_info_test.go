package adapter

import (
	"context"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type infoTestCoordinator struct {
	isLeader   bool
	raftLeader raft.ServerAddress
	clock      *kv.HLC
}

func (c *infoTestCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return &kv.CoordinateResponse{}, nil
}
func (c *infoTestCoordinator) IsLeader() bool                             { return c.isLeader }
func (c *infoTestCoordinator) VerifyLeader() error                        { return nil }
func (c *infoTestCoordinator) RaftLeader() raft.ServerAddress             { return c.raftLeader }
func (c *infoTestCoordinator) IsLeaderForKey([]byte) bool                 { return c.isLeader }
func (c *infoTestCoordinator) VerifyLeaderForKey([]byte) error            { return nil }
func (c *infoTestCoordinator) RaftLeaderForKey([]byte) raft.ServerAddress { return c.raftLeader }
func (c *infoTestCoordinator) Clock() *kv.HLC {
	if c.clock == nil {
		c.clock = kv.NewHLC()
	}
	return c.clock
}

func (c *infoTestCoordinator) LinearizableRead(_ context.Context) (uint64, error) { return 0, nil }
func (c *infoTestCoordinator) LeaseRead(ctx context.Context) (uint64, error) {
	return c.LinearizableRead(ctx)
}
func (c *infoTestCoordinator) LeaseReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	return c.LinearizableRead(ctx)
}

func TestRedisServer_Info_LeaderRole(t *testing.T) {
	r := &RedisServer{
		redisAddr:   "10.0.0.1:6379",
		leaderRedis: map[raft.ServerAddress]string{"raft-1": "10.0.0.1:6379"},
		coordinator: &infoTestCoordinator{isLeader: true, raftLeader: "raft-1"},
	}

	conn := &recordingConn{}
	r.info(conn, redcon.Command{})

	out := string(conn.bulk)
	assert.Contains(t, out, "# Server", "INFO reply must keep the Server section")
	assert.Contains(t, out, "# Replication", "INFO reply must expose a Replication section")
	assert.Contains(t, out, "role:master", "leader must advertise role:master")
	assert.Contains(t, out, "raft_leader_redis:10.0.0.1:6379",
		"leader must point raft_leader_redis at itself")
}

func TestRedisServer_Info_FollowerRole(t *testing.T) {
	r := &RedisServer{
		redisAddr: "10.0.0.2:6379",
		leaderRedis: map[raft.ServerAddress]string{
			"raft-1": "10.0.0.1:6379",
			"raft-2": "10.0.0.2:6379",
		},
		coordinator: &infoTestCoordinator{isLeader: false, raftLeader: "raft-1"},
	}

	conn := &recordingConn{}
	r.info(conn, redcon.Command{})

	out := string(conn.bulk)
	assert.Contains(t, out, "role:slave", "follower must advertise role:slave")
	assert.Contains(t, out, "raft_leader_redis:10.0.0.1:6379",
		"follower must point raft_leader_redis at the actual leader")
	// The role must appear in the Replication section so clients that only
	// scan that section still see the right value.
	idx := strings.Index(out, "# Replication")
	require.GreaterOrEqual(t, idx, 0)
	assert.Contains(t, out[idx:], "role:slave")
	assert.Contains(t, out[idx:], "raft_leader_redis:10.0.0.1:6379")
}

func TestRedisServer_Info_UnknownLeader(t *testing.T) {
	r := &RedisServer{
		redisAddr:   "10.0.0.3:6379",
		leaderRedis: map[raft.ServerAddress]string{},
		coordinator: &infoTestCoordinator{isLeader: false, raftLeader: ""},
	}

	conn := &recordingConn{}
	r.info(conn, redcon.Command{})

	out := string(conn.bulk)
	assert.Contains(t, out, "role:slave")
	assert.Contains(t, out, "raft_leader_redis:\r\n",
		"when the leader is unknown the field must be empty so clients know to keep probing")
}

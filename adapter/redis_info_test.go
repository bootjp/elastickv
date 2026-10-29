package adapter

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func TestRedisServer_Info_LeaderRole(t *testing.T) {
	r := &RedisServer{
		redisAddr:   "10.0.0.1:6379",
		leaderRedis: map[string]string{"raft-1": "10.0.0.1:6379"},
		coordinator: &stubAdapterCoordinator{leaderSet: true, leader: true, raftLeader: "raft-1"},
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
		leaderRedis: map[string]string{
			"raft-1": "10.0.0.1:6379",
			"raft-2": "10.0.0.2:6379",
		},
		coordinator: &stubAdapterCoordinator{leaderSet: true, leader: false, raftLeader: "raft-1"},
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
		leaderRedis: map[string]string{},
		coordinator: &stubAdapterCoordinator{leaderSet: true, leader: false, raftLeader: ""},
	}

	conn := &recordingConn{}
	r.info(conn, redcon.Command{})

	out := string(conn.bulk)
	assert.Contains(t, out, "role:slave")
	assert.Contains(t, out, "raft_leader_redis:\r\n",
		"when the leader is unknown the field must be empty so clients know to keep probing")
}

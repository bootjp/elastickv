package kv

import (
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCConnCache reuses gRPC connections per address. gRPC itself handles
// reconnection on transient failures; we only force a re-dial if the conn has
// already been closed (Shutdown).
type GRPCConnCache struct {
	mu    sync.Mutex
	conns map[raft.ServerAddress]*grpc.ClientConn
}

func (c *GRPCConnCache) ConnFor(addr raft.ServerAddress) (*grpc.ClientConn, error) {
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		c.conns = make(map[raft.ServerAddress]*grpc.ClientConn)
	}
	if conn, ok := c.conns[addr]; ok && conn != nil {
		if st := conn.GetState(); st == connectivity.Shutdown {
			delete(c.conns, addr)
		} else {
			if st == connectivity.TransientFailure {
				conn.ResetConnectBackoff()
			}
			return conn, nil
		}
	}

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *GRPCConnCache) Close() error {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.mu.Unlock()

	var first error
	for _, conn := range conns {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil && first == nil {
			first = errors.WithStack(err)
		}
	}
	return first
}

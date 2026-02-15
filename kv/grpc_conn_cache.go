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

func (c *GRPCConnCache) cachedConn(addr raft.ServerAddress) *grpc.ClientConn {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		c.conns = make(map[raft.ServerAddress]*grpc.ClientConn)
	}

	conn, ok := c.conns[addr]
	if !ok || conn == nil {
		return nil
	}

	st := conn.GetState()
	if st == connectivity.Shutdown {
		delete(c.conns, addr)
		return nil
	}
	if st == connectivity.TransientFailure {
		conn.ResetConnectBackoff()
	}
	return conn
}

func (c *GRPCConnCache) storeConn(addr raft.ServerAddress, conn *grpc.ClientConn) *grpc.ClientConn {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		c.conns = make(map[raft.ServerAddress]*grpc.ClientConn)
	}

	existing, ok := c.conns[addr]
	if ok && existing != nil {
		st := existing.GetState()
		if st != connectivity.Shutdown {
			if st == connectivity.TransientFailure {
				existing.ResetConnectBackoff()
			}
			return existing
		}
		delete(c.conns, addr)
	}

	c.conns[addr] = conn
	return conn
}

func (c *GRPCConnCache) ConnFor(addr raft.ServerAddress) (*grpc.ClientConn, error) {
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	if conn := c.cachedConn(addr); conn != nil {
		return conn, nil
	}

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	stored := c.storeConn(addr, conn)
	if stored != conn {
		_ = conn.Close()
	}
	return stored, nil
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

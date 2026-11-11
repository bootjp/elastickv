package kv

import (
	"sync"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// GRPCConnCache reuses gRPC connections per address. gRPC itself handles
// reconnection on transient failures; we only force a re-dial if the conn has
// already been closed (Shutdown).
type GRPCConnCache struct {
	mu          sync.Mutex
	conns       map[string]*grpc.ClientConn
	dialOptions GRPCDialOptionsForAddress
	used        bool
	closed      bool
	epoch       uint64
}

// GRPCDialOptionsForAddress is an immutable runtime-local dial capability. It
// must reject unknown addresses and supply the complete transport/auth options.
type GRPCDialOptionsForAddress func(string) ([]grpc.DialOption, error)

func NewGRPCConnCache(options GRPCDialOptionsForAddress) *GRPCConnCache {
	return &GRPCConnCache{dialOptions: options}
}

// SetDialOptions is startup-only. Never mix previously cached public/insecure
// connections with a newly installed protected runtime capability.
func (c *GRPCConnCache) SetDialOptions(options GRPCDialOptionsForAddress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if options == nil || c.used || c.closed || c.dialOptions != nil || len(c.conns) != 0 {
		return errors.New("gRPC dial capability must be installed once before first use")
	}
	c.dialOptions = options
	return nil
}

func (c *GRPCConnCache) cachedConn(addr string) *grpc.ClientConn {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		c.conns = make(map[string]*grpc.ClientConn)
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
	return conn
}

func (c *GRPCConnCache) storeConn(addr string, conn *grpc.ClientConn) *grpc.ClientConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.storeConnLocked(addr, conn)
}

func (c *GRPCConnCache) storeConnLocked(addr string, conn *grpc.ClientConn) *grpc.ClientConn {
	if c.conns == nil {
		c.conns = make(map[string]*grpc.ClientConn)
	}

	existing, ok := c.conns[addr]
	if ok && existing != nil {
		st := existing.GetState()
		if st != connectivity.Shutdown {
			return existing
		}
		delete(c.conns, addr)
	}

	c.conns[addr] = conn
	return conn
}

func (c *GRPCConnCache) ConnFor(addr string) (*grpc.ClientConn, error) {
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	c.mu.Lock()
	c.used = true
	provider, epoch := c.dialOptions, c.epoch
	closed := c.closed && provider != nil
	c.mu.Unlock()
	if closed {
		return nil, errors.New("protected gRPC connection cache is closed")
	}
	options := internalutil.GRPCDialOptions()
	if provider != nil {
		var err error
		options, err = provider(addr)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if conn := c.cachedConn(addr); conn != nil {
		return conn, nil
	}

	conn, err := grpc.NewClient(
		addr,
		append(options, grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))...,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c.mu.Lock()
	if c.epoch != epoch {
		c.mu.Unlock()
		_ = conn.Close()
		return nil, errors.New("gRPC connection cache closed during dial")
	}
	stored := c.storeConnLocked(addr, conn)
	c.mu.Unlock()
	if stored != conn {
		_ = conn.Close()
	}
	return stored, nil
}

func (c *GRPCConnCache) Close() error {
	c.mu.Lock()
	c.epoch++
	c.closed = true
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

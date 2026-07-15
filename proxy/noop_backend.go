package proxy

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// noopBackend is used for modes with no secondary backend. Keeping a concrete
// Backend avoids starting unnecessary leader-discovery goroutines in redis-only
// and elastickv-only modes while preserving DualWriter's simple shape.
type noopBackend struct {
	name string
}

// NewNoopBackend returns a Backend placeholder for an intentionally unused
// side of the proxy.
func NewNoopBackend(name string) Backend {
	return noopBackend{name: name}
}

func (n noopBackend) Do(ctx context.Context, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx, args...)
	cmd.SetErr(ErrNoLeaderBackend)
	return cmd
}

func (n noopBackend) Pipeline(context.Context, [][]any) ([]*redis.Cmd, error) {
	return nil, ErrNoLeaderBackend
}

func (n noopBackend) Close() error {
	return nil
}

func (n noopBackend) Name() string {
	return n.name
}

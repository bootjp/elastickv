package adapter

import (
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// SetInternalGRPCDialOptions is startup-only, before Serve/relay work begins.
func (r *RedisServer) SetInternalGRPCDialOptions(options kv.GRPCDialOptionsForAddress) error {
	return errors.WithStack(r.relayConnCache.SetDialOptions(options))
}

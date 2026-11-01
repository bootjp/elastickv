package peersession

import (
	"context"
	"sync"

	"google.golang.org/grpc/peer"
)

// CaptureServerTransportAbort captures only a current, natively authenticated
// server connection. The returned capability closes the entire physical
// connection, including unrelated multiplexed RPCs. Invoke it outside admission
// and owner locks; closing the transport may block. It never targets a replacement
// connection, and is safe to invoke after the captured connection has retired.
func CaptureServerTransportAbort(ctx context.Context) (func(), error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrDenied
	}
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil {
		return nil, ErrDenied
	}
	auth, ok := p.AuthInfo.(*ServerAuthInfo)
	if !ok || auth == nil || auth.connection == nil {
		return nil, ErrDenied
	}
	if _, valid := auth.TransportTLSInfo(); !valid {
		return nil, ErrDenied
	}
	c := auth.connection
	o := c.owner
	return sync.OnceFunc(func() { o.closeConnection(c) }), nil
}

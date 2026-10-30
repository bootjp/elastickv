package peersession

import "google.golang.org/grpc/credentials"

// TransportTLSInfo returns the owner's TLS snapshot only while this AuthInfo
// refers to a current native SERVER connection. The exported inspection copy
// of TLSInfo is deliberately not consulted: copying it into an unminted wrapper
// must not create a transport proof.
//
// This is a point-in-time confidentiality check, NOT a peer identity, mTLS,
// completed session or role/admission proof. Public TLS without a client
// certificate is supported; callers retain their existing token authorization.
func (a *ServerAuthInfo) TransportTLSInfo() (credentials.TLSInfo, bool) {
	if a == nil || a.connection == nil {
		return credentials.TLSInfo{}, false
	}
	c := a.connection
	o := c.owner
	if o == nil {
		return credentials.TLSInfo{}, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || c.closed || c.client || !o.connections[c] || c.transport == nil {
		return credentials.TLSInfo{}, false
	}
	return c.tls, true
}

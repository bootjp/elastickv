package peersession

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/credentials"
)

// AuthInfo types are exported for inspection only. Only this owner's transport
// credentials can populate their private physical-connection reference.
type ClientAuthInfo struct {
	credentials.TLSInfo
	connection *connection
}
type ServerAuthInfo struct {
	credentials.TLSInfo
	connection *connection
}

type transportCredentials struct {
	base  credentials.TransportCredentials
	owner *Owner
}
type ownedConn struct {
	net.Conn
	c *connection
}

func (c *ownedConn) Close() error { c.c.owner.closeConnection(c.c); return nil }

func (o *Owner) TransportCredentials(cfg *tls.Config, server bool) (credentials.TransportCredentials, error) {
	if err := o.validateTLS(cfg, server); err != nil {
		return nil, err
	}
	clone := cfg.Clone()
	if clone.MinVersion < tls.VersionTLS12 {
		clone.MinVersion = tls.VersionTLS12
	}
	return &transportCredentials{base: credentials.NewTLS(clone), owner: o}, nil
}

func (o *Owner) validateTLS(cfg *tls.Config, server bool) error {
	if cfg == nil || cfg.InsecureSkipVerify || cfg.GetConfigForClient != nil || cfg.GetCertificate != nil || cfg.GetClientCertificate != nil {
		return ErrDenied
	}
	if cfg.MaxVersion != 0 && cfg.MaxVersion < tls.VersionTLS12 {
		return ErrDenied
	}
	if cfg.Renegotiation != tls.RenegotiateNever {
		return ErrDenied
	}
	if !validTLSMode(cfg, server) {
		return ErrDenied
	}
	return o.validateCertificates(cfg.Certificates)
}

func validTLSMode(cfg *tls.Config, server bool) bool {
	if !server {
		return true
	}
	return cfg.ClientCAs != nil && (cfg.ClientAuth == tls.VerifyClientCertIfGiven || cfg.ClientAuth == tls.RequireAndVerifyClientCert)
}

func (o *Owner) validateCertificates(certs []tls.Certificate) error {
	if len(certs) == 0 {
		return ErrDenied
	}
	for _, cert := range certs {
		if len(cert.Certificate) == 0 {
			return ErrDenied
		}
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil || keyDigest(leaf.RawSubjectPublicKeyInfo) != o.local.Key {
			return ErrDenied
		}
	}
	return nil
}

func (t *transportCredentials) ClientHandshake(ctx context.Context, authority string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	c, ai, err := t.base.ClientHandshake(ctx, authority, raw)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return t.attach(c, ai, true)
}
func (t *transportCredentials) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	c, ai, err := t.base.ServerHandshake(raw)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return t.attach(c, ai, false)
}
func (t *transportCredentials) attach(raw net.Conn, auth credentials.AuthInfo, client bool) (net.Conn, credentials.AuthInfo, error) {
	info, ok := auth.(credentials.TLSInfo)
	if !ok || !info.State.HandshakeComplete || info.State.Version < tls.VersionTLS12 {
		_ = raw.Close()
		return nil, nil, ErrDenied
	}
	o := t.owner
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		_ = raw.Close()
		return nil, nil, ErrClosed
	}
	c := &connection{owner: o, tls: info, client: client, transport: raw, sessions: make(map[uint64]*session), begun: make(map[uint64]bool)}
	c.remote = o.provisionedPeerLocked(info)
	if client && c.remote == nil {
		o.mu.Unlock()
		_ = raw.Close()
		return nil, nil, ErrDenied
	}
	o.connections[c] = true
	o.mu.Unlock()
	conn := &ownedConn{Conn: raw, c: c}
	if client {
		return conn, &ClientAuthInfo{TLSInfo: info, connection: c}, nil
	}
	return conn, &ServerAuthInfo{TLSInfo: info, connection: c}, nil
}

func (o *Owner) provisionedPeerLocked(info credentials.TLSInfo) *Node {
	if len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return nil
	}
	p, exists := o.provisioned[keyDigest(info.State.PeerCertificates[0].RawSubjectPublicKeyInfo)]
	if !exists {
		return nil
	}
	n := p.Node
	return &n
}
func (t *transportCredentials) Info() credentials.ProtocolInfo { return t.base.Info() }
func (t *transportCredentials) Clone() credentials.TransportCredentials {
	return &transportCredentials{base: t.base.Clone(), owner: t.owner}
}
func (*transportCredentials) OverrideServerName(string) error {
	return errors.New("use grpc.WithAuthority for peer TLS authority")
}

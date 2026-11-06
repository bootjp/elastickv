package etcd

import (
	"context"
	"crypto/tls"
	"maps"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/peersession"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type peerSessionTransport struct {
	owner       *peersession.Owner
	group       uint64
	credentials credentials.TransportCredentials
	nodes       map[uint64]peersession.Node
}

// ConfigurePeerSessions is startup-only. The caller owns the session owner and
// must revoke it before publishing any changed applied group projection.
func (t *GRPCTransport) ConfigurePeerSessions(owner *peersession.Owner, group uint64, cfg *tls.Config, nodes map[uint64]peersession.Node) error {
	if owner == nil || len(nodes) == 0 {
		return errors.WithStack(peersession.ErrDenied)
	}
	creds, err := owner.TransportCredentials(cfg, false)
	if err != nil {
		return errors.WithStack(err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.peerSession != nil || t.peerSessionConfigFrozen || len(t.conns) != 0 || len(t.clients) != 0 {
		return errors.New("peer sessions must be configured before transport use")
	}
	if err := validateSessionPeerNodes(t.peers, nodes); err != nil {
		return err
	}
	t.peerSession = &peerSessionTransport{owner: owner, group: group, credentials: creds, nodes: maps.Clone(nodes)}
	return nil
}

func validateSessionPeerNodes(peers map[uint64]Peer, nodes map[uint64]peersession.Node) error {
	for id, peer := range peers {
		node, ok := nodes[id]
		if !ok || node.Member != id || node.Address != peer.Address {
			return errors.WithStack(peersession.ErrDenied)
		}
	}
	return nil
}

type sessionRaftClient struct {
	pb.EtcdRaftClient
	handle *peersession.Handle
}

// Serialize protected handshakes per address without launching detached work.
// Both a queued caller and the handshake itself observe caller/shutdown cancel.
func (t *GRPCTransport) protectedPeerClient(ctx context.Context, peer Peer) (pb.EtcdRaftClient, error) {
	peer = normalizeSessionPeer(peer)
	owned, release, err := t.protectedDialContext(ctx, peer.Address)
	if err != nil {
		return nil, err
	}
	defer release()
	return t.dialProtectedPeer(owned, peer)
}

func normalizeSessionPeer(peer Peer) Peer {
	if peer.NodeID == 0 {
		peer.NodeID = DeriveNodeID(peer.ID)
	}
	if peer.ID == "" {
		peer.ID = peer.Address
	}
	return peer
}

func (t *GRPCTransport) protectedDialContext(ctx context.Context, key string) (context.Context, func(), error) {
	t.mu.Lock()
	if t.peerDialGates == nil {
		t.peerDialGates = make(map[string]chan struct{})
	}
	gate := t.peerDialGates[key]
	if gate == nil {
		gate = make(chan struct{}, 1)
		t.peerDialGates[key] = gate
	}
	lifecycle := t.lifecycleCtx
	t.mu.Unlock()
	owned, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifecycle, cancel)
	select {
	case gate <- struct{}{}:
		return owned, func() { <-gate; stop(); cancel() }, nil
	case <-owned.Done():
		stop()
		cancel()
		return nil, nil, errors.WithStack(owned.Err())
	}
}

func (t *GRPCTransport) dialProtectedPeer(ctx context.Context, peer Peer) (pb.EtcdRaftClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	t.mu.RLock()
	current, exists := t.peers[peer.NodeID]
	generation, protected, closed := t.peerGeneration, t.peerSession, t.closed
	t.mu.RUnlock()
	if closed || !exists || current != peer {
		return nil, errors.WithStack(peersession.ErrDenied)
	}
	if client, ok := t.cachedPeerClient(peer.Address); ok {
		return client, nil
	}
	conn, client, err := protected.dial(ctx, peer)
	if err != nil {
		return nil, err
	}
	return t.publishProtectedPeer(ctx, peer, generation, conn, client)
}

func (t *GRPCTransport) publishProtectedPeer(ctx context.Context, peer Peer, generation uint64, conn *grpc.ClientConn, client pb.EtcdRaftClient) (pb.EtcdRaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	current, exists := t.peers[peer.NodeID]
	if t.closed || ctx.Err() != nil || !exists || current != peer || generation != t.peerGeneration {
		_ = conn.Close()
		return nil, errors.WithStack(peersession.ErrDenied)
	}
	t.conns[peer.Address] = conn
	t.clients[peer.Address] = client
	return client, nil
}

func (t *GRPCTransport) dialPeerClient(peer Peer) (*grpc.ClientConn, pb.EtcdRaftClient, error) {
	t.mu.RLock()
	protected, closed, lifecycle := t.peerSession, t.closed, t.lifecycleCtx
	t.mu.RUnlock()
	if closed && protected != nil {
		return nil, nil, errors.New("etcd raft transport is closed")
	}
	if protected != nil {
		return protected.dial(lifecycle, peer)
	}
	conn, err := grpcNewClient(peer.Address, internalutil.GRPCDialOptions()...)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return conn, pb.NewEtcdRaftClient(conn), nil
}

type sessionClientConn struct {
	conn   *grpc.ClientConn
	handle *peersession.Handle
}

func (c *sessionClientConn) Invoke(ctx context.Context, method string, request, reply any, opts ...grpc.CallOption) error {
	return errors.WithStack(c.handle.Invoke(ctx, c.conn, method, request, reply, opts...))
}

func (c *sessionClientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	stream, err := c.handle.NewStream(ctx, c.conn, desc, method, opts...)
	return stream, errors.WithStack(err)
}

func (s *peerSessionTransport) dial(parent context.Context, peer Peer) (*grpc.ClientConn, pb.EtcdRaftClient, error) {
	node, ok := s.nodes[peer.NodeID]
	if !ok || node.Address != peer.Address {
		return nil, nil, errors.WithStack(peersession.ErrDenied)
	}
	opts := append(internalutil.GRPCDialOptions(), grpc.WithTransportCredentials(s.credentials.Clone()))
	conn, err := grpcNewClient(peer.Address, opts...)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	ctx, cancel := context.WithTimeout(parent, defaultDispatchTimeout)
	defer cancel()
	handle, err := s.owner.Handshake(ctx, conn, s.group, node)
	if err != nil {
		_ = conn.Close()
		return nil, nil, errors.WithStack(err)
	}
	client := pb.NewEtcdRaftClient(&sessionClientConn{conn: conn, handle: handle})
	return conn, &sessionRaftClient{EtcdRaftClient: client, handle: handle}, nil
}

func (t *GRPCTransport) cachedPeerClient(address string) (pb.EtcdRaftClient, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Legacy engines historically reuse this transport after Close. Protected
	// transports instead require a fresh owner and transport for the next boot.
	if t.closed && t.peerSession != nil {
		return nil, false
	}
	t.peerSessionConfigFrozen = true
	client, ok := t.clients[address]
	if protected, isProtected := client.(*sessionRaftClient); isProtected {
		select {
		case <-protected.handle.Done():
			t.closePeerConnLocked(address)
			return nil, false
		default:
		}
	}
	return client, ok
}

package clusterbootstrap

import (
	"context"
	"sync"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type bootstrapControlClientGroup struct {
	owner *peersession.Owner
	Local peersession.Node
	creds credentials.TransportCredentials
	peers map[string]peersession.Node
}

type bootstrapControlPeerKey struct {
	Group   uint64
	address string
}

type bootstrapControlPeer struct {
	serial chan struct{}
	conn   *grpc.ClientConn
	handle *peersession.Handle
}

// Connections/sessions are per group and peer, never borrowed from the public
// GRPCConnCache. No received hint publishes policy or readiness. A failed call
// is never automatically retried with a refreshed session or public transport.
type bootstrapControlClient struct {
	Domain *BootstrapDomain
	groups map[uint64]bootstrapControlClientGroup
	Ctx    context.Context
	Cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	Done   chan struct{}
	active sync.WaitGroup
	peers  map[bootstrapControlPeerKey]*bootstrapControlPeer
}

// Construct after all local Owners have registered, before installing hooks or
// starting receiver work. The parent owns Close and installs Invoke explicitly
// on ShardStore and the dedicated TSO routing option.
func newBootstrapControlClient(ctx context.Context, domain *BootstrapDomain, configs map[uint64]*BootstrapPeerConfig) (*bootstrapControlClient, error) {
	if ctx == nil || domain == nil || domain.Admission == nil || domain.StartupToken == "" || len(configs) == 0 {
		return nil, errors.New("startup control client requires configured local owners and admission")
	}
	domain.mu.Lock()
	owners := make(map[uint64]*peersession.Owner, len(domain.Owners))
	for id, owner := range domain.Owners {
		owners[id] = owner
	}
	domain.mu.Unlock()
	if len(owners) != len(configs) {
		return nil, errors.New("startup control client requires every protected group configuration")
	}
	groups := make(map[uint64]bootstrapControlClientGroup, len(configs))
	for id, config := range configs {
		group, err := newBootstrapControlClientGroup(owners[id], config, id, domain.StartupToken)
		if err != nil {
			return nil, err
		}
		groups[id] = group
	}
	lifetime, cancel := context.WithCancel(ctx)
	return &bootstrapControlClient{Domain: domain, groups: groups, Ctx: lifetime, Cancel: cancel,
		Done: make(chan struct{}), peers: make(map[bootstrapControlPeerKey]*bootstrapControlPeer)}, nil
}

func newBootstrapControlClientGroup(owner *peersession.Owner, config *BootstrapPeerConfig, group uint64, token string) (bootstrapControlClientGroup, error) {
	if owner == nil || config == nil || config.ClientTLS == nil || config.StartupToken != token {
		return bootstrapControlClientGroup{}, errors.New("startup control group lacks matching identity, TLS, or startup credential")
	}
	creds, err := owner.TransportCredentials(config.ClientTLS, false)
	if err != nil {
		return bootstrapControlClientGroup{}, errors.WithStack(err)
	}
	peers := make(map[string]peersession.Node)
	for _, node := range bootstrapSessionNodes(config, group) {
		if _, duplicate := peers[node.Address]; duplicate {
			return bootstrapControlClientGroup{}, errors.New("startup control group has duplicate peer addresses")
		}
		peers[node.Address] = node
	}
	return bootstrapControlClientGroup{owner: owner, Local: config.Local, creds: creds, peers: peers}, nil
}

func (c *bootstrapControlClient) Invoke(ctx context.Context, group uint64, address, method string, request, reply any) error {
	call, stop, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer stop()
	input, output, err := bootstrapControlProtoMessages(request, reply)
	if err != nil {
		return err
	}
	local, peer, err := c.target(group, address)
	if err != nil {
		return err
	}
	descriptor, kind, found := BootstrapStartupControlDescriptor(group, method)
	if !found || bootstrapControlOutgoingMetadataPresent(call) {
		return errors.WithStack(peersession.ErrDenied)
	}
	message, err := descriptor.BoundRequest(peersession.RequestBinding{Group: group, From: local.Local, To: peer}, input)
	if err != nil || !bootstrapOutgoingControlTraffic(message.Traffic) {
		// Activation is the separately owned LOCAL recovery worker, not a
		// standalone remote client manufacturing a policy from reply hints.
		return errors.WithStack(peersession.ErrDenied)
	}
	owned, finish, err := c.Domain.Admission.Enter(call, kind, group)
	if err != nil {
		return errors.WithStack(err)
	}
	pending := output.ProtoReflect().New().Interface()
	_, err = RunBootstrapStartupControl(owned, input, func(ctx context.Context, request any) (any, error) {
		return nil, c.invokePeer(ctx, group, address, method, request, pending, local, peer)
	}, finish)
	if err != nil {
		return err
	}
	proto.Reset(output)
	proto.Merge(output, pending)
	return nil
}

func bootstrapOutgoingControlTraffic(traffic peersession.Traffic) bool {
	return traffic == peersession.Control || traffic == peersession.Registration
}

func bootstrapControlProtoMessages(request, reply any) (proto.Message, proto.Message, error) {
	input, ok := request.(proto.Message)
	output, replyOK := reply.(proto.Message)
	if !ok || !replyOK || !input.ProtoReflect().IsValid() || !output.ProtoReflect().IsValid() {
		return nil, nil, errors.WithStack(peersession.ErrFormat)
	}
	return proto.Clone(input), output, nil
}

func bootstrapControlOutgoingMetadataPresent(ctx context.Context) bool {
	md, _ := metadata.FromOutgoingContext(ctx)
	for _, header := range []string{BootstrapControlHeader, "x-elastickv-peer-session", "x-elastickv-peer-proof"} {
		if _, present := md[header]; present {
			return true
		}
	}
	return false
}

func (c *bootstrapControlClient) target(group uint64, address string) (bootstrapControlClientGroup, peersession.Node, error) {
	local, ok := c.groups[group]
	peer, known := local.peers[address]
	if !ok || !known {
		return local, peer, errors.WithStack(peersession.ErrDenied)
	}
	projection, found := local.owner.GroupProjection(group)
	if found {
		for _, member := range projection.Configuration.Members {
			if member.ID == peer.Member && member.Address == peer.Address && member.PublicKeyDigest == peer.Key {
				return local, peer, nil
			}
		}
	}
	return local, peer, errors.WithStack(peersession.ErrDenied)
}

func (c *bootstrapControlClient) begin(parent context.Context) (context.Context, func(), error) {
	if parent == nil {
		return nil, nil, errors.WithStack(peersession.ErrDenied)
	}
	c.mu.Lock()
	if c.closed || c.Ctx.Err() != nil {
		c.mu.Unlock()
		return nil, nil, errors.WithStack(peersession.ErrClosed)
	}
	c.active.Add(1)
	c.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	stop := context.AfterFunc(c.Ctx, func() { cancel(); close(done) })
	return ctx, func() {
		if !stop() {
			<-done
		}
		cancel()
		c.active.Done()
	}, nil
}

func (c *bootstrapControlClient) invokePeer(ctx context.Context, group uint64, address, method string, request, reply any, local bootstrapControlClientGroup, remote peersession.Node) error {
	c.mu.Lock()
	key := bootstrapControlPeerKey{Group: group, address: address}
	peer := c.peers[key]
	if peer == nil {
		peer = &bootstrapControlPeer{serial: make(chan struct{}, 1)}
		c.peers[key] = peer
	}
	c.mu.Unlock()
	select {
	case peer.serial <- struct{}{}:
		defer func() { <-peer.serial }()
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	}
	if err := peer.connect(ctx, group, address, local, remote); err != nil {
		peer.close()
		return err
	}
	if err := c.Domain.Admission.ValidateResult(ctx); err != nil {
		peer.close()
		return errors.WithStack(err)
	}
	err := peer.handle.Invoke(ctx, peer.conn, method, request, reply,
		grpc.PerRPCCredentials(bootstrapControlCredential{method: method, value: c.Domain.StartupToken}))
	if err != nil {
		peer.close()
	}
	return errors.WithStack(err)
}

func (p *bootstrapControlPeer) connect(ctx context.Context, group uint64, address string, local bootstrapControlClientGroup, remote peersession.Node) error {
	if p.handle != nil {
		select {
		case <-p.handle.Done():
			return errors.WithStack(peersession.ErrDenied)
		default:
			return nil
		}
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(local.creds.Clone()), internalutil.GRPCCallOptions())
	if err != nil {
		return errors.WithStack(err)
	}
	p.conn = conn
	p.handle, err = local.owner.Handshake(ctx, conn, group, remote)
	return errors.WithStack(err)
}

func (p *bootstrapControlPeer) close() {
	if p.handle != nil {
		p.handle.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.handle, p.conn = nil, nil
}

func (c *bootstrapControlClient) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.Done
		return
	}
	c.closed = true
	c.Cancel()
	c.mu.Unlock()
	c.active.Wait()
	for _, peer := range c.peers {
		peer.close()
	}
	close(c.Done)
}

type bootstrapControlCredential struct {
	method string
	value  string
}

func (bootstrapControlCredential) RequireTransportSecurity() bool { return true }

func (c bootstrapControlCredential) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	info, ok := credentials.RequestInfoFromContext(ctx)
	if !ok || info.Method != c.method || c.value == "" {
		return nil, errors.WithStack(peersession.ErrDenied)
	}
	return map[string]string{BootstrapControlHeader: c.value}, nil
}

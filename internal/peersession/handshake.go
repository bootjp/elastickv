package peersession

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"net"
	"sync"

	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const exporterLabel = "EXPORTER-elastickv-bootstrap-v1"

func denied() error {
	return errors.WithStack(status.Error(codes.PermissionDenied, "peer session denied"))
}
func malformed() error {
	return errors.WithStack(status.Error(codes.InvalidArgument, "invalid peer session record"))
}

func (o *Owner) serverConnection(ctx context.Context) (*connection, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, ErrDenied
	}
	ai, ok := p.AuthInfo.(*ServerAuthInfo)
	if !ok || ai.connection == nil || ai.connection.owner != o || ai.connection.remote == nil {
		return nil, ErrDenied
	}
	return ai.connection, nil
}

func (o *Owner) liveLocked(c *connection) bool {
	return !o.closed && !c.closed && o.connections[c] && c.remote != nil
}

func (o *Owner) Begin(ctx context.Context, frame *wire.Frame) (*wire.Frame, error) {
	if frame == nil {
		return nil, malformed()
	}
	h, err := parseHello(frame.Body)
	if err != nil {
		return nil, malformed()
	}
	c, err := o.serverConnection(ctx)
	if err != nil {
		return nil, denied()
	}
	o.mu.Lock()
	var obsolete []net.Conn
	defer func() { o.mu.Unlock(); closeTransports(obsolete) }()
	if !o.canBeginLocked(c, h) {
		return nil, denied()
	}
	v, err := o.groupViewLocked(h.Group)
	if err != nil {
		return nil, denied()
	}
	a := answer{Request: sha256.Sum256(frame.Body), Boot: o.boot, View: v}
	if _, err := rand.Read(a.Challenge[:]); err != nil {
		return nil, errors.WithStack(status.Error(codes.Internal, "peer entropy unavailable"))
	}
	b, err := encodeAnswer(a)
	if err != nil {
		return nil, malformed()
	}
	id := transcript(frame.Body, b)
	key, err := exportKey(c, id)
	if err != nil {
		return nil, denied()
	}
	obsolete, err = o.acceptBootLocked(c, h.Boot)
	if err != nil {
		return nil, denied()
	}
	c.begun[h.Group] = true
	o.newSessionLocked(c, h.Group, id, key, v, h.View, h.Boot)
	return &wire.Frame{Body: b}, nil
}

func (o *Owner) canBeginLocked(c *connection, h hello) bool {
	if !o.liveLocked(c) || c.client || c.begun[h.Group] || len(c.begun) >= maxGroupsPerConnection {
		return false
	}
	return h.Client == *c.remote && h.Server == o.local && o.allowedLocked(h.Client, h.Group)
}

func exportKey(c *connection, id raftbootstrap.Digest) (raftbootstrap.Digest, error) {
	var key raftbootstrap.Digest
	b, err := c.tls.State.ExportKeyingMaterial(exporterLabel, id[:], len(key))
	if err != nil {
		return key, errors.WithStack(err)
	}
	copy(key[:], b)
	clear(b)
	return key, nil
}

func proof(key raftbootstrap.Digest, role string, id raftbootstrap.Digest) raftbootstrap.Digest {
	h := hmac.New(sha256.New, key[:])
	h.Write([]byte("elastickv/bootstrap-v1/" + role + "\x00"))
	h.Write(id[:])
	var result raftbootstrap.Digest
	copy(result[:], h.Sum(nil))
	return result
}

func (o *Owner) Finish(ctx context.Context, frame *wire.Frame) (*wire.Frame, error) {
	if frame == nil {
		return nil, malformed()
	}
	id, supplied, err := parseProof(frame.Body, finishKind)
	if err != nil {
		return nil, malformed()
	}
	c, err := o.serverConnection(ctx)
	if err != nil {
		return nil, denied()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.liveLocked(c) {
		return nil, denied()
	}
	s := findSession(c, id)
	if s == nil || s.phase != pending || !o.currentLocked(c, s) {
		return nil, denied()
	}
	want := proof(s.key, "client-finish", id)
	if !hmac.Equal(want[:], supplied[:]) {
		s.cancel()
		clear(s.key[:])
		delete(c.sessions, s.binding.Group)
		return nil, denied()
	}
	s.phase = complete
	ack := proof(s.key, "server-ack", id)
	return proofFrame(ackKind, id, ack), nil
}

func findSession(c *connection, id raftbootstrap.Digest) *session {
	for _, s := range c.sessions {
		if s.id == id {
			return s
		}
	}
	return nil
}

func (o *Owner) currentLocked(c *connection, s *session) bool {
	g, ok := o.groups[s.binding.Group]
	return o.liveLocked(c) && ok && g.Generation == s.binding.Generation && c.sessions[s.binding.Group] == s && s.ctx.Err() == nil
}

type handshakePin struct {
	owner      *Owner
	remote     Node
	group      uint64
	method     string
	mu         sync.Mutex
	connection *connection
}

func (*handshakePin) RequireTransportSecurity() bool { return true }
func (p *handshakePin) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	c, err := clientConnection(ctx, p.owner, p.method)
	if err != nil {
		return nil, err
	}
	o := p.owner
	p.mu.Lock()
	defer p.mu.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.liveLocked(c) || !c.client || *c.remote != p.remote || !o.allowedLocked(p.remote, p.group) {
		return nil, denied()
	}
	if p.connection == nil {
		p.connection = c
	}
	if p.connection != c {
		return nil, denied()
	}
	return nil, nil
}

func clientConnection(ctx context.Context, o *Owner, method string) (*connection, error) {
	ri, ok := credentials.RequestInfoFromContext(ctx)
	if !ok || ri.Method != method {
		return nil, denied()
	}
	ai, ok := ri.AuthInfo.(*ClientAuthInfo)
	if !ok || ai.connection == nil || ai.connection.owner != o {
		return nil, denied()
	}
	return ai.connection, nil
}

func (o *Owner) Handshake(ctx context.Context, cc grpc.ClientConnInterface, group uint64, remote Node) (_ *Handle, err error) {
	h, err := o.clientHello(group, remote)
	if err != nil {
		return nil, err
	}
	pin := &handshakePin{owner: o, remote: remote, group: group, method: wire.PeerSession_Begin_FullMethodName}
	defer func() {
		if err != nil && pin.connection != nil {
			o.closeConnection(pin.connection)
		}
	}()
	a, id, err := exchangeBegin(ctx, cc, pin, h)
	if err != nil {
		return nil, err
	}
	s, err := o.clientPending(pin.connection, h, a, id)
	if err != nil {
		return nil, err
	}
	return o.finishClient(ctx, cc, pin, s)
}

func exchangeBegin(ctx context.Context, cc grpc.ClientConnInterface, pin *handshakePin, h hello) (answer, raftbootstrap.Digest, error) {
	b, err := encodeHello(h)
	if err != nil {
		return answer{}, raftbootstrap.Digest{}, err
	}
	var reply wire.Frame
	if err := cc.Invoke(ctx, pin.method, &wire.Frame{Body: b}, &reply, handshakeOptions(pin)...); err != nil {
		return answer{}, raftbootstrap.Digest{}, errors.WithStack(err)
	}
	a, err := parseAnswer(reply.Body)
	if err != nil || a.Request != sha256.Sum256(b) || pin.connection == nil {
		return answer{}, raftbootstrap.Digest{}, ErrDenied
	}
	return a, transcript(b, reply.Body), nil
}

func (o *Owner) clientFinishRequest(c *connection, s *session) (*wire.Frame, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.currentLocked(c, s) || s.phase != pending {
		return nil, ErrDenied
	}
	return proofFrame(finishKind, s.id, proof(s.key, "client-finish", s.id)), nil
}

func (o *Owner) finishClient(ctx context.Context, cc grpc.ClientConnInterface, pin *handshakePin, s *session) (*Handle, error) {
	finishPin := &handshakePin{owner: o, remote: pin.remote, group: pin.group, method: wire.PeerSession_Finish_FullMethodName, connection: pin.connection}
	request, err := o.clientFinishRequest(pin.connection, s)
	if err != nil {
		return nil, err
	}
	var ack wire.Frame
	if err := cc.Invoke(ctx, finishPin.method, request, &ack, handshakeOptions(finishPin)...); err != nil {
		return nil, errors.WithStack(err)
	}
	id, received, err := parseProof(ack.Body, ackKind)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	want := proof(s.key, "server-ack", s.id)
	if !o.currentLocked(pin.connection, s) || id != s.id || !hmac.Equal(want[:], received[:]) {
		return nil, ErrDenied
	}
	s.phase = complete
	return &Handle{owner: o, c: pin.connection, s: s}, nil
}

func handshakeOptions(pin *handshakePin) []grpc.CallOption {
	return []grpc.CallOption{grpc.PerRPCCredentials(pin), grpc.ForceCodec(StrictCodec{}), grpc.MaxCallRecvMsgSize(MaxControlMessageSize), grpc.MaxCallSendMsgSize(MaxControlMessageSize)}
}

func (o *Owner) clientHello(group uint64, remote Node) (hello, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || !o.allowedLocked(remote, group) {
		return hello{}, ErrDenied
	}
	v, err := o.groupViewLocked(group)
	if err != nil {
		return hello{}, err
	}
	h := hello{Client: o.local, Server: remote, Group: group, Boot: o.boot, View: v}
	if _, err := rand.Read(h.Challenge[:]); err != nil {
		return hello{}, errors.WithStack(err)
	}
	return h, nil
}

func (o *Owner) clientPending(c *connection, h hello, a answer, id raftbootstrap.Digest) (*session, error) {
	o.mu.Lock()
	var obsolete []net.Conn
	defer func() { o.mu.Unlock(); closeTransports(obsolete) }()
	if !o.liveLocked(c) || c.begun[h.Group] || len(c.begun) >= maxGroupsPerConnection {
		return nil, ErrDenied
	}
	v, err := o.groupViewLocked(h.Group)
	if err != nil || v != h.View {
		return nil, ErrDenied
	}
	key, err := exportKey(c, id)
	if err != nil {
		return nil, err
	}
	obsolete, err = o.acceptBootLocked(c, a.Boot)
	if err != nil {
		return nil, err
	}
	c.begun[h.Group] = true
	return o.newSessionLocked(c, h.Group, id, key, h.View, a.View, a.Boot), nil
}

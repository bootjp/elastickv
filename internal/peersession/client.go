package peersession

import (
	"context"
	"encoding/hex"
	"sync"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Handle is an owner-minted live reference to one group on one physical TLS
// connection. SessionID is not a capability. Zero/foreign/disconnected handles
// fail closed; no context flag or address-wide credential can substitute for it.
type Handle struct {
	owner *Owner
	c     *connection
	s     *session
}

func (h *Handle) SessionID() raftbootstrap.Digest {
	if h == nil || h.s == nil {
		return raftbootstrap.Digest{}
	}
	return h.s.id
}
func (h *Handle) Done() <-chan struct{} {
	if h == nil || h.s == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return h.s.ctx.Done()
}

// Close destroys the physical connection and every session on it.
func (h *Handle) Close() {
	if h != nil && h.owner != nil && h.c != nil {
		h.owner.closeConnection(h.c)
	}
}

func (*Handle) RequireTransportSecurity() bool { return true }
func (h *Handle) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	if !h.owned() {
		return nil, denied()
	}
	ri, ok := credentials.RequestInfoFromContext(ctx)
	if !ok {
		return nil, denied()
	}
	c, err := clientConnection(ctx, h.owner, ri.Method)
	if err != nil || c != h.c || !h.c.client {
		return nil, denied()
	}
	o := h.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.methods[ri.Method]; !ok {
		return nil, denied()
	}
	if !o.currentLocked(h.c, h.s) || h.s.phase != complete {
		return nil, denied()
	}
	mac := rpcProof(h.s, ri.Method)
	return map[string]string{sessionHeader: hex.EncodeToString(h.s.id[:]), proofHeader: hex.EncodeToString(mac[:])}, nil
}

func (h *Handle) method(name string) (Method, error) {
	if h == nil || h.owner == nil {
		return Method{}, ErrDenied
	}
	m, ok := h.owner.methods[name]
	if !ok {
		return Method{}, ErrDenied
	}
	return m, nil
}

// Invoke checks the decoded outbound payload before network release and the
// decoded reply on delivery. Per-RPC credentials independently enforce the
// physical connection on every native gRPC attempt, including retry/reconnect.
func (h *Handle) Invoke(ctx context.Context, cc grpc.ClientConnInterface, name string, req, reply any, opts ...grpc.CallOption) error {
	m, err := h.method(name)
	if err != nil {
		return err
	}
	if m.Request == nil || m.Reply == nil {
		return ErrDenied
	}
	request, err := m.Request(req)
	if err != nil {
		return err
	}
	if err := h.validate(ctx, request, true); err != nil {
		return err
	}
	owned, cancel := boundContext(ctx, h.s.ctx)
	defer cancel()
	options, err := h.options(opts, func(ctx context.Context) error { return h.validate(ctx, request, true) })
	if err != nil {
		return err
	}
	if err := cc.Invoke(owned, name, req, reply, options...); err != nil {
		return errors.WithStack(err)
	}
	message, err := m.Reply(request, reply)
	if err != nil {
		return err
	}
	return h.validate(owned, message, false)
}

func (h *Handle) owned() bool { return h != nil && h.owner != nil && h.s != nil && h.c != nil }

func (h *Handle) NewStream(ctx context.Context, cc grpc.ClientConnInterface, desc *grpc.StreamDesc, name string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	m, err := h.method(name)
	if err != nil {
		return nil, err
	}
	if h.c == nil || !h.c.client {
		return nil, ErrDenied
	}
	m, err = decodersForStream(h, m)
	if err != nil {
		return nil, err
	}
	owned, cancel := boundContext(ctx, h.s.ctx)
	validation := &streamValidation{handle: h}
	options, err := h.options(opts, validation.check)
	if err != nil {
		cancel()
		return nil, err
	}
	stream, err := cc.NewStream(owned, desc, name, options...)
	if err != nil {
		cancel()
		return nil, errors.WithStack(err)
	}
	return &clientStream{ClientStream: stream, handle: h, method: m, cancel: cancel, validation: validation, serverStreams: desc.ServerStreams}, nil
}

type clientStream struct {
	grpc.ClientStream
	handle        *Handle
	method        Method
	cancel        context.CancelFunc
	mu            sync.Mutex
	request       Message
	validation    *streamValidation
	serverStreams bool
}

func (s *clientStream) SendMsg(m any) error {
	msg, err := s.method.Request(m)
	if err != nil {
		return err
	}
	if err := s.handle.validate(s.Context(), msg, true); err != nil {
		return err
	}
	s.mu.Lock()
	s.request = msg
	s.mu.Unlock()
	s.validation.set(msg)
	return streamError(s.ClientStream.SendMsg(m))
}

type streamValidation struct {
	mu      sync.Mutex
	handle  *Handle
	message *Message
}

func (s *streamValidation) set(m Message) { s.mu.Lock(); defer s.mu.Unlock(); s.message = &m }
func (s *streamValidation) check(ctx context.Context) error {
	s.mu.Lock()
	var msg Message
	present := s.message != nil
	if present {
		msg = *s.message
	}
	s.mu.Unlock()
	if !present {
		return nil
	}
	return s.handle.validate(ctx, msg, true)
}
func (s *clientStream) RecvMsg(m any) error {
	if err := s.ClientStream.RecvMsg(m); err != nil {
		s.cancel()
		return streamError(err)
	}
	s.mu.Lock()
	request := s.request
	s.mu.Unlock()
	msg, err := s.method.Reply(request, m)
	if err != nil {
		return err
	}
	err = s.handle.validate(s.Context(), msg, false)
	if !s.serverStreams {
		s.cancel()
	}
	return err
}

// Inbound returns an owner-minted handle only from native authenticated server
// context with valid method/session proof. Revalidate with CheckIncoming at
// install/enqueue/result boundaries, under the runtime's own admission arbiter.
func (o *Owner) Inbound(ctx context.Context) (*Handle, error) {
	method, ok := grpc.Method(ctx)
	if !ok {
		return nil, ErrDenied
	}
	if _, ok := o.methods[method]; !ok {
		return nil, ErrDenied
	}
	return o.incoming(ctx, method)
}
func (h *Handle) CheckIncoming(ctx context.Context, name string, payload any) error {
	m, err := h.method(name)
	if err != nil {
		return err
	}
	if h.c == nil || h.c.client {
		return ErrDenied
	}
	if m.Request == nil {
		return ErrDenied
	}
	msg, err := m.Request(payload)
	if err != nil {
		return err
	}
	return h.validate(ctx, msg, false)
}

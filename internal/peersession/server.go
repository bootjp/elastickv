package peersession

import (
	"context"
	"crypto/tls"
	"io"
	"sync"

	shared "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/peersession/wire"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

const TransportMaxMessageSize = shared.GRPCMaxMessageBytes

func (o *Owner) Register(server grpc.ServiceRegistrar) { wire.RegisterPeerSessionServer(server, o) }

// ServerOptions preserves the shared 64 MiB native envelope. StrictCodec bounds
// new Frame records BEFORE body cloning/record decode, not before gRPC receives
// the encoded wire buffer. Pinned grpc-go has no per-method server receive cap.
// Existing business auth/stage interceptors remain additional conjunctive gates.
func (o *Owner) ServerOptions(cfg *tls.Config) ([]grpc.ServerOption, error) {
	tc, err := o.TransportCredentials(cfg, true)
	if err != nil {
		return nil, err
	}
	return []grpc.ServerOption{grpc.Creds(tc), grpc.ForceServerCodec(StrictCodec{}), grpc.MaxRecvMsgSize(TransportMaxMessageSize), grpc.MaxSendMsgSize(TransportMaxMessageSize),
		grpc.ChainUnaryInterceptor(o.UnaryServerInterceptor()), grpc.ChainStreamInterceptor(o.StreamServerInterceptor())}, nil
}

func (o *Owner) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if o.public[info.FullMethod] || isHandshake(info.FullMethod) {
			return handler(ctx, req)
		}
		method, ok := o.unaryMethod(info.FullMethod)
		if !ok {
			return nil, denied()
		}
		h, err := o.incoming(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		request, err := method.Request(req)
		if err != nil {
			return nil, malformed()
		}
		if err := h.validate(ctx, request, false); err != nil {
			return nil, err
		}
		owned, cancel := boundContext(ctx, h.s.ctx)
		defer cancel()
		reply, err := handler(owned, req)
		if err != nil {
			return nil, err
		}
		message, err := method.Reply(request, reply)
		if err != nil {
			return nil, malformed()
		}
		if err := h.validate(owned, message, true); err != nil {
			return nil, err
		}
		return reply, nil
	}
}

func (o *Owner) unaryMethod(name string) (Method, bool) {
	m, ok := o.methods[name]
	return m, ok && m.Request != nil && m.Reply != nil
}

func streamError(err error) error {
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	return errors.WithStack(err)
}

func boundContext(parent, lifetime context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(lifetime, cancel)
	stopContext := context.AfterFunc(ctx, func() { stop() })
	return ctx, func() { stopContext(); stop(); cancel() }
}

func (o *Owner) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if o.public[info.FullMethod] {
			return handler(srv, stream)
		}
		method, ok := o.methods[info.FullMethod]
		if !ok || isHandshake(info.FullMethod) {
			return denied()
		}
		h, err := o.incoming(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		method, err = decodersForStream(h, method)
		if err != nil {
			return malformed()
		}
		ctx, cancel := boundContext(stream.Context(), h.s.ctx)
		defer cancel()
		slot := &receiptSlot{}
		ctx = context.WithValue(ctx, receiptKey{}, slot)
		return handler(srv, &serverStream{ServerStream: stream, ctx: ctx, handle: h, method: method, methodName: info.FullMethod, receipts: slot})
	}
}

type serverStream struct {
	grpc.ServerStream
	ctx        context.Context
	handle     *Handle
	method     Method
	mu         sync.Mutex
	request    Message
	methodName string
	receipts   *receiptSlot
}

func (s *serverStream) Context() context.Context { return s.ctx }
func (s *serverStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return streamError(err)
	}
	msg, err := s.method.Request(m)
	if err != nil {
		return malformed()
	}
	if err := s.handle.validate(s.ctx, msg, false); err != nil {
		return err
	}
	s.mu.Lock()
	s.request = msg
	s.mu.Unlock()
	s.receipts.set(&Receipt{handle: s.handle, message: msg, method: s.methodName})
	return nil
}
func (s *serverStream) SendMsg(m any) error {
	s.mu.Lock()
	request := s.request
	s.mu.Unlock()
	msg, err := s.method.Reply(request, m)
	if err != nil {
		return malformed()
	}
	if err := s.handle.validate(s.ctx, msg, true); err != nil {
		return err
	}
	return streamError(s.ServerStream.SendMsg(m))
}

package main

import (
	"context"
	"strings"
	"sync"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func bootstrapBusinessMethod(method string) bool {
	for _, service := range []string{"RawKV", "TransactionalKV", "Internal", "Distribution", "Admin", "AdminForward", "RaftAdmin", "EncryptionAdmin", "S3BlobFetch"} {
		if strings.HasPrefix(method, "/"+service+"/") {
			return true
		}
	}
	return false
}

func bootstrapAdmissionUnavailable() error {
	return errors.WithStack(status.Error(codes.Unavailable, "protected runtime admission is closed"))
}

func finishBootstrapIngress(op *raftadmission.Operation, original error) error {
	if original != nil {
		op.CancelAndJoin()
		return original
	}
	if err := op.Finish(); err != nil {
		return bootstrapResultOutcomeUnknown(err)
	}
	return nil
}

func bootstrapResultOutcomeUnknown(cause error) error {
	return errors.Join(kv.ErrTransactionOutcomeUnknown, cause,
		status.Error(codes.Unknown, "operation outcome unknown: result admission expired"))
}

func (g *startupPublicKVGate) protectedUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
	if reply, handled, controlErr := handleBootstrapStartupControl(ctx, g.domain.startupToken, g.domain.admission, info.FullMethod, req, handler); handled {
		return reply, controlErr
	}
	if !bootstrapBusinessMethod(info.FullMethod) {
		return handler(ctx, req)
	}
	if g.blocked() {
		return nil, bootstrapAdmissionUnavailable()
	}
	owned, op, err := g.domain.admission.Begin(ctx, raftadmission.Data)
	if err != nil {
		return nil, bootstrapAdmissionUnavailable()
	}
	defer op.CancelAndJoin()
	response, err = handler(owned, req)
	if err = finishBootstrapIngress(op, err); err != nil {
		return nil, err
	}
	return response, nil
}

type bootstrapAdmissionStream struct {
	grpc.ServerStream
	ctx       context.Context
	admission *raftadmission.Arbiter
}

func (s *bootstrapAdmissionStream) Context() context.Context { return s.ctx }

func (s *bootstrapAdmissionStream) SendMsg(message any) error {
	if err := s.admission.ValidateResult(s.ctx); err != nil {
		return bootstrapAdmissionUnavailable()
	}
	return errors.WithStack(s.ServerStream.SendMsg(message))
}

func (s *bootstrapAdmissionStream) RecvMsg(message any) error {
	if err := s.admission.ValidateResult(s.ctx); err != nil {
		return bootstrapAdmissionUnavailable()
	}
	if err := s.ServerStream.RecvMsg(message); err != nil {
		return errors.WithStack(err)
	}
	if err := s.admission.ValidateResult(s.ctx); err != nil {
		return bootstrapAdmissionUnavailable()
	}
	return nil
}

func (g *startupPublicKVGate) protectedStream(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !bootstrapBusinessMethod(info.FullMethod) {
		return handler(srv, stream)
	}
	if g.blocked() {
		return bootstrapAdmissionUnavailable()
	}
	abort, err := peersession.CaptureServerTransportAbort(stream.Context())
	if err != nil {
		return bootstrapAdmissionUnavailable()
	}
	owned, op, err := g.domain.admission.Begin(stream.Context(), raftadmission.Data)
	if err != nil {
		return bootstrapAdmissionUnavailable()
	}
	joinAbort := bootstrapStreamTransportLifetime(owned, abort)
	defer func() {
		joinAbort()
		op.CancelAndJoin()
	}()
	err = handler(srv, &bootstrapAdmissionStream{ServerStream: stream, ctx: owned, admission: g.domain.admission})
	// Stop normal retirement from aborting a healthy transport; if revocation
	// already started closing it, join that close before retiring the operation.
	joinAbort()
	return finishBootstrapIngress(op, err)
}

func bootstrapStreamTransportLifetime(ctx context.Context, abort func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		abort()
	})
	return sync.OnceFunc(func() {
		if !stop() {
			<-done
		}
	})
}

package clusterbootstrap

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

func FinishBootstrapIngress(op *raftadmission.Operation, original error) error {
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

func (g *StartupPublicKVGate) protectedUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
	if reply, handled, controlErr := HandleBootstrapStartupControl(ctx, g.Domain.StartupToken, g.Domain.Admission, info.FullMethod, req, handler); handled {
		return reply, controlErr
	}
	if !bootstrapBusinessMethod(info.FullMethod) {
		return handler(ctx, req)
	}
	if g.Blocked() {
		return nil, bootstrapAdmissionUnavailable()
	}
	owned, op, err := g.Domain.Admission.Begin(ctx, raftadmission.Data)
	if err != nil {
		return nil, bootstrapAdmissionUnavailable()
	}
	defer op.CancelAndJoin()
	response, err = handler(owned, req)
	if err = FinishBootstrapIngress(op, err); err != nil {
		return nil, err
	}
	return response, nil
}

type BootstrapAdmissionStream struct {
	grpc.ServerStream
	Ctx       context.Context
	Admission *raftadmission.Arbiter
}

func (s *BootstrapAdmissionStream) Context() context.Context { return s.Ctx }

func (s *BootstrapAdmissionStream) SendMsg(message any) error {
	if err := s.Admission.ValidateResult(s.Ctx); err != nil {
		return bootstrapAdmissionUnavailable()
	}
	return errors.WithStack(s.ServerStream.SendMsg(message))
}

func (s *BootstrapAdmissionStream) RecvMsg(message any) error {
	if err := s.Admission.ValidateResult(s.Ctx); err != nil {
		return bootstrapAdmissionUnavailable()
	}
	if err := s.ServerStream.RecvMsg(message); err != nil {
		return errors.WithStack(err)
	}
	if err := s.Admission.ValidateResult(s.Ctx); err != nil {
		return bootstrapAdmissionUnavailable()
	}
	return nil
}

func (g *StartupPublicKVGate) ProtectedStream(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !bootstrapBusinessMethod(info.FullMethod) {
		return handler(srv, stream)
	}
	if g.Blocked() {
		return bootstrapAdmissionUnavailable()
	}
	abort, err := peersession.CaptureServerTransportAbort(stream.Context())
	if err != nil {
		return bootstrapAdmissionUnavailable()
	}
	owned, op, err := g.Domain.Admission.Begin(stream.Context(), raftadmission.Data)
	if err != nil {
		return bootstrapAdmissionUnavailable()
	}
	joinAbort := bootstrapStreamTransportLifetime(owned, abort)
	defer func() {
		joinAbort()
		op.CancelAndJoin()
	}()
	err = handler(srv, &BootstrapAdmissionStream{ServerStream: stream, Ctx: owned, Admission: g.Domain.Admission})
	// Stop normal retirement from aborting a healthy transport; if revocation
	// already started closing it, join that close before retiring the operation.
	joinAbort()
	return FinishBootstrapIngress(op, err)
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

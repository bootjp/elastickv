package clusterbootstrap

import (
	"context"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// BootstrapStartupAuthException removes only an unrelated unary auth dependency
// from a verified typed startup request. It neither mints admission nor replaces
// downstream A.Enter, method-specific proofs, or result validation. There is no
// stream variant. Unrecognized/public/invalid requests keep the original auth.
func BootstrapStartupAuthException(domain *BootstrapDomain, original grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if original == nil || next == nil {
			return nil, errors.New("startup auth exception requires original authorization and handler")
		}
		if bootstrapStartupAuthAccepted(domain, ctx, request, info) {
			reply, err := next(ctx, request)
			return reply, errors.WithStack(err)
		}
		reply, err := original(ctx, request, info, next)
		return reply, errors.WithStack(err)
	}
}

func bootstrapStartupAuthAccepted(domain *BootstrapDomain, ctx context.Context, request any, info *grpc.UnaryServerInfo) bool {
	if domain == nil || ctx == nil || info == nil {
		return false
	}
	receipt := peersession.CapturedReceipt(ctx)
	if receipt == nil {
		return false
	}
	descriptor, _, found := BootstrapStartupControlDescriptor(receipt.Message().Group, info.FullMethod)
	if !found {
		return false
	}
	return ValidateBootstrapStartupControl(ctx, domain.StartupToken, info.FullMethod, request, receipt, descriptor) == nil
}

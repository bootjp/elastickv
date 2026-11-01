package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const bootstrapControlHeader = "x-elastickv-startup-token"

// handleBootstrapStartupControl owns only the authenticated typed exception to
// public DataReady. Header-free public calls, token-only calls and unrelated
// methods are NOT handled. Once an exact shared method has a captured receipt,
// every failure is handled: the caller must not fall back to public admission.
// Existing method/admin/rollout authorization still runs in next.
func handleBootstrapStartupControl(ctx context.Context, token string, admission *raftadmission.Arbiter, method string, request any, next grpc.UnaryHandler) (any, bool, error) {
	if ctx == nil {
		return nil, false, nil
	}
	receipt := peersession.CapturedReceipt(ctx)
	if receipt == nil {
		return nil, false, nil
	}
	descriptor, kind, found := bootstrapStartupControlDescriptor(receipt.Message().Group, method)
	if !found {
		return nil, false, nil
	}
	if err := validateBootstrapStartupControl(ctx, token, method, request, receipt, descriptor); err != nil {
		return nil, true, err
	}
	if receipt.Message().Traffic == peersession.PolicyControl {
		kind = raftadmission.Recovery
	}
	if admission == nil || next == nil {
		return nil, true, errors.WithStack(status.Error(codes.Unavailable, "startup control admission is unavailable"))
	}
	owned, finish, err := admission.Enter(ctx, kind, receipt.Message().Group)
	if err != nil {
		return nil, true, errors.WithStack(err)
	}
	reply, err := runBootstrapStartupControl(owned, request, next, finish)
	return reply, true, err
}

func runBootstrapStartupControl(ctx context.Context, request any, next grpc.UnaryHandler, finish func(error) error) (reply any, err error) {
	completed := false
	defer func() {
		if !completed {
			err = errors.New("startup control handler did not return")
		}
		validated := finish(err)
		switch {
		case err == nil && validated != nil:
			err = bootstrapResultOutcomeUnknown(validated)
		case err == nil || errors.Is(validated, err):
			err = validated
		case validated != nil:
			err = errors.Join(err, validated)
		}
		if err != nil {
			reply = nil
		}
	}()
	reply, err = next(ctx, request)
	completed = true
	return reply, errors.WithStack(err)
}

func bootstrapStartupControlDescriptor(group uint64, method string) (peersession.Method, raftadmission.Kind, bool) {
	for _, descriptor := range bootstrapSharedGRPCMethods(group) {
		if descriptor.Name != method {
			continue
		}
		kind := raftadmission.Control
		if method == pb.Distribution_GetTimestamp_FullMethodName {
			kind = raftadmission.Allocator
		}
		return descriptor, kind, true
	}
	return peersession.Method{}, 0, false
}

func validateBootstrapStartupControl(ctx context.Context, token, method string, request any, receipt *peersession.Receipt, descriptor peersession.Method) error {
	actual, ok := grpc.Method(ctx)
	message := receipt.Message()
	if !ok || actual != method {
		return errors.WithStack(status.Error(codes.PermissionDenied, "typed startup control receipt required"))
	}
	if err := receipt.Check(ctx); err != nil {
		return errors.WithStack(err)
	}
	binding := peersession.RequestBinding{Group: message.Group,
		From: peersession.Node{Member: message.From}, To: peersession.Node{Member: message.To}}
	decoded, err := descriptor.BoundRequest(binding, request)
	if err != nil || decoded != message {
		return errors.WithStack(status.Error(codes.PermissionDenied, "request does not match typed startup control"))
	}
	if !bootstrapStartupTokenMatches(ctx, token) {
		return errors.WithStack(status.Error(codes.PermissionDenied, "startup control token required"))
	}
	return nil
}

func bootstrapStartupTokenMatches(ctx context.Context, token string) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get(bootstrapControlHeader)
	if token == "" || len(values) != 1 || values[0] == "" {
		return false
	}
	// Hash to fixed-size inputs so comparison does not short-circuit on a
	// differing token length. No other authorization header is inspected.
	expected := sha256.Sum256([]byte(token))
	actual := sha256.Sum256([]byte(values[0]))
	return subtle.ConstantTimeCompare(expected[:], actual[:]) == 1
}

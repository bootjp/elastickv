package clusterbootstrap

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

const BootstrapControlHeader = "x-elastickv-startup-token"

// HandleBootstrapStartupControl owns only the authenticated typed exception to
// public DataReady. Header-free public calls, token-only calls and unrelated
// methods are NOT handled. Once an exact shared method has a captured receipt,
// every failure is handled: the caller must not fall back to public admission.
// Existing method/admin/rollout authorization still runs in next.
func HandleBootstrapStartupControl(ctx context.Context, token string, admission *raftadmission.Arbiter, method string, request any, next grpc.UnaryHandler) (any, bool, error) {
	if ctx == nil {
		return nil, false, nil
	}
	receipt := peersession.CapturedReceipt(ctx)
	if receipt == nil {
		return nil, false, nil
	}
	descriptor, kind, found := BootstrapStartupControlDescriptor(receipt.Message().Group, method)
	if !found {
		return nil, false, nil
	}
	if err := ValidateBootstrapStartupControl(ctx, token, method, request, receipt, descriptor); err != nil {
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
	reply, err := RunBootstrapStartupControl(owned, request, next, finish)
	return reply, true, err
}

func RunBootstrapStartupControl(ctx context.Context, request any, next grpc.UnaryHandler, finish func(error) error) (reply any, err error) {
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

func BootstrapStartupControlDescriptor(group uint64, method string) (peersession.Method, raftadmission.Kind, bool) {
	for _, descriptor := range BootstrapSharedGRPCMethods(group) {
		if descriptor.Name != method {
			continue
		}
		kind := raftadmission.Control
		switch method {
		case pb.Distribution_GetTimestamp_FullMethodName:
			kind = raftadmission.Allocator
		case pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName:
			kind = raftadmission.Registration
		}
		return descriptor, kind, true
	}
	return peersession.Method{}, 0, false
}

func ValidateBootstrapStartupControl(ctx context.Context, token, method string, request any, receipt *peersession.Receipt, descriptor peersession.Method) error {
	actual, ok := grpc.Method(ctx)
	message := receipt.Message()
	if !ok || actual != method {
		return errors.WithStack(status.Error(codes.PermissionDenied, "typed startup control receipt required"))
	}
	if err := receipt.Check(ctx); err != nil {
		return errors.WithStack(err)
	}
	if message.Traffic == peersession.Registration && !bootstrapRegistrationReceiverMatches(ctx, message.Group) {
		return errors.WithStack(status.Error(codes.PermissionDenied, "registration requires the configured default-group session"))
	}
	binding := peersession.RequestBinding{Group: message.Group,
		From: peersession.Node{Member: message.From}, To: peersession.Node{Member: message.To}}
	decoded, err := descriptor.BoundRequest(binding, request)
	if err != nil || decoded != message {
		return errors.WithStack(status.Error(codes.PermissionDenied, "request does not match typed startup control"))
	}
	if !BootstrapStartupTokenMatches(ctx, token) {
		return errors.WithStack(status.Error(codes.PermissionDenied, "startup control token required"))
	}
	return nil
}

func BootstrapStartupTokenMatches(ctx context.Context, token string) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get(BootstrapControlHeader)
	if token == "" || len(values) != 1 || values[0] == "" {
		return false
	}
	// Hash to fixed-size inputs so comparison does not short-circuit on a
	// differing token length. No other authorization header is inspected.
	expected := sha256.Sum256([]byte(token))
	actual := sha256.Sum256([]byte(values[0]))
	return subtle.ConstantTimeCompare(expected[:], actual[:]) == 1
}

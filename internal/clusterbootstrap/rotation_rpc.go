package clusterbootstrap

import (
	"context"
	"crypto/subtle"
	"slices"
	"strings"

	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/bootjp/elastickv/internal/peersession"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// The controller captures its receiver-owned request intent in this context,
// verifies the actual purpose epoch and exact recipe, then validates completion.
// The frontdoor neither creates a Data handle nor grants native rotation rights.
type BootstrapRotationRPCHandler func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error)

func (d *BootstrapDomain) SetProtectedRotationHandler(handler BootstrapRotationRPCHandler) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.protectedRotationHandler = handler
	d.mu.Unlock()
}

func (d *BootstrapDomain) RotationRPCHandler() BootstrapRotationRPCHandler {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.protectedRotationHandler
}

// Call once in startRaftTransport after bootstrapServiceAuthOptions and the
// registration receiver hook, before appending the public gate. Legacy options
// are untouched. Sharing the returned options shares one process-wide bound.
func BootstrapRotationRPCOptions(options nodeauth.AdminGRPCInterceptors, domain *BootstrapDomain, localNodeID uint64, run BootstrapRotationRPCHandler) (nodeauth.AdminGRPCInterceptors, error) {
	if domain == nil {
		return options, nil
	}
	if options.Token == "" {
		options.Unary = append(slices.Clone(options.Unary), denyBootstrapRotationRPC)
		return options, nil
	}
	if run == nil {
		run = func(context.Context, *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
			return nil, bootstrapRotationRPCError(codes.FailedPrecondition, "protected rotation controller is not configured")
		}
	}
	rotation, err := NewBootstrapRotationRPC(options.Token, localNodeID, run)
	if err != nil {
		return options, err
	}
	options.Unary = append(slices.Clone(options.Unary), rotation)
	return options, nil
}

func denyBootstrapRotationRPC(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if info != nil && info.FullMethod == pb.EncryptionAdmin_RotateDEK_FullMethodName {
		return nil, bootstrapRotationRPCError(codes.Unauthenticated, "external rotation requires a configured ADMIN credential")
	}
	return next(ctx, request)
}

// Install after owner transport/service auth and BEFORE the public Data gate.
// Only literal RotateDEK is dispatched here. In particular, AdminTokenAuth alone
// does not cover EncryptionAdmin, so this boundary always checks ADMIN itself.
// Construct once per process and reuse across listeners to share the bound.
func NewBootstrapRotationRPC(adminCredential string, localNodeID uint64, run BootstrapRotationRPCHandler) (grpc.UnaryServerInterceptor, error) {
	if adminCredential == "" || localNodeID == 0 || run == nil {
		return nil, errors.New("protected rotation RPC requires ADMIN credential, local identity and controller")
	}
	active := make(chan struct{}, 1)
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if info == nil || info.FullMethod != pb.EncryptionAdmin_RotateDEK_FullMethodName {
			return next(ctx, request)
		}
		if err := AuthenticateBootstrapRotationRPC(ctx, adminCredential); err != nil {
			return nil, err
		}
		canonical, err := CanonicalBootstrapRotationRPCRequest(request, localNodeID)
		if err != nil {
			return nil, err
		}
		select {
		case active <- struct{}{}:
			defer func() { <-active }()
		default:
			return nil, bootstrapRotationRPCError(codes.ResourceExhausted, "protected rotation request already in flight")
		}
		return RunBootstrapRotationRPC(ctx, canonical, run)
	}, nil
}

func AuthenticateBootstrapRotationRPC(ctx context.Context, credential string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	_, startup := md[BootstrapControlHeader]
	if startup || peersession.CapturedReceipt(ctx) != nil {
		return bootstrapRotationRPCError(codes.Unauthenticated, "startup authority cannot authorize external rotation")
	}
	values := md.Get("authorization")
	if len(values) != 1 {
		return bootstrapRotationRPCError(codes.Unauthenticated, "external rotation requires one ADMIN bearer")
	}
	got, bearer := strings.CutPrefix(values[0], "Bearer ")
	if !bearer || subtle.ConstantTimeCompare([]byte(got), []byte(credential)) != 1 {
		return bootstrapRotationRPCError(codes.Unauthenticated, "external rotation requires ADMIN bearer")
	}
	if !bootstrapRotationRPCVerifiedMTLS(ctx) {
		return bootstrapRotationRPCError(codes.Unauthenticated, "external rotation requires verified client certificate")
	}
	return nil
}

func bootstrapRotationRPCVerifiedMTLS(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	auth, ok := p.AuthInfo.(*peersession.ServerAuthInfo)
	return ok && auth != nil && auth.State.HandshakeComplete && len(auth.State.VerifiedChains) != 0 && len(auth.State.PeerCertificates) != 0
}

func CanonicalBootstrapRotationRPCRequest(request any, localNodeID uint64) (*pb.RotateDEKRequest, error) {
	req, ok := request.(*pb.RotateDEKRequest)
	if !ok {
		return nil, bootstrapRotationRPCError(codes.InvalidArgument, "external rotation requires RotateDEK request")
	}
	if _, err := BootstrapRotationPayloadFromRequest(req); err != nil {
		return nil, bootstrapRotationRPCError(codes.InvalidArgument, "invalid protected rotation request")
	}
	if req.GetProposerNodeId() != localNodeID {
		return nil, bootstrapRotationRPCError(codes.PermissionDenied, "rotation proposer must match the local full node identity")
	}
	return CloneBootstrapRotationRequest(req), nil
}

func RunBootstrapRotationRPC(ctx context.Context, request *pb.RotateDEKRequest, run BootstrapRotationRPCHandler) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(status.FromContextError(err).Err())
	}
	reply, err := run(ctx, request)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, bootstrapResultOutcomeUnknown(err)
	}
	if reply == nil || reply.GetAppliedIndex() == 0 || len(reply.ProtoReflect().GetUnknown()) != 0 {
		return nil, bootstrapResultOutcomeUnknown(errors.New("rotation controller returned no exact applied receipt"))
	}
	return &pb.RotateDEKResponse{AppliedIndex: reply.GetAppliedIndex()}, nil
}

func bootstrapRotationRPCError(code codes.Code, message string) error {
	return errors.WithStack(status.Error(code, message))
}

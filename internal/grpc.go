package internal

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const GRPCMaxMessageBytes = 64 << 20

// GRPCServerOptions keeps Raft replication and the public/internal APIs aligned
// on the same message-size budget.
func GRPCServerOptions() []grpc.ServerOption {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(GRPCMaxMessageBytes),
		grpc.MaxSendMsgSize(GRPCMaxMessageBytes),
	}
	if cfg := grpcTLS.Load(); cfg != nil {
		opts = append(opts, grpc.Creds(cfg.server.Clone()))
	}
	return opts
}

// GRPCDialOptions returns the common transport and message-size options for
// node-local and node-to-node traffic. TLS is explicitly configured at startup.
func GRPCDialOptions() []grpc.DialOption {
	creds := insecure.NewCredentials()
	if cfg := grpcTLS.Load(); cfg != nil {
		creds = cfg.client.Clone()
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(forwardTokenClientInterceptor),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(GRPCMaxMessageBytes),
			grpc.MaxCallSendMsgSize(GRPCMaxMessageBytes),
		),
	}
}

// GRPCCallOptions returns the per-call message-size cap dial option used by
// callers that supply their own transport credentials (e.g. the admin
// binary's TLS-aware fanout). Without this, gRPC-Go's default ~4 MiB recv
// cap would silently fail RPCs once aggregated cluster-overview / matrix
// admin payloads exceed 4 MiB even though node servers (GRPCServerOptions)
// are configured for 64 MiB.
func GRPCCallOptions() grpc.DialOption {
	return grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(GRPCMaxMessageBytes),
		grpc.MaxCallSendMsgSize(GRPCMaxMessageBytes),
	)
}

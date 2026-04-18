package internal

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const GRPCMaxMessageBytes = 64 << 20

// GRPCServerOptions keeps Raft replication and the public/internal APIs aligned
// on the same message-size budget.
func GRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(GRPCMaxMessageBytes),
		grpc.MaxSendMsgSize(GRPCMaxMessageBytes),
	}
}

// GRPCDialOptions returns the common insecure dial options used by node-local
// and node-to-node traffic.
func GRPCDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(GRPCMaxMessageBytes),
			grpc.MaxCallSendMsgSize(GRPCMaxMessageBytes),
		),
	}
}

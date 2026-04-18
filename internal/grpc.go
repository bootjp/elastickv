package internal

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const GRPCMaxMessageBytes = 64 << 20

// keepalive timing constants.
// - PingTime: interval between keepalive pings while a stream is active.
// - PingTimeout: how long to wait for a ping ACK before declaring the peer dead.
// - ServerMinPingTime: minimum interval the server accepts from clients; must
//   be shorter than PingTime so the server never sends GOAWAY before the first ping.
//
// With these values a stalled TCP connection is detected within ~13 s
// (PingTime + PingTimeout) rather than the OS default of ~2 hours.
const (
	keepalivePingTime      = 10 * time.Second
	keepalivePingTimeout   = 3 * time.Second
	keepaliveServerMinTime = 5 * time.Second
)

// GRPCServerOptions keeps Raft replication and the public/internal APIs aligned
// on the same message-size budget.
func GRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(GRPCMaxMessageBytes),
		grpc.MaxSendMsgSize(GRPCMaxMessageBytes),
		// Accept client keepalive pings no more frequently than keepaliveServerMinTime.
		// Must be less than the client's PingTime so the server does not send
		// GOAWAY before the client's first ping.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             keepaliveServerMinTime,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepalivePingTime,
			Timeout: keepalivePingTimeout,
		}),
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
		// Send a keepalive ping every keepalivePingTime while a stream is active
		// so that stalled TCP connections (e.g. silently dropped by a NAT or
		// load balancer) are detected within ~13 s rather than the OS default
		// of ~2 h.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepalivePingTime,
			Timeout:             keepalivePingTimeout,
			PermitWithoutStream: false,
		}),
	}
}

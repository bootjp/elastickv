package splitmigration

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// voterSuffrage is the raftengine.Server suffrage of a voting member.
const voterSuffrage = "voter"

type CapabilityPeer struct {
	ID      string
	Address string
}

type CapabilityPeerSource func(context.Context) ([]CapabilityPeer, error)

type CapabilityProbe func(context.Context, string) error

func CapabilityPeersFromConfiguration(cfg raftengine.Configuration) []CapabilityPeer {
	servers := cfg.Servers
	if len(servers) == 0 {
		return nil
	}
	peers := make([]CapabilityPeer, 0, len(servers))
	seen := make(map[string]struct{}, len(servers))
	for _, server := range servers {
		key := server.ID + "\x00" + server.Address
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		id := server.ID
		if id == "" {
			id = server.Address
		}
		peers = append(peers, CapabilityPeer{
			ID:      id,
			Address: server.Address,
		})
	}
	return peers
}

func NewCapabilityGate(
	source CapabilityPeerSource,
	timeout time.Duration,
	probe CapabilityProbe,
	localGate adapter.SplitMigrationCapabilityGate,
) adapter.SplitMigrationCapabilityGate {
	if probe == nil {
		probe = ProbeCapabilityPeer
	}
	return func(ctx context.Context) error {
		if localGate != nil {
			if err := localGate(ctx); err != nil {
				return status.Errorf(codes.FailedPrecondition, "split migration local readiness is not available: %v", err)
			}
		}
		if source == nil {
			return status.Error(codes.FailedPrecondition, "split migration capability peers are not configured")
		}
		peers, err := source(ctx)
		if err != nil {
			return status.Errorf(codes.FailedPrecondition, "split migration capability peers are not available: %v", err)
		}
		if len(peers) == 0 {
			return status.Error(codes.FailedPrecondition, "split migration capability peers are not configured")
		}
		for _, peer := range peers {
			peerCtx := ctx
			cancel := func() {}
			if timeout > 0 {
				var cancelCtx context.CancelFunc
				peerCtx, cancelCtx = context.WithTimeout(ctx, timeout)
				cancel = cancelCtx
			}
			err := probe(peerCtx, peer.Address)
			cancel()
			if err != nil {
				return status.Errorf(codes.FailedPrecondition, "split migration capability peer %s is not ready: %v", peer.ID, err)
			}
		}
		return nil
	}
}

func ProbeCapabilityPeer(ctx context.Context, address string) error {
	return ProbeCapabilityPeerWithDial(ctx, address)
}

func ProbeCapabilityPeerWithDial(ctx context.Context, address string, providers ...kv.GRPCDialOptionsForAddress) error {
	if strings.TrimSpace(address) == "" {
		return errors.New("empty split migration capability peer address")
	}
	conn, err := kv.NewGRPCClient(address, providers...)
	if err != nil {
		return errors.Wrapf(err, "dial split migration capability peer %s", address)
	}
	defer func() {
		_ = conn.Close()
	}()
	resp, err := pb.NewDistributionClient(conn).GetSplitMigrationCapability(ctx, &pb.GetSplitMigrationCapabilityRequest{})
	if err != nil {
		return errors.Wrapf(err, "probe split migration capability peer %s", address)
	}
	if resp == nil || !resp.GetMigrationCapable() {
		return errors.New("peer does not advertise split migration capability")
	}
	if !slices.Contains(resp.GetCapabilities(), adapter.SplitMigrationCapabilityV2) {
		return errors.Errorf("peer does not advertise %s", adapter.SplitMigrationCapabilityV2)
	}
	return nil
}

type PromotionLeaderResolver func(distribution.SplitJob) (string, error)

type LeaderResolver func(uint64) (string, error)

func PromotionTargetLeaderResolver(shardGroups map[uint64]*kv.ShardGroup) PromotionLeaderResolver {
	return func(job distribution.SplitJob) (string, error) {
		sg := shardGroups[job.TargetGroupID]
		if sg == nil || sg.Engine == nil {
			return "", errors.Wrapf(kv.ErrLeaderNotFound, "split promotion target group %d", job.TargetGroupID)
		}
		addr := strings.TrimSpace(sg.Engine.Leader().Address)
		if addr == "" {
			return "", errors.Wrapf(kv.ErrLeaderNotFound, "split promotion target group %d", job.TargetGroupID)
		}
		return addr, nil
	}
}

func GroupLeaderResolver(shardGroups map[uint64]*kv.ShardGroup) LeaderResolver {
	return func(groupID uint64) (string, error) {
		sg := shardGroups[groupID]
		if sg == nil || sg.Engine == nil {
			return "", errors.Wrapf(kv.ErrLeaderNotFound, "split migration group %d", groupID)
		}
		addr := strings.TrimSpace(sg.Engine.Leader().Address)
		if addr == "" {
			return "", errors.Wrapf(kv.ErrLeaderNotFound, "split migration group %d", groupID)
		}
		return addr, nil
	}
}

func PromotionClientFactory(resolve PromotionLeaderResolver, connCache *kv.GRPCConnCache, token string) adapter.SplitPromotionClientFactory {
	return func(_ context.Context, job distribution.SplitJob) (adapter.SplitPromotionClient, error) {
		if resolve == nil {
			return nil, errors.New("split promotion leader resolver is not configured")
		}
		if connCache == nil {
			return nil, errors.New("split promotion gRPC connection cache is not configured")
		}
		addr, err := resolve(job)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		conn, err := connCache.ConnFor(addr)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newSplitMigrationInternalClient(conn, token), nil
	}
}

func ClientFactory(resolve LeaderResolver, connCache *kv.GRPCConnCache, token string) adapter.SplitMigrationClientFactory {
	groupClient := GroupClientFactory(resolve, connCache, token)
	return func(ctx context.Context, job distribution.SplitJob, sourceGroupID uint64) (adapter.SplitMigrationClient, adapter.SplitMigrationClient, error) {
		source, err := groupClient(ctx, sourceGroupID)
		if err != nil {
			return nil, nil, err
		}
		target, err := groupClient(ctx, job.TargetGroupID)
		if err != nil {
			return nil, nil, err
		}
		return source, target, nil
	}
}

func GroupClientFactory(resolve LeaderResolver, connCache *kv.GRPCConnCache, token string) adapter.SplitMigrationGroupClientFactory {
	return func(_ context.Context, groupID uint64) (adapter.SplitMigrationClient, error) {
		if resolve == nil {
			return nil, errors.New("split migration leader resolver is not configured")
		}
		if connCache == nil {
			return nil, errors.New("split migration gRPC connection cache is not configured")
		}
		addr, err := resolve(groupID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		conn, err := connCache.ConnFor(addr)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newSplitMigrationInternalClient(conn, token), nil
	}
}

func VoterFactory(shardGroups map[uint64]*kv.ShardGroup, connCache *kv.GRPCConnCache, token string) adapter.SplitMigrationVoterFactory {
	return func(ctx context.Context, groupID uint64) ([]adapter.SplitMigrationVoter, error) {
		sg := shardGroups[groupID]
		if sg == nil || sg.Engine == nil {
			return nil, errors.Wrapf(kv.ErrLeaderNotFound, "split migration group %d", groupID)
		}
		if connCache == nil {
			return nil, errors.New("split migration voter gRPC connection cache is not configured")
		}
		cfg, err := sg.Engine.Configuration(ctx)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		voters := make([]adapter.SplitMigrationVoter, 0, len(cfg.Servers))
		for _, member := range cfg.Servers {
			if member.Suffrage != voterSuffrage {
				continue
			}
			voter, err := splitMigrationVoter(groupID, member, connCache, token)
			if err != nil {
				return nil, err
			}
			voters = append(voters, voter)
		}
		if len(voters) == 0 {
			return nil, errors.Errorf("split migration group %d has no voters", groupID)
		}
		return voters, nil
	}
}

func splitMigrationVoter(groupID uint64, member raftengine.Server, connCache *kv.GRPCConnCache, token string) (adapter.SplitMigrationVoter, error) {
	addr := strings.TrimSpace(member.Address)
	if member.ID == "" || addr == "" {
		return adapter.SplitMigrationVoter{}, errors.Errorf("split migration group %d has voter with missing id/address", groupID)
	}
	conn, err := connCache.ConnFor(addr)
	if err != nil {
		return adapter.SplitMigrationVoter{}, errors.WithStack(err)
	}
	return adapter.SplitMigrationVoter{ID: member.ID, Address: addr, Client: newSplitMigrationInternalClient(conn, token)}, nil
}

package adapter

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

const namespaceMembershipHandshakeTimeout = 2 * time.Second

type MigrationNamespaceMembershipGate struct {
	mu       sync.Mutex
	store    store.MVCCStore
	engine   raftengine.Engine
	Proposer raftengine.Proposer
	probe    func(context.Context, string, *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error)
	Next     raftadmin.MembershipChangeInterceptor
	// deriveNodeID maps a Raft member ID to its numeric node ID; the binary
	// passes the concrete engine's derivation.
	deriveNodeID func(string) uint64
}

var _ raftadmin.MembershipChangeInterceptor = (*MigrationNamespaceMembershipGate)(nil)

var _ raftadmin.MembershipPromotionInterceptor = (*MigrationNamespaceMembershipGate)(nil)

func NewMigrationNamespaceMembershipGate(st store.MVCCStore, engine raftengine.Engine, proposer raftengine.Proposer, token string, next raftadmin.MembershipChangeInterceptor, deriveNodeID func(string) uint64, providers ...kv.GRPCDialOptionsForAddress) *MigrationNamespaceMembershipGate {
	return &MigrationNamespaceMembershipGate{store: st, engine: engine, Proposer: proposer, Next: next, deriveNodeID: deriveNodeID,
		probe: func(ctx context.Context, address string, request *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
			conn, err := kv.NewGRPCClient(address, providers...)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			defer func() { _ = conn.Close() }()
			response, err := newSplitMigrationInternalClient(conn, token).ProbeMigrationState(ctx, request)
			return response, errors.WithStack(err)
		}}
}

func (g *MigrationNamespaceMembershipGate) PreAddMember(ctx context.Context, id, address string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.authorize(ctx, id, address); err != nil {
		return err
	}
	if g.Next != nil {
		return errors.WithStack(g.Next.PreAddMember(ctx, id, address))
	}
	return nil
}

func (g *MigrationNamespaceMembershipGate) PrePromoteMember(ctx context.Context, id, address string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.authorize(ctx, id, address); err != nil {
		return err
	}
	if next, ok := g.Next.(raftadmin.MembershipPromotionInterceptor); ok {
		return errors.WithStack(next.PrePromoteMember(ctx, id, address))
	}
	return nil
}

func (g *MigrationNamespaceMembershipGate) authorize(ctx context.Context, id, address string) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	version, err := kv.MigrationNamespaceAdmissionVersion(ctx, g.store)
	if err != nil || version == 0 {
		return errors.WithStack(err)
	}
	peer, installedVersion, err := g.probeNamespaceCandidate(ctx, id, address, version)
	if err != nil {
		return err
	}
	snapshot := func(ctx context.Context) (NamespaceAdmissionSnapshot, error) { return g.snapshot(ctx) }
	current, err := snapshot(ctx)
	if err != nil {
		return err
	}
	joining, err := namespaceJoiningCandidate(current, peer)
	if err != nil {
		return err
	}
	activate := func(ctx context.Context, command kv.NamespaceAdmissionCommand) error {
		return errors.WithStack(kv.ActivateMigrationNamespace(ctx, g.store, g.Proposer, command))
	}
	if err := EnsureMigrationNamespaceAdmissionWithSnapshot(ctx, snapshot, activate, joining); err != nil {
		return errors.WithStack(err)
	}
	configuration, err := kv.MigrationConfigurationIndex(ctx, g.store)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(kv.CheckMigrationNamespacePrejoin(ctx, g.store, configuration, peer.MemberID, installedVersion))
}

func (g *MigrationNamespaceMembershipGate) probeNamespaceCandidate(ctx context.Context, id, address string, version uint32) (NamespaceAdmissionPeer, uint32, error) {
	if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) || address == "" || g.engine == nil || g.Proposer == nil || g.probe == nil {
		return NamespaceAdmissionPeer{}, 0, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	peer := g.peer(g.deriveNodeID(id), address)
	attempt, cancel := context.WithTimeout(ctx, namespaceMembershipHandshakeTimeout)
	response, err := peer.Client.ProbeMigrationState(attempt, &pb.ProbeMigrationStateRequest{JobId: 1, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_NAMESPACE_ADMISSION})
	cancel()
	if err != nil {
		return peer, 0, errors.WithStack(err)
	}
	if response.GetNamespaceAdmissionVersion() < version {
		return peer, 0, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	return peer, response.GetNamespaceAdmissionVersion(), nil
}

func namespaceJoiningCandidate(current NamespaceAdmissionSnapshot, peer NamespaceAdmissionPeer) ([]NamespaceAdmissionPeer, error) {
	joining := []NamespaceAdmissionPeer{peer}
	for _, member := range current.Members {
		if member.MemberID == peer.MemberID {
			if member.Address != peer.Address {
				return nil, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
			}
			joining = nil
		}
	}
	return joining, nil
}

type namespaceMembershipProbeClient struct {
	address string
	probe   func(context.Context, string, *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error)
}

func (c namespaceMembershipProbeClient) ProbeMigrationState(ctx context.Context, request *pb.ProbeMigrationStateRequest, _ ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	return c.probe(ctx, c.address, request)
}

func (g *MigrationNamespaceMembershipGate) peer(id uint64, address string) NamespaceAdmissionPeer {
	return NamespaceAdmissionPeer{MemberID: id, Address: address, Client: namespaceMembershipProbeClient{address: address, probe: g.probe}}
}

func (g *MigrationNamespaceMembershipGate) snapshot(ctx context.Context) (NamespaceAdmissionSnapshot, error) {
	if _, err := g.engine.LinearizableRead(ctx); err != nil {
		return NamespaceAdmissionSnapshot{}, errors.WithStack(err)
	}
	reader, ok := g.engine.(raftengine.AppliedConfigurationReader)
	if !ok {
		return NamespaceAdmissionSnapshot{}, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	before, err := g.namespaceSnapshotConfigurationIndex(reader)
	if err != nil {
		return NamespaceAdmissionSnapshot{}, err
	}
	configuration, err := g.engine.Configuration(ctx)
	if err != nil {
		return NamespaceAdmissionSnapshot{}, errors.WithStack(err)
	}
	result := NamespaceAdmissionSnapshot{ConfigurationIndex: before}
	for _, member := range configuration.Servers {
		if member.ID == "" || member.ID != strings.TrimSpace(member.ID) {
			return result, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
		}
		result.Members = append(result.Members, g.peer(g.deriveNodeID(member.ID), member.Address))
	}
	after, err := g.namespaceSnapshotConfigurationIndex(reader)
	if err != nil {
		return result, err
	}
	if before != after {
		return result, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	return result, nil
}

func (g *MigrationNamespaceMembershipGate) namespaceSnapshotConfigurationIndex(reader raftengine.AppliedConfigurationReader) (uint64, error) {
	index, supported := reader.AppliedConfigurationIndex()
	if !supported || index == 0 || g.engine.Status().PendingConfChange {
		return 0, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	return index, nil
}

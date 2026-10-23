package main

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

const namespaceMembershipHandshakeTimeout = 2 * time.Second

type migrationNamespaceMembershipGate struct {
	mu       sync.Mutex
	store    store.MVCCStore
	engine   raftengine.Engine
	proposer raftengine.Proposer
	probe    func(context.Context, string, *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error)
	next     raftadmin.MembershipChangeInterceptor
}

var _ raftadmin.MembershipChangeInterceptor = (*migrationNamespaceMembershipGate)(nil)
var _ raftadmin.MembershipPromotionInterceptor = (*migrationNamespaceMembershipGate)(nil)

func newMigrationNamespaceMembershipGate(st store.MVCCStore, engine raftengine.Engine, proposer raftengine.Proposer, token string, next raftadmin.MembershipChangeInterceptor) *migrationNamespaceMembershipGate {
	return &migrationNamespaceMembershipGate{store: st, engine: engine, proposer: proposer, next: next,
		probe: func(ctx context.Context, address string, request *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
			conn, err := grpc.NewClient(address, internalutil.GRPCDialOptions()...)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			defer func() { _ = conn.Close() }()
			response, err := newSplitMigrationInternalClient(conn, token).ProbeMigrationState(ctx, request)
			return response, errors.WithStack(err)
		}}
}

func (g *migrationNamespaceMembershipGate) PreAddMember(ctx context.Context, id, address string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.authorize(ctx, id, address); err != nil {
		return err
	}
	if g.next != nil {
		return errors.WithStack(g.next.PreAddMember(ctx, id, address))
	}
	return nil
}

func (g *migrationNamespaceMembershipGate) PrePromoteMember(ctx context.Context, id, address string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.authorize(ctx, id, address); err != nil {
		return err
	}
	if next, ok := g.next.(raftadmin.MembershipPromotionInterceptor); ok {
		return errors.WithStack(next.PrePromoteMember(ctx, id, address))
	}
	return nil
}

func (g *migrationNamespaceMembershipGate) authorize(ctx context.Context, id, address string) error {
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
	snapshot := func(ctx context.Context) (adapter.NamespaceAdmissionSnapshot, error) { return g.snapshot(ctx) }
	current, err := snapshot(ctx)
	if err != nil {
		return err
	}
	joining, err := namespaceJoiningCandidate(current, peer)
	if err != nil {
		return err
	}
	activate := func(ctx context.Context, command kv.NamespaceAdmissionCommand) error {
		return errors.WithStack(kv.ActivateMigrationNamespace(ctx, g.store, g.proposer, command))
	}
	if err := adapter.EnsureMigrationNamespaceAdmissionWithSnapshot(ctx, snapshot, activate, joining); err != nil {
		return errors.WithStack(err)
	}
	configuration, err := kv.MigrationConfigurationIndex(ctx, g.store)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(kv.CheckMigrationNamespacePrejoin(ctx, g.store, configuration, peer.MemberID, installedVersion))
}

func (g *migrationNamespaceMembershipGate) probeNamespaceCandidate(ctx context.Context, id, address string, version uint32) (adapter.NamespaceAdmissionPeer, uint32, error) {
	if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) || address == "" || g.engine == nil || g.proposer == nil || g.probe == nil {
		return adapter.NamespaceAdmissionPeer{}, 0, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	peer := g.peer(etcd.DeriveNodeID(id), address)
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

func namespaceJoiningCandidate(current adapter.NamespaceAdmissionSnapshot, peer adapter.NamespaceAdmissionPeer) ([]adapter.NamespaceAdmissionPeer, error) {
	joining := []adapter.NamespaceAdmissionPeer{peer}
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

func (g *migrationNamespaceMembershipGate) peer(id uint64, address string) adapter.NamespaceAdmissionPeer {
	return adapter.NamespaceAdmissionPeer{MemberID: id, Address: address, Client: namespaceMembershipProbeClient{address: address, probe: g.probe}}
}

func (g *migrationNamespaceMembershipGate) snapshot(ctx context.Context) (adapter.NamespaceAdmissionSnapshot, error) {
	if _, err := g.engine.LinearizableRead(ctx); err != nil {
		return adapter.NamespaceAdmissionSnapshot{}, errors.WithStack(err)
	}
	reader, ok := g.engine.(raftengine.AppliedConfigurationReader)
	if !ok {
		return adapter.NamespaceAdmissionSnapshot{}, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	before, err := g.namespaceSnapshotConfigurationIndex(reader)
	if err != nil {
		return adapter.NamespaceAdmissionSnapshot{}, err
	}
	configuration, err := g.engine.Configuration(ctx)
	if err != nil {
		return adapter.NamespaceAdmissionSnapshot{}, errors.WithStack(err)
	}
	result := adapter.NamespaceAdmissionSnapshot{ConfigurationIndex: before}
	for _, member := range configuration.Servers {
		if member.ID == "" || member.ID != strings.TrimSpace(member.ID) {
			return result, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
		}
		result.Members = append(result.Members, g.peer(etcd.DeriveNodeID(member.ID), member.Address))
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

func (g *migrationNamespaceMembershipGate) namespaceSnapshotConfigurationIndex(reader raftengine.AppliedConfigurationReader) (uint64, error) {
	index, supported := reader.AppliedConfigurationIndex()
	if !supported || index == 0 || g.engine.Status().PendingConfChange {
		return 0, errors.WithStack(kv.ErrMigrationNamespaceProtocol)
	}
	return index, nil
}

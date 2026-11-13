package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/nodeauth"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

func startupRoleStoreFromFlags(f adminHTTPFlags) admin.RoleStore {
	if !f.enabled {
		return nil
	}
	return roleStoreFromFlags(parseCSV(f.fullAccessKeys), parseCSV(f.readOnlyAccessKeys))
}

func setupAdminService(
	nodeID, grpcAddress string,
	runtimes []*clusterbootstrap.GroupRuntime,
	shardGroups map[uint64]*kv.ShardGroup,
	bootstrapServers []raftengine.Server,
	shardStore *kv.ShardStore,
	distCatalog *distribution.CatalogStore,
	coordinate kv.Coordinator,
	readTracker *kv.ActiveTimestampTracker,
	keyvizSampler *keyviz.MemSampler,
	connCache *kv.GRPCConnCache,
	autoSplitRuntime adapter.AutoSplitRuntime,
	backup backupFlags,
	nodeAuth nodeauth.Flags,
) (*adapter.AdminServer, nodeauth.AdminGRPCInterceptors, error) {
	members := adminMembersFromBootstrap(nodeID, bootstrapServers)
	// In multi-group mode the process does not listen on --address — each group
	// has its own rt.spec.address. Use the lowest-group-ID listener as the
	// canonical self address so GetClusterOverview.Self advertises an
	// endpoint the fan-out can actually dial. Falls back to the flag value
	// when no runtimes are registered (single-node dev runs).
	selfAddr := canonicalSelfAddress(grpcAddress, runtimes)
	srv, icept, err := configureAdminService(
		nodeAuth,
		adapter.S3BlobOffloadEnabledFromEnv(),
		adapter.NodeIdentity{NodeID: nodeID, GRPCAddress: selfAddr},
		members,
		connCache,
		adminBackupDependencies{
			store:      &adminBackupStore{ShardStore: shardStore, catalog: distCatalog},
			shardStore: shardStore, coordinate: coordinate, tracker: readTracker,
			config: backup,
		},
	)
	if err != nil {
		return nil, nodeauth.AdminGRPCInterceptors{}, err
	}
	if srv == nil {
		return nil, icept, nil
	}
	for _, rt := range runtimes {
		srv.RegisterGroup(rt.Spec.ID, rt.Engine)
		if group := shardGroups[rt.Spec.ID]; group != nil {
			group.SetLeaderReadToken(icept.Token)
			srv.RegisterBackupProposer(rt.Spec.ID, kv.NewLeaderAdminProposer(
				rt.Engine,
				group.Proposer(),
				connCache,
				kv.WithLeaderAdminToken(icept.Token),
			))
		}
	}
	// Only register a real sampler. Passing a typed-nil *MemSampler
	// would store a non-nil interface and make GetKeyVizMatrix
	// return a successful empty response instead of Unavailable —
	// operators want the explicit "keyviz disabled" signal.
	if keyvizSampler != nil {
		srv.RegisterSampler(keyvizSampler)
	}
	if autoSplitRuntime != nil {
		srv.RegisterAutoSplitRuntime(autoSplitRuntime)
	}
	if nodeAuth.AdminInsecureNoAuth {
		log.Printf("WARNING: --adminInsecureNoAuth is set; Admin gRPC service exposed without authentication")
	}
	return srv, icept, nil
}

// canonicalSelfAddress picks the listener address AdminServer should advertise
// as Self.GRPCAddress. The Admin gRPC service is registered on every Raft
// group's listener in startRaftServers, so any runtime's address is reachable;
// we pick the lowest group ID to make the choice deterministic across
// restarts. Returns the supplied fallback when no runtimes exist (e.g., a
// single-node dev invocation without --raftGroups).
func canonicalSelfAddress(fallback string, runtimes []*clusterbootstrap.GroupRuntime) string {
	var (
		bestID   uint64
		bestAddr string
		found    bool
	)
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		if !found || rt.Spec.ID < bestID {
			bestID, bestAddr, found = rt.Spec.ID, rt.Spec.Address, true
		}
	}
	if !found {
		return fallback
	}
	return bestAddr
}

// adminMembersFromBootstrap extracts the peer list (everyone except self) from
// the Raft bootstrap configuration so GetClusterOverview returns a populated
// members list. Without this the admin binary's membersFrom cache collapses to
// only the responding seed and stops fanning out across the cluster.
func adminMembersFromBootstrap(selfID string, servers []raftengine.Server) []adapter.NodeIdentity {
	if len(servers) == 0 {
		return nil
	}
	out := make([]adapter.NodeIdentity, 0, len(servers))
	for _, s := range servers {
		if s.ID == selfID {
			continue
		}
		out = append(out, adapter.NodeIdentity{
			NodeID:      s.ID,
			GRPCAddress: s.Address,
		})
	}
	return out
}

// configureAdminService builds the node-side AdminServer plus the interceptor
// set that enforces its bearer token, or returns (nil, {}, nil) when the
// service is intentionally disabled. It is mutually exclusive with
// --adminInsecureNoAuth so operators have to opt into the unauthenticated
// mode explicitly.
// backupFlags bounds live-backup pins and the peer fan-out that starts them.
type backupFlags struct {
	defaultTTL              time.Duration
	maxTTL                  time.Duration
	beginDeadline           time.Duration
	snapshotHeadroomEntries uint64
	scanPageSize            int
	maxActivePins           int
}

func (f *backupFlags) register(fs *flag.FlagSet) {
	fs.DurationVar(&f.defaultTTL, "backupDefaultTTL", defaultBackupTTL, "Default live-backup pin TTL")
	fs.DurationVar(&f.maxTTL, "backupMaxTTL", defaultBackupMaxTTL, "Maximum live-backup pin TTL per renewal")
	fs.DurationVar(&f.beginDeadline, "backupBeginDeadline", defaultBackupDeadline, "Deadline for live-backup peer gates and pin fan-out")
	fs.Uint64Var(&f.snapshotHeadroomEntries,
		"backupSnapshotHeadroomEntries", defaultBackupHeadroom,
		"Minimum Raft entries remaining before the next snapshot when a live backup begins",
	)
	fs.IntVar(&f.scanPageSize, "backupScanPageSize", defaultBackupPageSize, "Count-only scan page size used by live-backup baselines")
	fs.IntVar(&f.maxActivePins, "backupMaxActivePins", defaultBackupPinLimit, "Maximum concurrent logical live-backup pins")
}

type adminBackupDependencies struct {
	store      adapter.BackupStore
	shardStore *kv.ShardStore
	coordinate kv.Coordinator
	tracker    *kv.ActiveTimestampTracker
	config     backupFlags
}

type adminBackupStore struct {
	*kv.ShardStore
	catalog *distribution.CatalogStore
}

func (s *adminBackupStore) CaptureBackupRouteSnapshotAt(ctx context.Context, ts uint64) (kv.BackupRouteSnapshot, error) {
	if s == nil {
		return kv.BackupRouteSnapshot{}, errors.New("backup store is unavailable")
	}
	snapshot, err := kv.CaptureBackupRouteSnapshotAt(ctx, s.catalog, ts)
	return snapshot, errors.Wrap(err, "capture backup route snapshot")
}

func configureAdminService(
	nodeAuth nodeauth.Flags,
	s3BlobOffloadEnabled bool,
	self adapter.NodeIdentity,
	members []adapter.NodeIdentity,
	connCache *kv.GRPCConnCache,
	backupDeps ...adminBackupDependencies,
) (*adapter.AdminServer, nodeauth.AdminGRPCInterceptors, error) {
	auth, err := nodeAuth.LoadGRPCAuth()
	if err != nil {
		return nil, nodeauth.AdminGRPCInterceptors{}, errors.WithStack(err)
	}
	insecureNoAuth := nodeAuth.AdminInsecureNoAuth
	var srv *adapter.AdminServer
	if auth.AdminEnabled {
		// adminServerOptions carries the live-backup dependencies and the
		// admin token the backup proposer forwards with, so the options have
		// to be built here rather than at NewAdminServer's bare call.
		opts := adminServerOptions(auth.AdminToken, insecureNoAuth, connCache, backupDeps)
		srv = adapter.NewAdminServer(self, members, opts...)
		srv.SetCapability(
			adapter.S3BlobOffloadCapabilityName,
			s3BlobOffloadAdminCapability(auth.AdminToken, auth.PeerToken, s3BlobOffloadEnabled),
		)
	}
	// token is what setupAdminService hands to SetLeaderReadToken and the
	// per-group backup proposer, so it has to survive the interceptor build.
	icept := nodeauth.AdminGRPCInterceptors{Token: auth.AdminToken, MigrationToken: auth.MigrationToken, MigrationReady: nodeAuth.SplitMigrationLocalReadinessGate}
	adminUnary, adminStream := adapter.AdminTokenAuth(auth.AdminToken)
	nodeauth.AppendGRPCBearerInterceptors(&icept, adminUnary, adminStream)
	migrationUnary, migrationStream := adapter.SplitMigrationTokenAuth(auth.MigrationToken, insecureNoAuth, nodeAuth.SplitMigrationPeerAuthEnforced)
	nodeauth.AppendGRPCBearerInterceptors(&icept, migrationUnary, migrationStream)
	peerUnary, peerStream := adapter.S3BlobPeerTokenAuth(auth.PeerToken)
	nodeauth.AppendGRPCBearerInterceptors(&icept, peerUnary, peerStream)
	icept.S3BlobFetchEnabled = auth.PeerToken != ""
	return srv, icept, nil
}

func s3BlobOffloadAdminCapability(adminToken, peerToken string, enabled bool) bool {
	return adapter.S3BlobOffloadLocalCapability() && enabled && adminToken != "" && peerToken != ""
}

func adminServerOptions(
	token string,
	insecureNoAuth bool,
	connCache *kv.GRPCConnCache,
	backupDeps []adminBackupDependencies,
) []adapter.AdminOption {
	opts := []adapter.AdminOption{adapter.WithAdminNodeVersion(buildVersion())}
	if probe := adminLeaderVersionProbe(connCache); probe != nil {
		opts = append(opts, adapter.WithAdminLeaderVersionProbe(probe))
	}
	if len(backupDeps) > 0 {
		deps := backupDeps[0]
		tokenKey := []byte(token)
		if insecureNoAuth {
			tokenKey = []byte("elastickv-insecure-live-backup-token-v1")
		}
		opts = append(opts,
			adapter.WithAdminBackupControl(
				deps.store,
				adminBackupReadFence(deps.coordinate, deps.shardStore),
				adminBackupPeerProbe(connCache),
				deps.tracker,
				tokenKey,
			),
			adapter.WithAdminBackupConfig(adapter.AdminBackupConfig{
				DefaultTTL:              deps.config.defaultTTL,
				MinTTL:                  time.Minute,
				MaxTTL:                  deps.config.maxTTL,
				BeginDeadline:           deps.config.beginDeadline,
				SnapshotHeadroomEntries: deps.config.snapshotHeadroomEntries,
				ScanPageSize:            deps.config.scanPageSize,
				MaxActivePins:           deps.config.maxActivePins,
			}),
		)
	}
	return opts
}

func adminBackupReadFence(coordinate kv.Coordinator, shardStore *kv.ShardStore) adapter.BackupReadFence {
	if coordinate == nil || shardStore == nil || coordinate.Clock() == nil {
		return nil
	}
	return func(ctx context.Context) (uint64, error) {
		clock := coordinate.Clock()
		leaderCommitTS, err := kv.LeaseReadAllGroupsTimestampThrough(coordinate, ctx)
		if err != nil {
			return 0, errors.Wrap(err, "backup: fence raft groups")
		}
		lastCommitTS := shardStore.LastCommitTS()
		if leaderCommitTS > lastCommitTS {
			lastCommitTS = leaderCommitTS
		}
		clock.Observe(lastCommitTS)
		readTS, err := allocateBackupReadTimestamp(ctx, coordinate, lastCommitTS)
		if err != nil {
			return 0, errors.Wrap(err, "backup: issue read timestamp")
		}
		return readTS, nil
	}
}

func allocateBackupReadTimestamp(ctx context.Context, coordinate kv.Coordinator, min uint64) (uint64, error) {
	if alloc, ok := coordinate.(kv.TimestampAfterAllocator); ok {
		return alloc.NextAfter(ctx, min) //nolint:wrapcheck // Canonical coordinator/TSO error.
	}
	if alloc, ok := coordinate.(kv.TimestampAllocator); ok {
		ts, err := alloc.Next(ctx)
		if err != nil {
			return 0, err //nolint:wrapcheck // Wrapped at the backup call site.
		}
		if ts <= min {
			return 0, errors.Errorf("timestamp allocator returned %d, not after %d", ts, min)
		}
		return ts, nil
	}
	clock := coordinate.Clock()
	if clock == nil {
		return 0, errors.New("backup timestamp allocator is unavailable")
	}
	clock.Observe(min)
	return clock.NextFenced() //nolint:wrapcheck // Wrapped at the backup call site.
}

func adminBackupPeerProbe(connCache *kv.GRPCConnCache) adapter.BackupPeerProbe {
	if connCache == nil {
		return nil
	}
	return func(ctx context.Context, address string) (adapter.BackupPeerVersion, error) {
		conn, err := connCache.ConnFor(address)
		if err != nil {
			return adapter.BackupPeerVersion{}, errors.Wrap(err, "backup peer probe: dial peer")
		}
		resp, err := pb.NewAdminClient(conn).GetNodeVersion(ctx, &pb.GetNodeVersionRequest{})
		if err != nil {
			return adapter.BackupPeerVersion{}, errors.Wrap(err, "backup peer probe: get node version")
		}
		return adapter.BackupPeerVersion{
			NodeVersion: resp.GetNodeVersion(), BackupProtocolVersion: resp.GetBackupProtocolVersion(),
		}, nil
	}
}

func adminLeaderVersionProbe(connCache *kv.GRPCConnCache) adapter.LeaderVersionProbe {
	if connCache == nil {
		return nil
	}
	return func(ctx context.Context, address string) (string, error) {
		conn, err := connCache.ConnFor(address)
		if err != nil {
			return "", errors.Wrap(err, "admin leader version probe: dial peer")
		}
		resp, err := pb.NewAdminClient(conn).GetNodeVersion(ctx, &pb.GetNodeVersionRequest{})
		if err != nil {
			return "", errors.Wrap(err, "admin leader version probe: get node version")
		}
		return resp.GetNodeVersion(), nil
	}
}

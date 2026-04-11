package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	"github.com/bootjp/elastickv/internal/raftstore"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type raftGroupRuntime struct {
	spec   groupSpec
	raft   *raft.Raft
	engine raftengine.Engine
	store  store.MVCCStore

	registerTransport func(grpc.ServiceRegistrar)
	closeTransport    func()
	closeStores       func()
}

const raftCommitTimeout = 50 * time.Millisecond

type raftEngineType string

const (
	raftEngineHashicorp       raftEngineType = "hashicorp"
	raftEngineEtcd            raftEngineType = "etcd"
	etcdTickInterval                         = 10 * time.Millisecond
	etcdHeartbeatMinTicks                    = 1
	etcdElectionMinTicks                     = 2
	etcdRuntimeMaxSizePerMsg                 = 1 << 20
	etcdRuntimeMaxInflightMsg                = 256
)

var ErrUnsupportedRaftEngine = errors.New("unsupported raft engine")

func parseRaftEngineType(raw string) (raftEngineType, error) {
	switch engineType := raftEngineType(strings.ToLower(strings.TrimSpace(raw))); engineType {
	case "", raftEngineHashicorp:
		return raftEngineHashicorp, nil
	case raftEngineEtcd:
		return raftEngineEtcd, nil
	default:
		return "", errors.Wrapf(ErrUnsupportedRaftEngine, "%q", raw)
	}
}

func (r *raftGroupRuntime) Close() {
	if r == nil {
		return
	}
	if r.raft != nil {
		_ = r.raft.Shutdown().Error()
		r.raft = nil
	}
	if r.engine != nil {
		_ = r.engine.Close()
		r.engine = nil
	}
	if r.closeTransport != nil {
		r.closeTransport()
		r.closeTransport = nil
	}
	if r.closeStores != nil {
		r.closeStores()
		r.closeStores = nil
	}
	if r.store != nil {
		_ = r.store.Close()
		r.store = nil
	}
}

func (r *raftGroupRuntime) registerGRPC(server grpc.ServiceRegistrar) {
	if r == nil || r.registerTransport == nil || server == nil {
		return
	}
	r.registerTransport(server)
}

func closeRaftStore(raftStore **raftstore.PebbleStore) {
	if raftStore == nil || *raftStore == nil {
		return
	}
	_ = (*raftStore).Close()
	*raftStore = nil
}

func closeTransportManager(tm **transport.Manager) {
	if tm == nil || *tm == nil {
		return
	}
	_ = (*tm).Close()
	*tm = nil
}

const raftDirPerm = 0o755

func groupDataDir(baseDir, raftID string, groupID uint64, multi bool) string {
	if !multi {
		return filepath.Join(baseDir, raftID)
	}
	return filepath.Join(baseDir, raftID, fmt.Sprintf("group-%d", groupID))
}

func newRaftGroup(raftID string, group groupSpec, baseDir string, multi bool, bootstrap bool, bootstrapServers []raft.Server, fsm raft.FSM) (*raft.Raft, *transport.Manager, func(), error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(raftID)
	c.CommitTimeout = raftCommitTimeout
	c.HeartbeatTimeout = heartbeatTimeout
	c.ElectionTimeout = electionTimeout
	c.LeaderLeaseTimeout = leaderLease

	dir := groupDataDir(baseDir, raftID, group.id, multi)
	if err := os.MkdirAll(dir, raftDirPerm); err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	var raftStore *raftstore.PebbleStore
	var tm *transport.Manager

	closeStores := func() { closeRaftStore(&raftStore) }
	cleanup := func() {
		closeTransportManager(&tm)
		closeStores()
	}

	for _, legacy := range []string{"logs.dat", "stable.dat"} {
		if _, err := os.Stat(filepath.Join(dir, legacy)); err == nil {
			cleanup()
			return nil, nil, nil, errors.WithStack(errors.Newf(
				"legacy boltdb Raft storage %q found in %s; manual migration required before using Pebble-backed storage",
				legacy, dir,
			))
		}
	}

	var err error
	raftStore, err = raftstore.NewPebbleStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	fss, err := raft.NewFileSnapshotStore(dir, snapshotRetainCount, os.Stderr)
	if err != nil {
		cleanup()
		return nil, nil, nil, errors.WithStack(err)
	}

	tm = transport.New(raft.ServerAddress(group.address), internalutil.GRPCDialOptions())

	r, err := raft.NewRaft(c, fsm, raftStore, raftStore, fss, tm.Transport())
	if err != nil {
		cleanup()
		return nil, nil, nil, errors.WithStack(err)
	}

	if bootstrap {
		servers := bootstrapServers
		if len(servers) == 0 {
			servers = []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(raftID),
					Address:  raft.ServerAddress(group.address),
				},
			}
		}
		cfg := raft.Configuration{Servers: servers}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			_ = r.Shutdown().Error()
			cleanup()
			return nil, nil, nil, errors.WithStack(err)
		}
	}

	return r, tm, closeStores, nil
}

func newEtcdGroup(raftID string, group groupSpec, baseDir string, multi bool, bootstrap bool, bootstrapServers []raft.Server, fsm raft.FSM) (raftengine.Engine, func(grpc.ServiceRegistrar), error) {
	dir := groupDataDir(baseDir, raftID, group.id, multi)
	if err := os.MkdirAll(dir, raftDirPerm); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	peers := etcdPeersFromServers(bootstrapServers)
	var transport *etcdraftengine.GRPCTransport
	if len(peers) > 1 {
		transport = etcdraftengine.NewGRPCTransport(peers)
	}

	engine, err := etcdraftengine.Open(context.Background(), etcdraftengine.OpenConfig{
		LocalID:        raftID,
		LocalAddress:   group.address,
		DataDir:        dir,
		Peers:          peers,
		Bootstrap:      bootstrap,
		Transport:      transport,
		StateMachine:   etcdraftengine.AdaptHashicorpFSM(fsm),
		TickInterval:   etcdTickInterval,
		HeartbeatTick:  durationToTicks(heartbeatTimeout, etcdTickInterval, etcdHeartbeatMinTicks),
		ElectionTick:   durationToTicks(electionTimeout, etcdTickInterval, etcdElectionMinTicks),
		MaxSizePerMsg:  etcdRuntimeMaxSizePerMsg,
		MaxInflightMsg: etcdRuntimeMaxInflightMsg,
	})
	if err != nil {
		if transport != nil {
			_ = transport.Close()
		}
		return nil, nil, errors.WithStack(err)
	}

	var register func(grpc.ServiceRegistrar)
	if transport != nil {
		register = transport.Register
	}
	return engine, register, nil
}

func etcdPeersFromServers(servers []raft.Server) []etcdraftengine.Peer {
	if len(servers) == 0 {
		return nil
	}
	peers := make([]etcdraftengine.Peer, 0, len(servers))
	for _, server := range servers {
		peers = append(peers, etcdraftengine.Peer{
			ID:      string(server.ID),
			Address: string(server.Address),
		})
	}
	return peers
}

func durationToTicks(timeout time.Duration, tick time.Duration, min int) int {
	if tick <= 0 {
		return min
	}
	ticks := int(timeout / tick)
	if timeout%tick != 0 {
		ticks++
	}
	if ticks < min {
		return min
	}
	return ticks
}

func buildRuntimeForGroup(
	raftID string,
	group groupSpec,
	baseDir string,
	multi bool,
	bootstrap bool,
	bootstrapServers []raft.Server,
	st store.MVCCStore,
	fsm raft.FSM,
	engineType raftEngineType,
) (*raftGroupRuntime, error) {
	switch engineType {
	case raftEngineHashicorp:
		r, tm, closeStores, err := newRaftGroup(raftID, group, baseDir, multi, bootstrap, bootstrapServers, fsm)
		if err != nil {
			return nil, err
		}
		return &raftGroupRuntime{
			spec:              group,
			raft:              r,
			engine:            hashicorpraftengine.New(r),
			store:             st,
			registerTransport: tm.Register,
			closeTransport:    func() { closeTransportManager(&tm) },
			closeStores:       closeStores,
		}, nil
	case raftEngineEtcd:
		engine, register, err := newEtcdGroup(raftID, group, baseDir, multi, bootstrap, bootstrapServers, fsm)
		if err != nil {
			return nil, err
		}
		return &raftGroupRuntime{
			spec:              group,
			engine:            engine,
			store:             st,
			registerTransport: register,
		}, nil
	default:
		return nil, errors.Wrapf(ErrUnsupportedRaftEngine, "%q", engineType)
	}
}

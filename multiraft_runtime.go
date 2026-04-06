package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftstore"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

type raftGroupRuntime struct {
	spec   groupSpec
	raft   *raft.Raft
	engine raftengine.Engine
	tm     *transport.Manager
	store  store.MVCCStore

	closeStores func()
}

const raftCommitTimeout = 50 * time.Millisecond

func (r *raftGroupRuntime) Close() {
	if r == nil {
		return
	}
	if r.raft != nil {
		_ = r.raft.Shutdown().Error()
		r.raft = nil
	}
	if r.tm != nil {
		_ = r.tm.Close()
		r.tm = nil
	}
	if r.engine != nil {
		_ = r.engine.Close()
		r.engine = nil
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

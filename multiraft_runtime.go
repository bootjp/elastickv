package main

import (
	"fmt"
	"os"
	"path/filepath"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type raftGroupRuntime struct {
	spec  groupSpec
	raft  *raft.Raft
	tm    *transport.Manager
	store store.MVCCStore

	closeStores func()
}

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
	if r.closeStores != nil {
		r.closeStores()
		r.closeStores = nil
	}
	if r.store != nil {
		_ = r.store.Close()
		r.store = nil
	}
}

func closeBoltStores(ldb, sdb **boltdb.BoltStore) {
	if ldb == nil || sdb == nil {
		return
	}
	if *ldb != nil {
		_ = (*ldb).Close()
		*ldb = nil
	}
	if *sdb != nil {
		_ = (*sdb).Close()
		*sdb = nil
	}
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
	c.HeartbeatTimeout = heartbeatTimeout
	c.ElectionTimeout = electionTimeout
	c.LeaderLeaseTimeout = leaderLease

	dir := groupDataDir(baseDir, raftID, group.id, multi)
	if err := os.MkdirAll(dir, raftDirPerm); err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	var ldb *boltdb.BoltStore
	var sdb *boltdb.BoltStore
	var tm *transport.Manager

	closeStores := func() { closeBoltStores(&ldb, &sdb) }
	cleanup := func() {
		closeTransportManager(&tm)
		closeStores()
	}

	var err error
	ldb, err = boltdb.NewBoltStore(filepath.Join(dir, "logs.dat"))
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	sdb, err = boltdb.NewBoltStore(filepath.Join(dir, "stable.dat"))
	if err != nil {
		cleanup()
		return nil, nil, nil, errors.WithStack(err)
	}

	fss, err := raft.NewFileSnapshotStore(dir, snapshotRetainCount, os.Stderr)
	if err != nil {
		cleanup()
		return nil, nil, nil, errors.WithStack(err)
	}

	tm = transport.New(raft.ServerAddress(group.address), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
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

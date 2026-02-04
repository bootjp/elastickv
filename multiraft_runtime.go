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
}

const raftDirPerm = 0o755

func groupDataDir(baseDir, raftID string, groupID uint64, multi bool) string {
	if !multi {
		return filepath.Join(baseDir, raftID)
	}
	return filepath.Join(baseDir, raftID, fmt.Sprintf("group-%d", groupID))
}

func newRaftGroup(raftID string, group groupSpec, baseDir string, multi bool, bootstrap bool, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(raftID)
	c.HeartbeatTimeout = heartbeatTimeout
	c.ElectionTimeout = electionTimeout
	c.LeaderLeaseTimeout = leaderLease

	dir := groupDataDir(baseDir, raftID, group.id, multi)
	if err := os.MkdirAll(dir, raftDirPerm); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	ldb, err := boltdb.NewBoltStore(filepath.Join(dir, "logs.dat"))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(dir, "stable.dat"))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	fss, err := raft.NewFileSnapshotStore(dir, snapshotRetainCount, os.Stderr)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	tm := transport.New(raft.ServerAddress(group.address), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	if bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(raftID),
					Address:  raft.ServerAddress(group.address),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, errors.WithStack(err)
		}
	}

	return r, tm, nil
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

var (
	myAddr        = flag.String("address", "localhost:50051", "TCP host+port for this node")
	redisAddr     = flag.String("redis_address", "localhost:6379", "TCP host+port for redis")
	raftId        = flag.String("raft_id", "", "Node id used by Raft")
	raftDir       = flag.String("raft_data_dir", "data/", "Raft data dir")
	raftBootstrap = flag.Bool("raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
)

func main() {
	flag.Parse()

	if *raftId == "" {
		log.Fatalf("flag --raft_id is required")
	}

	ctx := context.Background()
	_, port, err := net.SplitHostPort(*myAddr)
	if err != nil {
		log.Fatalf("failed to parse local address (%q): %v", *myAddr, err)
	}

	grpcSock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := store.NewRbMemoryStore()
	lockStore := store.NewMemoryStoreDefaultTTL()
	kvFSM := kv.NewKvFSM(s, lockStore)

	r, tm, err := NewRaft(ctx, *raftId, *myAddr, kvFSM)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}

	gs := grpc.NewServer()
	trx := kv.NewTransaction(r)
	coordinate := kv.NewCoordinator(trx, r)
	pb.RegisterRawKVServer(gs, adapter.NewGRPCServer(s, coordinate))
	pb.RegisterTransactionalKVServer(gs, adapter.NewGRPCServer(s, coordinate))
	pb.RegisterInternalServer(gs, adapter.NewInternal(trx, r))
	tm.Register(gs)

	leaderhealth.Setup(r, gs, []string{"RawKV", "Example"})
	raftadmin.Register(gs, r)
	reflection.Register(gs)

	redisL, err := net.Listen("tcp", *redisAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	eg := errgroup.Group{}
	eg.Go(func() error {
		return errors.WithStack(gs.Serve(grpcSock))
	})
	eg.Go(func() error {
		return errors.WithStack(adapter.NewRedisServer(redisL, s, coordinate).Run())
	})

	err = eg.Wait()
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

const snapshotRetainCount = 3

func NewRaft(_ context.Context, myID, myAddress string, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	baseDir := filepath.Join(*raftDir, myID)

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, snapshotRetainCount, os.Stderr)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	if *raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(myID),
					Address:  raft.ServerAddress(myAddress),
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

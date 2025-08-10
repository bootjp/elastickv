package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"strconv"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	grpcAdders = []string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
	}
	redisAdders = []string{
		"localhost:63791",
		"localhost:63792",
		"localhost:63793",
	}
)

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
}

func main() {
	eg := &errgroup.Group{}
	if err := run(eg); err != nil {
		slog.Error(err.Error())
	}
	err := eg.Wait()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(eg *errgroup.Group) error {

	cfg := raft.Configuration{}
	ctx := context.Background()
	var lc net.ListenConfig

	for i := 0; i < 3; i++ {
		var suffrage raft.ServerSuffrage
		if i == 0 {
			suffrage = raft.Voter
		} else {
			suffrage = raft.Nonvoter
		}

		server := raft.Server{
			Suffrage: suffrage,
			ID:       raft.ServerID(strconv.Itoa(i)),
			Address:  raft.ServerAddress(grpcAdders[i]),
		}
		cfg.Servers = append(cfg.Servers, server)
	}

	for i := 0; i < 3; i++ {
		st := store.NewRbMemoryStore()
		trxSt := store.NewMemoryStoreDefaultTTL()
		fsm := kv.NewKvFSM(st, trxSt)

		r, tm, err := newRaft(strconv.Itoa(i), grpcAdders[i], fsm, i == 0, cfg)
		if err != nil {
			return errors.WithStack(err)
		}

		s := grpc.NewServer()
		trx := kv.NewTransaction(r)
		coordinator := kv.NewCoordinator(trx, r)
		gs := adapter.NewGRPCServer(st, coordinator)
		distEngine := distribution.NewEngine()
		distSrv := adapter.NewDistributionServer(distEngine)
		// example route for demo purposes
		distSrv.UpdateRoute([]byte("a"), []byte("z"), uint64(i))
		tm.Register(s)
		pb.RegisterRawKVServer(s, gs)
		pb.RegisterTransactionalKVServer(s, gs)
		pb.RegisterInternalServer(s, adapter.NewInternal(trx, r))
		pb.RegisterDistributionServer(s, distSrv)
		leaderhealth.Setup(r, s, []string{"RawKV"})
		raftadmin.Register(s, r)

		grpcSock, err := lc.Listen(ctx, "tcp", grpcAdders[i])
		if err != nil {
			return errors.WithStack(err)
		}

		eg.Go(func() error {
			return errors.WithStack(s.Serve(grpcSock))
		})

		l, err := lc.Listen(ctx, "tcp", redisAdders[i])
		if err != nil {
			return errors.WithStack(err)
		}
		rd := adapter.NewRedisServer(l, st, coordinator)

		eg.Go(func() error {
			return errors.WithStack(rd.Run())
		})
	}

	return nil
}

func newRaft(myID string, myAddress string, fsm raft.FSM, bootstrap bool, cfg raft.Configuration) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	// this config is for development
	ldb := raft.NewInmemStore()
	sdb := raft.NewInmemStore()
	fss := raft.NewInmemSnapshotStore()

	c.Logger = hclog.New(&hclog.LoggerOptions{
		Name:       "raft-" + myID,
		JSONFormat: true,
		Level:      hclog.NoLevel,
	})

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	if bootstrap {
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, errors.WithStack(err)
		}
	}

	return r, tm, nil
}

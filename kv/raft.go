package kv

import (
	"context"

	transport "github.com/Jille/raft-grpc-transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	//"k8s.io/client-go/transport"

	//"k8s.io/client-go/transport"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

const (
	defaultTimeout = 10
	maxPool        = 3
)

func NewRaft(_ context.Context, myID string, myAddress string, fsm raft.FSM, bootstrap bool, cfg raft.Configuration) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	// this config is for development
	ldb := raft.NewInmemStore()
	sdb := raft.NewInmemStore()
	fss := raft.NewInmemSnapshotStore()

	c.Logger = hclog.New(&hclog.LoggerOptions{
		Name: "raft-" + myID,
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

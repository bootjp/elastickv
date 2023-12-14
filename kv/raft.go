package kv

import (
	"context"
	"net"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

const (
	defaultTimeout = 10
	maxPool        = 3
)

func NewRaft(
	_ context.Context,
	myID string,
	myAddress string,
	fsm raft.FSM,
	bootstrap bool,
	cfg raft.Configuration) (
	*raft.Raft, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	// this config is for development
	ldb := raft.NewInmemStore()
	sdb := raft.NewInmemStore()
	fss := raft.NewInmemSnapshotStore()

	advertise, err := net.ResolveTCPAddr("tcp", myAddress)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	raftTransport, err := raft.NewTCPTransportWithLogger(
		myAddress,
		advertise,
		maxPool,
		defaultTimeout,
		hclog.New(&hclog.LoggerOptions{
			Name:   "raft-net",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		}))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, raftTransport)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if bootstrap {
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return r, nil
}

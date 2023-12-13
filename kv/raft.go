package kv

import (
	"context"
	"net"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
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
		3,
		10,
		nil,
	)
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

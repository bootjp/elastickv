package etcd

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// FactoryConfig holds etcd-specific engine parameters.
type FactoryConfig struct {
	TickInterval   time.Duration
	HeartbeatTick  int
	ElectionTick   int
	MaxSizePerMsg  uint64
	MaxInflightMsg int
}

// Factory creates etcd raft engine instances.
type Factory struct {
	cfg FactoryConfig
}

// NewFactory returns a Factory with the given etcd-specific settings.
func NewFactory(cfg FactoryConfig) *Factory {
	return &Factory{cfg: cfg}
}

func (f *Factory) EngineType() string { return "etcd" }

func (f *Factory) Create(cfg raftengine.FactoryConfig) (*raftengine.FactoryResult, error) {
	peers := peersFromServers(cfg.Peers)
	if persistedPeers, ok, err := LoadPersistedPeers(cfg.DataDir); err != nil {
		return nil, errors.WithStack(err)
	} else if ok {
		peers = persistedPeers
	}

	var transport *GRPCTransport
	if len(peers) > 1 {
		transport = NewGRPCTransport(peers)
	}

	engine, err := Open(context.Background(), OpenConfig{
		LocalID:        cfg.LocalID,
		LocalAddress:   cfg.LocalAddress,
		DataDir:        cfg.DataDir,
		Peers:          peers,
		Bootstrap:      cfg.Bootstrap,
		JoinAsLearner:  cfg.JoinAsLearner,
		Transport:      transport,
		StateMachine:   cfg.StateMachine,
		TickInterval:   f.cfg.TickInterval,
		HeartbeatTick:  f.cfg.HeartbeatTick,
		ElectionTick:   f.cfg.ElectionTick,
		MaxSizePerMsg:  f.cfg.MaxSizePerMsg,
		MaxInflightMsg: f.cfg.MaxInflightMsg,
	})
	if err != nil {
		var closeErr error
		if transport != nil {
			closeErr = errors.WithStack(transport.Close())
		}
		return nil, errors.WithStack(errors.CombineErrors(err, closeErr))
	}

	var register func(grpc.ServiceRegistrar)
	var closeFunc func() error
	if transport != nil {
		register = transport.Register
		closeFunc = func() error { return errors.WithStack(transport.Close()) }
	}

	return &raftengine.FactoryResult{
		Engine:            engine,
		RegisterTransport: register,
		Close:             closeFunc,
	}, nil
}

func peersFromServers(servers []raftengine.Server) []Peer {
	if len(servers) == 0 {
		return nil
	}
	peers := make([]Peer, 0, len(servers))
	for _, s := range servers {
		peers = append(peers, Peer{
			ID:      s.ID,
			Address: s.Address,
		})
	}
	return peers
}

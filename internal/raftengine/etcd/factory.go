package etcd

import (
	"context"
	"io"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// FactoryConfig holds etcd-specific engine parameters.
type FactoryConfig struct {
	namespaceOwnership io.Closer
	namespaceRecovery  NamespaceRecoveryVerifier
	// BootstrapStartup configures protected transport before any RawNode or
	// dispatch worker starts. It must use explicitly provisioned identity.
	BootstrapStartup func(raftengine.FactoryConfig, *GRPCTransport) (*BootstrapStartup, error)
	TickInterval     time.Duration
	HeartbeatTick    int
	ElectionTick     int
	MaxSizePerMsg    uint64
	MaxInflightMsg   int
	// ColdStartObserver receives cold-start snapshot-restore
	// lifecycle events for the Branch 3 skip gate
	// (raftengine.ColdStartObserver). The Factory threads it into
	// every OpenConfig it builds; nil disables metrics emission
	// (the skip itself still runs). Wire via main.go from
	// monitoring.Registry.ColdStartObserver(). See PR #934 round-1
	// codex P2 for the plumbing rationale.
	ColdStartObserver raftengine.ColdStartObserver
	RaftCipher        *encryption.Cipher
	RaftCutoverIndex  RaftCutoverIndex
}

type BootstrapStartup struct {
	NativeAdmission       raftengine.NativeAdmission
	NativeIdentity        *WALIdentity
	RecoveredAdmission    func(RecoveredState) error
	AdmissionChanged      func()
	BeforeAdmissionChange func(raftengine.AppliedEntryIdentity, []byte) error
	PublicReady           func() bool
	ServerOptions         []grpc.ServerOption
	Register              func(grpc.ServiceRegistrar)
	Close                 func()
}

// Factory creates etcd raft engine instances.
type Factory struct {
	cfg FactoryConfig
}

// NewFactory returns a Factory with the given etcd-specific settings.
func NewFactory(cfg FactoryConfig) *Factory {
	return &Factory{cfg: cfg}
}

// WithBootstrapStartup derives a factory without mutating shared settings,
// including the configured envelope cipher and cutover index.
func (f *Factory) WithBootstrapStartup(setup func(raftengine.FactoryConfig, *GRPCTransport) (*BootstrapStartup, error)) *Factory {
	copy := *f
	copy.cfg.BootstrapStartup = setup
	return &copy
}

func (f *Factory) EngineType() string { return "etcd" }

func (f *Factory) SetRaftEnvelope(cipher *encryption.Cipher, cutover RaftCutoverIndex) {
	f.cfg.RaftCipher = cipher
	f.cfg.RaftCutoverIndex = cutover
}

func (f *Factory) Create(cfg raftengine.FactoryConfig) (*raftengine.FactoryResult, error) {
	peers := peersFromServers(cfg.Peers)
	if persistedPeers, ok, err := LoadPersistedPeers(cfg.DataDir); err != nil {
		return nil, errors.WithStack(err)
	} else if ok {
		peers = persistedPeers
	}

	var transport *GRPCTransport
	if len(peers) > 1 || f.cfg.BootstrapStartup != nil {
		transport = NewGRPCTransport(peers)
	}
	startup, err := f.prepareBootstrapStartup(cfg, transport)
	if err != nil {
		if transport != nil {
			_ = transport.Close()
		}
		return nil, err
	}

	engine, err := Open(context.Background(), OpenConfig{
		namespaceOwnership:    f.cfg.namespaceOwnership,
		namespaceRecovery:     f.cfg.namespaceRecovery,
		NativeAdmission:       startup.NativeAdmission,
		GroupID:               cfg.GroupID,
		NativeIdentity:        startup.NativeIdentity,
		RecoveredAdmission:    startup.RecoveredAdmission,
		AdmissionChanged:      startup.AdmissionChanged,
		BeforeAdmissionChange: startup.BeforeAdmissionChange,
		LocalID:               cfg.LocalID,
		LocalAddress:          cfg.LocalAddress,
		DataDir:               cfg.DataDir,
		Peers:                 peers,
		BootstrapSeed:         peersFromServers(cfg.BootstrapSeed),
		Bootstrap:             cfg.Bootstrap,
		JoinAsLearner:         cfg.JoinAsLearner,
		Transport:             transport,
		StateMachine:          cfg.StateMachine,
		TickInterval:          f.cfg.TickInterval,
		HeartbeatTick:         f.cfg.HeartbeatTick,
		ElectionTick:          f.cfg.ElectionTick,
		MaxSizePerMsg:         f.cfg.MaxSizePerMsg,
		MaxInflightMsg:        f.cfg.MaxInflightMsg,
		RaftCipher:            f.cfg.RaftCipher,
		RaftCutoverIndex:      f.cfg.RaftCutoverIndex,
		ColdStartObserver:     f.cfg.ColdStartObserver,
	})
	if err != nil {
		closeBootstrapStartup(startup)
		var closeErr error
		if transport != nil {
			closeErr = errors.WithStack(transport.Close())
		}
		return nil, errors.WithStack(errors.CombineErrors(err, closeErr))
	}

	var register func(grpc.ServiceRegistrar)
	var closeFunc func() error
	if transport != nil {
		register = func(registrar grpc.ServiceRegistrar) {
			startup.registerTransport(registrar, transport)
		}
		closeFunc = func() error {
			closeBootstrapStartup(startup)
			return errors.WithStack(transport.Close())
		}
	}

	return &raftengine.FactoryResult{
		ServerOptions:     startup.ServerOptions,
		PublicReady:       startup.PublicReady,
		Engine:            engine,
		RegisterTransport: register,
		Close:             closeFunc,
	}, nil
}

func (f *Factory) prepareBootstrapStartup(cfg raftengine.FactoryConfig, transport *GRPCTransport) (*BootstrapStartup, error) {
	if f.cfg.BootstrapStartup == nil {
		return &BootstrapStartup{}, nil
	}
	startup, err := f.cfg.BootstrapStartup(cfg, transport)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if startup == nil {
		return nil, errors.New("bootstrap startup configuration is missing")
	}
	if startup.NativeIdentity != nil && startup.NativeIdentity.Identity.Group != cfg.GroupID {
		closeBootstrapStartup(startup)
		return nil, errors.Wrap(ErrWALIdentity, "factory group identity mismatch")
	}
	if !completeBootstrapStartup(startup) {
		closeBootstrapStartup(startup)
		return nil, errors.New("bootstrap startup configuration is incomplete")
	}
	return startup, nil
}

func completeBootstrapStartup(s *BootstrapStartup) bool {
	return s.NativeAdmission != nil && s.NativeIdentity != nil && s.RecoveredAdmission != nil && s.AdmissionChanged != nil && s.PublicReady != nil &&
		s.Register != nil && len(s.ServerOptions) != 0 && s.Close != nil
}

func closeBootstrapStartup(s *BootstrapStartup) {
	if s != nil && s.Close != nil {
		s.Close()
	}
}

func (s *BootstrapStartup) registerTransport(registrar grpc.ServiceRegistrar, transport *GRPCTransport) {
	if s.Register != nil {
		s.Register(registrar)
	}
	transport.Register(registrar)
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

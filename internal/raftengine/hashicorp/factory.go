package hashicorp

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftstore"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

const factoryDirPerm = 0o755

// FactoryConfig holds hashicorp-specific engine parameters.
type FactoryConfig struct {
	CommitTimeout       time.Duration
	HeartbeatTimeout    time.Duration
	ElectionTimeout     time.Duration
	LeaderLeaseTimeout  time.Duration
	SnapshotRetainCount int
}

// Factory creates hashicorp raft engine instances.
type Factory struct {
	cfg FactoryConfig
}

// NewFactory returns a Factory with the given hashicorp-specific settings.
func NewFactory(cfg FactoryConfig) *Factory {
	return &Factory{cfg: cfg}
}

func (f *Factory) EngineType() string { return "hashicorp" }

func (f *Factory) Create(cfg raftengine.FactoryConfig) (*raftengine.FactoryResult, error) {
	dir := cfg.DataDir
	if err := os.MkdirAll(dir, factoryDirPerm); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := rejectLegacyBoltDB(dir); err != nil {
		return nil, err
	}

	r, rs, tm, err := f.createRaft(dir, cfg)
	if err != nil {
		return nil, err
	}

	cleanup := func() error {
		return errors.CombineErrors(
			closeIfNotNil(tm),
			closeIfNotNil(rs),
		)
	}

	if cfg.Bootstrap {
		if err := bootstrapCluster(r, cfg); err != nil {
			if cleanupErr := cleanup(); cleanupErr != nil {
				fmt.Fprintf(os.Stderr, "warning: cleanup after bootstrap failure: %v\n", cleanupErr)
			}
			return nil, err
		}
	}

	return &raftengine.FactoryResult{
		Engine:            New(r),
		RegisterTransport: tm.Register,
		Close:             cleanup,
	}, nil
}

func rejectLegacyBoltDB(dir string) error {
	for _, legacy := range []string{"logs.dat", "stable.dat"} {
		if _, err := os.Stat(filepath.Join(dir, legacy)); err == nil {
			return errors.WithStack(errors.Newf(
				"legacy boltdb Raft storage %q found in %s; manual migration required before using Pebble-backed storage",
				legacy, dir,
			))
		}
	}
	return nil
}

func (f *Factory) createRaft(dir string, cfg raftengine.FactoryConfig) (*raft.Raft, *raftstore.PebbleStore, *transport.Manager, error) {
	rs, err := raftstore.NewPebbleStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	fss, err := raft.NewFileSnapshotStore(dir, f.cfg.SnapshotRetainCount, os.Stderr)
	if err != nil {
		return nil, nil, nil, errors.WithStack(errors.CombineErrors(err, rs.Close()))
	}

	tm := transport.New(raft.ServerAddress(cfg.LocalAddress), internalutil.GRPCDialOptions())

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(cfg.LocalID)
	c.CommitTimeout = f.cfg.CommitTimeout
	c.HeartbeatTimeout = f.cfg.HeartbeatTimeout
	c.ElectionTimeout = f.cfg.ElectionTimeout
	c.LeaderLeaseTimeout = f.cfg.LeaderLeaseTimeout

	r, err := raft.NewRaft(c, adaptStateMachineToFSM(cfg.StateMachine), rs, rs, fss, tm.Transport())
	if err != nil {
		return nil, nil, nil, errors.WithStack(errors.CombineErrors(err, errors.CombineErrors(tm.Close(), rs.Close())))
	}
	return r, rs, tm, nil
}

func bootstrapCluster(r *raft.Raft, cfg raftengine.FactoryConfig) error {
	servers := peersToRaftServers(cfg)
	raftCfg := raft.Configuration{Servers: servers}
	if err := r.BootstrapCluster(raftCfg).Error(); err != nil {
		if shutdownErr := r.Shutdown().Error(); shutdownErr != nil {
			fmt.Fprintf(os.Stderr, "warning: raft shutdown after bootstrap failure: %v\n", shutdownErr)
		}
		return errors.WithStack(err)
	}
	return nil
}

func peersToRaftServers(cfg raftengine.FactoryConfig) []raft.Server {
	if len(cfg.Peers) == 0 {
		return []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(cfg.LocalID),
				Address:  raft.ServerAddress(cfg.LocalAddress),
			},
		}
	}
	servers := make([]raft.Server, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(p.ID),
			Address:  raft.ServerAddress(p.Address),
		})
	}
	return servers
}

// adaptStateMachineToFSM converts an engine-agnostic StateMachine to
// hashicorp/raft's FSM interface.
func adaptStateMachineToFSM(sm raftengine.StateMachine) raft.FSM {
	return &stateMachineFSMAdapter{sm: sm}
}

type stateMachineFSMAdapter struct {
	sm raftengine.StateMachine
}

func (a *stateMachineFSMAdapter) Apply(log *raft.Log) interface{} {
	return a.sm.Apply(log.Data)
}

func (a *stateMachineFSMAdapter) Snapshot() (raft.FSMSnapshot, error) {
	snap, err := a.sm.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &snapshotFSMAdapter{snap: snap}, nil
}

func (a *stateMachineFSMAdapter) Restore(r io.ReadCloser) error {
	defer func() {
		if err := r.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close raft restore stream: %v\n", err)
		}
	}()
	return errors.WithStack(a.sm.Restore(r))
}

type snapshotFSMAdapter struct {
	snap raftengine.Snapshot
}

func (a *snapshotFSMAdapter) Persist(sink raft.SnapshotSink) error {
	if _, err := a.snap.WriteTo(sink); err != nil {
		_ = sink.Cancel()
		return errors.WithStack(err)
	}
	return errors.WithStack(sink.Close())
}

func (a *snapshotFSMAdapter) Release() {
	if err := a.snap.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to close snapshot: %v\n", err)
	}
}

// closeIfNotNil closes c if it is not nil and returns the error.
func closeIfNotNil(c io.Closer) error {
	if c == nil {
		return nil
	}
	return errors.WithStack(c.Close())
}

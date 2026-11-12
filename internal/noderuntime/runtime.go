package noderuntime

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

type GroupRuntime struct {
	Spec GroupSpec
	// EngineMu guards engine to allow concurrent readers
	// (specifically the keyviz leader-term publisher goroutine that
	// runs on a ticker independent of startup ordering) to coexist
	// with Close() which clears the field on shutdown. Direct field
	// reads from the startup path remain safe because they execute
	// single-threaded before any goroutine is launched; long-lived
	// goroutines that read after startup must go through
	// snapshotEngine() (Codex P2 on PR #720).
	EngineMu sync.RWMutex
	Engine   raftengine.Engine

	Store        store.MVCCStore
	StateMachine raftengine.StateMachine

	RegisterTransport      func(grpc.ServiceRegistrar)
	TransportServerOptions []grpc.ServerOption
	PublicReady            func() bool
	CloseFactory           func() error // releases factory-created resources (transport, stores)
}

// SnapshotEngine returns the current engine reference under engineMu.
// Long-lived goroutines that may run while Close() executes (the
// keyviz leader-term publisher; future racy readers as they're
// migrated) MUST go through this accessor instead of reading the
// engine field directly. nil-receiver-safe.
func (r *GroupRuntime) SnapshotEngine() raftengine.Engine {
	if r == nil {
		return nil
	}
	r.EngineMu.RLock()
	defer r.EngineMu.RUnlock()
	return r.Engine
}

const RaftEngineMarkerPerm = 0o600

type RaftEngineType string

const (
	RaftEngineEtcd       RaftEngineType = "etcd"
	RaftEngineMarkerFile string         = "raft-engine"
)

var (
	ErrUnsupportedRaftEngine  = errors.New("unsupported raft engine")
	ErrRaftEngineDataDir      = errors.New("raft data dir belongs to a different raft engine")
	ErrLegacyHashicorpDataDir = errors.New("raft data dir contains legacy hashicorp/raft artifacts; hashicorp backend has been removed, manual migration to etcd is required")
)

func ParseRaftEngineType(raw string) (RaftEngineType, error) {
	switch engineType := RaftEngineType(strings.ToLower(strings.TrimSpace(raw))); engineType {
	case "", RaftEngineEtcd:
		return RaftEngineEtcd, nil
	default:
		return "", errors.Wrapf(ErrUnsupportedRaftEngine, "%q", raw)
	}
}

func (r *GroupRuntime) Close() {
	if r == nil {
		return
	}
	r.EngineMu.Lock()
	engine := r.Engine
	r.Engine = nil
	r.EngineMu.Unlock()
	if engine != nil {
		if err := engine.Close(); err != nil {
			slog.Warn("failed to close raft engine", "error", err)
		}
	}
	if r.CloseFactory != nil {
		if err := r.CloseFactory(); err != nil {
			slog.Warn("failed to close factory resources", "error", err)
		}
		r.CloseFactory = nil
	}
	if r.Store != nil {
		if err := r.Store.Close(); err != nil {
			slog.Warn("failed to close store", "error", err)
		}
		r.Store = nil
	}
}

func (r *GroupRuntime) RegisterGRPC(server grpc.ServiceRegistrar) {
	if r == nil || r.RegisterTransport == nil || server == nil {
		return
	}
	r.RegisterTransport(server)
}

const RaftDirPerm = 0o755

func GroupDataDir(baseDir, raftID string, groupID uint64, multi bool) string {
	if !multi && groupID != 0 {
		return filepath.Join(baseDir, raftID)
	}
	return filepath.Join(baseDir, raftID, fmt.Sprintf("group-%d", groupID))
}

func ensureRaftEngineDataDir(dir string, engineType RaftEngineType) error {
	if err := os.MkdirAll(dir, RaftDirPerm); err != nil {
		return errors.WithStack(err)
	}

	// Refuse to start on top of a dir that still holds hashicorp/raft
	// artifacts. The hashicorp backend has been removed and silently
	// overwriting its state with etcd markers would commit to an
	// incompatible engine over committed data. Fail fast; operators must
	// migrate the dir explicitly before restarting.
	if hashicorpArtifacts, err := hasHashicorpRaftArtifacts(dir); err != nil {
		return err
	} else if hashicorpArtifacts {
		return errors.Wrapf(ErrLegacyHashicorpDataDir, "%s", dir)
	}

	markerPath := filepath.Join(dir, RaftEngineMarkerFile)
	if current, ok, err := ReadRaftEngineMarker(markerPath); err != nil {
		return err
	} else if ok {
		if current != engineType {
			return errors.Wrapf(ErrRaftEngineDataDir, "%s is initialized for %s, not %s", dir, current, engineType)
		}
		return nil
	}

	detected, ok, err := detectRaftEngineFromDataDir(dir)
	if err != nil {
		return err
	}
	if ok && detected != engineType {
		return errors.Wrapf(ErrRaftEngineDataDir, "%s contains %s state, not %s", dir, detected, engineType)
	}
	return writeRaftEngineMarker(markerPath, engineType)
}

// hasHashicorpRaftArtifacts reports whether dir contains any files that
// were produced by the removed hashicorp/raft backend (raft.db plus the
// boltdb log/stable files). Used to refuse startup on a legacy data dir
// rather than silently overwriting it with an etcd-shaped cluster.
func hasHashicorpRaftArtifacts(dir string) (bool, error) {
	return hasRaftArtifacts(dir, "raft.db", "logs.dat", "stable.dat")
}

func ReadRaftEngineMarker(path string) (RaftEngineType, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, errors.WithStack(err)
	}
	engineType, err := ParseRaftEngineType(strings.TrimSpace(string(data)))
	if err != nil {
		return "", false, errors.Wrapf(err, "invalid raft engine marker %s", path)
	}
	return engineType, true, nil
}

func detectRaftEngineFromDataDir(dir string) (RaftEngineType, bool, error) {
	etcdArtifacts, err := hasRaftArtifacts(dir,
		"wal",
		"snap",
		filepath.Join("member", "wal"),
		filepath.Join("member", "snap"),
		"etcd-raft-state.bin",
		"etcd-raft-meta.bin",
		"etcd-raft-entries.bin",
		"etcd-raft-peers.bin",
		"etcd-fsm-snapshot.bin",
	)
	if err != nil {
		return "", false, err
	}

	if etcdArtifacts {
		return RaftEngineEtcd, true, nil
	}
	return "", false, nil
}

func hasRaftArtifacts(dir string, paths ...string) (bool, error) {
	for _, rel := range paths {
		if _, err := os.Stat(filepath.Join(dir, rel)); err == nil {
			return true, nil
		} else if !os.IsNotExist(err) {
			return false, errors.WithStack(err)
		}
	}
	return false, nil
}

func writeRaftEngineMarker(path string, engineType RaftEngineType) error {
	if err := os.WriteFile(path, []byte(string(engineType)+"\n"), RaftEngineMarkerPerm); err != nil {
		return errors.WithStack(err)
	}
	return syncDataDir(filepath.Dir(path))
}

func syncDataDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func BuildRuntimeForGroup(
	raftID string,
	group GroupSpec,
	baseDir string,
	multi bool,
	bootstrap bool,
	bootstrapServers []raftengine.Server,
	bootstrapSeed []raftengine.Server,
	st store.MVCCStore,
	sm raftengine.StateMachine,
	factory raftengine.Factory,
	joinAsLearner bool,
) (*GroupRuntime, error) {
	dir := GroupDataDir(baseDir, raftID, group.ID, multi)
	engineType := RaftEngineType(factory.EngineType())
	if err := EnsureRaftEngineDataDirForFactory(dir, engineType, factory); err != nil {
		return nil, err
	}

	result, err := factory.Create(raftengine.FactoryConfig{
		GroupID:       group.ID,
		LocalID:       raftID,
		LocalAddress:  group.Address,
		DataDir:       dir,
		Peers:         bootstrapServers,
		BootstrapSeed: bootstrapSeed,
		Bootstrap:     bootstrap,
		StateMachine:  sm,
		JoinAsLearner: joinAsLearner,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &GroupRuntime{
		Spec:                   group,
		Engine:                 result.Engine,
		Store:                  st,
		StateMachine:           sm,
		RegisterTransport:      result.RegisterTransport,
		TransportServerOptions: result.ServerOptions,
		PublicReady:            result.PublicReady,
		CloseFactory:           result.Close,
	}, nil
}

func EnsureRaftEngineDataDirForFactory(dir string, engineType RaftEngineType, factory raftengine.Factory) error {
	if protected, ok := factory.(interface {
		PreflightRaftDataDir(string, RaftEngineType) error
	}); ok {
		return errors.WithStack(protected.PreflightRaftDataDir(dir, engineType))
	}
	return ensureRaftEngineDataDir(dir, engineType)
}

package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type raftGroupRuntime struct {
	spec   groupSpec
	engine raftengine.Engine
	store  store.MVCCStore

	registerTransport func(grpc.ServiceRegistrar)
	closeFactory      func() error // releases factory-created resources (transport, stores)
}

const raftEngineMarkerPerm = 0o600

type raftEngineType string

const (
	raftEngineHashicorp  raftEngineType = "hashicorp"
	raftEngineEtcd       raftEngineType = "etcd"
	raftEngineMarkerFile                = "raft-engine"
)

var (
	ErrUnsupportedRaftEngine    = errors.New("unsupported raft engine")
	ErrRaftEngineDataDir        = errors.New("raft data dir belongs to a different raft engine")
	ErrMixedRaftEngineArtifacts = errors.New("raft data dir contains artifacts for multiple raft engines")
)

func parseRaftEngineType(raw string) (raftEngineType, error) {
	switch engineType := raftEngineType(strings.ToLower(strings.TrimSpace(raw))); engineType {
	case "", raftEngineHashicorp:
		return raftEngineHashicorp, nil
	case raftEngineEtcd:
		return raftEngineEtcd, nil
	default:
		return "", errors.Wrapf(ErrUnsupportedRaftEngine, "%q", raw)
	}
}

func (r *raftGroupRuntime) Close() {
	if r == nil {
		return
	}
	if r.engine != nil {
		if err := r.engine.Close(); err != nil {
			slog.Warn("failed to close raft engine", "error", err)
		}
		r.engine = nil
	}
	if r.closeFactory != nil {
		if err := r.closeFactory(); err != nil {
			slog.Warn("failed to close factory resources", "error", err)
		}
		r.closeFactory = nil
	}
	if r.store != nil {
		if err := r.store.Close(); err != nil {
			slog.Warn("failed to close store", "error", err)
		}
		r.store = nil
	}
}

func (r *raftGroupRuntime) registerGRPC(server grpc.ServiceRegistrar) {
	if r == nil || r.registerTransport == nil || server == nil {
		return
	}
	r.registerTransport(server)
}

const raftDirPerm = 0o755

func groupDataDir(baseDir, raftID string, groupID uint64, multi bool) string {
	if !multi {
		return filepath.Join(baseDir, raftID)
	}
	return filepath.Join(baseDir, raftID, fmt.Sprintf("group-%d", groupID))
}

func ensureRaftEngineDataDir(dir string, engineType raftEngineType) error {
	if err := os.MkdirAll(dir, raftDirPerm); err != nil {
		return errors.WithStack(err)
	}

	markerPath := filepath.Join(dir, raftEngineMarkerFile)
	if current, ok, err := readRaftEngineMarker(markerPath); err != nil {
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

func readRaftEngineMarker(path string) (raftEngineType, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, errors.WithStack(err)
	}
	engineType, err := parseRaftEngineType(strings.TrimSpace(string(data)))
	if err != nil {
		return "", false, errors.Wrapf(err, "invalid raft engine marker %s", path)
	}
	return engineType, true, nil
}

func detectRaftEngineFromDataDir(dir string) (raftEngineType, bool, error) {
	hashicorpArtifacts, err := hasRaftArtifacts(dir,
		"raft.db",
		"logs.dat",
		"stable.dat",
	)
	if err != nil {
		return "", false, err
	}
	etcdArtifacts, err := hasRaftArtifacts(dir,
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

	switch {
	case hashicorpArtifacts && etcdArtifacts:
		return "", false, errors.Wrapf(ErrMixedRaftEngineArtifacts, "%s", dir)
	case hashicorpArtifacts:
		return raftEngineHashicorp, true, nil
	case etcdArtifacts:
		return raftEngineEtcd, true, nil
	default:
		return "", false, nil
	}
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

func writeRaftEngineMarker(path string, engineType raftEngineType) error {
	if err := os.WriteFile(path, []byte(string(engineType)+"\n"), raftEngineMarkerPerm); err != nil {
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

func bootstrapPeersToServers(bootstrapServers []raft.Server) []raftengine.Server {
	servers := make([]raftengine.Server, 0, len(bootstrapServers))
	for _, s := range bootstrapServers {
		servers = append(servers, raftengine.Server{
			ID:       string(s.ID),
			Address:  string(s.Address),
			Suffrage: "voter",
		})
	}
	return servers
}

func buildRuntimeForGroup(
	raftID string,
	group groupSpec,
	baseDir string,
	multi bool,
	bootstrap bool,
	bootstrapServers []raft.Server,
	st store.MVCCStore,
	sm raftengine.StateMachine,
	factory raftengine.Factory,
) (*raftGroupRuntime, error) {
	dir := groupDataDir(baseDir, raftID, group.id, multi)
	engineType := raftEngineType(factory.EngineType())
	if err := ensureRaftEngineDataDir(dir, engineType); err != nil {
		return nil, err
	}

	result, err := factory.Create(raftengine.FactoryConfig{
		LocalID:      raftID,
		LocalAddress: group.address,
		DataDir:      dir,
		Peers:        bootstrapPeersToServers(bootstrapServers),
		Bootstrap:    bootstrap,
		StateMachine: sm,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &raftGroupRuntime{
		spec:              group,
		engine:            result.Engine,
		store:             st,
		registerTransport: result.RegisterTransport,
		closeFactory:      result.Close,
	}, nil
}

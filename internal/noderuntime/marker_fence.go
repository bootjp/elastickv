package noderuntime

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

const fencedRaftEngineMarker = "etcd+m1b\n"

type RaftEngineMarker struct {
	Engine RaftEngineType
	Fenced bool
}

// ReadRaftEngineMarkerFence accepts both the legacy marker and the M1b fence.
// The legacy engine parser deliberately does not accept the fenced form.
func ReadRaftEngineMarkerFence(path string) (RaftEngineMarker, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return RaftEngineMarker{}, false, nil
		}
		return RaftEngineMarker{}, false, errors.WithStack(err)
	}
	if string(data) == fencedRaftEngineMarker {
		return RaftEngineMarker{Engine: RaftEngineEtcd, Fenced: true}, true, nil
	}
	engineType, err := ParseRaftEngineType(strings.TrimSpace(string(data)))
	if err != nil {
		return RaftEngineMarker{}, false, errors.Wrapf(err, "invalid raft engine marker %s", path)
	}
	return RaftEngineMarker{Engine: engineType}, true, nil
}

// PrepareFreshRaftEngineMarkerFence fences an uninitialized data group before
// its FSM store is opened. Existing Raft or FSM state is never upgraded here.
func PrepareFreshRaftEngineMarkerFence(dir string) error {
	if err := os.MkdirAll(dir, RaftDirPerm); err != nil {
		return errors.WithStack(err)
	}
	path := filepath.Join(dir, RaftEngineMarkerFile)
	if _, err := os.Stat(path); err == nil {
		return nil // The protected startup path owns its different marker format.
	} else if !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	fresh, err := freshRaftEngineDataDir(dir)
	if err != nil || !fresh {
		return err
	}
	return writeNewRaftEngineFence(path)
}

func freshRaftEngineDataDir(dir string) (bool, error) {
	legacy, err := hasHashicorpRaftArtifacts(dir)
	if err != nil {
		return false, err
	}
	if legacy {
		return false, errors.Wrapf(ErrLegacyHashicorpDataDir, "%s", dir)
	}
	if _, present, err := detectRaftEngineFromDataDir(dir); err != nil || present {
		return false, err
	}
	if _, err := os.Stat(filepath.Join(dir, "fsm.db")); err == nil {
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, errors.WithStack(err)
	}
	return true, nil
}

func writeNewRaftEngineFence(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, RaftEngineMarkerPerm)
	if os.IsExist(err) {
		return nil // Another startup owns the marker; later preflight validates it.
	}
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := file.WriteString(fencedRaftEngineMarker); err != nil {
		_ = file.Close()
		return errors.WithStack(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.WithStack(err)
	}
	if err := file.Close(); err != nil {
		return errors.WithStack(err)
	}
	return syncDataDir(filepath.Dir(path))
}

// UpgradeRaftEngineMarkerFence explicitly fences an existing etcd directory.
// Normal startup retains the legacy marker and never calls this function.
func UpgradeRaftEngineMarkerFence(dir string) error {
	path := filepath.Join(dir, RaftEngineMarkerFile)
	marker, ok, err := ReadRaftEngineMarkerFence(path)
	if err != nil {
		return err
	}
	if !ok {
		return errors.Wrapf(os.ErrNotExist, "raft engine marker %s", path)
	}
	if marker.Engine != RaftEngineEtcd {
		return errors.Wrapf(ErrUnsupportedRaftEngine, "%s", marker.Engine)
	}
	if marker.Fenced {
		return nil
	}

	tmp, err := os.CreateTemp(dir, ".raft-engine-*")
	if err != nil {
		return errors.WithStack(err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(fencedRaftEngineMarker); err != nil {
		_ = tmp.Close()
		return errors.WithStack(err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.WithStack(err)
	}
	if err := tmp.Close(); err != nil {
		return errors.WithStack(err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return errors.WithStack(err)
	}
	return syncDataDir(dir)
}

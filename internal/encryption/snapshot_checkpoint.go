package encryption

import (
	"crypto/sha256"
	"encoding/json"
	"maps"

	"github.com/cockroachdb/errors"
)

// SnapshotCheckpoint witnesses the last sidecar-mutating entry represented
// by an FSM snapshot. It is meaningful only inside a verified Raft snapshot.
type SnapshotCheckpoint struct {
	Index uint64
	State [sha256.Size]byte
}

// SidecarCheckpoint excludes node-local nonce epochs and creation times, which
// differ between replicas. Epoch rollback is checked separately against the
// writer registry, both before the startup bump and after replay.
func SidecarCheckpoint(sc *Sidecar) (SnapshotCheckpoint, error) {
	if sc == nil {
		return SnapshotCheckpoint{}, errors.New("encryption snapshot checkpoint: missing sidecar")
	}
	state := *sc
	state.Version = 0
	state.Keys = maps.Clone(sc.Keys)
	for id, key := range state.Keys {
		key.LocalEpoch = 0
		key.Created = ""
		state.Keys[id] = key
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return SnapshotCheckpoint{}, errors.Wrap(err, "encode encryption snapshot checkpoint")
	}
	return SnapshotCheckpoint{Index: sc.RaftAppliedIndex, State: sha256.Sum256(payload)}, nil
}

func (a *Applier) SnapshotCheckpoint() (SnapshotCheckpoint, error) {
	sc, err := ReadSidecar(a.sidecarPath)
	if err != nil {
		return SnapshotCheckpoint{}, err
	}
	return SidecarCheckpoint(sc)
}

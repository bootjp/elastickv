package encryption_test

import (
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
)

// TestStateCacheMirrorsRaftSlotAndSidecarIndex pins the §9.2
// observability mirrors. They are refreshed by the same
// RefreshFromSidecar call that maintains the decision mirrors, so a
// refresh that updated one and not the other would let the metrics
// report stale sidecar state indefinitely.
func TestStateCacheMirrorsRaftSlotAndSidecarIndex(t *testing.T) {
	t.Parallel()

	cache := encryption.NewStateCache()

	// Pre-bootstrap posture.
	id, ok := cache.ActiveRaftKeyID()
	require.Zero(t, id)
	require.False(t, ok)
	require.Zero(t, cache.SidecarRaftAppliedIndex())

	cache.RefreshFromSidecar(&encryption.Sidecar{
		Version:          1,
		RaftAppliedIndex: 9182,
		Active:           encryption.ActiveKeys{Storage: 7, Raft: 8},
	})

	id, ok = cache.ActiveRaftKeyID()
	require.Equal(t, uint32(8), id)
	require.True(t, ok)
	require.Equal(t, uint64(9182), cache.SidecarRaftAppliedIndex())

	// The storage mirror must still track its own slot.
	storageID, ok := cache.ActiveStorageKeyID()
	require.Equal(t, uint32(7), storageID)
	require.True(t, ok)

	// A later refresh must advance the index, not latch it.
	cache.RefreshFromSidecar(&encryption.Sidecar{
		Version:          1,
		RaftAppliedIndex: 9200,
		Active:           encryption.ActiveKeys{Storage: 7, Raft: 8},
	})
	require.Equal(t, uint64(9200), cache.SidecarRaftAppliedIndex())
}

func TestStateCacheObservabilityMirrorsAreNilSafe(t *testing.T) {
	t.Parallel()

	var cache *encryption.StateCache
	id, ok := cache.ActiveRaftKeyID()
	require.Zero(t, id)
	require.False(t, ok)
	require.Zero(t, cache.SidecarRaftAppliedIndex())
}

// TestApplierRefreshesTheCacheOnNoOpSidecarWrites is the regression for
// the metric-lag defect: the idempotency and stale-DEKID branches
// advance and persist sc.RaftAppliedIndex but do no other work, so a
// branch that skipped RefreshFromSidecar left the cache — and the §9.2
// elastickv_encryption_sidecar_raft_index gauge — behind the durable
// sidecar until the next fresh mutation or a restart. That reads as a
// false sidecar-divergence signal.
func TestApplierRefreshesTheCacheOnNoOpSidecarWrites(t *testing.T) {
	t.Parallel()

	const storageDEK = uint32(7)
	const raftDEK = uint32(8)
	nodeID := etcdraftengine.DeriveNodeID("n1")

	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, "keys.json")
	cache := encryption.NewStateCache()
	applier := newObservabilityApplier(t, sidecarPath, cache)

	require.NoError(t, applier.ApplyBootstrap(1, fsmwire.BootstrapPayload{
		StorageDEKID:   storageDEK,
		WrappedStorage: []byte("wrapped-storage-dek"),
		RaftDEKID:      raftDEK,
		WrappedRaft:    []byte("wrapped-raft-dek-distinct"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: storageDEK, FullNodeID: nodeID, LocalEpoch: 0},
		},
	}))
	require.Equal(t, uint64(1), cache.SidecarRaftAppliedIndex())

	// Fresh cutover.
	require.NoError(t, applier.ApplyRotation(2, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                storageDEK,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: storageDEK, FullNodeID: nodeID, LocalEpoch: 1},
	}))
	require.Equal(t, uint64(2), cache.SidecarRaftAppliedIndex())

	// A DUPLICATE cutover entry: the already-active no-op branch. It
	// persists the new applied index, so the cache must follow.
	require.NoError(t, applier.ApplyRotation(3, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                storageDEK,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: storageDEK, FullNodeID: nodeID, LocalEpoch: 2},
	}))
	onDisk, err := encryption.ReadSidecar(sidecarPath)
	require.NoError(t, err)
	require.Equal(t, uint64(3), onDisk.RaftAppliedIndex, "the no-op branch must persist the index")
	require.Equal(t, onDisk.RaftAppliedIndex, cache.SidecarRaftAppliedIndex(),
		"the cache must never lag the durable sidecar after a no-op write")

	// A STALE-DEKID cutover entry: the other no-op branch.
	require.NoError(t, applier.ApplyRotation(4, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                storageDEK + 100, // no longer the active DEK
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: storageDEK + 100, FullNodeID: nodeID, LocalEpoch: 3},
	}))
	onDisk, err = encryption.ReadSidecar(sidecarPath)
	require.NoError(t, err)
	require.Equal(t, uint64(4), onDisk.RaftAppliedIndex)
	require.Equal(t, onDisk.RaftAppliedIndex, cache.SidecarRaftAppliedIndex(),
		"the stale-DEKID no-op branch must refresh the cache too")
}

// newObservabilityApplier builds an applier wired to a real sidecar
// path and the supplied cache, matching the production topology in
// main_encryption_write_wiring.go.
func newObservabilityApplier(
	t *testing.T, sidecarPath string, cache *encryption.StateCache,
) *encryption.Applier {
	t.Helper()
	app, err := encryption.NewApplier(newMapRegistryStore(),
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(encryption.NewKeystore()),
		encryption.WithSidecarPath(sidecarPath),
		encryption.WithStateCache(cache),
	)
	require.NoError(t, err)
	return app
}

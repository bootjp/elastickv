package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/noderuntime"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/store"
)

// TestCheckSidecarBehindRaftLog_NilEngineFailsClosed verifies
// that a present default-group runtime whose engine has not been
// constructed (nil engine field after buildShardGroups) is
// reported as an error rather than silently passing the guard.
// Rationale: at this point in startup the runtime existed but
// the engine opener failed without surfacing an error, so the
// node never finished coming up. Silently returning nil here
// would let the guard pass on a node that cannot serve, defeating
// the §9.1 fail-closed contract.
func TestCheckSidecarBehindRaftLog_NilEngineFailsClosed(t *testing.T) {
	t.Parallel()
	sidecarPath := writeMinimalSidecar(t, 10)
	rt := &raftGroupRuntime{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 1}}} // engine field stays nil
	err := encryptionwiring.CheckSidecarBehindRaftLog(runtimeCores([]*raftGroupRuntime{rt}), 1, sidecarPath, true)
	if err == nil {
		t.Fatal("guard must fail-closed when default-group engine is nil")
	}
	if errors.Is(err, encryption.ErrSidecarBehindRaftLog) {
		t.Errorf("nil-engine must NOT surface as ErrSidecarBehindRaftLog; got %v", err)
	}
}

func TestCheckEnvelopeCutoverDivergenceStartupGuard_Fires(t *testing.T) {
	t.Parallel()
	sidecarPath := writeRaftCutoverSidecarForStartup(t, 8, 3, 100)
	rt := &raftGroupRuntime{
		GroupRuntime: &noderuntime.GroupRuntime{
			Spec:         noderuntime.GroupSpec{ID: 1},
			StateMachine: &stubRestoredCutoverStateMachine{cutover: 200},
		},
	}
	err := encryptionwiring.CheckEnvelopeCutoverDivergenceStartupGuard(runtimeCores([]*raftGroupRuntime{rt}), 1, sidecarPath, true)
	if !errors.Is(err, encryption.ErrEnvelopeCutoverDivergence) {
		t.Fatalf("cutover guard must fire ErrEnvelopeCutoverDivergence, got %v", err)
	}
}

func TestCheckEnvelopeCutoverDivergenceStartupGuard_Match(t *testing.T) {
	t.Parallel()
	sidecarPath := writeRaftCutoverSidecarForStartup(t, 8, 3, 100)
	rt := &raftGroupRuntime{
		GroupRuntime: &noderuntime.GroupRuntime{
			Spec:         noderuntime.GroupSpec{ID: 1},
			StateMachine: &stubRestoredCutoverStateMachine{cutover: 100},
		},
	}
	if err := encryptionwiring.CheckEnvelopeCutoverDivergenceStartupGuard(runtimeCores([]*raftGroupRuntime{rt}), 1, sidecarPath, true); err != nil {
		t.Fatalf("matching snapshot/sidecar cutover must pass, got %v", err)
	}
}

func TestCheckEnvelopeCutoverDivergenceStartupGuard_PreBootstrapSkips(t *testing.T) {
	t.Parallel()
	sidecarPath := writeMinimalSidecar(t, 1)
	rt := &raftGroupRuntime{
		GroupRuntime: &noderuntime.GroupRuntime{
			Spec:         noderuntime.GroupSpec{ID: 1},
			StateMachine: &stubRestoredCutoverStateMachine{cutover: 0},
		},
	}
	if err := encryptionwiring.CheckEnvelopeCutoverDivergenceStartupGuard(runtimeCores([]*raftGroupRuntime{rt}), 1, sidecarPath, true); err != nil {
		t.Fatalf("pre-bootstrap sidecar must skip cutover divergence guard, got %v", err)
	}
}

func TestCheckEncryptionMembershipStartupGuardsBeforeEngine_LocalEpochEqualAllowedBeforeBump(t *testing.T) {
	t.Parallel()
	sidecarPath := writeActiveStorageSidecarForStartup(t, 2)
	raftDir := t.TempDir()
	storePath := filepath.Join(noderuntime.GroupDataDir(raftDir, "n1", 1, false), "fsm.db")
	if err := os.MkdirAll(filepath.Dir(storePath), dirPerm); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	st, err := store.NewPebbleStore(storePath)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	writeRegistryRow(t, st, fullNodeID, 2)
	if err := st.Close(); err != nil {
		t.Fatalf("Close registry fixture store: %v", err)
	}
	err = encryptionwiring.CheckMembershipStartupGuardsBeforeEngine(encryptionwiring.MembershipStartupGuardInput{
		RaftID:            "n1",
		RaftDir:           raftDir,
		Groups:            []noderuntime.GroupSpec{{ID: 1}},
		DefaultGroup:      1,
		SidecarPath:       sidecarPath,
		EncryptionEnabled: true,
	})
	if err != nil {
		t.Fatalf("pre-bump guard must allow sidecar==registry; got %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar after guard: %v", err)
	}
	if got := sc.Keys["7"].LocalEpoch; got != 2 {
		t.Fatalf("pre-engine guard must not bump sidecar local_epoch; got %d", got)
	}
}

func TestCheckEncryptionMembershipStartupGuardsBeforeEngine_LocalEpochBehindRollback(t *testing.T) {
	t.Parallel()
	sidecarPath := writeActiveStorageSidecarForStartup(t, 1)
	raftDir := t.TempDir()
	storePath := filepath.Join(noderuntime.GroupDataDir(raftDir, "n1", 1, false), "fsm.db")
	if err := os.MkdirAll(filepath.Dir(storePath), dirPerm); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	st, err := store.NewPebbleStore(storePath)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	writeRegistryRow(t, st, fullNodeID, 2)
	if err := st.Close(); err != nil {
		t.Fatalf("Close registry fixture store: %v", err)
	}
	err = encryptionwiring.CheckMembershipStartupGuardsBeforeEngine(encryptionwiring.MembershipStartupGuardInput{
		RaftID:            "n1",
		RaftDir:           raftDir,
		Groups:            []noderuntime.GroupSpec{{ID: 1}},
		DefaultGroup:      1,
		SidecarPath:       sidecarPath,
		EncryptionEnabled: true,
	})
	if !errors.Is(err, encryption.ErrLocalEpochRollback) {
		t.Fatalf("membership guard must fire ErrLocalEpochRollback when sidecar<registry, got %v", err)
	}
}

func TestCheckLocalEpochRollbackAfterReplay_RejectsEqualEpochAfterBump(t *testing.T) {
	t.Parallel()
	sidecarPath := writeActiveStorageSidecarForStartup(t, 2)
	st, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "fsm.db"))
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("Close registry fixture store: %v", err)
		}
	})
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	writeRegistryRow(t, st, fullNodeID, 2)

	err = encryptionwiring.CheckLocalEpochRollbackAfterReplay(
		"n1",
		runtimeCores([]*raftGroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 1}, Store: st}}}),
		1,
		sidecarPath,
		true,
	)
	if !errors.Is(err, encryption.ErrLocalEpochRollback) {
		t.Fatalf("post-replay guard must reject sidecar==registry after bump, got %v", err)
	}
}

func TestCheckLocalEpochRollbackAfterReplay_AllowsBumpedAheadEpoch(t *testing.T) {
	t.Parallel()
	sidecarPath := writeActiveStorageSidecarForStartup(t, 3)
	st, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "fsm.db"))
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("Close registry fixture store: %v", err)
		}
	})
	fullNodeID := etcdraftengine.DeriveNodeID("n1")
	writeRegistryRow(t, st, fullNodeID, 2)

	err = encryptionwiring.CheckLocalEpochRollbackAfterReplay(
		"n1",
		runtimeCores([]*raftGroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{Spec: noderuntime.GroupSpec{ID: 1}, Store: st}}}),
		1,
		sidecarPath,
		true,
	)
	if err != nil {
		t.Fatalf("post-replay guard must allow sidecar>registry after bump, got %v", err)
	}
}

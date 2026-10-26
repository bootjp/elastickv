package kv

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

const multiGroupCrashDirectory = "ELASTICKV_MULTIGROUP_CRASH_DIRECTORY"

type killAfterPrimaryCommit struct {
	Transactional
	marker string
}

func (t killAfterPrimaryCommit) Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	response, err := t.Transactional.Commit(ctx, reqs)
	if err == nil && len(reqs) == 1 && reqs[0].Phase == pb.Phase_COMMIT {
		marker, openErr := os.OpenFile(t.marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if openErr != nil {
			return nil, openErr
		}
		if _, writeErr := marker.Write([]byte("primary committed")); writeErr != nil {
			_ = marker.Close()
			return nil, writeErr
		}
		if syncErr := marker.Sync(); syncErr != nil {
			_ = marker.Close()
			return nil, syncErr
		}
		if closeErr := marker.Close(); closeErr != nil {
			return nil, closeErr
		}
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	}
	return response, err
}

func openCrashTestGroups(t *testing.T, dir string, bootstrap bool) (*LockResolver, *ShardStore, map[uint64]*ShardGroup, func()) {
	t.Helper()
	router := distribution.NewEngine()
	router.UpdateRoute([]byte("a"), []byte("m"), 1)
	router.UpdateRoute([]byte("m"), nil, 2)
	groups := make(map[uint64]*ShardGroup, 2)
	var closeGroups []func()
	for _, gid := range []uint64{1, 2} {
		dataDir := filepath.Join(dir, fmt.Sprintf("raft-%d", gid))
		storeDir := filepath.Join(dir, fmt.Sprintf("store-%d", gid))
		st, err := store.NewPebbleStore(storeDir)
		require.NoError(t, err)
		id := fmt.Sprintf("crash-g%d", gid)
		result, err := newSingleRaftFactory().Create(raftengine.FactoryConfig{
			LocalID: id, LocalAddress: id, DataDir: dataDir, Bootstrap: bootstrap,
			StateMachine: NewKvFSMWithHLC(st, NewHLC()),
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool { return result.Engine.State() == raftengine.StateLeader },
			5*time.Second, 10*time.Millisecond)
		groups[gid] = &ShardGroup{Engine: result.Engine, Store: st, Txn: NewLeaderProxyWithEngine(result.Engine)}
		closeGroups = append(closeGroups, func() {
			require.NoError(t, result.Engine.Close())
			if result.Close != nil {
				require.NoError(t, result.Close())
			}
			require.NoError(t, st.Close())
		})
	}
	shards := NewShardStore(router, groups)
	resolver := NewLockResolver(shards, groups, nil)
	return resolver, shards, groups, func() {
		resolver.Close()
		for _, closeGroup := range closeGroups {
			closeGroup()
		}
	}
}

func TestMultiGroupPrimaryDecisionSurvivesProcessKill(t *testing.T) {
	if dir := os.Getenv(multiGroupCrashDirectory); dir != "" {
		_, shards, groups, _ := openCrashTestGroups(t, dir, true)
		groups[1].Txn = killAfterPrimaryCommit{Transactional: groups[1].Txn, marker: filepath.Join(dir, "primary-committed")}
		coord := NewShardedCoordinator(shards.engine, groups, 1, NewHLC(), shards)
		_, err := coord.dispatchMultiShardTxn(context.Background(), 10, 20, 0, []byte("b"),
			map[uint64][]*pb.Mutation{
				1: {{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("primary")}},
				2: {{Op: pb.Op_PUT, Key: []byte("x"), Value: []byte("secondary")}},
			}, []uint64{1, 2}, nil, 0, nil, true)
		require.NoError(t, err, "the primary-commit crash hook was not reached")
		return
	}

	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMultiGroupPrimaryDecisionSurvivesProcessKill$")
	cmd.Env = append(os.Environ(), multiGroupCrashDirectory+"="+dir)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok, string(output))
	require.Equal(t, syscall.SIGKILL, status.Signal(), string(output))
	require.FileExists(t, filepath.Join(dir, "primary-committed"))

	resolver, shards, groups, cleanup := openCrashTestGroups(t, dir, false)
	defer cleanup()
	ctx = context.Background()
	primary, err := shards.GetAt(ctx, []byte("b"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("primary"), primary)
	_, err = groups[2].Store.GetAt(ctx, []byte("x"), 20)
	require.ErrorIs(t, err, store.ErrKeyNotFound,
		"the secondary must still be prepared before recovery resolves it")
	rawLock, err := groups[2].Store.GetAt(ctx, txnLockKey([]byte("x")), ^uint64(0))
	require.NoError(t, err)
	lock, err := decodeTxnLock(rawLock)
	require.NoError(t, err)
	require.NoError(t, resolver.resolveExpiredLock(ctx, groups[2], []byte("x"), lock))
	secondary, err := shards.GetAt(ctx, []byte("x"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("secondary"), secondary)
}

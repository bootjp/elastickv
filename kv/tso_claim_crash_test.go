package kv

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/stretchr/testify/require"
)

const (
	tsoClaimCrashEnv   = "ELASTICKV_TEST_TSO_CLAIM_CRASH_DIR"
	tsoClaimCrashReady = "TSO_CLAIM_APPLIED"
)

func openTSOClaimCrashNode(t *testing.T, dir string) (*RaftTSOAllocator, raftengine.Engine) {
	t.Helper()
	clock := NewHLC()
	state := NewTSOStateMachine(clock)
	result, err := newSingleRaftFactory().Create(raftengine.FactoryConfig{
		LocalID: "n1", LocalAddress: "127.0.0.1:0", DataDir: dir,
		Bootstrap: true, StateMachine: state,
	})
	require.NoError(t, err)
	raftenginetest.WaitForLeader(t, 5*time.Second, result.Engine)
	allocator, err := newTestRaftTSOAllocator(&ShardGroup{Engine: result.Engine, TSOState: state}, clock)
	require.NoError(t, err)
	return allocator, result.Engine
}

// Only the subprocess enters the helper. The parent kills it after the applied
// acknowledgment, without calling Engine.Close or taking a graceful snapshot.
func TestTSOClaimCrashHelper(t *testing.T) {
	dir := os.Getenv(tsoClaimCrashEnv)
	if dir == "" {
		return
	}
	allocator, engine := openTSOClaimCrashNode(t, dir)
	for _, payload := range [][]byte{
		marshalHLCLeaseRenew(time.Now().Add(testTSOFutureCeiling).UnixMilli()),
		marshalTSOCutover(), marshalTSOPhaseD(1),
	} {
		require.NoError(t, proposeTSOClaim(t.Context(), engine, payload))
	}
	g, err := allocator.reserveClaimGrant(t.Context(), TSOClaimOwner{1}, 16, 0)
	require.NoError(t, err)
	p := claimTestRecord(g, 10, 0)
	require.NoError(t, allocator.commitClaimBatch(t.Context(), g.Term, []TSOClaimRecord{p.Record}))
	fmt.Println(tsoClaimCrashReady)
	<-t.Context().Done()
}

func TestTSOClaimNativeProcessCrashPreservesAppliedClaim(t *testing.T) {
	dir := t.TempDir()
	binary, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-test.run=^TestTSOClaimCrashHelper$", "-test.timeout=15s")
	command.Env = append(os.Environ(), tsoClaimCrashEnv+"="+dir)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, command.Start())
	scanner := bufio.NewScanner(stdout)
	ready := false
	for scanner.Scan() {
		if scanner.Text() == tsoClaimCrashReady {
			ready = true
			break
		}
	}
	killErr := command.Process.Kill()
	waitErr := command.Wait()
	require.True(t, ready, "child failed before applied acknowledgment: %s", stderr.String())
	require.NoError(t, killErr)
	require.Error(t, waitErr, "child must not have exited gracefully")
	require.NoError(t, scanner.Err())
	require.NoError(t, ctx.Err(), "crash test must finish before the timeout")
	allocator, engine := openTSOClaimCrashNode(t, dir)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	credential := TSOClaimCredential{ID: TSOClaimID{10}, Secret: [32]byte{10}}
	proof, err := allocator.reconcileClaim(t.Context(), credential, claimTestBinding(10))
	require.NoError(t, err)
	require.NoError(t, allocator.validateTimestampClaim(t.Context(), proof, proof.Record.Binding, TSOClaimResolution))
	g, err := allocator.reserveClaimGrant(t.Context(), proof.Record.Binding.Owner, 16, 0)
	require.NoError(t, err)
	oldUnclaimed := claimTestRecord(TSOClaimGrant{
		ID: proof.Record.Grant, Owner: proof.Record.Binding.Owner, Base: proof.Record.Timestamp, Count: 16,
	}, 11, 1)
	require.ErrorIs(t, allocator.commitClaimBatch(t.Context(), g.Term, []TSOClaimRecord{oldUnclaimed.Record}), ErrTSOClaimClosed)
}

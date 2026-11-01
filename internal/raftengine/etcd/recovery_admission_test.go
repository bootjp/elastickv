package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func recoveryWALConfig(t *testing.T, bound bool) OpenConfig {
	t.Helper()
	cfg := OpenConfig{NodeID: 7, LocalID: "n7", LocalAddress: "127.0.0.1:7007",
		DataDir: t.TempDir(), Bootstrap: true, StateMachine: &testStateMachine{}}
	if bound {
		cfg.NativeIdentity = identityFixture()
	}
	engine, err := Open(context.Background(), cfg)
	require.NoError(t, err)
	_, err = engine.Propose(context.Background(), []byte("durable"))
	require.NoError(t, err)
	require.NoError(t, engine.Close())
	cfg.Bootstrap = false
	cfg.StateMachine = &testStateMachine{}
	return cfg
}

func TestRecoveredAdmissionRealWALState(t *testing.T) {
	cfg := recoveryWALConfig(t, true)
	var captured RecoveredState
	calls := 0
	cfg.RecoveredAdmission = func(state RecoveredState) error {
		captured = state
		calls++
		return nil
	}
	engine, err := Open(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.Equal(t, 1, calls)
	id, ok := captured.Identity()
	require.True(t, ok)
	require.Equal(t, *cfg.NativeIdentity, id)
	require.NotZero(t, captured.HardState().GetTerm())
	require.GreaterOrEqual(t, captured.AppliedIndex(), captured.HardState().GetCommit())
	require.GreaterOrEqual(t, captured.AppliedIndex(), captured.ConfigurationIndex())
	require.Equal(t, []uint64{7}, captured.ConfState().GetVoters())
	stateMachine, ok := cfg.StateMachine.(*testStateMachine)
	require.True(t, ok)
	require.Equal(t, [][]byte{[]byte("durable")}, stateMachine.Applied())
	originalHS, originalCS, originalPeers := captured.HardState(), captured.ConfState(), captured.Peers()
	captured.HardState().Term = new(uint64)
	captured.ConfState().Voters[0] = 99
	peers := captured.Peers()
	require.NotEmpty(t, peers)
	peers[0].Address = "changed"
	id.NodeID++
	require.Equal(t, originalHS, captured.HardState())
	require.Equal(t, originalCS, captured.ConfState())
	require.Equal(t, originalPeers, captured.Peers())
	actualID, _ := captured.Identity()
	require.Equal(t, *cfg.NativeIdentity, actualID)
	configuration, err := engine.Configuration(context.Background())
	require.NoError(t, err)
	require.Len(t, configuration.Servers, 1)
	require.Equal(t, "n7", configuration.Servers[0].ID)
	require.Equal(t, "127.0.0.1:7007", configuration.Servers[0].Address)
}

func TestRecoveredAdmissionBlocksOpenAndPropagatesFailure(t *testing.T) {
	cfg := recoveryWALConfig(t, true)
	entered, release := make(chan struct{}), make(chan struct{})
	sentinel := errors.New("admission denied")
	cfg.RecoveredAdmission = func(RecoveredState) error {
		close(entered)
		<-release
		return sentinel
	}
	type result struct {
		engine *Engine
		err    error
	}
	returned := make(chan result, 1)
	go func() { e, err := Open(context.Background(), cfg); returned <- result{e, err} }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("hook not reached")
	}
	select {
	case r := <-returned:
		close(release)
		if r.engine != nil {
			_ = r.engine.Close()
		}
		t.Fatal("Open published engine before hook completed")
	default:
	}
	close(release)
	select {
	case r := <-returned:
		require.ErrorIs(t, r.err, sentinel)
		require.Nil(t, r.engine)
	case <-time.After(10 * time.Second):
		t.Fatal("Open did not propagate hook failure")
	}
	// Failure must release the real WAL so a corrected admission can retry.
	cfg.RecoveredAdmission = func(RecoveredState) error { return nil }
	cfg.StateMachine = &testStateMachine{}
	e, err := Open(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, e.Close())
}

func TestRecoveredAdmissionRequiresBoundWALAndLegacyRemainsCompatible(t *testing.T) {
	cfg := recoveryWALConfig(t, false)
	called := false
	cfg.RecoveredAdmission = func(RecoveredState) error { called = true; return nil }
	e, err := Open(context.Background(), cfg)
	require.ErrorIs(t, err, ErrWALIdentity)
	require.Nil(t, e)
	require.False(t, called)
	cfg.RecoveredAdmission = nil
	e, err = Open(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, e.Close())
}

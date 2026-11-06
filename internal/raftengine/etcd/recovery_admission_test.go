package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// Configuration proof is captured by the ordered hook, never inferred from
// the snapshot index. Snapshot and Restore carry it alongside application data.
type recoveryProofFSM struct {
	testStateMachine
	proofMu sync.Mutex
	index   uint64
	members []uint64
}

func (f *recoveryProofFSM) RecordAppliedNamespaceConfiguration(index uint64, members []uint64) error {
	f.proofMu.Lock()
	defer f.proofMu.Unlock()
	if index > f.index {
		f.index, f.members = index, slices.Clone(members)
	}
	return nil
}

func (f *recoveryProofFSM) RecordAppliedConfiguration(uint64) error {
	return errors.New("fixture requires full ordered configuration hook")
}

func (f *recoveryProofFSM) AppliedConfigurationIndex() uint64 {
	f.proofMu.Lock()
	defer f.proofMu.Unlock()
	return f.index
}

func (f *recoveryProofFSM) RecoverNamespaceConfiguration(snapshotIndex uint64, members []uint64) error {
	f.proofMu.Lock()
	defer f.proofMu.Unlock()
	if f.index == 0 || f.index > snapshotIndex || !slices.Equal(f.members, members) {
		return errors.New("fixture configuration proof mismatch")
	}
	return nil
}

type recoveryProofSnapshot struct{ data []byte }

func (s *recoveryProofSnapshot) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(s.data)
	return int64(n), err
}
func (*recoveryProofSnapshot) Close() error { return nil }

type recoveryProofRecord struct {
	Index   uint64
	Members []uint64
	Data    []byte
}

func (f *recoveryProofFSM) Snapshot() (raftengine.Snapshot, error) {
	f.proofMu.Lock()
	defer f.proofMu.Unlock()
	s, err := f.testStateMachine.Snapshot()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	var b bytes.Buffer
	if _, err := s.WriteTo(&b); err != nil {
		return nil, err
	}
	data, err := json.Marshal(recoveryProofRecord{Index: f.index, Members: f.members, Data: b.Bytes()})
	return &recoveryProofSnapshot{data: data}, err
}

func (f *recoveryProofFSM) Restore(r io.Reader) error {
	var record recoveryProofRecord
	if err := json.NewDecoder(r).Decode(&record); err != nil {
		return err
	}
	f.proofMu.Lock()
	defer f.proofMu.Unlock()
	if err := f.testStateMachine.Restore(bytes.NewReader(record.Data)); err != nil {
		return err
	}
	f.index, f.members = record.Index, slices.Clone(record.Members)
	return nil
}

func recoveryWALConfig(t *testing.T, bound bool) (OpenConfig, uint64) {
	t.Helper()
	t.Setenv(snapshotEveryEnvVar, "1")
	cfg := OpenConfig{NodeID: 7, LocalID: "n7", LocalAddress: "127.0.0.1:7007",
		DataDir: t.TempDir(), Bootstrap: true, StateMachine: &recoveryProofFSM{}}
	if bound {
		cfg.NativeIdentity = identityFixture()
		cfg.GroupID = cfg.NativeIdentity.Identity.Group
		cfg.NativeAdmission = fixtureNativeAdmission{}
	}
	engine, err := Open(context.Background(), cfg)
	require.NoError(t, err)
	_, err = engine.Propose(context.Background(), []byte("durable"))
	require.NoError(t, err)
	fsm, ok := cfg.StateMachine.(*recoveryProofFSM)
	require.True(t, ok)
	n := fsm.AppliedConfigurationIndex()
	require.NotZero(t, n)
	require.Eventually(t, func() bool { return engine.Status().LastSnapshotIndex > n }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, engine.Close())
	cfg.Bootstrap = false
	cfg.StateMachine = &recoveryProofFSM{}
	return cfg, n
}

func TestRecoveredAdmissionRealWALState(t *testing.T) {
	cfg, expectedIndex := recoveryWALConfig(t, true)
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
	stateMachine, ok := cfg.StateMachine.(*recoveryProofFSM)
	require.True(t, ok)
	require.Equal(t, [][]byte{[]byte("durable")}, stateMachine.Applied())
	require.Equal(t, stateMachine.AppliedConfigurationIndex(), captured.ConfigurationIndex())
	require.NotZero(t, captured.ConfigurationIndex())
	require.Equal(t, expectedIndex, captured.ConfigurationIndex())
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
	cfg, expectedIndex := recoveryWALConfig(t, true)
	entered, release := make(chan struct{}), make(chan struct{})
	sentinel := errors.New("admission denied")
	cfg.RecoveredAdmission = func(state RecoveredState) error {
		require.Equal(t, expectedIndex, state.ConfigurationIndex())
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
	cfg.StateMachine = &recoveryProofFSM{}
	e, err := Open(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, e.Close())
}

func TestRecoveredAdmissionRequiresBoundWALAndLegacyRemainsCompatible(t *testing.T) {
	cfg, _ := recoveryWALConfig(t, false)
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

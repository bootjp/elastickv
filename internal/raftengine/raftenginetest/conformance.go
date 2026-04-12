// Package raftenginetest provides a shared conformance test suite for
// raftengine.Engine implementations. Each engine package should call
// RunConformanceSuite with its own Factory in a _test.go file.
package raftenginetest

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// TestStateMachine is a simple in-memory state machine for testing. It
// records all applied entries and supports snapshot/restore via a binary
// encoding.
type TestStateMachine struct {
	mu      sync.Mutex
	applied [][]byte
}

func (s *TestStateMachine) Apply(data []byte) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, append([]byte(nil), data...))
	return string(data)
}

func (s *TestStateMachine) Applied() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.applied))
	for i, item := range s.applied {
		out[i] = append([]byte(nil), item...)
	}
	return out
}

func (s *TestStateMachine) Snapshot() (raftengine.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	//nolint:gosec // length is bounded by memory
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(s.applied))); err != nil {
		return nil, err
	}
	for _, item := range s.applied {
		//nolint:gosec // length is bounded by memory
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(item))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(item); err != nil {
			return nil, err
		}
	}
	return &testSnapshot{data: buf.Bytes()}, nil
}

func (s *TestStateMachine) Restore(r io.Reader) error {
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return err
	}

	applied := make([][]byte, 0, count)
	for range count {
		var length uint32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return err
		}
		item := make([]byte, length)
		if _, err := io.ReadFull(r, item); err != nil {
			return err
		}
		applied = append(applied, item)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = applied
	return nil
}

type testSnapshot struct {
	data []byte
}

func (s *testSnapshot) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(s.data)
	return int64(n), err
}

func (s *testSnapshot) Close() error {
	return nil
}

const (
	leaderElectionTimeout  = 5 * time.Second
	leaderElectionInterval = 10 * time.Millisecond
)

// RunConformanceSuite runs the shared conformance tests against the given
// factory. The factory should create single-node, self-bootstrapping engines.
func RunConformanceSuite(t *testing.T, factory raftengine.Factory) {
	t.Run("SingleNodeProposeAndRead", func(t *testing.T) {
		testSingleNodeProposeAndRead(t, factory)
	})
	t.Run("StatusReporting", func(t *testing.T) {
		testStatusReporting(t, factory)
	})
	t.Run("Configuration", func(t *testing.T) {
		testConfiguration(t, factory)
	})
	t.Run("ProposeContextCancellation", func(t *testing.T) {
		testProposeContextCancellation(t, factory)
	})
	t.Run("CloseAndReopen", func(t *testing.T) {
		testCloseAndReopen(t, factory)
	})
}

func createSingleNode(t *testing.T, factory raftengine.Factory, dir string, sm *TestStateMachine) *raftengine.FactoryResult {
	t.Helper()
	result, err := factory.Create(raftengine.FactoryConfig{
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:0",
		DataDir:      dir,
		Bootstrap:    true,
		StateMachine: sm,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = result.Engine.Close()
		if result.Close != nil {
			result.Close()
		}
	})

	// Wait for leader election
	require.Eventually(t, func() bool {
		return result.Engine.State() == raftengine.StateLeader
	}, leaderElectionTimeout, leaderElectionInterval, "engine did not become leader")

	return result
}

func testSingleNodeProposeAndRead(t *testing.T, factory raftengine.Factory) {
	sm := &TestStateMachine{}
	result := createSingleNode(t, factory, t.TempDir(), sm)

	ctx := context.Background()
	pr, err := result.Engine.Propose(ctx, []byte("hello"))
	require.NoError(t, err)
	require.NotNil(t, pr)
	require.NotZero(t, pr.CommitIndex)
	require.Equal(t, "hello", pr.Response)

	readIdx, err := result.Engine.LinearizableRead(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, readIdx, pr.CommitIndex)

	applied := sm.Applied()
	require.Len(t, applied, 1)
	require.Equal(t, []byte("hello"), applied[0])
}

func testStatusReporting(t *testing.T, factory raftengine.Factory) {
	sm := &TestStateMachine{}
	result := createSingleNode(t, factory, t.TempDir(), sm)

	ctx := context.Background()
	_, err := result.Engine.Propose(ctx, []byte("status-test"))
	require.NoError(t, err)

	status := result.Engine.Status()
	require.Equal(t, raftengine.StateLeader, status.State)
	require.NotZero(t, status.CommitIndex)
	require.NotZero(t, status.AppliedIndex)
	require.Equal(t, "n1", status.Leader.ID)
}

func testConfiguration(t *testing.T, factory raftengine.Factory) {
	sm := &TestStateMachine{}
	result := createSingleNode(t, factory, t.TempDir(), sm)

	cfg, err := result.Engine.Configuration(context.Background())
	require.NoError(t, err)
	require.Len(t, cfg.Servers, 1)
	require.Equal(t, "n1", cfg.Servers[0].ID)
}

func testProposeContextCancellation(t *testing.T, factory raftengine.Factory) {
	sm := &TestStateMachine{}
	result := createSingleNode(t, factory, t.TempDir(), sm)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := result.Engine.Propose(ctx, []byte("should-fail"))
	require.Error(t, err)
}

func testCloseAndReopen(t *testing.T, factory raftengine.Factory) {
	sm := &TestStateMachine{}
	dir := t.TempDir()

	// First open: propose data
	result1, err := factory.Create(raftengine.FactoryConfig{
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:0",
		DataDir:      dir,
		Bootstrap:    true,
		StateMachine: sm,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return result1.Engine.State() == raftengine.StateLeader
	}, leaderElectionTimeout, leaderElectionInterval)

	_, err = result1.Engine.Propose(context.Background(), []byte("persist-me"))
	require.NoError(t, err)

	_ = result1.Engine.Close()
	if result1.Close != nil {
		result1.Close()
	}

	// Second open: verify data persisted
	sm2 := &TestStateMachine{}
	result2, err := factory.Create(raftengine.FactoryConfig{
		LocalID:      "n1",
		LocalAddress: "127.0.0.1:0",
		DataDir:      dir,
		StateMachine: sm2,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = result2.Engine.Close()
		if result2.Close != nil {
			result2.Close()
		}
	})

	require.Eventually(t, func() bool {
		return result2.Engine.State() == raftengine.StateLeader
	}, leaderElectionTimeout, leaderElectionInterval)

	// Wait for the FSM to replay the log
	require.Eventually(t, func() bool {
		return len(sm2.Applied()) >= 1
	}, leaderElectionTimeout, leaderElectionInterval)

	applied := sm2.Applied()
	require.Equal(t, []byte("persist-me"), applied[len(applied)-1])
}

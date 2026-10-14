package kv

import (
	"context"
	"os"
	"os/exec"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// These tests pin audit gap G17: a batched Raft entry (several raw requests
// in one RaftCommand, the shape the raw batcher produces) must land on a
// Pebble store as one unit. Before the fix each request committed its own
// batch and the first one already persisted the entry's applied index, so a
// crash between two requests left a replica whose durable applied index
// named an entry it only half applied; on restart the engine's cold-start
// duplicate skip dropped the entry without calling the FSM and the tail was
// never applied on that replica.

const (
	g17CrashChildEnv  = "ELASTICKV_TEST_G17_CRASH_DIR"
	g17CrashExitCode  = 3
	g17EntryIndex     = uint64(5)
	g17FirstRequestTS = uint64(10)
)

// g17Store wraps a store so a test can observe, or abort, the moment the
// FSM hands the second request of an entry to the store.
type g17Store struct {
	store.MVCCStore
	raftApplyCalls        int
	beforeSecondRaftApply func()
}

func (s *g17Store) ApplyMutationsRaftAt(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS, appliedIndex uint64) error {
	s.raftApplyCalls++
	if s.raftApplyCalls == 2 && s.beforeSecondRaftApply != nil {
		s.beforeSecondRaftApply()
	}
	return s.MVCCStore.ApplyMutationsRaftAt(ctx, mutations, readKeys, startTS, commitTS, appliedIndex)
}

func (s *g17Store) ApplyRaftEntry(ctx context.Context, appliedIndex uint64, fn func(ctx context.Context) error) error {
	applier, ok := s.MVCCStore.(store.RaftEntryApplier)
	if !ok {
		return fn(ctx)
	}
	return applier.ApplyRaftEntry(ctx, appliedIndex, fn)
}

type g17AppliedIndexReader interface {
	LastAppliedIndex() (uint64, bool, error)
}

func g17RawPut(ts uint64, key, value string) *pb.Request {
	return &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Ts:    ts,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(key), Value: []byte(value)},
		},
	}
}

func g17RawDelPrefix(ts uint64, prefix string) *pb.Request {
	return &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Ts:    ts,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_DEL_PREFIX, Key: []byte(prefix)},
		},
	}
}

func g17Entry(t *testing.T, reqs ...*pb.Request) []byte {
	t.Helper()
	data, err := marshalRaftCommand(reqs)
	require.NoError(t, err)
	return data
}

func g17TwoPutEntry(t *testing.T) []byte {
	t.Helper()
	return g17Entry(t,
		g17RawPut(g17FirstRequestTS, "k1", "v1"),
		g17RawPut(g17FirstRequestTS+1, "k2", "v2"),
	)
}

func g17OpenStore(t *testing.T, dir string) store.MVCCStore {
	t.Helper()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func g17Exists(t *testing.T, st store.MVCCStore, key string) bool {
	t.Helper()
	ok, err := st.ExistsAt(context.Background(), []byte(key), ^uint64(0))
	require.NoError(t, err)
	return ok
}

func g17AppliedIndex(t *testing.T, st store.MVCCStore) (uint64, bool) {
	t.Helper()
	reader, ok := st.(g17AppliedIndexReader)
	require.True(t, ok, "%T must expose LastAppliedIndex", st)
	idx, present, err := reader.LastAppliedIndex()
	require.NoError(t, err)
	return idx, present
}

func g17FSM(t *testing.T, st store.MVCCStore, opts ...FSMOption) *kvFSM {
	t.Helper()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC(), opts...).(*kvFSM)
	require.True(t, ok)
	fsm.SetApplyIndex(g17EntryIndex)
	return fsm
}

// TestFSMApplyBatch_EntryIsNotDurableUntilItCommits observes the store's
// durable state at the moment the second request of a two-request entry
// reaches it: the first request's write and the entry's applied index must
// not be durable yet. Red before the fix (the first request had already
// committed its own batch carrying the applied index).
func TestFSMApplyBatch_EntryIsNotDurableUntilItCommits(t *testing.T) {
	st := g17OpenStore(t, t.TempDir())
	wrapped := &g17Store{MVCCStore: st}
	var (
		midIdx     uint64
		midPresent bool
		midK1      bool
	)
	wrapped.beforeSecondRaftApply = func() {
		midIdx, midPresent = g17AppliedIndex(t, st)
		midK1 = g17Exists(t, st, "k1")
	}
	fsm := g17FSM(t, wrapped)

	require.Nil(t, fsm.Apply(g17TwoPutEntry(t)))

	require.Equal(t, 2, wrapped.raftApplyCalls)
	require.False(t, midPresent, "the applied index must not name the entry while it is half applied (G17); got %d", midIdx)
	require.False(t, midK1, "the first request must not be durable while the entry is half applied (G17)")
	idx, present := g17AppliedIndex(t, st)
	require.True(t, present)
	require.Equal(t, g17EntryIndex, idx)
	require.True(t, g17Exists(t, st, "k1"))
	require.True(t, g17Exists(t, st, "k2"))
}

// TestFSMApplyBatch_CrashBetweenRequestsLeavesNoPartialEntry is the crash
// test the audit asks for. A child process applies a two-request entry and
// exits, without closing the store, the moment the second request reaches
// the store. The parent reopens the data directory: nothing of the entry
// may be durable, re-delivering the entry (what the engine does when the
// durable applied index is below the entry) must apply it in full, and the
// result must match a replica that never crashed. Red before the fix: the
// child's first request had committed with the applied index, so the
// reopened store named the entry as applied while missing its second
// request, and the engine would have dropped the re-delivery.
func TestFSMApplyBatch_CrashBetweenRequestsLeavesNoPartialEntry(t *testing.T) {
	if dir := os.Getenv(g17CrashChildEnv); dir != "" {
		g17RunCrashChild(t, dir)
		return
	}

	dir := t.TempDir()
	//nolint:gosec // re-executes the test binary itself with a fixed argument list
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestFSMApplyBatch_CrashBetweenRequestsLeavesNoPartialEntry$", "-test.count=1")
	child.Env = append(os.Environ(), g17CrashChildEnv+"="+dir)
	out, err := child.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "the child must exit through os.Exit; output:\n%s", out)
	require.Equal(t, g17CrashExitCode, exitErr.ExitCode(), "child output:\n%s", out)

	st := g17OpenStore(t, dir)
	_, present := g17AppliedIndex(t, st)
	require.False(t, present, "a half-applied entry must not have persisted its applied index (G17)")
	require.False(t, g17Exists(t, st, "k1"), "a half-applied entry must not have persisted its first request (G17)")
	require.False(t, g17Exists(t, st, "k2"))

	// Re-delivery after the restart applies the whole entry.
	require.Nil(t, g17FSM(t, st).Apply(g17TwoPutEntry(t)))

	reference := g17OpenStore(t, t.TempDir())
	require.Nil(t, g17FSM(t, reference).Apply(g17TwoPutEntry(t)))

	for _, key := range []string{"k1", "k2"} {
		require.True(t, g17Exists(t, st, key), "key %s after re-delivery", key)
		require.Equal(t, g17Exists(t, reference, key), g17Exists(t, st, key), "key %s must match the replica that never crashed", key)
	}
	idx, present := g17AppliedIndex(t, st)
	require.True(t, present)
	require.Equal(t, g17EntryIndex, idx)
	refIdx, _ := g17AppliedIndex(t, reference)
	require.Equal(t, refIdx, idx)
	require.Equal(t, reference.LastCommitTS(), st.LastCommitTS())
}

// g17RunCrashChild is the child half of the crash test. It never returns
// normally: the store hook terminates the process the moment the second
// request of the entry reaches the store.
func g17RunCrashChild(t *testing.T, dir string) {
	t.Helper()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	wrapped := &g17Store{MVCCStore: st, beforeSecondRaftApply: func() { os.Exit(g17CrashExitCode) }}
	g17FSM(t, wrapped).Apply(g17TwoPutEntry(t))
	t.Fatal("the child must have exited inside the second request")
}

// TestFSMApplyBatch_PebbleKeepsPerRequestResults is the Pebble twin of
// TestFSMApplyBatchKeepsPerRequestResults: a request rejected by validation
// is reported on its own slot while the requests around it land, and the
// entry's applied index is written once.
func TestFSMApplyBatch_PebbleKeepsPerRequestResults(t *testing.T) {
	st := g17OpenStore(t, t.TempDir())
	fsm := g17FSM(t, st)

	resp := fsm.Apply(g17Entry(t,
		g17RawPut(10, "good", "v1"),
		g17RawPut(11, "", "bad"),
		g17RawPut(12, "after", "v2"),
	))
	applyResp, ok := resp.(*fsmApplyResponse)
	require.True(t, ok, "got %T", resp)
	require.Len(t, applyResp.results, 3)
	require.NoError(t, applyResp.results[0])
	require.ErrorIs(t, applyResp.results[1], ErrInvalidRequest)
	require.NoError(t, applyResp.results[2])

	require.True(t, g17Exists(t, st, "good"))
	require.True(t, g17Exists(t, st, "after"))
	idx, present := g17AppliedIndex(t, st)
	require.True(t, present)
	require.Equal(t, g17EntryIndex, idx)
	require.Equal(t, uint64(12), st.LastCommitTS())
}

// TestFSMApplyBatch_PrefixDeleteSeesEarlierRequestOfTheSameEntry pins that a
// DEL_PREFIX request staged after a PUT of the same entry tombstones it,
// exactly as it did when each request committed on its own.
func TestFSMApplyBatch_PrefixDeleteSeesEarlierRequestOfTheSameEntry(t *testing.T) {
	st := g17OpenStore(t, t.TempDir())
	fsm := g17FSM(t, st)

	require.Nil(t, fsm.Apply(g17Entry(t,
		g17RawPut(20, "h/f", "1"),
		g17RawDelPrefix(21, "h/"),
		g17RawPut(22, "other", "x"),
	)))

	require.False(t, g17Exists(t, st, "h/f"), "the prefix delete must see the write staged before it")
	require.True(t, g17Exists(t, st, "other"))
	idx, _ := g17AppliedIndex(t, st)
	require.Equal(t, g17EntryIndex, idx)
}

type g17Observer struct {
	t     *testing.T
	st    store.MVCCStore
	seen  []string
	ready []bool
}

func (o *g17Observer) OnApply(_ pb.Op, key []byte) {
	o.seen = append(o.seen, string(key))
	o.ready = append(o.ready, g17Exists(o.t, o.st, string(key)))
}

// TestFSMApplyBatch_ObserversSeeOnlyCommittedWrites pins that apply observers
// are notified after the entry commits, so an observer that reads the key
// it was told about finds it durable, for every request of the entry.
func TestFSMApplyBatch_ObserversSeeOnlyCommittedWrites(t *testing.T) {
	st := g17OpenStore(t, t.TempDir())
	obs := &g17Observer{t: t, st: st}
	fsm := g17FSM(t, st, WithApplyObserver(obs))

	require.Nil(t, fsm.Apply(g17TwoPutEntry(t)))

	require.Equal(t, []string{"k1", "k2"}, obs.seen)
	require.Equal(t, []bool{true, true}, obs.ready, "observers must run after the entry commits")
	require.False(t, fsm.deferApplyNotices)
	require.Nil(t, fsm.deferredApplyNotices)
}

// TestFSMApplyBatch_InMemoryStoreUnchanged keeps the in-memory store on the
// same per-request semantics as before.
func TestFSMApplyBatch_InMemoryStoreUnchanged(t *testing.T) {
	st := store.NewMVCCStore()
	fsm := g17FSM(t, st)
	require.Nil(t, fsm.Apply(g17TwoPutEntry(t)))
	require.True(t, g17Exists(t, st, "k1"))
	require.True(t, g17Exists(t, st, "k2"))
}

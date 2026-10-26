package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type holdPreparedTxn struct {
	kv.Transactional
	prepared chan struct{}
	release  chan struct{}
	once     sync.Once
}

type synchronizeSessionReadsBackend struct {
	operationJournalBackend
	sessionKey []byte
	reads      atomic.Int32
	both       chan struct{}
}

func (b *synchronizeSessionReadsBackend) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	value, err := b.operationJournalBackend.GetAt(ctx, key, ts)
	if !bytes.Equal(key, b.sessionKey) {
		return value, err
	}
	count := b.reads.Add(1)
	if count == 2 {
		close(b.both)
	}
	if count <= 2 {
		select {
		case <-b.both:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return value, err
}

func (h *holdPreparedTxn) Commit(ctx context.Context, reqs []*pb.Request) (*kv.TransactionResponse, error) {
	response, err := h.Transactional.Commit(ctx, reqs)
	if err == nil && len(reqs) == 1 && reqs[0].Phase == pb.Phase_PREPARE {
		h.once.Do(func() {
			close(h.prepared)
			<-h.release
		})
	}
	return response, err
}

func TestOperationJournalTwoGroupCreateOpenWrite(t *testing.T) {
	routes := distribution.NewEngine()
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte{}, End: []byte("!fs|op|"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("!fs|op|"), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))
	factory := etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval: 10 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10,
		MaxSizePerMsg: 1 << 20, MaxInflightMsg: 256,
	})
	groups := make(map[uint64]*kv.ShardGroup, 2)
	for _, gid := range []uint64{1, 2} {
		st := store.NewMVCCStore()
		id := fmt.Sprintf("fs-journal-g%d", gid)
		result, err := factory.Create(raftengine.FactoryConfig{
			LocalID: id, LocalAddress: id, DataDir: t.TempDir(), Bootstrap: true,
			StateMachine: kv.NewKvFSMWithHLC(st, kv.NewHLC()),
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, result.Engine.Close())
			if result.Close != nil {
				require.NoError(t, result.Close())
			}
		})
		require.Eventually(t, func() bool { return result.Engine.State() == raftengine.StateLeader },
			5*time.Second, 10*time.Millisecond)
		groups[gid] = &kv.ShardGroup{Engine: result.Engine, Store: st, Txn: kv.NewLeaderProxyWithEngine(result.Engine)}
	}
	shards := kv.NewShardStore(routes, groups)
	coord := kv.NewShardedCoordinator(routes, groups, 1, kv.NewHLC(), shards)
	svc, err := NewService(shards, coord)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	filesystemID, err := journal.EnsureFilesystem(ctx)
	require.NoError(t, err)
	session, err := journal.RegisterSession(ctx, filesystemID, "benchmark-mount")
	require.NoError(t, err)
	id := OperationID{Filesystem: session.Filesystem, Session: session.Session, Sequence: 1}
	file, err := svc.CreateWithOperation(ctx, id, RootInode, []byte("benchmark.dat"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	createID := id
	id.Sequence++
	fh, err := svc.OpenWithOperation(ctx, id, file.Inode, []byte("benchmark-mount"))
	require.NoError(t, err)
	held := &holdPreparedTxn{Transactional: groups[1].Txn, prepared: make(chan struct{}), release: make(chan struct{})}
	groups[1].Txn = held
	id.Sequence++
	assertWriteAfterConcurrentAcknowledgement(t, ctx, journal, svc, createID, id, file.Inode, held)
	assertConcurrentJournalAdmissions(t, ctx, journal, session)
	id.Sequence++
	require.NoError(t, svc.ReleaseWithOperation(ctx, id, file.Inode, fh, []byte("benchmark-mount")))
	assertJournalCommitsDoNotCollideOnPreparedSession(t, ctx, svc, groups[1], session)
}

func assertConcurrentJournalAdmissions(t *testing.T, ctx context.Context, journal *OperationJournal, session OperationSessionID) {
	t.Helper()
	backend := &synchronizeSessionReadsBackend{
		operationJournalBackend: journal.backend,
		sessionKey:              session.key(),
		both:                    make(chan struct{}),
	}
	journal = newOperationJournalWithBackend(backend)
	var sequence atomic.Uint64
	sequence.Store(4)
	var workers sync.WaitGroup
	errorsCh := make(chan error, 2)
	for worker := range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range 4 {
				id := OperationID{Filesystem: session.Filesystem, Session: session.Session, Sequence: sequence.Add(1)}
				_, err := journal.Admit(ctx, id, []byte{byte(worker), byte(index)}, 16, 1024)
				if err != nil {
					errorsCh <- fmt.Errorf("admit operation %d: %w", id.Sequence, err)
					return
				}
			}
		}()
	}
	workers.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	ts, err := journal.backend.Snapshot(ctx)
	require.NoError(t, err)
	actual, exists, err := journal.loadSessionAt(ctx, session, ts)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint32(8), actual.Pending)
}

func assertJournalCommitsDoNotCollideOnPreparedSession(t *testing.T, ctx context.Context, svc *Service, group *kv.ShardGroup, session OperationSessionID) {
	t.Helper()
	held := &holdPreparedTxn{Transactional: group.Txn, prepared: make(chan struct{}), release: make(chan struct{})}
	group.Txn = held
	backend := serviceOperationBackend{service: svc}
	sessionKey := fskeys.OperationSessionKey(session.Filesystem, session.Session)
	commitWithContext := func(commitCtx context.Context, sequence uint64) error {
		ts, err := backend.Snapshot(commitCtx)
		if err != nil {
			return err
		}
		primary := fskeys.OperationKey(session.Filesystem, session.Session, sequence)
		_, err = backend.Commit(commitCtx, ts, []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: primary, Value: []byte{1}},
			{Op: kv.Put, Key: sessionKey, Value: []byte{2}},
		}, [][]byte{primary, sessionKey})
		return err
	}
	commit := func(sequence uint64) error { return commitWithContext(ctx, sequence) }
	first := make(chan error, 1)
	go func() { first <- commit(1000) }()
	select {
	case <-held.prepared:
	case <-time.After(5 * time.Second):
		t.Fatal("first journal commit did not prepare the session")
	}
	second := make(chan error, 1)
	go func() { second <- commit(1001) }()
	select {
	case err := <-second:
		close(held.release)
		t.Fatalf("second journal commit returned during prepared session lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	assertJournalCommitWaiterCanCancel(t, ctx, held, commitWithContext)
	close(held.release)
	select {
	case err := <-first:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("first journal commit did not finish")
	}
	select {
	case err := <-second:
		require.NotErrorIs(t, err, kv.ErrTxnLocked)
	case <-time.After(5 * time.Second):
		t.Fatal("second journal commit did not finish")
	}
}

func assertJournalCommitWaiterCanCancel(t *testing.T, ctx context.Context, held *holdPreparedTxn, commit func(context.Context, uint64) error) {
	t.Helper()
	cancelCtx, cancel := context.WithCancel(ctx)
	cancelled := make(chan error, 1)
	go func() { cancelled <- commit(cancelCtx, 1002) }()
	select {
	case err := <-cancelled:
		close(held.release)
		t.Fatalf("journal commit returned before its waiter was cancelled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-cancelled:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		close(held.release)
		t.Fatal("cancelled journal commit remained blocked")
	}
}

func assertWriteAfterConcurrentAcknowledgement(t *testing.T, ctx context.Context, journal *OperationJournal, svc *Service, createID, writeID OperationID, inode uint64, held *holdPreparedTxn) {
	t.Helper()
	ackDone := make(chan error, 1)
	go func() { ackDone <- journal.Acknowledge(ctx, createID) }()
	select {
	case <-held.prepared:
	case <-time.After(5 * time.Second):
		t.Fatal("acknowledgement did not prepare its session participant")
	}
	type writeResult struct {
		count int
		err   error
	}
	writeDone := make(chan writeResult, 1)
	go func() {
		count, writeErr := svc.WriteWithOperation(ctx, writeID, inode, 0, []byte("data"))
		writeDone <- writeResult{count, writeErr}
	}()
	select {
	case result := <-writeDone:
		close(held.release)
		t.Fatalf("write returned while the acknowledgement held the prepared session lock: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}
	close(held.release)
	select {
	case err := <-ackDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("acknowledgement did not complete")
	}
	select {
	case result := <-writeDone:
		require.NoError(t, result.err)
		require.Equal(t, 4, result.count)
	case <-time.After(5 * time.Second):
		t.Fatal("write did not complete after acknowledgement")
	}
}

func TestRetryJournalLockedKeepsUnknownOutcome(t *testing.T) {
	tries := 0
	value, err := retryJournalLocked(context.Background(), func(context.Context) ([]byte, error) {
		tries++
		if tries < 3 {
			return nil, kv.ErrTxnLocked
		}
		return []byte("visible"), nil
	})
	require.NoError(t, err)
	require.Equal(t, []byte("visible"), value)
	require.Equal(t, 3, tries)

	tries = 0
	_, err = retryJournalLocked(context.Background(), func(context.Context) ([]byte, error) {
		tries++
		return nil, fmt.Errorf("%w: %w", kv.ErrTransactionOutcomeUnknown, kv.ErrTxnLocked)
	})
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, tries)
}

func TestRepeatedAcknowledgementKeepsReservation(t *testing.T) {
	session := operationSession{RejectBefore: 1, AckGaps: []uint64{3}, RetainedCount: 1, RetainedBytes: 100}
	record := operationRecord{ID: OperationID{Sequence: 3}, Reservation: 10}
	require.ErrorIs(t, reserveAckRetirement(&session, record), ErrOperationRetired)
	require.Equal(t, []uint64{3}, session.AckGaps)
	require.Equal(t, uint32(1), session.RetainedCount)
	require.Equal(t, uint64(100), session.RetainedBytes)
}

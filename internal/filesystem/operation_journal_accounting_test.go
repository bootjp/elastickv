package filesystem

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestOperationJournalRetainedAccountingAcrossSequenceBoundary(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		journal, _, _ := testOperationJournal(t)
		assertOperationJournalRetainedAccountingAcrossSequenceBoundary(t, journal)
	})
	t.Run("pebble", func(t *testing.T) {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		service, err := NewService(st, &testCoordinator{st: st})
		require.NoError(t, err)
		journal, err := NewOperationJournal(service)
		require.NoError(t, err)
		assertOperationJournalRetainedAccountingAcrossSequenceBoundary(t, journal)
	})
}

func TestOperationJournalConcurrentAcknowledgementChurn(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		journal, _, _ := testOperationJournal(t)
		assertConcurrentAcknowledgementChurn(t, journal)
	})
	t.Run("pebble", func(t *testing.T) {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		service, err := NewService(st, &testCoordinator{st: st})
		require.NoError(t, err)
		journal, err := NewOperationJournal(service)
		require.NoError(t, err)
		assertConcurrentAcknowledgementChurn(t, journal)
	})
}

func TestOperationJournalGenerationFenceRacesAcknowledgement(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		journal, _, _ := testOperationJournal(t)
		assertGenerationFenceRacesAcknowledgement(t, journal)
	})
	t.Run("pebble", func(t *testing.T) {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		service, err := NewService(st, &testCoordinator{st: st})
		require.NoError(t, err)
		journal, err := NewOperationJournal(service)
		require.NoError(t, err)
		assertGenerationFenceRacesAcknowledgement(t, journal)
	})
	t.Run("raft-pebble", func(t *testing.T) {
		journal := testRaftPebbleOperationJournal(t)
		assertGenerationFenceRacesAcknowledgement(t, journal)
	})
}

func testRaftPebbleOperationJournal(t *testing.T) *OperationJournal {
	t.Helper()
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	factory := etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval: 10 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10,
		MaxSizePerMsg: 1 << 20, MaxInflightMsg: 256,
	})
	result, err := factory.Create(raftengine.FactoryConfig{
		LocalID: "fs-ack-accounting", LocalAddress: "fs-ack-accounting",
		DataDir: t.TempDir(), Bootstrap: true,
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
	routes := distribution.NewEngineWithDefaultRoute()
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{{
			RouteID: 1, GroupID: 1, State: distribution.RouteStateActive,
		}},
	}))
	groups := map[uint64]*kv.ShardGroup{1: {
		Engine: result.Engine, Store: st, Txn: kv.NewLeaderProxyWithEngine(result.Engine),
	}}
	shards := kv.NewShardStore(routes, groups)
	coord := kv.NewShardedCoordinator(routes, groups, 1, kv.NewHLC(), shards)
	service, err := NewService(shards, coord)
	require.NoError(t, err)
	require.NoError(t, service.InitializeRoot(context.Background(), testRootMode, 1000, 1000))
	journal, err := NewOperationJournal(service)
	require.NoError(t, err)
	return journal
}

func assertGenerationFenceRacesAcknowledgement(t *testing.T, journal *OperationJournal) {
	t.Helper()
	ctx := context.Background()
	pendingID := testOperationID(t, journal, 1)
	terminalID := OperationID{Filesystem: pendingID.Filesystem, Session: pendingID.Session, Sequence: 2}
	_, err := journal.Admit(ctx, pendingID, []byte("write"), 128, 1024)
	require.NoError(t, err)
	pending, err := journal.BeginAttempt(ctx, pendingID)
	require.NoError(t, err)
	_, err = journal.Admit(ctx, terminalID, []byte("write"), 128, 1024)
	require.NoError(t, err)
	terminal, err := journal.BeginAttempt(ctx, terminalID)
	require.NoError(t, err)
	publicationRoute, ready := journal.backend.JournalRouteVersion()
	require.True(t, ready)
	_, err = journal.Publish(ctx, terminalID, terminal.AttemptID, operationAttemptPlan{
		Intent: []byte("write"), Result: []byte("done"), RouteVersion: publicationRoute,
	})
	require.NoError(t, err)

	sessionID := OperationSessionID{Filesystem: pendingID.Filesystem, Session: pendingID.Session}
	backend := &synchronizeSessionReadsBackend{
		operationJournalBackend: journal.backend,
		sessionKey:              sessionID.key(),
		both:                    make(chan struct{}),
	}
	concurrent := newOperationJournalWithBackend(backend)
	observed, ready := backend.JournalRouteVersion()
	require.True(t, ready)
	type result struct {
		record operationRecord
		err    error
	}
	fenceDone := make(chan result, 1)
	ackDone := make(chan error, 1)
	go func() {
		record, fenceErr := concurrent.advanceGenerationOnce(ctx, pendingID, pending.AttemptID, observed)
		fenceDone <- result{record: record, err: fenceErr}
	}()
	go func() { ackDone <- concurrent.acknowledgeOnce(ctx, terminalID) }()
	fence := <-fenceDone
	ackErr := <-ackDone
	require.True(t, (fence.err == nil) != (ackErr == nil), "fence: %v; ack: %v", fence.err, ackErr)

	var fenced operationRecord
	if fence.err != nil {
		require.True(t, IsWriteConflict(fence.err), "fence: %v", fence.err)
		fenced, err = journal.AdvanceGeneration(ctx, pendingID, pending.AttemptID)
		require.NoError(t, err)
	} else {
		fenced = fence.record
	}
	if ackErr != nil {
		require.True(t, IsWriteConflict(ackErr), "ack: %v", ackErr)
		require.NoError(t, journal.Acknowledge(ctx, terminalID))
	}
	require.Equal(t, pending.Reservation+operationFenceReservation, fenced.Reservation)

	ts, err := journal.backend.Snapshot(ctx)
	require.NoError(t, err)
	session, found, err := journal.loadSessionAt(ctx, sessionID, ts)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint32(1), session.RetainedCount)
	require.Equal(t, fenced.Reservation, session.RetainedBytes)
	_, err = journal.Cancel(ctx, pendingID)
	require.NoError(t, err)
	require.NoError(t, journal.Acknowledge(ctx, pendingID))
}

func assertConcurrentAcknowledgementChurn(t *testing.T, journal *OperationJournal) {
	t.Helper()
	ctx := context.Background()
	first := testOperationID(t, journal, 1)
	const operations = uint64(256)
	for sequence := uint64(1); sequence <= operations; sequence++ {
		id := OperationID{Filesystem: first.Filesystem, Session: first.Session, Sequence: sequence}
		_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
		require.NoError(t, err)
		attempt, err := journal.BeginAttempt(ctx, id)
		require.NoError(t, err)
		_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
			Intent: []byte("write"), Result: []byte("done"),
		})
		require.NoError(t, err)
	}

	var next atomic.Uint64
	var workers sync.WaitGroup
	errorsCh := make(chan error, operations)
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				sequence := next.Add(1)
				if sequence > operations {
					return
				}
				id := OperationID{Filesystem: first.Filesystem, Session: first.Session, Sequence: sequence}
				if err := journal.Acknowledge(ctx, id); err != nil {
					errorsCh <- err
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
	session, found, err := journal.loadSessionAt(ctx, OperationSessionID{
		Filesystem: first.Filesystem, Session: first.Session,
	}, ts)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, operations, session.RejectBefore)
	require.Empty(t, session.AckGaps)
	require.Zero(t, session.Pending)
	require.Zero(t, session.RetainedCount)
	require.Zero(t, session.RetainedBytes)
}

func TestOperationJournalConcurrentAcknowledgementsPreserveAccounting(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		journal, _, _ := testOperationJournal(t)
		assertConcurrentAcknowledgementsPreserveAccounting(t, journal)
	})
	t.Run("pebble", func(t *testing.T) {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		service, err := NewService(st, &testCoordinator{st: st})
		require.NoError(t, err)
		journal, err := NewOperationJournal(service)
		require.NoError(t, err)
		assertConcurrentAcknowledgementsPreserveAccounting(t, journal)
	})
}

func assertConcurrentAcknowledgementsPreserveAccounting(t *testing.T, journal *OperationJournal) {
	t.Helper()
	ctx := context.Background()
	first := testOperationID(t, journal, 1)
	ids := []OperationID{first, {Filesystem: first.Filesystem, Session: first.Session, Sequence: 2}}
	for _, id := range ids {
		_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
		require.NoError(t, err)
		attempt, err := journal.BeginAttempt(ctx, id)
		require.NoError(t, err)
		_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
			Intent: []byte("write"), Result: []byte("done"),
		})
		require.NoError(t, err)
	}

	sessionID := OperationSessionID{Filesystem: first.Filesystem, Session: first.Session}
	backend := &synchronizeSessionReadsBackend{
		operationJournalBackend: journal.backend,
		sessionKey:              sessionID.key(),
		both:                    make(chan struct{}),
	}
	concurrent := newOperationJournalWithBackend(backend)
	type outcome struct {
		id  OperationID
		err error
	}
	results := make(chan outcome, len(ids))
	var workers sync.WaitGroup
	for _, id := range ids {
		workers.Add(1)
		go func() {
			defer workers.Done()
			results <- outcome{id: id, err: concurrent.acknowledgeOnce(ctx, id)}
		}()
	}
	workers.Wait()
	close(results)

	succeeded, conflicted := 0, 0
	for result := range results {
		if result.err == nil {
			succeeded++
			continue
		}
		require.True(t, IsWriteConflict(result.err), "ack sequence %d: %v", result.id.Sequence, result.err)
		conflicted++
		require.NoError(t, journal.Acknowledge(ctx, result.id))
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, conflicted)

	ts, err := journal.backend.Snapshot(ctx)
	require.NoError(t, err)
	session, found, err := journal.loadSessionAt(ctx, sessionID, ts)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(2), session.RejectBefore)
	require.Zero(t, session.Pending)
	require.Zero(t, session.RetainedCount)
	require.Zero(t, session.RetainedBytes)
}

func assertOperationJournalRetainedAccountingAcrossSequenceBoundary(t *testing.T, journal *OperationJournal) {
	t.Helper()
	ctx := context.Background()
	first := testOperationID(t, journal, 1)
	const batchSize = 16
	const operations = 1280

	for start := uint64(1); start <= operations; start += batchSize {
		for sequence := start; sequence < start+batchSize; sequence++ {
			id := OperationID{Filesystem: first.Filesystem, Session: first.Session, Sequence: sequence}
			_, err := journal.Admit(ctx, id, []byte("write"), 128, 1024)
			require.NoError(t, err, "admit sequence %d", sequence)
			attempt, err := journal.BeginAttempt(ctx, id)
			require.NoError(t, err, "begin sequence %d", sequence)
			if sequence%3 == 0 {
				attempt, err = journal.AdvanceGeneration(ctx, id, attempt.AttemptID)
				require.NoError(t, err, "advance sequence %d", sequence)
			}
			_, err = journal.Publish(ctx, id, attempt.AttemptID, operationAttemptPlan{
				Intent: []byte("write"), Result: []byte("done"),
			})
			require.NoError(t, err, "publish sequence %d", sequence)
		}
		for sequence := start + batchSize - 1; sequence >= start; sequence-- {
			id := OperationID{Filesystem: first.Filesystem, Session: first.Session, Sequence: sequence}
			require.NoError(t, journal.Acknowledge(ctx, id), "ack sequence %d", sequence)
		}
	}

	ts, err := journal.backend.Snapshot(ctx)
	require.NoError(t, err)
	session, found, err := journal.loadSessionAt(ctx, OperationSessionID{
		Filesystem: first.Filesystem, Session: first.Session,
	}, ts)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(operations), session.RejectBefore)
	require.Zero(t, session.Pending)
	require.Zero(t, session.RetainedCount)
	require.Zero(t, session.RetainedBytes)
}

package adapter

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type auditScannerStub struct {
	version uint64
	term    uint64
	checks  int
	change  func(*auditScannerStub)
}

type auditPinObservingStore struct {
	store.MVCCStore
	tracker *kv.ActiveTimestampTracker
	reads   int
	readTS  uint64
	failAt  int
}

type auditBlockingStore struct {
	store.MVCCStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *auditBlockingStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newBlockingLegacyMetadataAuditor(t *testing.T) (*LegacyMetadataAuditor, *auditBlockingStore, *kv.ActiveTimestampTracker) {
	t.Helper()
	st := &auditBlockingStore{MVCCStore: store.NewMVCCStore(), entered: make(chan struct{}), release: make(chan struct{})}
	require.NoError(t, st.PutAt(t.Context(), store.HashMetaKey([]byte("apple")), []byte("legacy"), 5, 0))
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	shards := kv.NewShardStore(routes, map[uint64]*kv.ShardGroup{
		1: {Store: st, Engine: &localCompactionReadEngine{}},
	})
	tracker := kv.NewActiveTimestampTracker()
	return NewLegacyMetadataAuditor(shards, newLocalAdapterCoordinator(shards), tracker), st, tracker
}

func TestLegacyMetadataAuditorBoundsConcurrentRuns(t *testing.T) {
	auditor, st, tracker := newBlockingLegacyMetadataAuditor(t)
	first := make(chan error, 1)
	go func() {
		_, err := auditor.AuditLegacyMetadata(t.Context(), 1, 8)
		first <- err
	}()
	select {
	case <-st.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first audit did not reach the physical scan")
	}
	_, err := auditor.AuditLegacyMetadata(t.Context(), 1, 8)
	require.ErrorIs(t, err, ErrLegacyMetadataAuditBusy)
	require.NotZero(t, tracker.Oldest())
	close(st.release)
	require.NoError(t, <-first)
	require.Zero(t, tracker.Oldest())
	_, err = auditor.AuditLegacyMetadata(t.Context(), 1, legacyMetadataAuditMaxPages+1)
	require.Error(t, err, "direct callers must obey the same page budget as the RPC")
}

func TestLegacyMetadataAuditorReleasesPinAtServerDeadline(t *testing.T) {
	auditor, st, tracker := newBlockingLegacyMetadataAuditor(t)
	auditor.timeout = 250 * time.Millisecond
	result, err := auditor.AuditLegacyMetadata(t.Context(), 1, 8)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-st.entered:
	default:
		t.Fatal("audit deadline fired before the physical scan")
	}
	require.Empty(t, result.Passes)
	require.Zero(t, tracker.Oldest())
	close(st.release)
}

type legacyAuditNoWriteCoordinator struct{ kv.Coordinator }

func (*legacyAuditNoWriteCoordinator) IsLeaderForKey([]byte) bool { return false }

type legacyAuditFollowerEngine struct{ raftengine.Engine }

func (*legacyAuditFollowerEngine) State() raftengine.State { return raftengine.StateFollower }

func (*legacyAuditFollowerEngine) Status() raftengine.Status {
	return raftengine.Status{State: raftengine.StateFollower, Term: 1}
}

func (*legacyAuditFollowerEngine) Leader() raftengine.LeaderInfo { return raftengine.LeaderInfo{} }

func TestLegacyMetadataAuditMatchesBackgroundPassOnSplitShardStore(t *testing.T) {
	ctx := t.Context()
	routes := distribution.NewEngine()
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{Version: 10, Routes: []distribution.RouteDescriptor{
		{RouteID: 1, End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
		{RouteID: 2, Start: []byte("m"), End: []byte("t"), GroupID: 2, State: distribution.RouteStateActive},
		{RouteID: 3, Start: []byte("t"), End: []byte("x"), GroupID: 3, State: distribution.RouteStateActive},
		{RouteID: 4, Start: []byte("x"), GroupID: 4, State: distribution.RouteStateActive,
			StagedVisibilityActive: true, MigrationJobID: 9},
	}}))
	legacy := store.NewMVCCStore()
	owner := store.NewMVCCStore()
	shards := kv.NewShardStore(routes, map[uint64]*kv.ShardGroup{
		1: {Store: legacy, Engine: &localCompactionReadEngine{}},
		2: {Store: owner, Engine: &localCompactionReadEngine{}},
		3: {Store: store.NewMVCCStore(), Engine: &legacyAuditFollowerEngine{}},
		4: {Store: store.NewMVCCStore(), Engine: &localCompactionReadEngine{}},
	})
	families := []func([]byte) []byte{store.HashMetaKey, store.SetMetaKey, store.ZSetMetaKey, store.StreamMetaKey}
	for _, keyFor := range families {
		for _, userKey := range []string{"m-served", "n-shadowed", "u-remote", "z-staged"} {
			require.NoError(t, legacy.PutAt(ctx, keyFor([]byte(userKey)), []byte("legacy"), 5, 0))
		}
		require.NoError(t, owner.PutAt(ctx, keyFor([]byte("n-shadowed")), []byte("new"), 6, 0))
	}
	// Nested metadata rows share the scan prefixes but are not base backlog.
	require.NoError(t, legacy.PutAt(ctx, store.HashMetaDeltaKey([]byte("m-served"), 5, 0), []byte("delta"), 5, 0))
	require.NoError(t, legacy.PutAt(ctx, []byte("audit-floor"), []byte("x"), 10, 0))
	coord := newLocalAdapterCoordinator(shards)
	tracker := kv.NewActiveTimestampTracker()
	audit, err := NewLegacyMetadataAuditor(shards, coord, tracker).AuditLegacyMetadata(ctx, 1, 8)
	require.NoError(t, err)
	require.Equal(t, LegacyMetadataAuditComplete, audit.Status)
	require.Len(t, audit.Passes, 4)
	require.Zero(t, tracker.Oldest())

	compactor := NewDeltaCompactor(shards, &legacyAuditNoWriteCoordinator{Coordinator: coord})
	for _, handler := range compactor.ttlInlineMigrationHandlers() {
		if !isLegacyMetadataMigrationHandler(handler.typeName) {
			continue
		}
		plan := ttlInlineMigrationScanPlan{
			cursorName: ttlInlineMigrationGroupCursorName(handler.typeName, 1), groupID: 1, scanner: shards,
		}
		require.NoError(t, compactor.migrateTTLInlineScanPlan(ctx, handler, plan, audit.ReadTS, audit.Passes[0].StartedAt))
	}
	background, err := compactor.CompleteLegacyMetadataGroupPasses(ctx, 1)
	require.NoError(t, err)
	require.Len(t, background, 4)
	for i, pass := range audit.Passes {
		require.Equal(t, pass.Plan, background[i].Plan)
		require.Equal(t, pass.CatalogVersion, background[i].CatalogVersion)
		require.Equal(t, pass.LeaderTerm, background[i].LeaderTerm)
		require.Equal(t, pass.Counts, background[i].Counts)
		require.Equal(t, LegacyMetadataReasonCounts{Served: 1, OwnerHasVersion: 1}, pass.Counts[2])
		require.Equal(t, LegacyMetadataReasonCounts{OwnerLedElsewhere: 1}, pass.Counts[3])
		require.Equal(t, LegacyMetadataReasonCounts{StagedVisibility: 1}, pass.Counts[4])
	}
	value, err := legacy.GetAt(ctx, store.HashMetaKey([]byte("m-served")), audit.ReadTS)
	require.NoError(t, err)
	require.Equal(t, []byte("legacy"), value, "diagnostic scans must not rewrite physical legacy rows")
}

func (s *auditPinObservingStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if s.tracker.Oldest() != ts {
		return nil, errors.New("audit read timestamp is not pinned")
	}
	if s.readTS != 0 && s.readTS != ts {
		return nil, errors.New("audit changed read timestamp between pages")
	}
	s.readTS = ts
	s.reads++
	if s.reads == s.failAt {
		return nil, errors.New("injected scan failure")
	}
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func TestLegacyMetadataAuditorPinsOneReadAcrossFourRanges(t *testing.T) {
	tracker := kv.NewActiveTimestampTracker()
	base := store.NewMVCCStore()
	key := store.HashMetaKey([]byte("apple"))
	require.NoError(t, base.PutAt(t.Context(), key, []byte("meta"), 5, 0))
	observed := &auditPinObservingStore{MVCCStore: base, tracker: tracker}
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	shards := kv.NewShardStore(routes, map[uint64]*kv.ShardGroup{
		1: {Store: observed, Engine: &localCompactionReadEngine{}},
	})
	auditor := NewLegacyMetadataAuditor(shards, newLocalAdapterCoordinator(shards), tracker)
	result, err := auditor.AuditLegacyMetadata(t.Context(), 1, 8)
	require.NoError(t, err)
	require.Equal(t, LegacyMetadataAuditComplete, result.Status)
	require.Len(t, result.Passes, 4)
	require.GreaterOrEqual(t, observed.reads, 4)
	require.Equal(t, result.ReadTS, observed.readTS)
	require.Zero(t, tracker.Oldest(), "request close must release the audit pin")
	observed.reads = 0
	observed.readTS = 0
	observed.failAt = 2
	failed, err := auditor.AuditLegacyMetadata(t.Context(), 1, 8)
	require.ErrorContains(t, err, "injected scan failure")
	require.Empty(t, failed.Passes)
	require.Zero(t, tracker.Oldest(), "failed audit must release the pin")
}

func (s *auditScannerStub) GroupOwnedScanIdentity(context.Context, uint64) (uint64, uint64, error) {
	s.checks++
	if s.change != nil {
		s.change(s)
	}
	return s.version, s.term, nil
}

func TestLegacyMetadataAuditCountsFourPlansAtOneTimestamp(t *testing.T) {
	scanner := &auditScannerStub{version: 11, term: 3}
	var starts [][]byte
	page := 0
	result, err := runLegacyMetadataAudit(t.Context(), scanner, 7, 123, 8,
		func(_ context.Context, start, end []byte) (kv.GroupOwnedScanPage, error) {
			require.Equal(t, uint64(11), scanner.version)
			require.Less(t, bytes.Compare(start, end), 0)
			starts = append(starts, bytes.Clone(start))
			page++
			out := kv.GroupOwnedScanPage{CatalogVersion: 11, LeaderTerm: 3}
			switch page {
			case 1:
				out.Resume = append([]byte(store.HashMetaPrefix), []byte("cursor")...)
				out.LegacyRows = []kv.LegacyMetadataRow{legacyTestRow(9, kv.LegacyMetadataServed)}
			case 2:
				out.LegacyRows = []kv.LegacyMetadataRow{legacyTestRow(9, kv.LegacyMetadataOwnerLedElsewhere)}
			case 3:
				out.LegacyRows = []kv.LegacyMetadataRow{legacyTestRow(8, kv.LegacyMetadataOwnerHasVersion)}
			case 4:
				out.LegacyRows = []kv.LegacyMetadataRow{legacyTestRow(8, kv.LegacyMetadataStaged)}
			}
			return out, nil
		})
	require.NoError(t, err)
	require.Equal(t, LegacyMetadataAuditComplete, result.Status)
	require.Equal(t, uint64(123), result.ReadTS)
	require.Equal(t, uint32(5), result.PagesScanned)
	require.Len(t, result.Passes, 4)
	require.Equal(t, uint64(1), result.Passes[0].Counts[9].Served)
	require.Equal(t, uint64(1), result.Passes[0].Counts[9].OwnerLedElsewhere)
	require.Equal(t, uint64(1), result.Passes[1].Counts[8].OwnerHasVersion)
	require.Equal(t, uint64(1), result.Passes[2].Counts[8].StagedVisibility)
	require.Empty(t, result.Passes[3].Counts)
	require.True(t, bytes.HasSuffix(starts[1], []byte{0}), "physical-page resume must advance")
}

func TestLegacyMetadataAuditIncompleteNeverReturnsPartialCounts(t *testing.T) {
	scanner := &auditScannerStub{version: 11, term: 3}
	reads := 0
	result, err := runLegacyMetadataAudit(t.Context(), scanner, 7, 123, 1,
		func(context.Context, []byte, []byte) (kv.GroupOwnedScanPage, error) {
			reads++
			return kv.GroupOwnedScanPage{CatalogVersion: 11, LeaderTerm: 3,
				LegacyRows: []kv.LegacyMetadataRow{legacyTestRow(9, kv.LegacyMetadataServed)}}, nil
		})
	require.NoError(t, err)
	require.Equal(t, LegacyMetadataAuditIncomplete, result.Status)
	require.Equal(t, 1, reads)
	require.Empty(t, result.Passes)
}

func TestLegacyMetadataAuditRestartsOnCatalogOrTermChange(t *testing.T) {
	for _, field := range []string{"catalog", "term"} {
		t.Run(field, func(t *testing.T) {
			scanner := &auditScannerStub{version: 11, term: 3}
			scanner.change = func(s *auditScannerStub) {
				if s.checks == 3 {
					if field == "catalog" {
						s.version++
					} else {
						s.term++
					}
				}
			}
			result, err := runLegacyMetadataAudit(t.Context(), scanner, 7, 123, 8,
				func(context.Context, []byte, []byte) (kv.GroupOwnedScanPage, error) {
					return kv.GroupOwnedScanPage{CatalogVersion: 11, LeaderTerm: 3,
						LegacyRows: []kv.LegacyMetadataRow{legacyTestRow(9, kv.LegacyMetadataServed)}}, nil
				})
			require.NoError(t, err)
			require.Equal(t, LegacyMetadataAuditRestarted, result.Status)
			require.Empty(t, result.Passes)
		})
	}
}

func TestLegacyMetadataAuditReadFailureDoesNotReturnZero(t *testing.T) {
	scanner := &auditScannerStub{version: 11, term: 3}
	result, err := runLegacyMetadataAudit(t.Context(), scanner, 7, 123, 8,
		func(context.Context, []byte, []byte) (kv.GroupOwnedScanPage, error) {
			return kv.GroupOwnedScanPage{}, errors.New("read failed")
		})
	require.ErrorContains(t, err, "read failed")
	require.Empty(t, result.Passes)
}

type auditSourceStub struct {
	result LegacyMetadataAuditResult
	err    error
}

func (s auditSourceStub) AuditLegacyMetadata(context.Context, uint64, uint32) (LegacyMetadataAuditResult, error) {
	return s.result, s.err
}

func TestLegacyMetadataAuditAdminMapsResourceLimits(t *testing.T) {
	srv := NewAdminServer(NodeIdentity{}, nil)
	for _, tc := range []struct {
		err  error
		want codes.Code
	}{
		{ErrLegacyMetadataAuditBusy, codes.ResourceExhausted},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
	} {
		srv.RegisterLegacyMetadataAuditSource(auditSourceStub{err: tc.err})
		_, err := srv.AuditRedisLegacyMetadata(t.Context(), &pb.AuditRedisLegacyMetadataRequest{GroupId: 1})
		require.Equal(t, tc.want, status.Code(err))
	}
}

func TestLegacyMetadataAuditAdminBearerAndIncompleteProtocol(t *testing.T) {
	srv := NewAdminServer(NodeIdentity{}, nil)
	srv.RegisterLegacyMetadataAuditSource(auditSourceStub{result: LegacyMetadataAuditResult{
		Status: LegacyMetadataAuditIncomplete, GroupID: 7, ReadTS: 123, CatalogVersion: 11, LeaderTerm: 3, PagesScanned: 1,
	}})
	unary, _ := AdminTokenAuth("admin-token")
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(unary))
	pb.RegisterAdminServer(server, srv)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewAdminClient(conn)
	req := &pb.AuditRedisLegacyMetadataRequest{GroupId: 7, MaxPages: 1}
	_, err = client.AuditRedisLegacyMetadata(t.Context(), req)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer admin-token")
	response, err := client.AuditRedisLegacyMetadata(ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.RedisLegacyMetadataAuditStatus_REDIS_LEGACY_METADATA_AUDIT_INCOMPLETE, response.GetStatus())
	require.Empty(t, response.GetPasses())
	_, err = client.AuditRedisLegacyMetadata(ctx, &pb.AuditRedisLegacyMetadataRequest{GroupId: 7, MaxPages: legacyMetadataAuditMaxPages + 1})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestLegacyMetadataAuditAdminRejectsInvalidSourceEvidence(t *testing.T) {
	srv := NewAdminServer(NodeIdentity{}, nil)
	req := &pb.AuditRedisLegacyMetadataRequest{GroupId: 7}
	_, err := srv.AuditRedisLegacyMetadata(t.Context(), req)
	require.Equal(t, codes.Unavailable, status.Code(err))
	base := LegacyMetadataAuditResult{Status: LegacyMetadataAuditIncomplete, GroupID: 7, ReadTS: 123,
		CatalogVersion: 11, LeaderTerm: 3, Passes: completeLegacyMetadataFixture()[:1]}
	srv.RegisterLegacyMetadataAuditSource(auditSourceStub{result: base})
	_, err = srv.AuditRedisLegacyMetadata(t.Context(), req)
	require.Equal(t, codes.Internal, status.Code(err))
	base.Status = LegacyMetadataAuditComplete
	base.Passes = completeLegacyMetadataFixture()[:3]
	srv.RegisterLegacyMetadataAuditSource(auditSourceStub{result: base})
	_, err = srv.AuditRedisLegacyMetadata(t.Context(), req)
	require.Equal(t, codes.Internal, status.Code(err))
	base.Passes = completeLegacyMetadataFixture()
	srv.RegisterLegacyMetadataAuditSource(auditSourceStub{result: base})
	response, err := srv.AuditRedisLegacyMetadata(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, pb.RedisLegacyMetadataAuditStatus_REDIS_LEGACY_METADATA_AUDIT_COMPLETE, response.GetStatus())
	require.Len(t, response.GetPasses(), 4)
}

package adapter

import (
	"bytes"
	"context"
	"encoding/base64"
	stderrors "errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logicalbackup "github.com/bootjp/elastickv/internal/backup"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	kvstore "github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	backupSubtypePin       byte = 0x01
	backupSubtypeExtend    byte = 0x02
	backupSubtypeRelease   byte = 0x03
	backupSubtypeReserve   byte = 0x04
	backupSubtypeUnreserve byte = 0x05
)

type backupTestGroup struct {
	mu      sync.Mutex
	status  raftengine.Status
	servers []raftengine.Server
	every   uint64
	cfgErr  error
}

func (g *backupTestGroup) Status() raftengine.Status {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.status
}

func (g *backupTestGroup) Configuration(context.Context) (raftengine.Configuration, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cfgErr != nil {
		return raftengine.Configuration{}, g.cfgErr
	}
	return raftengine.Configuration{Servers: append([]raftengine.Server(nil), g.servers...)}, nil
}

func (g *backupTestGroup) SnapshotEvery() uint64 { return g.every }

func (g *backupTestGroup) setApplied(applied uint64) {
	g.mu.Lock()
	g.status.AppliedIndex = applied
	g.mu.Unlock()
}

type backupTestProposer struct {
	mu             sync.Mutex
	entries        [][]byte
	commit         uint64
	failures       map[byte]int
	transportError map[byte]error
	responseError  map[byte]error
	onPropose      func(byte, uint64)
}

func newBackupTestProposer() *backupTestProposer {
	return &backupTestProposer{
		failures: make(map[byte]int), transportError: make(map[byte]error), responseError: make(map[byte]error),
	}
}

func (p *backupTestProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.ProposeAdmin(ctx, data)
}

func (p *backupTestProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entry := append([]byte(nil), data...)
	p.entries = append(p.entries, entry)
	p.commit++
	subtype := entry[1]
	if p.failures[subtype] > 0 {
		p.failures[subtype]--
		return nil, p.transportError[subtype]
	}
	if p.onPropose != nil {
		p.onPropose(subtype, p.commit)
	}
	return &raftengine.ProposalResult{CommitIndex: p.commit, Response: p.responseError[subtype]}, nil
}

func (p *backupTestProposer) subtypes() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]byte, 0, len(p.entries))
	for _, entry := range p.entries {
		out = append(out, entry[1])
	}
	return out
}

type backupTestStore struct {
	mu           sync.Mutex
	keys         [][]byte
	readTS       []uint64
	capturedTS   []uint64
	onCapture    func()
	onExhaust    func()
	scanDelay    time.Duration
	keyCloseErr  error
	pairCloseErr error
	captureErr   error
	validateErr  error
	valueKeys    [][]byte
	values       map[string][]byte
}

func (s *backupTestStore) ValidateBackupSnapshotAt(context.Context, kv.BackupRouteSnapshot, uint64, int) error {
	return s.validateErr
}

func (s *backupTestStore) CaptureBackupRouteSnapshotAt(_ context.Context, ts uint64) (kv.BackupRouteSnapshot, error) {
	s.mu.Lock()
	s.capturedTS = append(s.capturedTS, ts)
	onCapture := s.onCapture
	s.mu.Unlock()
	if onCapture != nil {
		onCapture()
	}
	return kv.BackupRouteSnapshot{}, s.captureErr
}

func (s *backupTestStore) NewBackupKeyScannerAtSnapshot(_ kv.BackupRouteSnapshot, ts uint64, _ int) kv.BackupKeyScanner {
	s.mu.Lock()
	s.readTS = append(s.readTS, ts)
	keys := make([][]byte, len(s.keys))
	for i := range s.keys {
		keys[i] = append([]byte(nil), s.keys[i]...)
	}
	onExhaust := s.onExhaust
	delay := s.scanDelay
	closeErr := s.keyCloseErr
	s.mu.Unlock()
	return &backupSliceScanner{keys: keys, onExhaust: onExhaust, delay: delay, closeErr: closeErr}
}

func (s *backupTestStore) NewBackupScannerAtSnapshot(_ kv.BackupRouteSnapshot, ts uint64, _ int) kv.BackupScanner {
	return s.newBackupScannerAtSnapshot(ts, nil)
}

func (s *backupTestStore) NewFilteredBackupScannerAtSnapshot(
	_ kv.BackupRouteSnapshot,
	ts uint64,
	_ int,
	keyFilter kv.BackupKeyFilter,
) kv.BackupScanner {
	return s.newBackupScannerAtSnapshot(ts, keyFilter)
}

func (s *backupTestStore) newBackupScannerAtSnapshot(ts uint64, keyFilter kv.BackupKeyFilter) kv.BackupScanner {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readTS = append(s.readTS, ts)
	pairs := make([]*kvstore.KVPair, 0, len(s.keys))
	var filterErr error
	for _, key := range s.keys {
		if keyFilter != nil {
			selected, err := keyFilter(key)
			if err != nil {
				filterErr = err
				break
			}
			if !selected {
				continue
			}
		}
		s.valueKeys = append(s.valueKeys, append([]byte(nil), key...))
		value := s.values[string(key)]
		if value == nil {
			value = backupTestDefaultValueForKey(key)
		}
		pairs = append(pairs, &kvstore.KVPair{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	}
	onExhaust := s.onExhaust
	delay := s.scanDelay
	closeErr := s.pairCloseErr
	return &backupPairScanner{pairs: pairs, err: filterErr, onExhaust: onExhaust, delay: delay, closeErr: closeErr}
}

func backupTestDefaultValueForKey(key []byte) []byte {
	scope, scoped, err := logicalbackup.ScopeForKey(key)
	if err != nil || !scoped {
		return []byte("value")
	}
	if scope.Adapter != "dynamodb" {
		return []byte("value")
	}
	if bytes.HasPrefix(key, []byte(logicalbackup.DDBTableMetaPrefix)) {
		value, err := encodeStoredDynamoTableSchema(&dynamoTableSchema{
			TableName:            scope.Name,
			AttributeDefinitions: map[string]string{"id": "S"},
			PrimaryKey:           dynamoKeySchema{HashKey: "id"},
			Generation:           1,
		})
		if err != nil {
			panic(err)
		}
		return value
	}
	if bytes.HasPrefix(key, []byte(logicalbackup.DDBItemPrefix)) {
		id := "value"
		value, err := encodeStoredDynamoItem(map[string]attributeValue{"id": {S: &id}})
		if err != nil {
			panic(err)
		}
		return value
	}
	return []byte("value")
}

type backupSliceScanner struct {
	keys      [][]byte
	index     int
	onExhaust func()
	once      sync.Once
	delay     time.Duration
	closeErr  error
}

func (s *backupSliceScanner) Next(ctx context.Context) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.delay > 0 && s.index < len(s.keys) {
		timer := time.NewTimer(s.delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		case <-timer.C:
		}
	}
	if s.index >= len(s.keys) {
		s.once.Do(func() {
			if s.onExhaust != nil {
				s.onExhaust()
			}
		})
		return nil, false, nil
	}
	key := append([]byte(nil), s.keys[s.index]...)
	s.index++
	return key, true, nil
}

func (s *backupSliceScanner) Close() error { return s.closeErr }

type backupPairScanner struct {
	pairs     []*kvstore.KVPair
	index     int
	err       error
	onExhaust func()
	once      sync.Once
	delay     time.Duration
	closeErr  error
}

func (s *backupPairScanner) Next(ctx context.Context) (*kvstore.KVPair, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.err != nil {
		err := s.err
		s.err = nil
		return nil, false, err
	}
	if s.delay > 0 && s.index < len(s.pairs) {
		timer := time.NewTimer(s.delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		case <-timer.C:
		}
	}
	if s.index >= len(s.pairs) {
		s.once.Do(func() {
			if s.onExhaust != nil {
				s.onExhaust()
			}
		})
		return nil, false, nil
	}
	pair := s.pairs[s.index]
	s.index++
	return pair, true, nil
}

func (s *backupPairScanner) Close() error { return s.closeErr }

type backupTestStream struct {
	grpc.ServerStream
	ctx     context.Context
	got     []*pb.BackupKV
	sendErr error
	onSend  func()
}

func (s *backupTestStream) Context() context.Context { return s.ctx }

func (s *backupTestStream) Send(pair *pb.BackupKV) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.got = append(s.got, pair)
	if s.onSend != nil {
		s.onSend()
	}
	return nil
}

func newBackupControlTestServer(
	t *testing.T,
	store *backupTestStore,
	groups map[uint64]*backupTestGroup,
	proposers map[uint64]*backupTestProposer,
	probe BackupPeerProbe,
	opts ...AdminOption,
) *AdminServer {
	t.Helper()
	tracker := kv.NewActiveTimestampTracker(kv.WithActiveTimestampTrackerSweepInterval(0))
	t.Cleanup(tracker.Close)
	if probe == nil {
		probe = func(context.Context, string) (BackupPeerVersion, error) {
			return BackupPeerVersion{NodeVersion: "test", BackupProtocolVersion: backupProtocolVersionV1}, nil
		}
	}
	base := []AdminOption{
		WithAdminNodeVersion("test"),
		WithAdminBackupControl(store, func(context.Context) (uint64, error) { return 42, nil }, probe, tracker, []byte("test-token-key")),
		WithAdminBackupConfig(AdminBackupConfig{SnapshotHeadroomEntries: 10, RenewAttempts: 3, RenewBackoff: time.Millisecond}),
	}
	base = append(base, opts...)
	srv := NewAdminServer(NodeIdentity{NodeID: "n1", GRPCAddress: "n1:50051"}, nil, base...)
	for id, group := range groups {
		srv.RegisterGroup(id, group)
		srv.RegisterBackupProposer(id, proposers[id])
	}
	return srv
}

func TestBeginBackupLifecycleAndBaselineAtPinnedTimestamp(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	store := &backupTestStore{
		keys: [][]byte{
			[]byte(logicalbackup.DDBTableMetaPrefix + base64.RawURLEncoding.EncodeToString([]byte("orders"))),
			[]byte(logicalbackup.RedisStringPrefix + "key"),
		},
		onExhaust: func() { group.setApplied(125) },
	}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)

	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(42), begin.GetReadTs())
	require.NotEmpty(t, begin.GetPinToken())
	require.Equal(t, []byte{
		backupSubtypeReserve, backupSubtypePin, backupSubtypeReserve, backupSubtypePin,
	}, proposer.subtypes())
	require.Len(t, begin.GetExpectedKeys(), 2)
	for _, expected := range begin.GetExpectedKeys() {
		require.Equal(t, uint64(125), expected.GetAppliedIndexAtCount())
		require.Equal(t, uint64(1), expected.GetKeyCount())
	}

	scopes, err := srv.ListAdaptersAndScopes(context.Background(), &pb.ListAdaptersAndScopesRequest{PinToken: begin.GetPinToken()})
	require.NoError(t, err)
	require.Len(t, scopes.GetScopes(), 2)

	renewed, err := srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.NoError(t, err)
	require.NotEmpty(t, renewed.GetPinToken())
	_, err = srv.EndBackup(context.Background(), &pb.EndBackupRequest{PinToken: begin.GetPinToken()})
	require.NoError(t, err)
	require.Equal(t, []byte{
		backupSubtypeReserve, backupSubtypePin, backupSubtypeReserve, backupSubtypePin,
		backupSubtypeReserve, backupSubtypePin,
		backupSubtypeRelease, backupSubtypeUnreserve,
	}, proposer.subtypes())
	for _, ts := range store.readTS {
		require.Equal(t, uint64(42), ts)
	}
}

func TestBeginBackupExpectedKeysUseRetainedCounts(t *testing.T) {
	t.Parallel()
	const table = "orders"
	activeID := "active"
	staleID := "stale"
	schema, err := encodeStoredDynamoTableSchema(&dynamoTableSchema{
		TableName:            table,
		AttributeDefinitions: map[string]string{"id": "S"},
		PrimaryKey:           dynamoKeySchema{HashKey: "id"},
		Generation:           2,
	})
	require.NoError(t, err)
	activeItem, err := encodeStoredDynamoItem(map[string]attributeValue{"id": {S: &activeID}})
	require.NoError(t, err)
	staleItem, err := encodeStoredDynamoItem(map[string]attributeValue{"id": {S: &staleID}})
	require.NoError(t, err)
	schemaKey := logicalbackup.EncodeDDBTableMetaKey(table)
	activeKey := logicalbackup.EncodeDDBItemKey(table, 2, activeID, "")
	staleKey := logicalbackup.EncodeDDBItemKey(table, 1, staleID, "")
	store := &backupTestStore{
		keys: [][]byte{staleKey, activeKey, schemaKey},
		values: map[string][]byte{
			string(schemaKey): schema,
			string(activeKey): activeItem,
			string(staleKey):  staleItem,
		},
	}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)

	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	require.Len(t, begin.GetExpectedKeys(), 1)
	require.Equal(t, "dynamodb", begin.GetExpectedKeys()[0].GetAdapter())
	require.Equal(t, table, begin.GetExpectedKeys()[0].GetScope())
	require.EqualValues(t, 2, begin.GetExpectedKeys()[0].GetKeyCount())
}

func TestSnapshotBackupGroupsExcludesReservedTSOGroup(t *testing.T) {
	t.Parallel()
	groups := map[uint64]*backupTestGroup{
		0: {status: raftengine.Status{AppliedIndex: 100}, every: 10_000},
		1: {status: raftengine.Status{AppliedIndex: 100}, every: 10_000},
	}
	proposers := map[uint64]*backupTestProposer{
		0: newBackupTestProposer(),
		1: newBackupTestProposer(),
	}
	srv := newBackupControlTestServer(t, &backupTestStore{}, groups, proposers, nil)

	got, err := srv.snapshotBackupGroups()
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, uint64(1), got[0].id)
}

func TestBeginBackupCapturesRouteSnapshotAfterReadFence(t *testing.T) {
	t.Parallel()
	stage := atomic.Int32{}
	proposer := newBackupTestProposer()
	store := &backupTestStore{}
	store.onCapture = func() {
		require.Equal(t, int32(1), stage.Load())
		require.Equal(t, []byte{backupSubtypeReserve, backupSubtypePin}, proposer.subtypes())
		stage.Store(2)
	}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	probe := func(context.Context, string) (BackupPeerVersion, error) {
		return BackupPeerVersion{NodeVersion: "test", BackupProtocolVersion: backupProtocolVersionV1}, nil
	}
	tracker := kv.NewActiveTimestampTracker(kv.WithActiveTimestampTrackerSweepInterval(0))
	t.Cleanup(tracker.Close)
	readFence := func(context.Context) (uint64, error) {
		require.True(t, stage.CompareAndSwap(0, 1))
		return 42, nil
	}
	srv := newBackupControlTestServer(
		t,
		store,
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		probe,
		WithAdminBackupControl(store, readFence, probe, tracker, []byte("test-token-key")),
	)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	require.Equal(t, int32(2), stage.Load())
	require.Equal(t, []uint64{42}, store.capturedTS)
}

func TestBeginBackupWaitsForLocalPinApplyBeforeCatalogCapture(t *testing.T) {
	store := &backupTestStore{}
	group := &backupTestGroup{status: raftengine.Status{}, every: 10_000}
	proposer := newBackupTestProposer()
	proposer.onPropose = func(subtype byte, commit uint64) {
		if subtype != backupSubtypePin {
			return
		}
		go func() {
			time.Sleep(25 * time.Millisecond)
			group.setApplied(commit)
		}()
	}
	store.onCapture = func() {
		require.GreaterOrEqual(t, group.Status().AppliedIndex, uint64(2))
	}
	srv := newBackupControlTestServer(
		t, store,
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
		WithAdminBackupConfig(AdminBackupConfig{BeginDeadline: time.Second}),
	)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
}

func TestBeginBackupCompensatesWhenPinnedRouteCaptureFails(t *testing.T) {
	t.Parallel()
	store := &backupTestStore{captureErr: stderrors.New("catalog unavailable")}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(
		t, store,
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, []byte{
		backupSubtypeReserve,
		backupSubtypePin,
		backupSubtypeRelease,
		backupSubtypeUnreserve,
	}, proposer.subtypes())
}

func TestBeginBackupRenewsWhileBaselineRuns(t *testing.T) {
	t.Parallel()
	store := &backupTestStore{
		keys:      [][]byte{[]byte(logicalbackup.RedisStringPrefix + "key")},
		scanDelay: 80 * time.Millisecond,
	}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil,
		WithAdminBackupConfig(AdminBackupConfig{DefaultTTL: 30 * time.Millisecond, MinTTL: time.Millisecond, MaxTTL: time.Second}),
	)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	var reservations int
	for _, subtype := range proposer.subtypes() {
		if subtype == backupSubtypeReserve {
			reservations++
		}
	}
	require.GreaterOrEqual(t, reservations, 3, "initial, in-baseline and final reservation renewals")
}

func TestStreamBackupUsesPinTimestampAndScopeFilter(t *testing.T) {
	t.Parallel()
	ddbKey := []byte(logicalbackup.DDBTableMetaPrefix + base64.RawURLEncoding.EncodeToString([]byte("orders")))
	redisKey := []byte(logicalbackup.RedisStringPrefix + "key")
	store := &backupTestStore{keys: [][]byte{ddbKey, redisKey}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	store.mu.Lock()
	store.valueKeys = nil
	store.mu.Unlock()

	stream := &backupTestStream{ctx: context.Background()}
	err = srv.StreamBackup(&pb.StreamBackupRequest{
		PinToken: begin.GetPinToken(),
		Scopes:   []*pb.BackupScope{{Adapter: "redis", Scope: "db_0"}},
	}, stream)
	require.NoError(t, err)
	require.Len(t, stream.got, 1)
	require.Equal(t, redisKey, stream.got[0].GetKey())
	require.Equal(t, uint64(42), store.readTS[len(store.readTS)-1])
	require.Equal(t, [][]byte{redisKey}, store.valueKeys)
}

func TestStreamBackupPreservesContextStatusAndReportsCloseErrors(t *testing.T) {
	t.Parallel()
	store := &backupTestStore{keys: [][]byte{[]byte(logicalbackup.RedisStringPrefix + "key")}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err = srv.StreamBackup(&pb.StreamBackupRequest{PinToken: begin.GetPinToken()}, &backupTestStream{ctx: canceled})
	require.Equal(t, codes.Canceled, status.Code(err))

	err = srv.StreamBackup(&pb.StreamBackupRequest{PinToken: begin.GetPinToken()}, &backupTestStream{
		ctx: context.Background(), sendErr: context.DeadlineExceeded,
	})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	store.mu.Lock()
	store.pairCloseErr = stderrors.New("close failed")
	store.mu.Unlock()
	err = srv.StreamBackup(&pb.StreamBackupRequest{PinToken: begin.GetPinToken()}, &backupTestStream{ctx: context.Background()})
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestStreamBackupStopsWhenTokenExpiresMidStream(t *testing.T) {
	t.Parallel()
	const ttl = 30 * time.Millisecond
	nowMS := atomic.Int64{}
	base := time.Unix(1_000_000, 0)
	nowMS.Store(base.UnixMilli())
	store := &backupTestStore{keys: [][]byte{
		[]byte(logicalbackup.RedisStringPrefix + "a"),
		[]byte(logicalbackup.RedisStringPrefix + "b"),
	}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil,
		WithAdminBackupConfig(AdminBackupConfig{DefaultTTL: ttl, MinTTL: time.Millisecond, MaxTTL: time.Second}),
	)
	srv.SetClock(func() time.Time { return time.UnixMilli(nowMS.Load()) })
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)

	var expireOnce sync.Once
	stream := &backupTestStream{
		ctx: context.Background(),
		onSend: func() {
			expireOnce.Do(func() {
				nowMS.Add(ttl.Milliseconds())
			})
		},
	}
	err = srv.StreamBackup(&pb.StreamBackupRequest{PinToken: begin.GetPinToken()}, stream)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "expired")
	require.Len(t, stream.got, 1)
}

func TestStreamBackupHonorsRenewedSessionDeadline(t *testing.T) {
	t.Parallel()
	const ttl = 30 * time.Millisecond
	nowMS := atomic.Int64{}
	base := time.Unix(1_000_000, 0)
	nowMS.Store(base.UnixMilli())
	store := &backupTestStore{keys: [][]byte{
		[]byte(logicalbackup.RedisStringPrefix + "a"),
		[]byte(logicalbackup.RedisStringPrefix + "b"),
	}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil,
		WithAdminBackupConfig(AdminBackupConfig{DefaultTTL: ttl, MinTTL: time.Millisecond, MaxTTL: time.Second}),
	)
	srv.SetClock(func() time.Time { return time.UnixMilli(nowMS.Load()) })
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)

	var renewOnce sync.Once
	stream := &backupTestStream{
		ctx: context.Background(),
		onSend: func() {
			renewOnce.Do(func() {
				nowMS.Store(base.Add(10 * time.Millisecond).UnixMilli())
				_, err := srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
				require.NoError(t, err)
				nowMS.Store(base.Add(ttl + time.Millisecond).UnixMilli())
			})
		},
	}
	err = srv.StreamBackup(&pb.StreamBackupRequest{PinToken: begin.GetPinToken()}, stream)
	require.NoError(t, err)
	require.Len(t, stream.got, 2)
}

func TestRenewBackupFailsWhenSessionEndsDuringRenew(t *testing.T) {
	t.Parallel()
	store := &backupTestStore{keys: [][]byte{[]byte(logicalbackup.RedisStringPrefix + "key")}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	tok, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)

	proposer.onPropose = func(subtype byte, _ uint64) {
		if subtype == backupSubtypePin {
			srv.forgetBackupSession(tok.pinID)
		}
	}
	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, proposer.subtypes(), backupSubtypeRelease)
	require.Contains(t, proposer.subtypes(), backupSubtypeUnreserve)
}

func TestStreamBackupFailsClosedWithoutPinnedRouteSnapshot(t *testing.T) {
	t.Parallel()
	store := &backupTestStore{keys: [][]byte{[]byte(logicalbackup.RedisStringPrefix + "key")}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	decoded, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)
	srv.forgetBackupSession(decoded.pinID)

	err = srv.StreamBackup(&pb.StreamBackupRequest{PinToken: begin.GetPinToken()}, &backupTestStream{ctx: context.Background()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "route snapshot is unavailable")
}

func TestListBackupScopesReportsScannerCloseError(t *testing.T) {
	t.Parallel()
	store := &backupTestStore{keys: [][]byte{[]byte(logicalbackup.RedisStringPrefix + "key")}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)

	store.mu.Lock()
	store.pairCloseErr = stderrors.New("close failed")
	store.mu.Unlock()
	_, err = srv.ListAdaptersAndScopes(context.Background(), &pb.ListAdaptersAndScopesRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestListBackupScopesFailsWhenSessionEndsDuringScan(t *testing.T) {
	t.Parallel()
	store := &backupTestStore{keys: [][]byte{[]byte(logicalbackup.RedisStringPrefix + "key")}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	decoded, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)
	store.mu.Lock()
	store.onExhaust = func() {
		srv.forgetBackupSession(decoded.pinID)
	}
	store.mu.Unlock()

	_, err = srv.ListAdaptersAndScopes(context.Background(), &pb.ListAdaptersAndScopesRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "expired")
}

func TestBackupProtocolVersionRequiresCompleteControlWiring(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(NodeIdentity{NodeID: "n1"}, nil,
		WithAdminBackupControl(nil, nil, nil, nil, nil),
	)
	resp, err := srv.GetNodeVersion(context.Background(), &pb.GetNodeVersionRequest{})
	require.NoError(t, err)
	require.Zero(t, resp.GetBackupProtocolVersion())
}

func TestBeginBackupCompensatesPartialFanout(t *testing.T) {
	t.Parallel()
	groups := map[uint64]*backupTestGroup{
		1: {status: raftengine.Status{AppliedIndex: 100}, every: 10_000},
		2: {status: raftengine.Status{AppliedIndex: 100}, every: 10_000},
	}
	p1 := newBackupTestProposer()
	p2 := newBackupTestProposer()
	p2.failures[backupSubtypePin] = 1
	p2.transportError[backupSubtypePin] = stderrors.New("leader unavailable")
	srv := newBackupControlTestServer(t, &backupTestStore{}, groups, map[uint64]*backupTestProposer{1: p1, 2: p2}, nil)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, p1.subtypes(), backupSubtypeRelease)
	require.Contains(t, p1.subtypes(), backupSubtypeUnreserve)
	require.NotContains(t, p2.subtypes(), backupSubtypeRelease)
}

func TestBeginBackupGatesOnDynamicPeerVersionAndPropagatesAuth(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{
		status: raftengine.Status{AppliedIndex: 100}, every: 10_000,
		servers: []raftengine.Server{{ID: "n1", Address: "n1:50051"}, {ID: "n4", Address: "n4:50051"}},
	}
	proposer := newBackupTestProposer()
	var gotAuthorization []string
	probe := func(ctx context.Context, address string) (BackupPeerVersion, error) {
		require.Equal(t, "n4:50051", address)
		md, _ := metadata.FromOutgoingContext(ctx)
		gotAuthorization = md.Get("authorization")
		return BackupPeerVersion{NodeVersion: "old", BackupProtocolVersion: 0}, nil
	}
	srv := newBackupControlTestServer(t, &backupTestStore{}, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, probe)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))

	_, err := srv.BeginBackup(ctx, &pb.BeginBackupRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "node n4 reports version old")
	require.Equal(t, []string{"Bearer secret"}, gotAuthorization)
	require.Empty(t, proposer.subtypes())
}

func TestBeginBackupFailsClosedOnIncompleteMembership(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{
		status: raftengine.Status{AppliedIndex: 100}, every: 10_000,
		cfgErr: stderrors.New("configuration timeout"),
	}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, &backupTestStore{}, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "backup membership for raft group 1")
	require.Empty(t, proposer.subtypes())
}

func TestBeginBackupFailsClosedOnMemberWithoutProbeAddress(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{
		status: raftengine.Status{AppliedIndex: 100}, every: 10_000,
		servers: []raftengine.Server{{ID: "n1", Address: "n1:50051"}, {ID: "n4"}},
	}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "backup membership for node n4 has no gRPC address")
	require.Empty(t, proposer.subtypes())
}

func TestBeginBackupRefusesNearSnapshotThreshold(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{
		status: raftengine.Status{AppliedIndex: 950, LastSnapshotIndex: 0}, every: 1000,
	}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, &backupTestStore{}, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil,
		WithAdminBackupConfig(AdminBackupConfig{SnapshotHeadroomEntries: 100}),
	)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "50 snapshot entries remaining")
	require.Empty(t, proposer.subtypes())
}

func TestRenewBackupRetriesAndRejectsTamperedToken(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, &backupTestStore{}, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)

	proposer.mu.Lock()
	proposer.failures[backupSubtypePin] = 2
	proposer.transportError[backupSubtypePin] = stderrors.New("election")
	proposer.mu.Unlock()
	renewed, err := srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.NoError(t, err)
	require.NotEmpty(t, renewed.GetPinToken())

	tampered := append([]byte(nil), begin.GetPinToken()...)
	tampered[len(tampered)-1] ^= 0xff
	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: tampered})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken(), TtlMs: 999})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRenewBackupWaitsForLocalPinApplyBeforePublishing(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
		WithAdminBackupConfig(AdminBackupConfig{BeginDeadline: time.Second}),
	)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	group.setApplied(0)

	pinCommit := make(chan uint64, 1)
	releaseApply := make(chan struct{})
	proposer.mu.Lock()
	proposer.onPropose = func(subtype byte, commit uint64) {
		if subtype != backupSubtypePin {
			return
		}
		pinCommit <- commit
		go func() {
			<-releaseApply
			group.setApplied(commit)
		}()
	}
	proposer.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := srv.RenewBackup(ctx, &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
		done <- err
	}()
	var commit uint64
	select {
	case commit = <-pinCommit:
	case <-ctx.Done():
		require.FailNow(t, "RenewBackup did not propose renewed pin")
	}
	select {
	case err := <-done:
		require.Failf(t, "RenewBackup returned before local apply", "err=%v", err)
	default:
	}
	close(releaseApply)
	require.NoError(t, <-done)
	require.GreaterOrEqual(t, group.Status().AppliedIndex, commit)
}

func TestBackupTokenDeadlineRotatesAndFailsClosed(t *testing.T) {
	t.Parallel()
	const ttl = 30 * time.Millisecond
	nowMS := atomic.Int64{}
	base := time.Unix(1_000_000, 0)
	nowMS.Store(base.UnixMilli())
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, &backupTestStore{}, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil,
		WithAdminBackupConfig(AdminBackupConfig{DefaultTTL: ttl, MinTTL: time.Millisecond, MaxTTL: time.Second}),
	)
	srv.SetClock(func() time.Time { return time.UnixMilli(nowMS.Load()) })

	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	decoded, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)
	require.Equal(t, base.Add(ttl), decoded.deadline)
	require.True(t, srv.nowSnapshot().Before(decoded.deadline))
	nowMS.Add((10 * time.Millisecond).Milliseconds())
	renewed, err := srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.NoError(t, err)
	require.NotEqual(t, begin.GetPinToken(), renewed.GetPinToken())

	nowMS.Add((20 * time.Millisecond).Milliseconds())
	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = srv.ListAdaptersAndScopes(context.Background(), &pb.ListAdaptersAndScopesRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	err = srv.StreamBackup(&pb.StreamBackupRequest{PinToken: begin.GetPinToken()}, &backupTestStream{ctx: context.Background()})
	require.NoError(t, err, "streams use the renewed server-side session deadline")

	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: renewed.GetPinToken()})
	require.NoError(t, err)
	_, err = srv.EndBackup(context.Background(), &pb.EndBackupRequest{PinToken: begin.GetPinToken()})
	require.NoError(t, err, "expired tokens must remain usable for cleanup")
}

func TestRenewBackupReleasesResourcesWhenTokenExpiresDuringRenewal(t *testing.T) {
	t.Parallel()
	const ttl = 30 * time.Millisecond
	nowMS := atomic.Int64{}
	base := time.Unix(1_000_000, 0)
	nowMS.Store(base.UnixMilli())
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, &backupTestStore{}, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil,
		WithAdminBackupConfig(AdminBackupConfig{DefaultTTL: ttl, MinTTL: time.Millisecond, MaxTTL: time.Second}),
	)
	srv.SetClock(func() time.Time { return time.UnixMilli(nowMS.Load()) })

	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	decoded, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)
	proposer.mu.Lock()
	proposer.onPropose = func(subtype byte, _ uint64) {
		if subtype == backupSubtypePin {
			nowMS.Store(base.Add(ttl).UnixMilli())
		}
	}
	proposer.mu.Unlock()

	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, []byte{
		backupSubtypeReserve, backupSubtypePin, backupSubtypeReserve, backupSubtypePin,
		backupSubtypeReserve, backupSubtypePin, backupSubtypeRelease, backupSubtypeUnreserve,
	}, proposer.subtypes())
	_, err = srv.backupRouteSnapshotForToken(decoded)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestRenewBackupReleasesResourcesAfterPinFanoutFailure(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(
		t,
		&backupTestStore{},
		map[uint64]*backupTestGroup{1: group},
		map[uint64]*backupTestProposer{1: proposer},
		nil,
	)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.NoError(t, err)
	decoded, err := srv.decodeBackupToken(begin.GetPinToken())
	require.NoError(t, err)
	proposer.mu.Lock()
	proposer.failures[backupSubtypePin] = 3
	proposer.transportError[backupSubtypePin] = stderrors.New("leader unavailable")
	proposer.mu.Unlock()

	_, err = srv.RenewBackup(context.Background(), &pb.RenewBackupRequest{PinToken: begin.GetPinToken()})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, []byte{
		backupSubtypeReserve, backupSubtypePin, backupSubtypeReserve, backupSubtypePin,
		backupSubtypeReserve, backupSubtypePin, backupSubtypePin, backupSubtypePin,
		backupSubtypeRelease, backupSubtypeUnreserve,
	}, proposer.subtypes())
	_, err = srv.backupRouteSnapshotForToken(decoded)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestBeginBackupMapsCapacityReservationToResourceExhausted(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	proposer.responseError[backupSubtypeReserve] = kv.ErrTooManyActiveBackups
	srv := newBackupControlTestServer(t, &backupTestStore{}, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestBeginBackupMapsForwardedCapacityReservationStatusToResourceExhausted(t *testing.T) {
	t.Parallel()
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	proposer.failures[backupSubtypeReserve] = 1
	proposer.transportError[backupSubtypeReserve] = status.Errorf(codes.ResourceExhausted, "%s", kv.ErrTooManyActiveBackups)
	srv := newBackupControlTestServer(t, &backupTestStore{}, map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)

	_, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

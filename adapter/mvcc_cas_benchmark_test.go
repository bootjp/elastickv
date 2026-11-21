package adapter

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const (
	casBenchService = "benchmark.FencedOnePhase"
	casBenchPrefix  = "/" + casBenchService + "/"
	casBenchHandle  = "bench-handle"
	casBenchClient  = "bench-client"
	casBenchMeasure = "bench-measure"
	casBenchPinTTL  = 5 * time.Second
)

type casBenchMeter struct {
	proposals, completed, readFences, verifyLeader   atomic.Uint64
	preReads, passedPrechecks, handlePinNS           atomic.Uint64
	handleExpirations, casAdmissions, readAdmissions atomic.Uint64
	applyVerdicts                                    sync.Map // typed apply reason -> *atomic.Uint64
	rpcs                                             sync.Map // method -> *atomic.Uint64
	gate                                             chan casBenchParked
}

type casBenchParked struct {
	commitTS uint64
	client   int
	release  chan struct{}
	done     chan struct{}
}

type casBenchMeterKey struct{}

func casBenchMeasured(ctx context.Context) *casBenchMeter {
	meter, _ := ctx.Value(casBenchMeterKey{}).(*casBenchMeter)
	return meter
}

// Embed the real engine so optional leases, serving checks and apply
// capabilities remain available to the production paths.
type casBenchEngine struct {
	*etcdraftengine.Engine
}

func (e *casBenchEngine) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	m := casBenchMeasured(ctx)
	if m == nil {
		return e.Engine.Propose(ctx, data)
	}
	if m.gate != nil {
		parked, err := m.parkProposal(ctx, data)
		if err != nil {
			return nil, err
		}
		defer close(parked.done)
	}
	m.proposals.Add(1)
	res, err := e.Engine.Propose(ctx, data)
	if err == nil && res != nil {
		m.completed.Add(1)
		casBenchIncrement(&m.applyVerdicts, casBenchApplyVerdict(res.Response))
	}
	return res, err
}

func (m *casBenchMeter) parkProposal(ctx context.Context, data []byte) (casBenchParked, error) {
	parked := casBenchParked{release: make(chan struct{}), done: make(chan struct{})}
	// The single-request Raft encoding is one tag byte followed by Request.
	// Parse only in the diagnostic trial, never in the benchmark.
	req, err := casBenchDecodeProposal(data)
	if err != nil || len(req.GetMutations()) == 0 {
		return casBenchParked{}, errors.New("parked trial requires one stamped 1PC request")
	}
	meta, err := kv.DecodeTxnMeta(req.GetMutations()[0].GetValue())
	if err != nil || meta.CommitTS == 0 {
		return casBenchParked{}, errors.New("parked trial could not find the 1PC commit timestamp")
	}
	parked.commitTS = meta.CommitTS
	md, _ := metadata.FromIncomingContext(ctx)
	parked.client, err = strconv.Atoi(casBenchMD(md, casBenchClient))
	if err != nil {
		return casBenchParked{}, err
	}
	select {
	case m.gate <- parked:
	case <-ctx.Done():
		return casBenchParked{}, ctx.Err()
	}
	select {
	case <-parked.release:
	case <-ctx.Done():
		return casBenchParked{}, ctx.Err()
	}
	return parked, nil
}

func casBenchIncrement(counts *sync.Map, key string) {
	counter, ok := counts.Load(key)
	if !ok {
		counter, _ = counts.LoadOrStore(key, new(atomic.Uint64))
	}
	casBenchCounter(counter).Add(1)
}

func casBenchCounter(value any) *atomic.Uint64 {
	counter, ok := value.(*atomic.Uint64)
	if !ok {
		panic("invalid CAS benchmark counter")
	}
	return counter
}

func casBenchRangeCounters(counts *sync.Map, visit func(string, uint64)) {
	counts.Range(func(key, value any) bool {
		name, ok := key.(string)
		if !ok {
			panic("invalid CAS benchmark counter name")
		}
		visit(name, casBenchCounter(value).Load())
		return true
	})
}

// Record typed FSM verdicts before the public RPC projects its error details.
func casBenchApplyVerdict(response any) string {
	if response == nil {
		return "success"
	}
	err, ok := response.(error)
	if !ok {
		return "other"
	}
	switch {
	case errors.Is(err, kv.ErrCASMismatch):
		return "exact_mismatch"
	case errors.Is(err, kv.ErrTxnLocked):
		return "locked"
	case errors.Is(err, kv.ErrRouteWriteFenced):
		return "route_fenced"
	case errors.Is(err, store.ErrWriteConflict):
		return "write_conflict"
	default:
		return "other"
	}
}

func casBenchDecodeProposal(data []byte) (*pb.Request, error) {
	if raftengine.IsVersionCASRaftPayload(data) {
		return kv.DecodeVersionCASRaftPayload(data)
	}
	req := new(pb.Request)
	if len(data) < 2 || data[0] != 0 {
		return nil, errors.New("expected a single 1PC request")
	}
	return req, proto.Unmarshal(data[1:], req)
}

func (e *casBenchEngine) LinearizableRead(ctx context.Context) (uint64, error) {
	if m := casBenchMeasured(ctx); m != nil {
		m.readFences.Add(1)
	}
	return e.Engine.LinearizableRead(ctx)
}

func (e *casBenchEngine) VerifyLeader(ctx context.Context) error {
	if m := casBenchMeasured(ctx); m != nil {
		m.verifyLeader.Add(1)
	}
	return e.Engine.VerifyLeader(ctx)
}

// Handle lifetime belongs to the load-generator session. The acquisition
// RPC's context ends with its reply, so its read pin is handed off separately.
type casBenchPin struct {
	client, key      string
	startTS, version uint64
	started          time.Time
	release          func()
	timer            *time.Timer
	meter            *casBenchMeter
}

type casBenchControl struct {
	coord   *kv.ShardedCoordinator
	shards  *kv.ShardStore
	tracker *kv.ActiveTimestampTracker
	pinTTL  time.Duration

	mu   sync.Mutex
	next uint64
	pins map[string]*casBenchPin
}

func casBenchMD(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 1 {
		return values[0]
	}
	return ""
}

func (c *casBenchControl) read(ctx context.Context, key []byte, visit func(store.VersionedValue, uint64) error) error {
	if m := casBenchMeasured(ctx); m != nil {
		m.preReads.Add(1)
	}
	read := kv.NewReadRequest(ctx, c.coord, c.shards, c.tracker)
	defer read.Close()
	snapshot, err := read.Latest(read.Context(), "benchmark fenced 1PC", key)
	if err != nil {
		return err
	}
	version, err := read.GetVersion(read.Context(), snapshot, key)
	if err != nil {
		return err
	}
	return visit(version, snapshot.Timestamp())
}

func (c *casBenchControl) issue(ctx context.Context, key []byte, startTS, version uint64) error {
	md, _ := metadata.FromIncomingContext(ctx)
	client := casBenchMD(md, casBenchClient)
	if client == "" {
		return status.Error(codes.InvalidArgument, "missing benchmark client")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.next++
	id := strconv.FormatUint(c.next, 10)
	pin := &casBenchPin{
		client: client, key: string(key), startTS: startTS, version: version,
		started: time.Now(), release: c.tracker.Pin(startTS).Release, meter: casBenchMeasured(ctx),
	}
	pin.timer = time.AfterFunc(c.pinTTL, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.pins[id] != nil {
			c.release(id, pin)
			if pin.meter != nil {
				pin.meter.handleExpirations.Add(1)
			}
		}
	})
	c.pins[id] = pin
	c.mu.Unlock()
	if err := grpc.SetHeader(ctx, metadata.Pairs(casBenchHandle, id)); err != nil {
		c.mu.Lock()
		c.release(id, pin)
		c.mu.Unlock()
		return err
	}
	return nil
}

// release is called under mu. Only the caller that removes the live handle
// observes the lifetime and releases the pin.
func (c *casBenchControl) release(id string, pin *casBenchPin) {
	if c.pins[id] != pin {
		return
	}
	delete(c.pins, id)
	pin.timer.Stop()
	pin.release()
	if pin.meter != nil {
		pin.meter.handlePinNS.Add(uint64(time.Since(pin.started)))
	}
}

func (c *casBenchControl) take(ctx context.Context, req *pb.CompareAndSwapRequest) (*casBenchPin, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	id := casBenchMD(md, casBenchHandle)
	c.mu.Lock()
	defer c.mu.Unlock()
	pin := c.pins[id]
	if pin == nil || pin.client != casBenchMD(md, casBenchClient) || pin.key != string(req.Key) || pin.version != req.ExpectedCommitTs {
		return nil, status.Error(codes.FailedPrecondition, "invalid or expired benchmark handle")
	}
	if pin.meter == nil {
		pin.meter = casBenchMeasured(ctx)
	}
	c.release(id, pin)
	if time.Since(pin.started) >= c.pinTTL {
		if pin.meter != nil {
			pin.meter.handleExpirations.Add(1)
		}
		return nil, status.Error(codes.FailedPrecondition, "expired benchmark handle")
	}
	return pin, nil
}

func (c *casBenchControl) acquire(ctx context.Context, req *pb.CompareAndSwapRequest) (*pb.GetResponse, error) {
	resp := &pb.GetResponse{}
	err := c.read(ctx, req.Key, func(v store.VersionedValue, startTS uint64) error {
		resp.Value = v.Value
		resp.Version = &pb.GetResponse_CommitTs{CommitTs: v.TS}
		if req.ExpectedCommitTs != 0 && req.ExpectedCommitTs != v.TS {
			return nil
		}
		if m := casBenchMeasured(ctx); m != nil {
			m.passedPrechecks.Add(1)
		}
		return c.issue(ctx, req.Key, startTS, v.TS)
	})
	return resp, err
}

func casBenchMismatch() *pb.CompareAndSwapResponse {
	return &pb.CompareAndSwapResponse{Result: &pb.CompareAndSwapResponse_Mismatch{Mismatch: true}}
}

func (c *casBenchControl) submit(ctx context.Context, req *pb.CompareAndSwapRequest, startTS uint64) (*pb.CompareAndSwapResponse, error) {
	reply, err := kv.DispatchWithLeaderIssuedCommitTS(ctx, c.coord, &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: startTS, ReadKeys: [][]byte{req.Key},
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: req.Key, Value: req.Value}},
	})
	if err != nil {
		return nil, casBenchControlError(err)
	}
	return &pb.CompareAndSwapResponse{Result: &pb.CompareAndSwapResponse_NewCommitTs{NewCommitTs: reply.CommitTS}}, nil
}

func casBenchControlError(err error) error {
	switch {
	case errors.Is(err, kv.ErrTransactionOutcomeUnknown):
		return status.Error(codes.Unknown, "1PC submission outcome unknown")
	case errors.Is(err, kv.ErrTxnLocked):
		return status.Error(codes.Aborted, "1PC lock conflict")
	case errors.Is(err, kv.ErrRouteWriteFenced), errors.Is(err, raftengine.ErrNotLeader):
		return status.Error(codes.Unavailable, "1PC route or leadership rejection")
	case errors.Is(err, store.ErrWriteConflict):
		return status.Error(codes.Aborted, "1PC write conflict")
	default:
		return err
	}
}

func (c *casBenchControl) fresh(ctx context.Context, req *pb.CompareAndSwapRequest) (*pb.CompareAndSwapResponse, error) {
	var start uint64
	err := c.read(ctx, req.Key, func(v store.VersionedValue, startTS uint64) error {
		if v.TS == req.ExpectedCommitTs {
			start = startTS
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if start == 0 {
		return casBenchMismatch(), nil
	}
	if m := casBenchMeasured(ctx); m != nil {
		m.passedPrechecks.Add(1)
	}
	return c.submit(ctx, req, start)
}

func (c *casBenchControl) commit(ctx context.Context, req *pb.CompareAndSwapRequest) (*pb.CompareAndSwapResponse, error) {
	pin, err := c.take(ctx, req)
	if err != nil {
		return nil, err
	}
	return c.submit(ctx, req, pin.startTS)
}

func (c *casBenchControl) cancel(ctx context.Context, req *pb.CompareAndSwapRequest) (*pb.CompareAndSwapResponse, error) {
	_, err := c.take(ctx, req)
	return &pb.CompareAndSwapResponse{}, err
}

func casBenchMethod[T any](name string, call func(context.Context, *pb.CompareAndSwapRequest) (*T, error)) grpc.MethodDesc {
	return grpc.MethodDesc{
		MethodName: name,
		Handler: func(srv any, ctx context.Context, decode func(any) error, intercept grpc.UnaryServerInterceptor) (any, error) {
			req := new(pb.CompareAndSwapRequest)
			if err := decode(req); err != nil {
				return nil, err
			}
			return intercept(ctx, req, &grpc.UnaryServerInfo{Server: srv, FullMethod: casBenchPrefix + name},
				func(ctx context.Context, req any) (any, error) {
					request, ok := req.(*pb.CompareAndSwapRequest)
					if !ok {
						panic("invalid CAS benchmark request type")
					}
					return call(ctx, request)
				})
		},
	}
}

type casBenchFixture struct {
	control *casBenchControl
	conn    *grpc.ClientConn
	public  pb.TransactionalKVClient
	local   store.MVCCStore
	meter   *casBenchMeter
}

func casBenchLocalProof(ctx context.Context, engine *casBenchEngine, local store.MVCCStore) (context.Context, error) {
	st := engine.Status()
	cfg, err := engine.Configuration(ctx)
	if err != nil {
		return nil, err
	}
	if st.State != raftengine.StateLeader || st.Term == 0 || st.ConfigurationIndex == 0 ||
		st.PendingConfChange || !casBenchSingleVoter(cfg, st.Leader.ID) {
		return nil, kv.ErrVersionCASUnavailable
	}
	active, err := kv.CommitTSFenceEnabled(ctx, local)
	if err != nil || !active {
		return nil, errors.Join(kv.ErrVersionCASUnavailable, err)
	}
	return raftengine.WithVersionCASProof(ctx, raftengine.VersionCASProof{
		Term: st.Term, ConfigurationIndex: st.ConfigurationIndex,
	}), nil
}

func casBenchSingleVoter(cfg raftengine.Configuration, leaderID string) bool {
	return len(cfg.Servers) == 1 && cfg.Servers[0].ID == leaderID &&
		cfg.Servers[0].Suffrage == etcdraftengine.SuffrageVoter
}

func newCASBenchFixture(t testing.TB, parked bool) *casBenchFixture {
	t.Helper()
	t.Setenv("ELASTICKV_FSM_SYNC_MODE", "sync")
	routes := distribution.NewEngine()
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{
		{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive},
	}}))
	local, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	hlc := kv.NewHLC()
	tracker := kv.NewActiveTimestampTracker()
	factory := etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval: 10 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10,
		MaxSizePerMsg: 1 << 20, MaxInflightMsg: 256,
	})
	node, err := factory.Create(raftengine.FactoryConfig{
		LocalID: "cas-bench-1", LocalAddress: "cas-bench-1", DataDir: t.TempDir(), Bootstrap: true,
		StateMachine: kv.NewKvFSMWithHLCAndTracker(local, hlc, tracker, kv.WithRouteHistory(kv.WrapDistributionEngine(routes), 1),
			kv.WithRaftBootstrapIdentity(raftbootstrap.UUID{1}, 1)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Engine.Close())
		if node.Close != nil {
			require.NoError(t, node.Close())
		}
	})
	require.Eventually(t, func() bool { return node.Engine.State() == raftengine.StateLeader }, 5*time.Second, 10*time.Millisecond)
	raft, ok := node.Engine.(*etcdraftengine.Engine)
	require.True(t, ok, "unexpected benchmark engine type %T", node.Engine)
	engine := &casBenchEngine{Engine: raft}
	proxy := kv.NewLeaderProxyWithEngine(engine)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })
	groups := map[uint64]*kv.ShardGroup{1: {Engine: engine, Store: local, Txn: proxy}}
	shards := kv.NewShardStore(routes, groups)
	coord := kv.NewShardedCoordinator(routes, groups, 1, hlc, shards)
	t.Cleanup(func() { require.NoError(t, coord.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	activation, err := engine.Propose(ctx, kv.EncodeCommitTSFenceActivation())
	require.NoError(t, err)
	require.NotNil(t, activation)
	require.Nil(t, activation.Response)
	replicateCASFixtureCatalog(t, ctx, engine, local, 1)
	active, err := kv.CommitTSFenceEnabled(ctx, local)
	require.NoError(t, err)
	require.True(t, active)
	proofCtx, err := casBenchLocalProof(ctx, engine, local)
	require.NoError(t, err)
	identity, err := engine.Propose(proofCtx, kv.EncodeMigrationVersionIdentityActivation())
	require.NoError(t, err)
	require.NotNil(t, identity)
	require.Nil(t, identity.Response)
	identityActive, err := kv.MigrationVersionIdentityEnabled(ctx, local)
	require.NoError(t, err)
	require.True(t, identityActive)
	casBenchConfigureAdmission(engine, local, proxy, groups[1])
	require.NoError(t, coord.RecoverHLCLease(ctx))
	renewCtx, stopRenew := context.WithCancel(context.Background())
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		coord.RunHLCLeaseRenewal(renewCtx)
	}()
	t.Cleanup(func() { stopRenew(); <-renewDone })

	control := &casBenchControl{coord: coord, shards: shards, tracker: tracker, pinTTL: casBenchPinTTL, pins: make(map[string]*casBenchPin)}
	t.Cleanup(func() {
		control.mu.Lock()
		defer control.mu.Unlock()
		for id, pin := range control.pins {
			control.release(id, pin)
		}
	})
	meter := &casBenchMeter{}
	if parked {
		meter.gate = make(chan casBenchParked)
	}
	conn := casBenchConnect(t, control, meter)
	return &casBenchFixture{control: control, conn: conn, public: pb.NewTransactionalKVClient(conn), local: local, meter: meter}
}

func casBenchAdmit(ctx context.Context, engine *casBenchEngine, local store.MVCCStore) (context.Context, error) {
	owned, err := casBenchLocalProof(ctx, engine, local)
	if err != nil {
		return nil, err
	}
	active, err := kv.MigrationVersionIdentityEnabled(owned, local)
	if err != nil || !active {
		return nil, errors.Join(kv.ErrVersionCASUnavailable, err)
	}
	return owned, nil
}

func casBenchConfigureAdmission(engine *casBenchEngine, local store.MVCCStore, proxy *kv.LeaderProxy, group *kv.ShardGroup) {
	proxy.ConfigureVersionCASAdmission(func(ctx context.Context) (context.Context, error) {
		if m := casBenchMeasured(ctx); m != nil {
			m.casAdmissions.Add(1)
		}
		return casBenchAdmit(ctx, engine, local)
	})
	group.ConfigureVersionReadAdmission(func(ctx context.Context) error {
		if m := casBenchMeasured(ctx); m != nil {
			m.readAdmissions.Add(1)
		}
		owned, err := casBenchAdmit(ctx, engine, local)
		if err != nil {
			return err
		}
		proof, _ := raftengine.VersionCASProposalProof(owned)
		st := engine.Status()
		if st.State != raftengine.StateLeader || st.Term != proof.Term ||
			st.ConfigurationIndex != proof.ConfigurationIndex || st.PendingConfChange {
			return kv.ErrVersionCASUnavailable
		}
		return nil
	})
}

func casBenchConnect(t testing.TB, control *casBenchControl, meter *casBenchMeter) *grpc.ClientConn {
	t.Helper()
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if casBenchMD(md, casBenchMeasure) != "" {
			ctx = context.WithValue(ctx, casBenchMeterKey{}, meter)
		}
		return next(ctx, req)
	}))
	public := NewGRPCServer(control.shards, control.coord)
	public.readTracker = control.tracker
	pb.RegisterTransactionalKVServer(server, public)
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: casBenchService, HandlerType: (*interface{})(nil),
		Methods: []grpc.MethodDesc{
			casBenchMethod("Acquire", control.acquire), casBenchMethod("Fresh", control.fresh),
			casBenchMethod("Commit", control.commit), casBenchMethod("Cancel", control.cancel),
		},
	}, control)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///cas-bench",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
			invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			md, _ := metadata.FromOutgoingContext(ctx)
			if casBenchMD(md, casBenchMeasure) != "" {
				casBenchIncrement(&meter.rpcs, method)
			}
			return invoke(ctx, method, req, reply, cc, opts...)
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

type casBenchArm string

const (
	casBenchCAS      casBenchArm = "CAS"
	casBenchFresh    casBenchArm = "Fresh"
	casBenchTwoCall  casBenchArm = "TwoCall"
	casBenchRetained casBenchArm = "Retained"
)

var casBenchArms = []casBenchArm{casBenchCAS, casBenchFresh, casBenchTwoCall, casBenchRetained}

type casBenchWorkload struct {
	name      string
	workers   int
	stale     int // number of disjoint keys advanced after its original read
	hot       bool
	inclusive bool
}

type casBenchAttempt struct {
	key, value     []byte
	client, handle string
	expected       uint64
	before         uint64
	injectedStale  bool
}

func (f *casBenchFixture) acquire(ctx context.Context, attempt casBenchAttempt, expected uint64) (uint64, string, error) {
	var header metadata.MD
	resp := &pb.GetResponse{}
	err := f.conn.Invoke(ctx, casBenchPrefix+"Acquire",
		&pb.CompareAndSwapRequest{Key: attempt.key, ExpectedCommitTs: expected}, resp, grpc.Header(&header))
	return resp.GetCommitTs(), casBenchMD(header, casBenchHandle), err
}

func (f *casBenchFixture) get(ctx context.Context, key []byte) (*pb.GetResponse, error) {
	return f.public.Get(ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
}

func (f *casBenchFixture) seed(t testing.TB, ctx context.Context, key, value []byte) uint64 {
	t.Helper()
	r, err := kv.DispatchWithLeaderIssuedCommitTS(ctx, f.control.coord, &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: max(f.local.LastCommitTS(), 1),
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: value}},
	})
	require.NoError(t, err)
	require.Positive(t, r.CommitTS)
	return r.CommitTS
}

func (f *casBenchFixture) prepare(t testing.TB, arm casBenchArm, workload casBenchWorkload, round int) []casBenchAttempt {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempts := make([]casBenchAttempt, workload.workers)
	initial := bytes.Repeat([]byte{'a'}, 1024)
	for i := range attempts {
		keyIndex := i
		if workload.hot {
			keyIndex = 0
		}
		key := fmt.Appendf(nil, "cas-bench/%04d", keyIndex)
		var version uint64
		if workload.hot && i > 0 {
			version = attempts[0].expected
		} else {
			version = f.seed(t, ctx, key, initial)
		}
		value := bytes.Clone(initial)
		binary.BigEndian.PutUint64(value, uint64(round))
		binary.BigEndian.PutUint64(value[8:], uint64(i))
		attempt := casBenchAttempt{
			key: key, value: value, client: strconv.Itoa(i),
			expected: version, before: version, injectedStale: i < workload.stale,
		}
		if !workload.inclusive {
			if arm == casBenchRetained {
				readCtx := metadata.AppendToOutgoingContext(ctx, casBenchClient, attempt.client)
				observed, handle, err := f.acquire(readCtx, attempt, 0)
				require.NoError(t, err)
				require.Equal(t, version, observed)
				require.NotEmpty(t, handle)
				attempt.handle = handle
			} else {
				observed, err := f.get(ctx, key)
				require.NoError(t, err)
				require.Equal(t, version, observed.GetCommitTs())
				require.Equal(t, initial, observed.GetValue())
			}
		}
		if attempt.injectedStale {
			// Rewrite identical bytes. The old token has become stale without
			// changing the logical value or creating a racing writer.
			attempt.before = f.seed(t, ctx, key, initial)
			require.Greater(t, attempt.before, version)
		}
		attempts[i] = attempt
	}
	return attempts
}

type casBenchResult struct {
	latency time.Duration
	version uint64
	reason  string
	err     error
}

func (f *casBenchFixture) attempt(ctx context.Context, arm casBenchArm, a casBenchAttempt, inclusive bool) casBenchResult {
	ctx = metadata.AppendToOutgoingContext(ctx, casBenchMeasure, "1", casBenchClient, a.client)
	if inclusive && arm != casBenchRetained {
		observed, err := f.get(ctx, a.key)
		if err != nil {
			return casBenchResult{reason: "read_error", err: err}
		}
		if observed.GetCommitTs() != a.expected {
			return casBenchResult{reason: "read_error", err: errors.New("unexpected acquisition version")}
		}
	}
	if arm == casBenchTwoCall || (arm == casBenchRetained && inclusive) {
		handle, result := f.acquireForAttempt(ctx, a)
		if result.reason != "" {
			return result
		}
		a.handle = handle
	}
	resp, err := f.submitAttempt(ctx, arm, a)
	return casBenchAttemptResult(arm, a.expected, resp, err)
}

func casBenchAttemptResult(arm casBenchArm, expected uint64, resp *pb.CompareAndSwapResponse, err error) casBenchResult {
	if err != nil {
		return casBenchResult{reason: casBenchRejection(arm, err), err: err}
	}
	if resp.GetNewCommitTs() > expected {
		return casBenchResult{version: resp.GetNewCommitTs(), reason: "applied"}
	}
	if resp.GetMismatch() {
		if arm == casBenchCAS {
			return casBenchResult{reason: "exact_mismatch"}
		}
		return casBenchResult{reason: "precheck_mismatch"}
	}
	return casBenchResult{reason: "invalid_reply", err: errors.New("missing outcome or non-advancing version")}
}

func (f *casBenchFixture) acquireForAttempt(ctx context.Context, a casBenchAttempt) (string, casBenchResult) {
	version, handle, err := f.acquire(ctx, a, a.expected)
	if err != nil {
		return "", casBenchResult{reason: "read_error", err: err}
	}
	if version == 0 {
		return "", casBenchResult{reason: "read_error", err: errors.New("acquisition omitted version")}
	}
	if version != a.expected {
		return "", casBenchResult{reason: "precheck_mismatch"}
	}
	if handle == "" {
		return "", casBenchResult{reason: "read_error", err: errors.New("acquisition omitted handle")}
	}
	return handle, casBenchResult{}
}

func (f *casBenchFixture) submitAttempt(ctx context.Context, arm casBenchArm, a casBenchAttempt) (*pb.CompareAndSwapResponse, error) {
	req := &pb.CompareAndSwapRequest{Key: a.key, ExpectedCommitTs: a.expected, Value: a.value}
	resp := &pb.CompareAndSwapResponse{}
	var err error
	switch arm {
	case casBenchCAS:
		resp, err = f.public.CompareAndSwap(ctx, req)
	case casBenchFresh:
		err = f.conn.Invoke(ctx, casBenchPrefix+"Fresh", req, resp)
	case casBenchTwoCall, casBenchRetained:
		handleCtx := metadata.AppendToOutgoingContext(ctx, casBenchHandle, a.handle)
		err = f.conn.Invoke(handleCtx, casBenchPrefix+"Commit", req, resp)
		if code := status.Code(err); code == codes.Canceled || code == codes.DeadlineExceeded {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cleanupCtx = metadata.AppendToOutgoingContext(cleanupCtx, casBenchMeasure, "1", casBenchClient, a.client, casBenchHandle, a.handle)
			_ = f.conn.Invoke(cleanupCtx, casBenchPrefix+"Cancel", req, &pb.CompareAndSwapResponse{})
		}
	}
	return resp, err
}

func casBenchRejection(arm casBenchArm, err error) string {
	st := status.Convert(err)
	switch st.Message() {
	case "1PC lock conflict":
		return "lock"
	case "1PC route or leadership rejection":
		return "route"
	case "1PC write conflict":
		// Existing 1PC uses the same error type for its timestamp fence and
		// OCC. The parked trial disambiguates by ordering known stamps.
		return "one_phase_conflict"
	case "invalid or expired benchmark handle", "expired benchmark handle":
		return "handle_rejected"
	}
	return casBenchStatusRejection(arm, st)
}

func casBenchStatusRejection(arm casBenchArm, st *status.Status) string {
	switch st.Code() {
	case codes.DeadlineExceeded, codes.Canceled:
		return "deadline_unknown"
	case codes.Aborted:
		if arm != casBenchCAS {
			return "aborted"
		}
		if casBenchCASLocked(st) {
			return "lock"
		}
		return "cas_timestamp_conflict"
	case codes.Unavailable:
		return "unavailable"
	case codes.FailedPrecondition:
		return "not_admitted"
	case codes.OK, codes.Unknown, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
		codes.PermissionDenied, codes.ResourceExhausted, codes.OutOfRange, codes.Unimplemented,
		codes.Internal, codes.DataLoss, codes.Unauthenticated:
		return "unknown"
	default:
		return "unknown"
	}
}

func casBenchCASLocked(st *status.Status) bool {
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && info.GetDomain() == publicGRPCFailureDomain && info.GetReason() == "CAS_LOCKED" {
			return true
		}
	}
	return false
}

func (f *casBenchFixture) run(t testing.TB, arm casBenchArm, workload casBenchWorkload, attempts []casBenchAttempt) []casBenchResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make([]casBenchResult, len(attempts))
	if workload.stale > 0 {
		// Keep injected staleness independent of the shard-wide timestamp
		// ordering race. The hot-key cells exercise that race separately.
		for i, attempt := range attempts {
			issued := time.Now()
			results[i] = f.attempt(ctx, arm, attempt, workload.inclusive)
			results[i].latency = time.Since(issued)
		}
		return results
	}
	var released time.Time
	var ready, done sync.WaitGroup
	ready.Add(len(attempts))
	done.Add(len(attempts))
	for i, attempt := range attempts {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			results[i] = f.attempt(ctx, arm, attempt, workload.inclusive)
			results[i].latency = time.Since(released)
		}()
	}
	ready.Wait()
	released = time.Now()
	close(start)
	var order []casBenchParked
	if f.meter.gate != nil {
		var err error
		order, err = f.releaseParked(ctx, len(attempts), workload.name == "parked-reversed")
		if err != nil {
			cancel()
			done.Wait()
			t.Fatalf("parked proposals: %v (results: %+v)", err, results)
		}
	}
	done.Wait()
	if len(order) > 0 {
		casBenchVerifyParked(t, arm, order, results)
	}
	return results
}

func casBenchVerifyParked(t testing.TB, arm casBenchArm, order []casBenchParked, results []casBenchResult) {
	t.Helper()
	winner := order[0]
	require.Equal(t, winner.commitTS, results[winner.client].version)
	for _, loser := range order[1:] {
		got := results[loser.client].reason
		switch {
		case arm != casBenchCAS:
			require.Equal(t, "one_phase_conflict", got)
			if loser.commitTS <= winner.commitTS {
				got = "timestamp_order"
			} else {
				got = "occ_conflict"
			}
		case loser.commitTS <= winner.commitTS:
			require.Equal(t, "cas_timestamp_conflict", got)
			got = "timestamp_order"
		default:
			require.Equal(t, "exact_mismatch", got)
		}
		results[loser.client].reason = got
	}
}

func (f *casBenchFixture) releaseParked(ctx context.Context, count int, reverse bool) ([]casBenchParked, error) {
	parked := make([]casBenchParked, count)
	for i := range parked {
		select {
		case parked[i] = <-f.meter.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	slices.SortFunc(parked, func(a, b casBenchParked) int { return cmp.Compare(a.commitTS, b.commitTS) })
	for i := 1; i < len(parked); i++ {
		if parked[i].commitTS <= parked[i-1].commitTS {
			return nil, errors.New("non-increasing 1PC timestamps")
		}
	}
	if reverse {
		slices.Reverse(parked)
	}
	for _, contender := range parked {
		close(contender.release)
		select {
		case <-contender.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return parked, nil
}

// Reconcile replies against all versions appended since the setup watermark,
// including invisible extra writes that a final Get alone could miss.
func (f *casBenchFixture) verify(t testing.TB, arm casBenchArm, attempts []casBenchAttempt, results []casBenchResult) {
	t.Helper()
	type keyHistory struct {
		before  uint64
		applied []store.MVCCVersion
	}
	history := make(map[string]*keyHistory)
	for i, a := range attempts {
		key := string(a.key)
		if history[key] == nil {
			history[key] = &keyHistory{before: a.before}
		}
		r := results[i]
		switch r.reason {
		case "applied":
			require.False(t, a.injectedStale, "stale token applied")
			require.Greater(t, r.version, a.before)
			history[key].applied = append(history[key].applied, store.MVCCVersion{CommitTS: r.version, Value: a.value})
		case "exact_mismatch", "precheck_mismatch", "one_phase_conflict", "cas_timestamp_conflict", "timestamp_order", "occ_conflict":
			if a.injectedStale {
				require.Equal(t, casBenchStaleReason(arm), r.reason)
			}
		default:
			t.Fatalf("%s: unexpected outcome %s on key %s: %v", arm, r.reason, a.key, r.err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for key, h := range history {
		f.verifyHistory(t, ctx, key, h.before, h.applied)
	}
}

func casBenchStaleReason(arm casBenchArm) string {
	switch arm {
	case casBenchCAS:
		return "exact_mismatch"
	case casBenchRetained:
		return "one_phase_conflict"
	case casBenchFresh, casBenchTwoCall:
		return "precheck_mismatch"
	default:
		return "precheck_mismatch"
	}
}

func (f *casBenchFixture) verifyHistory(t testing.TB, ctx context.Context, key string, before uint64, applied []store.MVCCVersion) {
	t.Helper()
	const (
		maxWinners  = 1
		maxVersions = 1 + maxWinners + 1 // Seed, winner, and room to detect an extra version.
	)
	require.LessOrEqual(t, len(applied), maxWinners, "multiple winners at one version for %s", key)
	versions, err := f.local.ExportVersions(ctx, store.ExportVersionsOptions{
		StartKey: []byte(key), EndKey: append([]byte(key), 0), MinCommitTSExclusive: before - 1,
		MaxVersions: maxVersions,
	})
	require.NoError(t, err)
	require.True(t, versions.Done)
	require.Len(t, versions.Versions, 1+len(applied))
	byTimestamp := make(map[uint64]store.MVCCVersion, len(versions.Versions))
	for _, version := range versions.Versions {
		require.False(t, version.Tombstone)
		byTimestamp[version.CommitTS] = version
	}
	require.Equal(t, bytes.Repeat([]byte{'a'}, 1024), byTimestamp[before].Value)
	for _, version := range applied {
		require.Equal(t, version.Value, byTimestamp[version.CommitTS].Value)
	}
}

func TestMVCCCASContention(t *testing.T) {
	for _, arm := range casBenchArms {
		t.Run(string(arm), func(t *testing.T) {
			for _, workload := range []casBenchWorkload{
				{name: "known", workers: 1},
				{name: "stale", workers: 8, stale: 4},
				{name: "hot", workers: 8, hot: true},
				{name: "parked", workers: 8, hot: true},
				{name: "parked-reversed", workers: 8, hot: true},
			} {
				t.Run(workload.name, func(t *testing.T) {
					checkCASBenchContention(t, arm, workload)
				})
			}
		})
	}
}

func checkCASBenchContention(t *testing.T, arm casBenchArm, workload casBenchWorkload) {
	t.Helper()
	parked := strings.HasPrefix(workload.name, "parked")
	f := newCASBenchFixture(t, parked)
	outcomes := make(map[string]int)
	for round := range 3 {
		attempts := f.prepare(t, arm, workload, round)
		results := f.run(t, arm, workload, attempts)
		f.verify(t, arm, attempts, results)
		successes := 0
		for _, result := range results {
			outcomes[result.reason]++
			if result.reason == "applied" {
				successes++
			}
		}
		if workload.hot {
			require.Equal(t, 1, successes)
		} else {
			require.Equal(t, workload.workers-workload.stale, successes)
			proposals := workload.workers
			if arm == casBenchFresh || arm == casBenchTwoCall {
				proposals -= workload.stale
			}
			require.EqualValues(t, (round+1)*proposals, f.meter.proposals.Load())
			rpcs := workload.workers
			if arm == casBenchTwoCall {
				rpcs += successes
			}
			require.EqualValues(t, (round+1)*rpcs, f.meter.rpcCount())
		}
	}
	if parked {
		require.EqualValues(t, 3*workload.workers, f.meter.proposals.Load())
		require.Equal(t, f.meter.proposals.Load(), f.meter.completed.Load())
	}
	t.Logf("rpc=%d proposals=%d completed=%d prechecks_passed=%d outcomes=%v", f.meter.rpcCount(), f.meter.proposals.Load(), f.meter.completed.Load(), f.meter.passedPrechecks.Load(), outcomes)
	require.Zero(t, f.control.tracker.Oldest(), "all read and retained-handle pins must be released")
}

func TestMVCCCASContentionHandles(t *testing.T) {
	f := newCASBenchFixture(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	key := []byte("cas-bench/handle")
	version := f.seed(t, ctx, key, []byte("initial"))
	a := casBenchAttempt{key: key}
	clientCtx := metadata.AppendToOutgoingContext(ctx, casBenchClient, "owner", casBenchMeasure, "1")
	bind := func() string {
		observed, handle, err := f.acquire(clientCtx, a, version)
		require.NoError(t, err)
		require.Equal(t, version, observed)
		require.NotEmpty(t, handle)
		return handle
	}

	handle := bind()
	req := &pb.CompareAndSwapRequest{Key: key, ExpectedCommitTs: version}
	wrongClient := metadata.AppendToOutgoingContext(ctx, casBenchClient, "other", casBenchHandle, handle)
	err := f.conn.Invoke(wrongClient, casBenchPrefix+"Commit", req, &pb.CompareAndSwapResponse{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Positive(t, f.control.tracker.Oldest(), "another client cannot consume a handle")
	owner := metadata.AppendToOutgoingContext(clientCtx, casBenchHandle, handle)
	require.NoError(t, f.conn.Invoke(owner, casBenchPrefix+"Cancel", req, &pb.CompareAndSwapResponse{}))
	err = f.conn.Invoke(owner, casBenchPrefix+"Commit", req, &pb.CompareAndSwapResponse{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, f.control.tracker.Oldest())

	// Shorten only this diagnostic's deadline; all workload cells use five seconds.
	f.control.mu.Lock()
	f.control.pinTTL = 50 * time.Millisecond
	f.control.mu.Unlock()
	expired := metadata.AppendToOutgoingContext(clientCtx, casBenchHandle, bind())
	require.Eventually(t, func() bool {
		return f.meter.handleExpirations.Load() == 1
	}, time.Second, 5*time.Millisecond)
	err = f.conn.Invoke(expired, casBenchPrefix+"Commit", req, &pb.CompareAndSwapResponse{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, f.control.tracker.Oldest())
	require.Zero(t, f.meter.proposals.Load())
}

func (m *casBenchMeter) rpcCount() uint64 {
	var total uint64
	m.rpcs.Range(func(_, value any) bool {
		total += casBenchCounter(value).Load()
		return true
	})
	return total
}

func BenchmarkMVCCCASContention(b *testing.B) {
	workloads := []casBenchWorkload{
		{name: "known", workers: 1},
		{name: "known-inclusive", workers: 1, inclusive: true},
		{name: "stale50", workers: 8, stale: 4},
		{name: "stale100", workers: 8, stale: 8},
		{name: "hot2", workers: 2, hot: true},
		{name: "hot8", workers: 8, hot: true},
		{name: "hot32", workers: 32, hot: true},
	}
	for _, workload := range workloads {
		for _, arm := range casBenchArms {
			b.Run(workload.name+"/"+string(arm), func(b *testing.B) {
				b.StopTimer()
				f := newCASBenchFixture(b, false)
				outcomes := make(map[string]int)
				var all, applied []time.Duration
				for round := range b.N {
					attempts := f.prepare(b, arm, workload, round)
					b.StartTimer()
					results := f.run(b, arm, workload, attempts)
					b.StopTimer()
					f.verify(b, arm, attempts, results)
					for _, result := range results {
						outcomes[result.reason]++
						all = append(all, result.latency)
						if result.reason == "applied" {
							applied = append(applied, result.latency)
						}
					}
				}
				f.report(b, workload, outcomes)
				casBenchLatency(b, "all", all)
				casBenchLatency(b, "applied", applied)
			})
		}
	}
}

func (f *casBenchFixture) report(b *testing.B, workload casBenchWorkload, outcomes map[string]int) {
	attempts := float64(b.N * workload.workers)
	successes := float64(outcomes["applied"])
	proposals := float64(f.meter.proposals.Load())
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/attempts, "ns/attempt")
	b.ReportMetric(float64(workload.stale)/float64(workload.workers), "injected_stale/attempt")
	b.ReportMetric(proposals, "proposals_total")
	b.ReportMetric(proposals/attempts, "proposals/attempt")
	b.ReportMetric(float64(f.meter.completed.Load())/attempts, "proposal_replies/attempt")
	b.ReportMetric(float64(f.meter.readFences.Load())/attempts, "applied_fences/attempt")
	b.ReportMetric(float64(f.meter.verifyLeader.Load())/attempts, "leader_checks/attempt")
	b.ReportMetric(float64(f.meter.preReads.Load())/attempts, "control_reads/attempt")
	b.ReportMetric(float64(f.meter.passedPrechecks.Load())/attempts, "precheck_passes/attempt")
	b.ReportMetric(float64(f.meter.handlePinNS.Load())/attempts, "handle_pin_ns/attempt")
	b.ReportMetric(float64(f.meter.handleExpirations.Load()), "handle_expirations_total")
	b.ReportMetric(float64(f.meter.casAdmissions.Load())/attempts, "cas_admission_checks/attempt")
	b.ReportMetric(float64(f.meter.readAdmissions.Load())/attempts, "version_read_admission_checks/attempt")
	casBenchRangeCounters(&f.meter.applyVerdicts, func(reason string, count uint64) {
		b.ReportMetric(float64(count)/attempts, "fsm_"+reason+"/attempt")
	})
	var rpcs float64
	casBenchRangeCounters(&f.meter.rpcs, func(method string, count uint64) {
		b.ReportMetric(float64(count)/attempts, method[strings.LastIndexByte(method, '/')+1:]+"_rpc/attempt")
		rpcs += float64(count)
	})
	b.ReportMetric(rpcs/attempts, "rpc/attempt")
	if successes > 0 {
		b.ReportMetric(successes/b.Elapsed().Seconds(), "applied/s")
		b.ReportMetric(proposals/successes, "proposals/success")
		b.ReportMetric(rpcs/successes, "rpc/success")
	}
	for reason, count := range outcomes {
		b.ReportMetric(float64(count)/attempts, reason+"/attempt")
	}
}

func casBenchLatency(b *testing.B, name string, samples []time.Duration) {
	if len(samples) == 0 {
		return
	}
	slices.Sort(samples)
	for _, percentile := range []int{50, 95, 99} {
		i := (percentile*len(samples)+99)/100 - 1
		b.ReportMetric(float64(samples[i].Nanoseconds()), fmt.Sprintf("%s_p%d_ns", name, percentile))
	}
}

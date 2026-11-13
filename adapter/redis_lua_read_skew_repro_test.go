package adapter

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	goproto "google.golang.org/protobuf/proto"
)

// Read-set reproduction harness, shared with redis_del_rpush_repro_test.go.
//
// One single-node Raft group (newSingleRaftForS3Test) running the production
// kvFSM, a kv.TransactionManager whose proposer logs every proposal, a
// kv.Coordinate sharing the FSM's HLC, and a RedisServer reached over TCP by
// two go-redis clients: the same wiring setupNodes (test_util.go) builds,
// without the gRPC, DynamoDB and SQS servers. The RedisServer reads through
// kv.NewLeaderRoutedStore(readSetReproStore{st}), so a test can hold one
// client's command right after a chosen snapshot read returns while the other
// client commits.
//
// The hold is at a read, not at Propose. The Redis adapter allocates a
// command's commit timestamp before it proposes, so a command held at Propose
// has a commit timestamp below anything that commits while it waits, and its
// late apply is the apply-order gap (kv/apply_order_repro_test.go) rather than
// a read-set gap. Holding at the read lets the concurrent write commit first
// with the lower commit timestamp; apply order and timestamp order then agree,
// and only the held command's read set can reject it.

// readSetReproWaitTimeout bounds every wait on a parked or released command so
// a harness bug fails the test instead of hanging it.
const readSetReproWaitTimeout = 5 * time.Second

// readSetReproCleanupTimeout bounds how long t.Cleanup waits for the command
// goroutines after releasing them.
const readSetReproCleanupTimeout = 10 * time.Second

// readSetReproClientTimeout is the go-redis read/write timeout. It is longer
// than any park so a held command never times out client-side.
const readSetReproClientTimeout = 15 * time.Second

// readSetReproDumpLimit caps the raw-key dump of the final store state.
const readSetReproDumpLimit = 1000

const (
	readSetReproOpGetAt    = "GetAt"
	readSetReproOpExistsAt = "ExistsAt"
	readSetReproOpScanAt   = "ScanAt"
)

// Tag byte of a TransactionManager proposal payload. Mirrors the unexported
// raftEncodeSingle / raftEncodeBatch in kv/fsm.go.
const (
	readSetReproEncodeSingle byte = 0x00
	readSetReproEncodeBatch  byte = 0x01
)

// readSetReproLog is the ordered event log shared by the store wrapper, the
// proposer wrapper and the test body.
type readSetReproLog struct {
	mu     sync.Mutex
	events []string
}

func (l *readSetReproLog) record(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, fmt.Sprintf("%02d ", len(l.events))+fmt.Sprintf(format, args...))
}

func (l *readSetReproLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.events, "\n")
}

// readSetReproPark is one "hold this read" rule. The first read that match
// accepts is held, after it returned, until release is closed; every later
// match passes straight through.
type readSetReproPark struct {
	name    string
	match   func(op string, key []byte) bool
	armed   bool
	parked  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *readSetReproPark) releaseNow() {
	p.once.Do(func() { close(p.release) })
}

// readSetReproStore sits between LeaderRoutedStore and the node's MVCC store.
// It forwards every call; after a matching GetAt / ExistsAt / ScanAt (matched on
// the key or the scan start) returns, it holds the calling goroutine until the
// rule is released. The FSM applies to the unwrapped store.
type readSetReproStore struct {
	store.MVCCStore
	log   *readSetReproLog
	mu    sync.Mutex
	parks []*readSetReproPark
}

func (s *readSetReproStore) addPark(name string, match func(op string, key []byte) bool) *readSetReproPark {
	park := &readSetReproPark{
		name:    name,
		match:   match,
		armed:   true,
		parked:  make(chan struct{}),
		release: make(chan struct{}),
	}
	s.mu.Lock()
	s.parks = append(s.parks, park)
	s.mu.Unlock()
	return park
}

func (s *readSetReproStore) releaseAll() {
	s.mu.Lock()
	parks := append([]*readSetReproPark(nil), s.parks...)
	s.mu.Unlock()
	for _, park := range parks {
		park.releaseNow()
	}
}

func (s *readSetReproStore) claim(op string, key []byte) *readSetReproPark {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, park := range s.parks {
		if park.armed && park.match(op, key) {
			park.armed = false
			return park
		}
	}
	return nil
}

// hold parks the caller when (op, key) matches an armed rule. It runs after the
// underlying read returned, so the held command already has what it read at
// its snapshot timestamp.
func (s *readSetReproStore) hold(ctx context.Context, op string, key []byte, ts uint64) error {
	park := s.claim(op, key)
	if park == nil {
		return nil
	}
	s.log.record("store PARKED   %s %s at ts=%d (%s)", op, readSetReproKey(key), ts, park.name)
	close(park.parked)
	select {
	case <-park.release:
		s.log.record("store released %s %s (%s)", op, readSetReproKey(key), park.name)
		return nil
	case <-ctx.Done():
		s.log.record("store ctx-done while parked %s %s (%s)", op, readSetReproKey(key), park.name)
		return errors.WithStack(ctx.Err())
	}
}

func (s *readSetReproStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	val, err := s.MVCCStore.GetAt(ctx, key, ts)
	if holdErr := s.hold(ctx, readSetReproOpGetAt, key, ts); holdErr != nil {
		return nil, holdErr
	}
	return val, err
}

func (s *readSetReproStore) ExistsAt(ctx context.Context, key []byte, ts uint64) (bool, error) {
	exists, err := s.MVCCStore.ExistsAt(ctx, key, ts)
	if holdErr := s.hold(ctx, readSetReproOpExistsAt, key, ts); holdErr != nil {
		return false, holdErr
	}
	return exists, err
}

func (s *readSetReproStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	kvs, err := s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
	if holdErr := s.hold(ctx, readSetReproOpScanAt, start, ts); holdErr != nil {
		return nil, holdErr
	}
	return kvs, err
}

// readSetReproProposal is one decoded request the node proposed, with the
// outcome of its apply.
type readSetReproProposal struct {
	startTS  uint64
	commitTS uint64
	writes   [][]byte
	reads    [][]byte
	outcome  string
}

// readSetReproProposer wraps the Raft engine behind the TransactionManager and
// logs every proposal before it is proposed and after it applied. Propose on
// the etcd engine returns only after the FSM applied the entry, so "applied"
// events are in apply order.
type readSetReproProposer struct {
	inner     raftengine.Proposer
	log       *readSetReproLog
	mu        sync.Mutex
	proposals []readSetReproProposal
}

var _ raftengine.Proposer = (*readSetReproProposer)(nil)

func (p *readSetReproProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	reqs, decodeErr := decodeReadSetReproRequests(data)
	desc := describeReadSetReproRequests(reqs, decodeErr)
	p.log.record("raft  propose  %s", desc)
	res, err := p.inner.Propose(ctx, data)
	outcome := describeReadSetReproOutcome(res, err)
	p.log.record("raft  applied  %s -> %s", desc, outcome)
	p.remember(reqs, outcome)
	return res, err
}

func (p *readSetReproProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.inner.ProposeAdmin(ctx, data)
}

func (p *readSetReproProposer) remember(reqs []*pb.Request, outcome string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, req := range reqs {
		meta, muts := readSetReproTxnMeta(req)
		writes := make([][]byte, 0, len(muts))
		for _, mut := range muts {
			writes = append(writes, mut.GetKey())
		}
		p.proposals = append(p.proposals, readSetReproProposal{
			startTS:  req.GetTs(),
			commitTS: meta.CommitTS,
			writes:   writes,
			reads:    req.GetReadKeys(),
			outcome:  outcome,
		})
	}
}

// lastWriting returns the most recent proposal whose write set contains key.
func (p *readSetReproProposer) lastWriting(key []byte) (readSetReproProposal, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.proposals) - 1; i >= 0; i-- {
		if readSetReproContainsKey(p.proposals[i].writes, key) {
			return p.proposals[i], true
		}
	}
	return readSetReproProposal{}, false
}

func decodeReadSetReproRequests(data []byte) ([]*pb.Request, error) {
	if len(data) == 0 {
		return nil, errors.New("empty proposal")
	}
	switch data[0] {
	case readSetReproEncodeSingle:
		req := &pb.Request{}
		if err := goproto.Unmarshal(data[1:], req); err != nil {
			return nil, errors.WithStack(err)
		}
		return []*pb.Request{req}, nil
	case readSetReproEncodeBatch:
		cmd := &pb.RaftCommand{}
		if err := goproto.Unmarshal(data[1:], cmd); err != nil {
			return nil, errors.WithStack(err)
		}
		return cmd.GetRequests(), nil
	default:
		return nil, errors.Newf("unexpected proposal tag 0x%02x", data[0])
	}
}

// readSetReproTxnMeta splits a transactional request's leading txn-meta
// mutation from its data mutations.
func readSetReproTxnMeta(req *pb.Request) (kv.TxnMeta, []*pb.Mutation) {
	muts := req.GetMutations()
	if !req.GetIsTxn() || len(muts) == 0 || !bytes.HasPrefix(muts[0].GetKey(), []byte(kv.TxnMetaPrefix)) {
		return kv.TxnMeta{}, muts
	}
	meta, err := kv.DecodeTxnMeta(muts[0].GetValue())
	if err != nil {
		return kv.TxnMeta{}, muts
	}
	return meta, muts[1:]
}

func describeReadSetReproRequests(reqs []*pb.Request, decodeErr error) string {
	if decodeErr != nil {
		return fmt.Sprintf("<undecodable: %v>", decodeErr)
	}
	parts := make([]string, 0, len(reqs))
	for _, req := range reqs {
		parts = append(parts, describeReadSetReproRequest(req))
	}
	return strings.Join(parts, " | ")
}

func describeReadSetReproRequest(req *pb.Request) string {
	if !req.GetIsTxn() {
		return fmt.Sprintf("RAW ts=%d writes=%s", req.GetTs(), describeReadSetReproMutations(req.GetMutations()))
	}
	meta, muts := readSetReproTxnMeta(req)
	return fmt.Sprintf("%s startTS=%d commitTS=%d writes=%s reads=%s",
		req.GetPhase(), req.GetTs(), meta.CommitTS, describeReadSetReproMutations(muts), readSetReproKeys(req.GetReadKeys()))
}

func describeReadSetReproMutations(muts []*pb.Mutation) string {
	parts := make([]string, 0, len(muts))
	for _, mut := range muts {
		parts = append(parts, mut.GetOp().String()+" "+readSetReproKey(mut.GetKey()))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func describeReadSetReproOutcome(res *raftengine.ProposalResult, err error) string {
	switch {
	case err != nil:
		return fmt.Sprintf("propose error: %v", err)
	case res == nil:
		return "nil result"
	}
	if respErr, ok := res.Response.(error); ok {
		return fmt.Sprintf("FSM error index=%d: %v", res.CommitIndex, respErr)
	}
	if res.Response != nil {
		return fmt.Sprintf("index=%d response=%+v", res.CommitIndex, res.Response)
	}
	return fmt.Sprintf("ok index=%d", res.CommitIndex)
}

func readSetReproKey(key []byte) string {
	return strconv.Quote(string(key))
}

func readSetReproKeys(keys [][]byte) string {
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, readSetReproKey(key))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func readSetReproContainsKey(keys [][]byte, key []byte) bool {
	for _, k := range keys {
		if bytes.Equal(k, key) {
			return true
		}
	}
	return false
}

// readSetReproReply is what a client command returned.
type readSetReproReply struct {
	val any
	err error
}

func (r readSetReproReply) String() string {
	switch {
	case errors.Is(r.err, redis.Nil):
		return "(nil)"
	case r.err != nil:
		return "error: " + r.err.Error()
	default:
		return fmt.Sprintf("%v", r.val)
	}
}

type readSetReproHarness struct {
	ctx      context.Context
	st       store.MVCCStore
	server   *RedisServer
	reads    *readSetReproStore
	proposer *readSetReproProposer
	log      *readSetReproLog
	clientA  *redis.Client
	clientB  *redis.Client
	wg       sync.WaitGroup
}

func newReadSetReproClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr,
		// A client-side retry would run a held EVAL / DEL a second time.
		MaxRetries:   -1,
		ReadTimeout:  readSetReproClientTimeout,
		WriteTimeout: readSetReproClientTimeout,
	})
}

func newReadSetReproHarness(t *testing.T, raftID string) *readSetReproHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	st := store.NewMVCCStore()
	hlc := kv.NewHLC()
	engine, stopRaft := newSingleRaftForS3Test(t, raftID, kv.NewKvFSMWithHLC(st, hlc))
	t.Cleanup(stopRaft)

	log := &readSetReproLog{}
	proposer := &readSetReproProposer{inner: engine, log: log}
	tm := kv.NewTransactionWithProposer(proposer)
	t.Cleanup(tm.Close)
	coord := kv.NewCoordinatorWithEngine(tm, engine, kv.WithHLC(hlc))
	t.Cleanup(func() { _ = coord.Close() })
	renewCtx, stopRenew := context.WithCancel(ctx)
	go coord.RunHLCLeaseRenewal(renewCtx)
	t.Cleanup(stopRenew)

	reads := &readSetReproStore{MVCCStore: st, log: log}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := NewRedisServer(lis, lis.Addr().String(), kv.NewLeaderRoutedStore(reads, coord), coord, nil, nil)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = server.Run()
	}()
	t.Cleanup(func() {
		server.Stop()
		select {
		case <-served:
		case <-time.After(readSetReproCleanupTimeout):
			t.Error("redis server did not stop")
		}
	})

	h := &readSetReproHarness{
		ctx:      ctx,
		st:       st,
		server:   server,
		reads:    reads,
		proposer: proposer,
		log:      log,
		clientA:  newReadSetReproClient(lis.Addr().String()),
		clientB:  newReadSetReproClient(lis.Addr().String()),
	}
	// Registered last, so it runs first: release every park and wait for the
	// command goroutines before the server and Raft stop.
	t.Cleanup(func() {
		reads.releaseAll()
		h.waitCommands(t)
		_ = h.clientA.Close()
		_ = h.clientB.Close()
	})
	return h
}

func (h *readSetReproHarness) waitCommands(t *testing.T) {
	t.Helper()
	waited := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(readSetReproCleanupTimeout):
		t.Error("command goroutines did not exit after release")
	}
}

// goCommand runs one client command on its own goroutine.
func (h *readSetReproHarness) goCommand(run func() (any, error)) <-chan readSetReproReply {
	done := make(chan readSetReproReply, 1)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		val, err := run()
		done <- readSetReproReply{val: val, err: err}
	}()
	return done
}

func (h *readSetReproHarness) latestCommitTS(t *testing.T, key []byte) uint64 {
	t.Helper()
	ts, _, err := h.st.LatestCommitTS(h.ctx, key)
	require.NoError(t, err)
	return ts
}

// rawKeysContaining lists every live raw key that contains userKey, with the
// commit timestamp of its newest version.
func (h *readSetReproHarness) rawKeysContaining(t *testing.T, userKey []byte) string {
	t.Helper()
	kvs, err := h.st.ScanAt(h.ctx, nil, nil, readSetReproDumpLimit, h.st.LastCommitTS())
	require.NoError(t, err)
	lines := make([]string, 0, len(kvs))
	for _, pair := range kvs {
		if !bytes.Contains(pair.Key, userKey) {
			continue
		}
		lines = append(lines, fmt.Sprintf("  %s (%d-byte value, commitTS=%d)",
			readSetReproKey(pair.Key), len(pair.Value), h.latestCommitTS(t, pair.Key)))
	}
	return strings.Join(lines, "\n")
}

func waitReadSetReproParked(t *testing.T, park *readSetReproPark, done <-chan readSetReproReply, who string) {
	t.Helper()
	select {
	case <-park.parked:
	case res := <-done:
		t.Fatalf("%s returned before it was parked: %s", who, res)
	case <-time.After(readSetReproWaitTimeout):
		t.Fatalf("timed out waiting for %s to be parked", who)
	}
}

func waitReadSetReproDone(t *testing.T, done <-chan readSetReproReply, who string) readSetReproReply {
	t.Helper()
	select {
	case res := <-done:
		return res
	case <-time.After(readSetReproWaitTimeout):
		t.Fatalf("timed out waiting for released %s to return", who)
	}
	return readSetReproReply{}
}

const (
	luaReadSkewKeyA = "g3-a"
	luaReadSkewKeyB = "g3-b"
	// luaReadSkewScript reads KEYS[1] and writes KEYS[2] only.
	luaReadSkewScript = "local v=redis.call('GET',KEYS[1]); redis.call('SET',KEYS[2], v or 'none'); return v"
)

// TestLua_ReadOfUnwrittenKeyIsNotValidated checks that a Lua script's read of
// a key it does not write is validated at commit.
//
// Variant: interleaving test over the harness above (hold at a read, not at
// Propose; see the harness comment for why).
//
// Client A runs EVAL "GET a; SET b (a or 'none')". A is held right after its
// first read of b's string key, i.e. after GET a returned nil at A's snapshot.
// Client B then runs SET a x, which commits. A is released, allocates its
// commit timestamp (above B's) and commits b. If the GET of a were in A's read
// set, the FSM would see a's version newer than A's startTS and reject the
// commit; the adapter would retry the script, which would read a=x and write
// b=x. If A instead commits b="none" at a commit timestamp above a="x", the
// snapshot at A's commit timestamp holds a="x" and b="none" -- a state no
// serial order of A and B produces.
func TestLua_ReadOfUnwrittenKeyIsNotValidated(t *testing.T) {
	t.Parallel()

	h := newReadSetReproHarness(t, "g3")
	ctx := h.ctx
	strKeyA := redisStrKey([]byte(luaReadSkewKeyA))
	strKeyB := redisStrKey([]byte(luaReadSkewKeyB))

	// One committed write so A's startTS is a real HLC timestamp.
	require.NoError(t, h.clientB.Set(ctx, "g3-warm", "0", 0).Err())

	park := h.reads.addPark("A: first read of b's string key, after GET a returned", func(op string, key []byte) bool {
		return op != readSetReproOpScanAt && bytes.Equal(key, strKeyB)
	})

	// (1) A runs the script and is held after it read a.
	aDone := h.goCommand(func() (any, error) {
		return h.clientA.Eval(ctx, luaReadSkewScript, []string{luaReadSkewKeyA, luaReadSkewKeyB}).Result()
	})
	waitReadSetReproParked(t, park, aDone, "A (EVAL)")

	// (2) B writes a; it commits while A is held.
	require.NoError(t, h.clientB.Set(ctx, luaReadSkewKeyA, "x", 0).Err())
	h.log.record("B  SET %s x -> OK", luaReadSkewKeyA)

	// (3) Release A.
	park.releaseNow()
	aRes := waitReadSetReproDone(t, aDone, "A (EVAL)")
	h.log.record("A  EVAL returned %s", aRes)

	aVal, aErr := h.clientB.Get(ctx, luaReadSkewKeyA).Result()
	bVal, bErr := h.clientB.Get(ctx, luaReadSkewKeyB).Result()
	aTS := h.latestCommitTS(t, strKeyA)
	bTS := h.latestCommitTS(t, strKeyB)
	aCommit, found := h.proposer.lastWriting(strKeyB)

	t.Logf("event log:\n%s", h.log)
	t.Logf("final: a=%s (commitTS=%d) b=%s (commitTS=%d)",
		readSetReproReply{val: aVal, err: aErr}, aTS, readSetReproReply{val: bVal, err: bErr}, bTS)
	t.Logf("A's committed script (found=%v): startTS=%d commitTS=%d reads=%s outcome=%s",
		found, aCommit.startTS, aCommit.commitTS, readSetReproKeys(aCommit.reads), aCommit.outcome)
	t.Logf("A's read set contains a's string key %s: %v", readSetReproKey(strKeyA), readSetReproContainsKey(aCommit.reads, strKeyA))
	require.NoError(t, aErr)
	require.NoError(t, bErr)

	if bVal == "none" && aVal == "x" && aCommit.startTS < aTS && aTS < bTS {
		t.Errorf("Lua script committed b=%q after a concurrent SET a=%q landed below its commit; the GET of a was not in the read set "+
			"(script startTS=%d < SET a commitTS=%d < script commitTS=%d)",
			bVal, aVal, aCommit.startTS, aTS, bTS)
	}
}

func TestLua_AbsentStringReadConflictsWithListCreate(t *testing.T) {
	t.Parallel()

	h := newReadSetReproHarness(t, "g3-list")
	ctx := h.ctx
	readKey := []byte("g3-list-read")
	writeKey := []byte("g3-list-write")
	require.NoError(t, h.clientB.Set(ctx, "g3-list-warm", "0", 0).Err())

	park := h.reads.addPark("A: read of write key after absent GET", func(op string, key []byte) bool {
		return op != readSetReproOpScanAt && bytes.Equal(key, redisStrKey(writeKey))
	})
	aDone := h.goCommand(func() (any, error) {
		return h.clientA.Eval(ctx, luaReadSkewScript, []string{string(readKey), string(writeKey)}).Result()
	})
	waitReadSetReproParked(t, park, aDone, "A (EVAL)")

	length, err := h.clientB.RPush(ctx, string(readKey), "v").Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, length)
	park.releaseNow()
	aRes := waitReadSetReproDone(t, aDone, "A (EVAL)")

	if aRes.err == nil {
		value, getErr := h.clientB.Get(ctx, string(writeKey)).Result()
		if getErr == nil && value == "none" {
			t.Errorf("Lua committed an absent GET across a concurrent list creation; read set did not fence the type creator")
		}
	}
}

// TestLua_ReadSetCoversStringReads pins the physical keys and type-creation
// fences needed to validate a string GET, including an absent GET.
func TestLua_ReadSetCoversStringReads(t *testing.T) {
	t.Parallel()

	readKey := []byte("lua:readset:read")
	writeKey := []byte("lua:readset:write")
	stringValue := [][]byte{redisStrKey(readKey), redisHLLKey(readKey), readKey}
	absent := redisTxnReadFencePointKeys(readKey)

	cases := []struct {
		name string
		cmd  string
		seed bool
		want [][]byte
	}{
		{name: "GET absent", cmd: "GET", want: absent},
		{name: "GET string", cmd: "GET", seed: true, want: stringValue},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.NewMVCCStore()
			if tc.seed {
				require.NoError(t, st.PutAt(context.Background(), redisStrKey(readKey), encodeRedisStr([]byte("v"), nil), 1, 0))
			}
			coord := newReadKeyRecordingCoordinator(st)
			server := NewRedisServer(nil, "", st, coord, nil, nil)

			conn := &recordingConn{}
			script := `redis.call("` + tc.cmd + `", KEYS[1]); return redis.call("SET", KEYS[2], "w")`
			server.runLuaScript(conn, script, [][]byte{[]byte("2"), readKey, writeKey})
			require.Empty(t, conn.err)
			require.Equal(t, 1, coord.dispatches)

			want := append(append([][]byte{}, tc.want...), redisTxnWideCollectionFenceKeys(writeKey)...)
			requireReadKeysMatch(t, coord.lastReadKeys, want)
		})
	}
}

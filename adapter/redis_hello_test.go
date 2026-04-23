package adapter

import (
	"context"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// helloReplyElement is a captured reply element in the order it was
// written to the redcon.Conn by a HELLO handler. RESP2 encodes the
// HELLO reply as a flat array of alternating bulk-string keys and
// per-type values; preserving the original type (bulk-string / int /
// array-header) is the only way to distinguish `proto` (integer 2)
// from `version` (bulk "7.0.0") or `modules` (empty array header).
type helloReplyElement struct {
	kind string // "bulk" | "int" | "arrayHeader"
	str  string
	num  int64
}

// helloRecordingConn is a redcon.Conn stub specifically for HELLO
// tests. Unlike the generic recordingConn in redis_retry_test.go it
// preserves the FULL ordered write sequence so we can assert the
// HELLO reply shape (alternating keys/values with mixed types). It
// also implements SetContext / Context so the per-connection state
// (client name, conn ID) used by HELLO and CLIENT survives across
// calls in a single test.
type helloRecordingConn struct {
	writes []helloReplyElement
	err    string
	str    string
	ctx    any
}

func (c *helloRecordingConn) RemoteAddr() string    { return "127.0.0.1:0" }
func (c *helloRecordingConn) Close() error          { return nil }
func (c *helloRecordingConn) WriteError(msg string) { c.err = msg }
func (c *helloRecordingConn) WriteString(s string) {
	c.str = s
	c.writes = append(c.writes, helloReplyElement{kind: "bulk", str: s})
}
func (c *helloRecordingConn) WriteBulk(bulk []byte) {
	c.writes = append(c.writes, helloReplyElement{kind: "bulk", str: string(bulk)})
}
func (c *helloRecordingConn) WriteBulkString(bulk string) {
	c.writes = append(c.writes, helloReplyElement{kind: "bulk", str: bulk})
}
func (c *helloRecordingConn) WriteInt(num int) {
	c.writes = append(c.writes, helloReplyElement{kind: "int", num: int64(num)})
}
func (c *helloRecordingConn) WriteInt64(num int64) {
	c.writes = append(c.writes, helloReplyElement{kind: "int", num: num})
}
func (c *helloRecordingConn) WriteUint64(num uint64) {
	if num > math.MaxInt64 {
		c.writes = append(c.writes, helloReplyElement{kind: "int", num: math.MaxInt64})
		return
	}
	c.writes = append(c.writes, helloReplyElement{kind: "int", num: int64(num)})
}
func (c *helloRecordingConn) WriteArray(count int) {
	c.writes = append(c.writes, helloReplyElement{kind: "arrayHeader", num: int64(count)})
}
func (c *helloRecordingConn) WriteNull() {
	c.writes = append(c.writes, helloReplyElement{kind: "null"})
}
func (c *helloRecordingConn) WriteRaw(data []byte)           {}
func (c *helloRecordingConn) WriteAny(v any)                 {}
func (c *helloRecordingConn) Context() any                   { return c.ctx }
func (c *helloRecordingConn) SetContext(v any)               { c.ctx = v }
func (c *helloRecordingConn) SetReadBuffer(bytes int)        {}
func (c *helloRecordingConn) Detach() redcon.DetachedConn    { return nil }
func (c *helloRecordingConn) ReadPipeline() []redcon.Command { return nil }
func (c *helloRecordingConn) PeekPipeline() []redcon.Command { return nil }
func (c *helloRecordingConn) NetConn() net.Conn              { return nil }

// helloTestCoordinator is the minimal kv.Coordinator stub HELLO
// exercises. HELLO only inspects IsLeader for the role field; the rest
// of the surface is satisfied with sensible defaults to avoid
// NPE when handler helpers call through to unrelated methods.
type helloTestCoordinator struct {
	isLeader bool
	clock    *kv.HLC
}

func (c *helloTestCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return &kv.CoordinateResponse{}, nil
}
func (c *helloTestCoordinator) IsLeader() bool                  { return c.isLeader }
func (c *helloTestCoordinator) VerifyLeader() error             { return nil }
func (c *helloTestCoordinator) RaftLeader() string              { return "" }
func (c *helloTestCoordinator) IsLeaderForKey([]byte) bool      { return c.isLeader }
func (c *helloTestCoordinator) VerifyLeaderForKey([]byte) error { return nil }
func (c *helloTestCoordinator) RaftLeaderForKey([]byte) string  { return "" }
func (c *helloTestCoordinator) Clock() *kv.HLC {
	if c.clock == nil {
		c.clock = kv.NewHLC()
	}
	return c.clock
}
func (c *helloTestCoordinator) LinearizableRead(_ context.Context) (uint64, error) { return 0, nil }
func (c *helloTestCoordinator) LeaseRead(ctx context.Context) (uint64, error) {
	return c.LinearizableRead(ctx)
}
func (c *helloTestCoordinator) LeaseReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	return c.LinearizableRead(ctx)
}

func newHelloTestServer(t *testing.T, leader bool) *RedisServer {
	t.Helper()
	return &RedisServer{
		coordinator: &helloTestCoordinator{isLeader: leader},
	}
}

// decodeHelloReply walks the flat RESP2 alternating key/value sequence
// the HELLO handler writes and returns a map keyed by the HELLO field
// names, plus separate accessors for non-string values so tests don't
// have to re-parse type-variant fields (`proto` is int, `modules` is
// an array header).
type decodedHello struct {
	serverName string
	version    string
	proto      int64
	id         int64
	mode       string
	role       string
	modulesLen int64
}

func decodeHello(t *testing.T, writes []helloReplyElement) decodedHello {
	t.Helper()
	require.GreaterOrEqual(t, len(writes), 1, "HELLO must emit at least an array header")
	require.Equal(t, "arrayHeader", writes[0].kind, "HELLO must start with an array header")
	require.Equal(t, int64(14), writes[0].num, "HELLO reply must be a 14-element flat array")

	body := writes[1:]
	require.Equal(t, 14, len(body), "HELLO body must be 14 elements after the header")

	// Pair up alternating key/value slots. Keys are always bulk strings.
	var out decodedHello
	for i := 0; i < len(body); i += 2 {
		key := body[i]
		val := body[i+1]
		require.Equal(t, "bulk", key.kind, "HELLO key at index %d must be bulk string", i)
		switch key.str {
		case "server":
			require.Equal(t, "bulk", val.kind)
			out.serverName = val.str
		case "version":
			require.Equal(t, "bulk", val.kind)
			out.version = val.str
		case "proto":
			require.Equal(t, "int", val.kind)
			out.proto = val.num
		case "id":
			require.Equal(t, "int", val.kind)
			out.id = val.num
		case "mode":
			require.Equal(t, "bulk", val.kind)
			out.mode = val.str
		case "role":
			require.Equal(t, "bulk", val.kind)
			out.role = val.str
		case "modules":
			require.Equal(t, "arrayHeader", val.kind)
			out.modulesLen = val.num
		default:
			t.Fatalf("unexpected HELLO key %q", key.str)
		}
	}
	return out
}

func TestHello_NoArgs_ReturnsExpectedMap(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}

	r.hello(conn, redcon.Command{Args: [][]byte{[]byte(cmdHello)}})

	require.Empty(t, conn.err, "HELLO with no args must succeed")
	out := decodeHello(t, conn.writes)
	require.Equal(t, "redis", out.serverName)
	require.Equal(t, helloReplyVersion, out.version)
	require.Equal(t, int64(2), out.proto)
	require.Equal(t, "standalone", out.mode)
	require.Equal(t, "master", out.role)
	require.Equal(t, int64(0), out.modulesLen)
	require.Greater(t, out.id, int64(0), "HELLO must assign a connection id >= 1")
}

func TestHello_Proto2_ReturnsExpectedMap(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}

	r.hello(conn, redcon.Command{Args: [][]byte{[]byte(cmdHello), []byte("2")}})

	require.Empty(t, conn.err)
	out := decodeHello(t, conn.writes)
	require.Equal(t, int64(2), out.proto)
}

func TestHello_Proto3_RejectsWithNOPROTO(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}

	r.hello(conn, redcon.Command{Args: [][]byte{[]byte(cmdHello), []byte("3")}})

	require.Contains(t, conn.err, "NOPROTO",
		"HELLO 3 must be rejected with NOPROTO so clients fall back to RESP2")
	require.Empty(t, conn.writes, "no reply body should be emitted on NOPROTO")
}

func TestHello_NonNumericProtover_RejectsWithNOPROTO(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}

	r.hello(conn, redcon.Command{Args: [][]byte{[]byte(cmdHello), []byte("foo")}})

	require.Contains(t, conn.err, "NOPROTO",
		"non-numeric protover must be rejected with NOPROTO")
}

func TestHello_Auth_RejectsWithNOPERM(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}

	r.hello(conn, redcon.Command{Args: [][]byte{
		[]byte(cmdHello), []byte("2"), []byte("AUTH"), []byte("user"), []byte("pass"),
	}})

	require.Contains(t, conn.err, "NOPERM",
		"HELLO AUTH must be rejected because elastickv has no auth layer")
}

func TestHello_SetName_PersistsInConnStateAndCLIENT_GETNAME(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}

	r.hello(conn, redcon.Command{Args: [][]byte{
		[]byte(cmdHello), []byte("2"), []byte("SETNAME"), []byte("myclient"),
	}})

	require.Empty(t, conn.err, "HELLO SETNAME must succeed")
	_ = decodeHello(t, conn.writes)

	// The subsequent CLIENT GETNAME must return the name we just set,
	// which proves HELLO SETNAME wrote into the same slot CLIENT reads
	// from instead of silently dropping the name on the floor.
	getConn := &helloRecordingConn{ctx: conn.ctx}
	r.client(getConn, redcon.Command{Args: [][]byte{[]byte(cmdClient), []byte("GETNAME")}})
	require.Empty(t, getConn.err)
	require.Len(t, getConn.writes, 1)
	require.Equal(t, "bulk", getConn.writes[0].kind)
	require.Equal(t, "myclient", getConn.writes[0].str)
}

func TestHello_Role_ReflectsCoordinatorIsLeader(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		isLeader bool
		want     string
	}{
		{"leader reports master", true, "master"},
		{"follower reports slave", false, "slave"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newHelloTestServer(t, tc.isLeader)
			conn := &helloRecordingConn{}
			r.hello(conn, redcon.Command{Args: [][]byte{[]byte(cmdHello)}})
			require.Empty(t, conn.err)
			out := decodeHello(t, conn.writes)
			require.Equal(t, tc.want, out.role)
		})
	}
}

func TestHello_SyntaxErrorOnDanglingSetName(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}

	// SETNAME with no value: must surface a syntax error, not panic
	// on args[i+1] indexing.
	r.hello(conn, redcon.Command{Args: [][]byte{
		[]byte(cmdHello), []byte("2"), []byte("SETNAME"),
	}})
	require.NotEmpty(t, conn.err)
}

func TestHello_SetNameNotPersistedOnLaterAuthError(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}
	state := getConnState(conn)
	state.clientName = "prev"

	// SETNAME parses OK but then AUTH fails -- the clientName must
	// NOT have been committed, so GETNAME should still return "prev".
	r.hello(conn, redcon.Command{Args: [][]byte{
		[]byte(cmdHello), []byte("2"),
		[]byte("SETNAME"), []byte("newname"),
		[]byte("AUTH"), []byte("u"), []byte("p"),
	}})
	require.Contains(t, conn.err, "NOPERM")
	require.Equal(t, "prev", state.clientName,
		"HELLO must be all-or-nothing: SETNAME side effect must not persist on later error")
}

func TestHello_SetNameNotPersistedOnLaterUnknownOption(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}
	state := getConnState(conn)
	state.clientName = "prev"

	// SETNAME parses OK but a later unknown option aborts parsing.
	// clientName must remain "prev".
	r.hello(conn, redcon.Command{Args: [][]byte{
		[]byte(cmdHello), []byte("2"),
		[]byte("SETNAME"), []byte("newname"),
		[]byte("BOGUS"),
	}})
	require.NotEmpty(t, conn.err)
	require.Equal(t, "prev", state.clientName)
}

func TestHello_ConnIDIsStableWithinConnection(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	conn := &helloRecordingConn{}

	r.hello(conn, redcon.Command{Args: [][]byte{[]byte(cmdHello)}})
	first := decodeHello(t, conn.writes).id

	// Reset just the captured writes; keep the stored connState so
	// the second HELLO must see the same already-assigned id rather
	// than allocating a fresh one.
	conn.writes = nil
	r.hello(conn, redcon.Command{Args: [][]byte{[]byte(cmdHello)}})
	second := decodeHello(t, conn.writes).id

	require.Equal(t, first, second,
		"HELLO called twice on the same connection must return the same id")
}

func TestHello_ConnIDsAreDistinctAcrossConnections(t *testing.T) {
	t.Parallel()

	r := newHelloTestServer(t, true)
	connA := &helloRecordingConn{}
	connB := &helloRecordingConn{}

	r.hello(connA, redcon.Command{Args: [][]byte{[]byte(cmdHello)}})
	r.hello(connB, redcon.Command{Args: [][]byte{[]byte(cmdHello)}})

	idA := decodeHello(t, connA.writes).id
	idB := decodeHello(t, connB.writes).id
	require.NotEqual(t, idA, idB, "distinct connections must get distinct HELLO ids")
}

// TestHello_MetricsRecordsKnownCommand verifies that a HELLO request
// dispatched through the metrics wrapper records against the "HELLO"
// label, not the "unknown" bucket. Before this change the adapter
// rejected HELLO as an unsupported command, which meant every go-redis
// / ioredis connection-setup HELLO inflated the unsupported-command
// metric — exactly the motivation for implementing the command.
func TestHello_MetricsRecordsKnownCommand(t *testing.T) {
	t.Parallel()

	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	r := &RedisServer{
		coordinator:     &helloTestCoordinator{isLeader: true},
		requestObserver: registry.RedisObserver(),
	}

	conn := &helloRecordingConn{}
	handler := r.hello
	cmd := redcon.Command{Args: [][]byte{[]byte(cmdHello)}}
	r.dispatchCommand(conn, cmdHello, handler, cmd, time.Now())

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_redis_requests_total Total number of Redis API requests by command and outcome.
# TYPE elastickv_redis_requests_total counter
elastickv_redis_requests_total{command="HELLO",node_address="10.0.0.1:50051",node_id="n1",outcome="success"} 1
`),
		"elastickv_redis_requests_total",
	)
	require.NoError(t, err)
}

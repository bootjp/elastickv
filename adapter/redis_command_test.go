package adapter

import (
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// commandRecorder captures the sequence of writes performed by a handler
// so array-structured replies (like COMMAND INFO) can be asserted
// element-by-element rather than via the flat `recordingConn.bulk` field,
// which only remembers the last write.
//
// Each entry in `writes` is one of:
//
//	{op: "array",  n: <count>}
//	{op: "bulk",   s: <string>}
//	{op: "int",    i: <int64>}
//	{op: "string", s: <string>}
//	{op: "null"}
//	{op: "error",  s: <message>}
//
// Tests walk `writes` with a small cursor-based helper rather than trying
// to reconstruct a tree — reconstruction is more code than the tests it
// enables, and a flat trace is trivially greppable.
type commandRecorderEntry struct {
	op string
	s  string
	i  int64
	n  int
}

type commandRecorder struct {
	writes []commandRecorderEntry
	ctx    any
}

func (c *commandRecorder) RemoteAddr() string { return "" }
func (c *commandRecorder) Close() error       { return nil }
func (c *commandRecorder) WriteError(msg string) {
	c.writes = append(c.writes, commandRecorderEntry{op: "error", s: msg})
}
func (c *commandRecorder) WriteString(str string) {
	c.writes = append(c.writes, commandRecorderEntry{op: "string", s: str})
}
func (c *commandRecorder) WriteBulk(b []byte) {
	c.writes = append(c.writes, commandRecorderEntry{op: "bulk", s: string(b)})
}
func (c *commandRecorder) WriteBulkString(s string) {
	c.writes = append(c.writes, commandRecorderEntry{op: "bulk", s: s})
}
func (c *commandRecorder) WriteInt(num int) {
	c.writes = append(c.writes, commandRecorderEntry{op: "int", i: int64(num)})
}
func (c *commandRecorder) WriteInt64(num int64) {
	c.writes = append(c.writes, commandRecorderEntry{op: "int", i: num})
}
func (c *commandRecorder) WriteUint64(num uint64) {
	c.writes = append(c.writes, commandRecorderEntry{op: "int", i: int64(num)}) //nolint:gosec
}
func (c *commandRecorder) WriteArray(count int) {
	c.writes = append(c.writes, commandRecorderEntry{op: "array", n: count})
}
func (c *commandRecorder) WriteNull() {
	c.writes = append(c.writes, commandRecorderEntry{op: "null"})
}
func (c *commandRecorder) WriteRaw([]byte)                {}
func (c *commandRecorder) WriteAny(any)                   {}
func (c *commandRecorder) Context() any                   { return c.ctx }
func (c *commandRecorder) SetContext(v any)               { c.ctx = v }
func (c *commandRecorder) SetReadBuffer(int)              {}
func (c *commandRecorder) Detach() redcon.DetachedConn    { return nil }
func (c *commandRecorder) ReadPipeline() []redcon.Command { return nil }
func (c *commandRecorder) PeekPipeline() []redcon.Command { return nil }
func (c *commandRecorder) NetConn() net.Conn              { return nil }

// helper: skip `count` entries starting from writes[start], treating the
// current entry as an array header. Returns (start+1, size). Unused for
// now — kept simple by manually walking indices in individual tests.

// newCommandTestServer returns a RedisServer sufficient for COMMAND tests.
// COMMAND has no dependency on coordinator / store, so all fields stay
// zero. We use a literal construction rather than NewRedisServer because
// the latter requires a real listener + coordinator.
func newCommandTestServer() *RedisServer {
	return &RedisServer{}
}

func runCommand(t *testing.T, args ...string) *commandRecorder {
	t.Helper()
	srv := newCommandTestServer()
	conn := &commandRecorder{}
	cmdArgs := make([][]byte, 0, len(args))
	for _, a := range args {
		cmdArgs = append(cmdArgs, []byte(a))
	}
	srv.command(conn, redcon.Command{Args: cmdArgs})
	return conn
}

func TestCommandCount_MatchesTableSize(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "COUNT")
	require.Len(t, conn.writes, 1)
	require.Equal(t, "int", conn.writes[0].op)
	// COUNT reports the size of the routed set (argsLen); the table must
	// have the same size by the route-matches-table invariant.
	require.Equal(t, int64(len(argsLen)), conn.writes[0].i)
	require.Equal(t, len(argsLen), len(redisCommandTable))
}

func TestCommandNoArgs_ReturnsAllEntries(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND")
	require.NotEmpty(t, conn.writes)
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, len(argsLen), conn.writes[0].n)
}

func TestCommandInfo_Get(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "INFO", "GET")
	// outer array of 1 + 6-element inner.
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, 1, conn.writes[0].n)
	require.Equal(t, "array", conn.writes[1].op)
	require.Equal(t, 6, conn.writes[1].n)
	require.Equal(t, "bulk", conn.writes[2].op)
	require.Equal(t, "get", conn.writes[2].s)
	require.Equal(t, "int", conn.writes[3].op)
	require.Equal(t, int64(2), conn.writes[3].i)
	// flags array
	require.Equal(t, "array", conn.writes[4].op)
	flagN := conn.writes[4].n
	flags := make([]string, 0, flagN)
	for i := 0; i < flagN; i++ {
		flags = append(flags, conn.writes[5+i].s)
	}
	require.Contains(t, flags, "readonly")
	// first / last / step
	require.Equal(t, int64(1), conn.writes[5+flagN].i)
	require.Equal(t, int64(1), conn.writes[6+flagN].i)
	require.Equal(t, int64(1), conn.writes[7+flagN].i)
}

func TestCommandInfo_Set(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "INFO", "SET")
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, 1, conn.writes[0].n)
	require.Equal(t, "array", conn.writes[1].op)
	require.Equal(t, 6, conn.writes[1].n)
	require.Equal(t, "set", conn.writes[2].s)
	require.Equal(t, int64(-3), conn.writes[3].i)
	require.Equal(t, "array", conn.writes[4].op)
	flagN := conn.writes[4].n
	flags := make([]string, 0, flagN)
	for i := 0; i < flagN; i++ {
		flags = append(flags, conn.writes[5+i].s)
	}
	require.Contains(t, flags, "write")
	require.Equal(t, int64(1), conn.writes[5+flagN].i)
	require.Equal(t, int64(1), conn.writes[6+flagN].i)
	require.Equal(t, int64(1), conn.writes[7+flagN].i)
}

func TestCommandInfo_UnknownReturnsNil(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "INFO", "nosuchcommand")
	// outer array of 1, then a nil entry.
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, 1, conn.writes[0].n)
	require.Equal(t, "null", conn.writes[1].op)
}

func TestCommandInfo_MixedKnownUnknown(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "INFO", "GET", "NOSUCH", "SET")
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, 3, conn.writes[0].n)
	// first entry: 6-element GET array header.
	require.Equal(t, "array", conn.writes[1].op)
	require.Equal(t, 6, conn.writes[1].n)
	// Walk past GET: outer-array-header(1) + inner-array-header(1) + name(1)
	// + arity(1) + flags-header(1) + flagN flag strings + 3 ints.
	flagN := conn.writes[4].n
	cursor := 5 + flagN + 3
	// cursor now at NOSUCH → must be nil.
	require.Equal(t, "null", conn.writes[cursor].op)
	cursor++
	// SET entry: 6-element header then its fields.
	require.Equal(t, "array", conn.writes[cursor].op)
	require.Equal(t, 6, conn.writes[cursor].n)
	require.Equal(t, "set", conn.writes[cursor+1].s)
}

func TestCommandGetKeys_SingleKey(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "GETKEYS", "SET", "foo", "bar")
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, 1, conn.writes[0].n)
	require.Equal(t, "bulk", conn.writes[1].op)
	require.Equal(t, "foo", conn.writes[1].s)
}

func TestCommandGetKeys_MSETLike_DEL(t *testing.T) {
	t.Parallel()
	// DEL has first_key=1, last_key=-1, step=1 (every arg after DEL).
	conn := runCommand(t, "COMMAND", "GETKEYS", "DEL", "k1", "k2", "k3")
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, 3, conn.writes[0].n)
	require.Equal(t, "k1", conn.writes[1].s)
	require.Equal(t, "k2", conn.writes[2].s)
	require.Equal(t, "k3", conn.writes[3].s)
}

// MSET is not in the table (elastickv doesn't route it), so we verify the
// sibling shape via MSET-like args using another multi-key command we DO
// support — HMSET has first_key=1, last_key=1, step=1 (single key). The
// spec-provided MSET example in the ticket is checked against the raw
// redisCommandGetKeys helper below rather than the handler.
func TestRedisCommandGetKeysHelper_MSET(t *testing.T) {
	t.Parallel()
	// Synthetic MSET-shaped metadata: first=1, last=-1, step=2.
	meta := redisCommandMeta{FirstKey: 1, LastKey: -1, Step: 2}
	argv := [][]byte{[]byte("MSET"), []byte("k1"), []byte("v1"), []byte("k2"), []byte("v2")}
	keys := redisCommandGetKeys(meta, argv)
	got := make([]string, 0, len(keys))
	for _, k := range keys {
		got = append(got, string(k))
	}
	require.Equal(t, []string{"k1", "k2"}, got)
}

func TestCommandGetKeys_UnknownCommand(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "GETKEYS", "NOSUCH")
	require.Len(t, conn.writes, 1)
	require.Equal(t, "error", conn.writes[0].op)
	require.Contains(t, conn.writes[0].s, "Invalid command specified")
}

// BZPOPMIN declares last_key=-2: the final argv entry is a blocking timeout
// that must NOT be reported as a key. This pins the negative-offset
// semantics so a future change that collapses all negatives to "to end"
// would trip the test instead of silently mis-routing client writes.
func TestCommandGetKeys_BZPOPMIN_ExcludesTimeout(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "GETKEYS", "BZPOPMIN", "k1", "k2", "0")
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, 2, conn.writes[0].n)
	require.Equal(t, "bulk", conn.writes[1].op)
	require.Equal(t, "k1", conn.writes[1].s)
	require.Equal(t, "bulk", conn.writes[2].op)
	require.Equal(t, "k2", conn.writes[2].s)
}

// Pin the helper against synthetic -2 metadata as well, matching the
// MSET helper test above. Guards the len(argv)+last arithmetic directly.
func TestRedisCommandGetKeysHelper_NegativeLastKeyOffset(t *testing.T) {
	t.Parallel()
	// BZPOPMIN-shaped: first=1, last=-2 (exclude trailing timeout), step=1.
	meta := redisCommandMeta{FirstKey: 1, LastKey: -2, Step: 1}
	argv := [][]byte{[]byte("BZPOPMIN"), []byte("k1"), []byte("k2"), []byte("0")}
	keys := redisCommandGetKeys(meta, argv)
	got := make([]string, 0, len(keys))
	for _, k := range keys {
		got = append(got, string(k))
	}
	require.Equal(t, []string{"k1", "k2"}, got)
}

func TestCommandList_ReturnsAllNames(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "LIST")
	require.Equal(t, "array", conn.writes[0].op)
	// The outer length equals the number of routed commands (argsLen is
	// the 1:1 route-keyed set; redisCommandTable mirrors it because of
	// TestCommand_RouteMatchesTable's invariant).
	require.Equal(t, len(argsLen), conn.writes[0].n)
	names := make([]string, 0, conn.writes[0].n)
	for i := 1; i <= conn.writes[0].n; i++ {
		require.Equal(t, "bulk", conn.writes[i].op)
		names = append(names, conn.writes[i].s)
	}
	require.Contains(t, names, "get")
	require.Contains(t, names, "set")
	require.Contains(t, names, "command")
	// Names must be sorted by UPPERCASE key. Reconstruct the expected
	// ordering from the table rather than a separate sort helper so this
	// remains a single source of truth.
	upper := make([]string, 0, len(names))
	for _, n := range names {
		upper = append(upper, strings.ToUpper(n))
	}
	require.True(t, slices.IsSorted(upper), "names must be emitted in sorted UPPER order, got %v", upper)
}

func TestCommandList_RejectsFilterBy(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "LIST", "FILTERBY", "MODULE", "foo")
	require.Len(t, conn.writes, 1)
	require.Equal(t, "error", conn.writes[0].op)
	require.Contains(t, conn.writes[0].s, "unsupported")
}

func TestCommandDocs_Get(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "DOCS", "GET")
	// RESP2 flat-map shape: outer array of 2 (name + doc-map pair),
	// then a name bulk string, then a 4-element map-shaped doc-map.
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, 2, conn.writes[0].n, "outer array must be (name, docs) pair")
	require.Equal(t, "bulk", conn.writes[1].op)
	require.Equal(t, "get", conn.writes[1].s)
	require.Equal(t, "array", conn.writes[2].op)
	require.Equal(t, 4, conn.writes[2].n)
	require.Equal(t, "summary", conn.writes[3].s)
	require.Equal(t, "", conn.writes[4].s)
	require.Equal(t, "arguments", conn.writes[5].s)
	require.Equal(t, "array", conn.writes[6].op)
	require.Equal(t, 0, conn.writes[6].n)
}

// TestCommandDocs_BareReturnsAllDocs pins that bare COMMAND DOCS
// (no command names) returns docs for every routed command, matching
// real Redis semantics. Previously bare DOCS returned an empty
// array, which broke redis-cli --docs and any client that relied on
// the default full-docs behaviour.
func TestCommandDocs_BareReturnsAllDocs(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "DOCS")
	require.Equal(t, "array", conn.writes[0].op)
	// Outer array is 2 × routed-command-count (each command contributes
	// a name key and a doc-map value slot).
	wantOuterLen := len(routedRedisCommandMetas()) * 2
	require.Equal(t, wantOuterLen, conn.writes[0].n,
		"bare COMMAND DOCS must emit (name, docs) for every routed command")
	// First pair: name bulk, then 4-element doc map.
	require.Equal(t, "bulk", conn.writes[1].op)
	require.NotEmpty(t, conn.writes[1].s)
	require.Equal(t, "array", conn.writes[2].op)
	require.Equal(t, 4, conn.writes[2].n)
	require.Equal(t, "summary", conn.writes[3].s)
}

// TestCommandDocs_UnknownReturnsNamedNil pins the Redis "unknown
// command" shape for DOCS: the requested name key is still written
// (preserving the flat-map layout) but its value is nil. A client
// decoding the response as a map then sees `"FOO" -> nil`, which is
// the canonical "we acknowledged your question, we just have no
// entry" reply.
func TestCommandDocs_UnknownReturnsNamedNil(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "DOCS", "NOSUCH")
	require.Equal(t, "array", conn.writes[0].op)
	require.Equal(t, 2, conn.writes[0].n)
	require.Equal(t, "bulk", conn.writes[1].op)
	require.Equal(t, "NOSUCH", conn.writes[1].s)
	require.Equal(t, "null", conn.writes[2].op)
}

func TestCommand_UnknownSubcommand(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "BADSUB")
	require.Len(t, conn.writes, 1)
	require.Equal(t, "error", conn.writes[0].op)
	require.Contains(t, conn.writes[0].s, "Unknown COMMAND subcommand")
}

func TestCommand_GetKeysAndFlagsRejected(t *testing.T) {
	t.Parallel()
	conn := runCommand(t, "COMMAND", "GETKEYSANDFLAGS", "GET", "k")
	require.Len(t, conn.writes, 1)
	require.Equal(t, "error", conn.writes[0].op)
	require.Contains(t, conn.writes[0].s, "unsupported")
}

func TestCommand_RoutedViaServer(t *testing.T) {
	t.Parallel()
	// Verify the route wiring: a COMMAND request reaching RedisServer.route
	// dispatches to r.command. We cannot exercise redcon.Serve in a unit
	// test without a network listener, so we instead reach into the route
	// map as the live server would.
	srv := &RedisServer{}
	srv.route = map[string]func(conn redcon.Conn, cmd redcon.Command){
		cmdCommand: srv.command,
	}
	handler, ok := srv.route[cmdCommand]
	require.True(t, ok, "COMMAND must be routed")
	conn := &commandRecorder{}
	handler(conn, redcon.Command{Args: [][]byte{[]byte("COMMAND"), []byte("COUNT")}})
	require.Len(t, conn.writes, 1)
	require.Equal(t, "int", conn.writes[0].op)
}

// TestCommand_ArgsLenAllowsBare ensures the validateCmd arity check
// accepts bare `COMMAND` (no subcommand), matching real Redis where
// `COMMAND` with no args is the canonical "dump all" call.
func TestCommand_ArgsLenAllowsBare(t *testing.T) {
	t.Parallel()
	require.Equal(t, -1, argsLen[cmdCommand])
}

// TestCommand_RouteMatchesTable asserts that every command currently
// routed has a metadata row in redisCommandTable. Catches the common
// mistake of adding a new handler without extending the table — the
// runtime path would fall back to zero-metadata + a log warning, but we
// want hard failure in CI.
func TestCommand_RouteMatchesTable(t *testing.T) {
	t.Parallel()
	srv := &RedisServer{}
	// Rebuild the route map via NewRedisServer would need a listener; we
	// instead iterate argsLen (which every routed command has an entry
	// in — see redis.go) as the closest proxy.
	for name := range argsLen {
		_, ok := redisCommandTable[name]
		require.Truef(t, ok, "command %q routed but missing from redisCommandTable", name)
	}
	_ = srv
}

// TestRedisCommandSpecs_NoDuplicateConstants guards the structural
// invariant the spec list maintains: every Constant appears exactly
// once. Without this, the derived argsLen / redisCommandTable maps
// would silently shadow earlier entries (whichever spec listed last
// would win), and a subtle regression in Arity / Flags / key positions
// could ride in unnoticed.
func TestRedisCommandSpecs_NoDuplicateConstants(t *testing.T) {
	t.Parallel()
	seen := make(map[string]int, len(redisCommandSpecs))
	for i, s := range redisCommandSpecs {
		if prev, dup := seen[s.Constant]; dup {
			t.Fatalf("duplicate spec for %q at indices %d and %d", s.Constant, prev, i)
		}
		seen[s.Constant] = i
	}
}

// TestRedisCommandSpecs_HELLOPresent is the explicit regression test
// for the round-N CI failure that triggered this refactor: HELLO was
// added to r.route + argsLen on main but missed from
// redisCommandTable, so COMMAND INFO HELLO returned nil and TestCommand
// _RouteMatchesTable fired. With the unified spec list, HELLO must be
// in redisCommandTable by construction; this test pins the invariant
// in case anyone is tempted to delete the row "for symmetry".
func TestRedisCommandSpecs_HELLOPresent(t *testing.T) {
	t.Parallel()
	hello, ok := redisCommandTable[cmdHello]
	require.Truef(t, ok, "HELLO must have a redisCommandTable row (regression #607)")
	require.Equal(t, "hello", hello.Name)
	require.Equal(t, -1, hello.Arity)
	require.Equal(t, 0, hello.FirstKey)
}

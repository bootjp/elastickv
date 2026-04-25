package adapter

// redis_command_info.go holds the COMMAND-family helpers. The metadata
// table itself (redisCommandTable) and the routed-set source of truth
// (argsLen) both live in adapter/redis_command_specs.go and are derived
// from the canonical redisCommandSpecs slice — adding a new command is
// a single row there, with no risk of drifting between r.route /
// argsLen / redisCommandTable the way HELLO did when it was added to
// the route + arity check but missed the metadata table.
//
// Shape notes (Redis reference):
//   - arity: exact positive arity, or negative meaning "at least |arity|"
//   - flags: one of "readonly" | "write" | "admin". We do NOT currently
//     emit "denyoom" / "pubsub" / "loading" / "stale" / "fast" etc. —
//     real Redis clients only consume this field for coarse routing.
//   - first_key / last_key / step describe the key positions inside the
//     argv. first_key=0 means the command operates on zero keys (pure
//     connection / server commands). last_key=-1 means "all remaining
//     args are keys" (MSET-shaped). step=1 means keys are consecutive;
//     step=2 is used by MSET-like key/value pairs.

import (
	"log"
	"sort"
	"strings"
	"sync"
)

// redisCommandFlag values are string constants so the raw strings are not
// duplicated across the table.
const (
	redisCmdFlagReadonly = "readonly"
	redisCmdFlagWrite    = "write"
	redisCmdFlagAdmin    = "admin"
)

// redisCommandMeta is a single row in the COMMAND table.
type redisCommandMeta struct {
	// Name is the lowercase command name as reported by Redis. Keyed in the
	// table by uppercase for dispatch-time lookup; the lowercase form is
	// what goes onto the wire in COMMAND INFO.
	Name     string
	Arity    int
	Flags    []string
	FirstKey int
	LastKey  int
	Step     int
}

// redisCommandFallbackWarnedOnce deduplicates the "missing metadata" log so
// that a hostile or buggy client probing the same unknown-but-routed name
// cannot generate unbounded log spam. The fallback is a safety net for
// commands that get added to the route but where the table row is
// forgotten; the unit test `TestCommand_RouteMatchesTable` is the hard
// gate, but in production we prefer a degraded reply + one log line over
// a silently-missing command.
var (
	redisCommandFallbackWarnedOnceMu sync.Mutex
	redisCommandFallbackWarnedOnce   = map[string]struct{}{}
)

// routedRedisCommandMetas returns the metadata rows for every command
// currently routed (keyed via argsLen, which is populated 1:1 with the
// route map — see redis.go). Rows are returned in sorted UPPER-case order
// so wire output is deterministic. Names present in redisCommandTable
// produce their real row; names absent from the table but routed produce
// a zero-metadata row and a one-shot log warning. This is the source of
// truth for `COMMAND` (no args) and `COMMAND LIST`; `COMMAND INFO <name>`
// goes through redisCommandTable directly so unknowns produce the nil
// reply required by Redis semantics.
func routedRedisCommandMetas() []redisCommandMeta {
	names := make([]string, 0, len(argsLen))
	for name := range argsLen {
		names = append(names, strings.ToUpper(name))
	}
	sort.Strings(names)
	metas := make([]redisCommandMeta, 0, len(names))
	for _, name := range names {
		if meta, ok := redisCommandTable[name]; ok {
			metas = append(metas, meta)
			continue
		}
		warnMissingRedisCommandMeta(name)
		metas = append(metas, redisCommandMeta{
			Name:     strings.ToLower(name),
			Arity:    -1,
			Flags:    nil,
			FirstKey: 0,
			LastKey:  0,
			Step:     0,
		})
	}
	return metas
}

// warnMissingRedisCommandMeta emits a one-shot warning when a routed
// command has no entry in redisCommandTable. Subsequent calls for the
// same name are silent so a hot dispatch path does not produce log spam.
func warnMissingRedisCommandMeta(upper string) {
	redisCommandFallbackWarnedOnceMu.Lock()
	_, warned := redisCommandFallbackWarnedOnce[upper]
	if !warned {
		redisCommandFallbackWarnedOnce[upper] = struct{}{}
	}
	redisCommandFallbackWarnedOnceMu.Unlock()
	if !warned {
		log.Printf("redis-command: routed command %q has no entry in redisCommandTable; emitting zero-metadata fallback. Add a row to adapter/redis_command_info.go.", upper)
	}
}

// redisCommandGetKeys extracts the key positions from a full command-form
// argv (argv[0] is the command name, argv[1:] are its arguments).
// Returns an error when the command is unknown; returns an empty slice when
// the command is routed but has no keys.
//
// Semantics mirror Redis's own COMMAND GETKEYS:
//   - first_key=0: no keys (empty slice).
//   - last_key=-1: "all args after first_key are keys". step controls spacing
//     (step=1 → every arg; step=2 → every other arg, as in MSET).
//   - last_key=-N (N>1): last key index is len(argv)-N. Commands like
//     BZPOPMIN use -2 to exclude a trailing timeout arg that is NOT a key;
//     treating every negative as "to end" would wrongly expose the timeout
//     via COMMAND GETKEYS and break client key-routing decisions.
//   - otherwise: args in [first_key .. last_key] at `step` stride.
//
// This is *positional*. It does not understand option prefixes (e.g. the
// `EX`/`PX` flags of SET); clients that need option-aware parsing would
// look at Redis 7's key-specs shape, which we explicitly do not emit. For
// the commands elastickv supports the naive positional scheme is correct.
func redisCommandGetKeys(meta redisCommandMeta, argv [][]byte) [][]byte {
	if meta.FirstKey <= 0 {
		return nil
	}
	if meta.Step <= 0 {
		return nil
	}
	if meta.FirstKey >= len(argv) {
		return nil
	}
	last := meta.LastKey
	if last < 0 {
		// Negative last_key is an offset from the end: -1 means the
		// final arg, -2 means the second-to-last, and so on. Use
		// len(argv)+last so BZPOPMIN (-2) excludes its trailing
		// timeout argument instead of claiming the timeout as a key.
		last = len(argv) + last
	}
	if last >= len(argv) {
		last = len(argv) - 1
	}
	if last < meta.FirstKey {
		return nil
	}
	keys := make([][]byte, 0, (last-meta.FirstKey)/meta.Step+1)
	for i := meta.FirstKey; i <= last; i += meta.Step {
		keys = append(keys, argv[i])
	}
	return keys
}

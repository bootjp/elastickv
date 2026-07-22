package adapter

import (
	"time"
)

const (
	redisPairWidth      = 2
	redisTripletWidth   = 3
	pubsubPatternArgMin = 3
	pubsubFirstChannel  = 2
	// redisBlockWaitFallback is the safety-net poll interval that fires
	// in blocking-command wait loops (XREAD BLOCK, BZPOPMIN — and the
	// future BLPOP / BRPOP / BLMOVE) when no in-process write signal
	// arrives. The signal path covers all in-process XADD / ZADD /
	// ZINCRBY on the same node; the fallback covers paths that bypass
	// Signal (Lua flush, follower-applied entries — both addressed by
	// the FSM ApplyObserver tracked in
	// docs/design/2026_04_26_implemented_fsm_apply_observer.md).
	// Most wakeups should come from the waiter signal path; the fallback is
	// only a safety net for missed signals, wrong-type detection, and the
	// BLOCK deadline. Keep it coarse enough that many idle blocking commands
	// do not turn into a constant stream of full key-type scans.
	redisBlockWaitFallback = time.Second
	redisKeywordCount      = "COUNT"

	// setWideColOverhead is the number of extra elements reserved in a set
	// wide-column mutation slice beyond the per-member elements: one for the
	// metadata key and one for the legacy-blob deletion tombstone.
	setWideColOverhead = 2

	// wideColumnBulkScanThreshold is the minimum batch size at which a full
	// prefix scan is used to check field/member existence instead of one
	// ExistsAt per field. Below this threshold the per-key lookups are cheaper
	// because they avoid scanning an arbitrarily large collection.
	wideColumnBulkScanThreshold = 16
)

// HELLO reply and protocol constants. Kept as named constants so the
// linter's "no magic numbers" rule accepts the wire-format values.
const (
	// helloReplyVersion is the version string returned by HELLO's
	// `version` field. Clients like ioredis and jedis perform loose
	// version checks against this field; returning a recent Redis
	// version string keeps maximum compatibility. Intentionally not
	// elastickv's own version — clients that fail to parse a
	// non-Redis version string would treat elastickv as an
	// unsupported backend.
	helloReplyVersion = "7.0.0"
	// helloReplyProto is the RESP protocol version advertised by the
	// server. elastickv is RESP2-only because redcon exposes no RESP3
	// map-reply API.
	helloReplyProto = 2
	// helloReplyArrayLen is the number of elements in the flat
	// alternating key/value reply: 7 pairs = 14 elements.
	helloReplyArrayLen = 14
	// clientSetNameArgCount is the EXACT cmd.Args length for CLIENT
	// SETNAME (CLIENT + SETNAME + name = 3). Kept as an exact-arity
	// constant — not a minimum — because checkClientArity compares
	// `len(cmd.Args) == want`; renaming it to *MinArgs would invite a
	// future refactor that "just" swaps the helper for a >= check and
	// silently re-introduces the wrong-arity silent-OK bug.
	clientSetNameArgCount = 3
	// clientSetInfoArgCount is the EXACT cmd.Args length for CLIENT
	// SETINFO (CLIENT + SETINFO + attr + value = 4). Real Redis
	// rejects any other arity for `client|setinfo`; without this
	// check we would keep returning OK for `CLIENT SETINFO` with no
	// operands, matching exactly the silent-success behaviour this
	// PR is supposed to eliminate for every CLIENT subcommand.
	clientSetInfoArgCount = 4
)

const (
	// helloAuthOptionArity is the total token count a HELLO AUTH
	// clause consumes: keyword + username + password.
	helloAuthOptionArity = 3
	// helloSetNameOptionArity is keyword + name.
	helloSetNameOptionArity = 2
	// streamZeroID is the canonical "empty stream" / "smallest possible ID"
	// sentinel used by XREAD '$' on an empty or missing stream.
	streamZeroID = "0-0"
)

package adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	cockerrors "github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

type zrangeOptions struct {
	withScores bool
	reverse    bool
}

type bzpopminResult struct {
	key   []byte
	entry redisZSetEntry
}

const zsetOpsPerEntry = 2

// buildZSetLegacyMigrationElems returns ops that atomically migrate a legacy
// !redis|zset| blob to wide-column !zs|mem| + !zs|scr| keys. Returns nil if no
// legacy blob exists. The base meta key is also written with the migrated count
// so that resolveZSetMeta works correctly after migration.
func (r *RedisServer) buildZSetLegacyMigrationElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := r.store.GetAt(ctx, redisZSetKey(key), readTS)
	if cockerrors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	value, err := unmarshalZSetValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value.Entries)*zsetOpsPerEntry+setWideColOverhead)
	for _, entry := range value.Entries {
		elems = append(elems,
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetMemberKey(key, []byte(entry.Member)),
				Value: store.MarshalZSetScore(entry.Score),
			},
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetScoreKey(key, entry.Score, []byte(entry.Member)),
				Value: []byte{},
			},
		)
	}
	// Delete the legacy blob.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisZSetKey(key)})
	// Write a base meta so that resolveZSetMeta starts from an accurate count.
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.ZSetMetaKey(key),
		Value: store.MarshalZSetMeta(store.ZSetMeta{Len: int64(len(value.Entries))}),
	})
	return elems, nil
}

// zsetMemberFastScore probes the wide-column score entry for (key,
// member) directly and reports whether it is present and TTL-alive.
// Priority-alignment scope mirrors hashFieldFastLookup: only the
// redisStrKey dual-encoding case is guarded (see
// hasHigherPriorityStringEncoding's narrow-scope caveats). Callers
// must fall back to the full zsetState loader on hit=false to cover
// legacy-blob zsets and nil / WRONGTYPE disambiguation.
//
// Probe ORDER matches hashFieldFastLookup / setMemberFastExists /
// hashFieldFastExists post-PR #565: hit the wide-column score key
// first so the negative case (missing, legacy-blob, wrong-type) does
// not pay the priority-guard seek.
func (r *RedisServer) zsetMemberFastScore(ctx context.Context, key, member []byte, readTS uint64) (score float64, hit, alive bool, err error) {
	raw, err := r.store.GetAt(ctx, store.ZSetMemberKey(key, member), readTS)
	if err != nil {
		if cockerrors.Is(err, store.ErrKeyNotFound) {
			return 0, false, false, nil
		}
		return 0, false, false, cockerrors.WithStack(err)
	}
	score, err = store.UnmarshalZSetScore(raw)
	if err != nil {
		return 0, false, false, cockerrors.WithStack(err)
	}
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return 0, false, false, hErr
	} else if higher {
		return 0, false, false, nil
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return 0, false, false, cockerrors.WithStack(expErr)
	}
	return score, true, !expired, nil
}

// zsetRangeByScoreFast streams the score index for key over the
// caller-supplied [startKey, endKey) byte range, returning the
// decoded entries up to offset+limit. This replaces the
// load-the-whole-zset path used by cmdZRangeByScore / cmdZRevRangeByScore
// when the caller has no script-local mutations and the zset is in
// wide-column form. For a delay-queue poll ("next 10 jobs due by
// now") the cost goes from O(N) member GetAts to O(range_width +
// offset + limit) score-index entries.
//
// hit=false means the fast path cannot safely answer (legacy-blob
// zset present, string-encoding corruption, or empty-result case
// where we cannot distinguish "zset is empty in this range" from
// "key exists as another type / is missing"). Callers MUST take
// the slow path on hit=false so keyTypeAt disambiguation fires.
// reason carries the specific hit=false branch so observers can
// subdivide fallback rates for dashboarding; "" when hit=true.
//
// scoreInRange filter is applied post-scan for exclusive bound
// edge cases; the caller supplies precomputed scan bounds that
// over-approximate toward INclusive and lets this helper filter.
func (r *RedisServer) zsetRangeByScoreFast(
	ctx context.Context,
	key, startKey, endKey []byte,
	reverse bool,
	offset, limit int,
	scoreFilter func(float64) bool,
	readTS uint64,
) ([]redisZSetEntry, bool, monitoring.LuaFastPathFallbackReason, error) {
	if eligible, err := r.zsetFastPathEligible(ctx, key, readTS); err != nil || !eligible {
		return nil, false, monitoring.LuaFastPathFallbackIneligible, err
	}
	// Large-offset short-circuit: once offset >= maxWideScanLimit,
	// the fast path can only scan maxWideScanLimit rows then skip all
	// of them -- guaranteed wasted I/O. Defer to the slow path
	// immediately so it can answer from the full member load without
	// the redundant score-index scan.
	if offset >= maxWideScanLimit {
		return nil, false, monitoring.LuaFastPathFallbackLargeOffset, nil
	}
	scanLimit := zsetFastScanLimit(offset, limit)
	if scanLimit <= 0 || bytes.Compare(startKey, endKey) >= 0 {
		hit, reason, err := r.zsetRangeEmptyFastResult(ctx, key, readTS)
		return nil, hit, reason, err
	}
	scanLimit = zsetFastScanLimitWithTieSentinel(scanLimit, limit)
	kvs, err := r.zsetScoreScan(ctx, startKey, endKey, scanLimit, reverse, readTS)
	if err != nil {
		return nil, false, monitoring.LuaFastPathFallbackOther, err
	}
	return r.finalizeZSetFastRange(ctx, key, kvs, reverse, offset, limit, scanLimit, scoreFilter, readTS)
}

// finalizeZSetFastRange runs the post-scan priority guard, decodes
// the candidate score rows into redisZSetEntry, and applies the TTL
// filter. Factored out so zsetRangeByScoreFast stays under the
// cyclomatic-complexity cap.
//
// Takes scanLimit so we can detect a saturated scan: if the scanner
// returned exactly scanLimit rows AND the caller's request is not
// satisfied (unbounded limit, or fewer than offset+limit matching
// entries), there MAY be more entries beyond the scan window. In
// that case we return hit=false so the slow path can produce the
// authoritative answer -- the fast path MUST NOT silently truncate.
// Bounded ranges with duplicate scores also fall back: the MVCC
// timestamp suffix can disturb physical ordering among equal-score
// member keys, so the slow full-load path owns exact LIMIT semantics
// for those ties.
func (r *RedisServer) finalizeZSetFastRange(
	ctx context.Context, key []byte, kvs []*store.KVPair, reverse bool,
	offset, limit, scanLimit int, scoreFilter func(float64) bool, readTS uint64,
) ([]redisZSetEntry, bool, monitoring.LuaFastPathFallbackReason, error) {
	// Priority guard runs after a candidate hit (mirrors post-PR #565
	// ordering). Skip it on empty result -- the empty-result tail
	// handles disambiguation.
	if len(kvs) > 0 {
		if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
			return nil, false, monitoring.LuaFastPathFallbackOther, hErr
		} else if higher {
			return nil, false, monitoring.LuaFastPathFallbackWrongType, nil
		}
	}
	entries := decodeZSetScoreRange(key, kvs, scoreFilter)
	// Truncation guard: the raw scanner hit its cap AND the caller did
	// not get a satisfied result. Entries beyond the window may
	// exist; defer to the slow path for correctness.
	if zsetFastPathTruncated(len(kvs), scanLimit, len(entries), offset, limit) {
		return nil, false, monitoring.LuaFastPathFallbackTruncated, nil
	}
	if zsetFastPathNeedsTieFallback(entries, limit) {
		return nil, false, monitoring.LuaFastPathFallbackTruncated, nil
	}
	sortZSetEntries(entries)
	if reverse {
		reverseZSetEntries(entries)
	}
	entries = applyZRangeLimit(entries, offset, limit)
	if len(entries) == 0 {
		hit, reason, err := r.zsetRangeEmptyFastResult(ctx, key, readTS)
		return nil, hit, reason, err
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return nil, false, monitoring.LuaFastPathFallbackOther, cockerrors.WithStack(expErr)
	}
	if expired {
		return nil, true, "", nil
	}
	return entries, true, "", nil
}

// zsetFastPathTruncated reports whether the bounded score-index scan
// may have dropped entries that the caller's request would otherwise
// include. Returns true when the scanner returned the full quota
// (scannedRows == scanLimit) AND the caller's request is still
// unsatisfied (unbounded limit or fewer than offset+limit matching
// entries). In that case the caller must fall back to the slow
// full-load path to get the authoritative result.
func zsetFastPathTruncated(scannedRows, scanLimit, matchingEntries, offset, limit int) bool {
	if scannedRows < scanLimit {
		return false
	}
	if limit < 0 {
		return true
	}
	needed := offset + limit
	if needed < offset || needed > maxWideScanLimit {
		return true
	}
	return matchingEntries < needed
}

// zsetFastPathEligible returns false (without error) when a legacy-
// blob zset is present; the caller must take the slow path so
// ensureZSetLoaded / blob decoding runs.
func (r *RedisServer) zsetFastPathEligible(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	legacyExists, err := r.store.ExistsAt(ctx, redisZSetKey(key), readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	return !legacyExists, nil
}

// zsetFastScanLimit clamps offset+limit to maxWideScanLimit so an
// unbounded or malicious LIMIT cannot force an O(N) scan of a large
// zset. A negative limit means "unbounded" at the Redis level; cap it
// at the collection OOM limit.
//
// Check bounds BEFORE adding to avoid signed-integer overflow on
// hostile input (e.g. a Lua script passing offset=limit=math.MaxInt).
// A wrap would produce a negative scanLimit and cause the caller's
// `scanLimit <= 0` branch to misroute a live zset into the
// empty-result tail.
func zsetFastScanLimit(offset, limit int) int {
	// limit == 0: the caller wants zero entries regardless of offset.
	// Return 0 so the caller's `scanLimit <= 0` branch routes to the
	// empty-result tail (which still runs resolveZSetMeta for proper
	// WRONGTYPE / existence disambiguation) instead of a pointless
	// full-quota scan.
	if limit == 0 {
		return 0
	}
	if limit < 0 {
		return maxWideScanLimit
	}
	if offset >= maxWideScanLimit {
		return maxWideScanLimit
	}
	if limit > maxWideScanLimit-offset {
		return maxWideScanLimit
	}
	return offset + limit
}

func zsetFastScanLimitWithTieSentinel(scanLimit, limit int) int {
	if limit <= 0 || scanLimit >= maxWideScanLimit {
		return scanLimit
	}
	return scanLimit + 1
}

// zsetScoreScan picks Forward / Reverse ScanAt based on direction.
func (r *RedisServer) zsetScoreScan(
	ctx context.Context, startKey, endKey []byte, scanLimit int, reverse bool, readTS uint64,
) ([]*store.KVPair, error) {
	if reverse {
		kvs, err := r.store.ReverseScanAt(ctx, startKey, endKey, scanLimit, readTS)
		return kvs, cockerrors.WithStack(err)
	}
	kvs, err := r.store.ScanAt(ctx, startKey, endKey, scanLimit, readTS)
	return kvs, cockerrors.WithStack(err)
}

// decodeZSetScoreRange decodes score-index scan results into
// redisZSetEntry, applying the post-scan score filter for exclusive
// bound edges. Entries that fail to decode are silently dropped --
// they can only appear under data corruption.
func decodeZSetScoreRange(
	key []byte, kvs []*store.KVPair, scoreFilter func(float64) bool,
) []redisZSetEntry {
	entries := make([]redisZSetEntry, 0, len(kvs))
	for _, kv := range kvs {
		score, member, ok := store.ExtractZSetScoreAndMember(kv.Key, key)
		if !ok {
			continue
		}
		if scoreFilter != nil && !scoreFilter(score) {
			continue
		}
		entries = append(entries, redisZSetEntry{Member: string(member), Score: score})
	}
	return entries
}

func zsetFastPathNeedsTieFallback(entries []redisZSetEntry, limit int) bool {
	if limit < 0 {
		return false
	}
	seen := make(map[uint64]struct{}, len(entries))
	for _, entry := range entries {
		bits := math.Float64bits(entry.Score)
		if entry.Score == 0 {
			bits = math.Float64bits(0)
		}
		if _, ok := seen[bits]; ok {
			return true
		}
		seen[bits] = struct{}{}
	}
	return false
}

// zsetRangeEmptyFastResult is the empty-result tail: either the
// score range is genuinely empty on a live zset (return empty +
// hit=true) or the zset does not exist in wide-column form (return
// hit=false so the caller takes the slow path for WRONGTYPE / missing
// disambiguation).
//
// Uses resolveZSetMeta so delta-only wide zsets (a fresh zset whose
// base meta has not been persisted yet, only delta rows) are detected
// as "exists". Using a plain ExistsAt on ZSetMetaKey would miss those
// and force the slow path unnecessarily. Also runs the string-priority
// guard so a corrupted redisStrKey + zset meta surfaces WRONGTYPE via
// the slow path rather than an empty array.
// zsetRangeEmptyFastResult returns (hit, reason, err) for the empty-
// result tail. hit=true means the key is a live zset whose score
// range is simply empty (callers return an empty array and no
// fallback); hit=false carries a specific fallback reason so the
// caller can route its slow-path observation accordingly.
func (r *RedisServer) zsetRangeEmptyFastResult(ctx context.Context, key []byte, readTS uint64) (bool, monitoring.LuaFastPathFallbackReason, error) {
	_, zsetExists, err := r.resolveZSetMeta(ctx, key, readTS)
	if err != nil {
		return false, monitoring.LuaFastPathFallbackOther, cockerrors.WithStack(err)
	}
	if !zsetExists {
		// The key has no ZSet encoding at readTS. Redis semantics:
		//   - key truly absent  → ZRANGEBYSCORE returns empty
		//   - key is another type → ZRANGEBYSCORE returns WRONGTYPE
		// Production metric (PR #572) showed this branch is the
		// hot-path dominant outcome (~96% of ZRANGEBYSCORE calls on
		// BullMQ-style workloads that poll an empty delayed queue).
		// Punting every such call to the slow path repeats the same
		// 3-probe member/meta/delta scan we just did and then
		// re-probes all other types anyway -- pure duplicate I/O.
		//
		// Short-circuit: use keyTypeAt (logical type after TTL check)
		// to distinguish "truly absent" from "wrong type". If None,
		// return hit=true with an empty result -- that is the correct
		// Redis answer and saves the slow-path round-trip. Otherwise
		// fall back so the slow path can produce WRONGTYPE.
		typ, typErr := r.keyTypeAtExpect(ctx, key, readTS, redisTypeZSet)
		if typErr != nil {
			return false, monitoring.LuaFastPathFallbackOther, cockerrors.WithStack(typErr)
		}
		if typ == redisTypeNone {
			return true, "", nil
		}
		return false, monitoring.LuaFastPathFallbackWrongType, nil
	}
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return false, monitoring.LuaFastPathFallbackOther, hErr
	} else if higher {
		return false, monitoring.LuaFastPathFallbackWrongType, nil
	}
	// hasExpired is called for its error-surfacing side effect only:
	// whether the zset is expired or not, a live zset with no members
	// in range returns an empty hit=true result. Keep the call so
	// storage errors during TTL resolution still propagate.
	if _, expErr := r.hasExpired(ctx, key, readTS, true); expErr != nil {
		return false, monitoring.LuaFastPathFallbackOther, cockerrors.WithStack(expErr)
	}
	return true, "", nil
}

type zaddFlags struct {
	nx bool // only add new elements
	xx bool // only update existing elements
	gt bool // only update when new score > current score
	lt bool // only update when new score < current score
}

func parseZAddFlags(args [][]byte) (zaddFlags, int, error) {
	var flags zaddFlags
	i := 2
	for i < len(args) {
		if !flags.applyFlag(strings.ToUpper(string(args[i]))) {
			break
		}
		i++
	}
	if err := flags.validate(); err != nil {
		return zaddFlags{}, 0, err
	}
	return flags, i, nil
}

func (f *zaddFlags) applyFlag(name string) bool {
	switch name {
	case "NX":
		f.nx = true
	case "XX":
		f.xx = true
	case "GT":
		f.gt = true
	case "LT":
		f.lt = true
	default:
		return false
	}
	return true
}

func (f zaddFlags) allows(exists bool, oldScore, newScore float64) bool {
	if (f.nx && exists) || (f.xx && !exists) {
		return false
	}
	return !exists || f.scoreAllowed(oldScore, newScore)
}

func (f zaddFlags) scoreAllowed(oldScore, newScore float64) bool {
	if f.gt && newScore <= oldScore {
		return false
	}
	if f.lt && newScore >= oldScore {
		return false
	}
	return true
}

func (f zaddFlags) validate() error {
	if f.nx && f.xx {
		return fmt.Errorf("ERR XX and NX options at the same time are not compatible")
	}
	if f.nx && (f.gt || f.lt) {
		return fmt.Errorf("ERR GT, LT, and NX options at the same time are not compatible")
	}
	return nil
}

type zaddPair struct {
	score  float64
	member string
}

func parseZAddPairs(remaining [][]byte) ([]zaddPair, error) {
	pairs := make([]zaddPair, 0, len(remaining)/redisPairWidth)
	for i := 0; i < len(remaining); i += redisPairWidth {
		score, err := strconv.ParseFloat(string(remaining[i]), 64)
		if err != nil {
			return nil, fmt.Errorf("parse zadd score: %w", err)
		}
		pairs = append(pairs, zaddPair{score: score, member: string(remaining[i+1])})
	}
	return pairs, nil
}

func (r *RedisServer) zadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	flags, pairStart, err := parseZAddFlags(cmd.Args)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	remaining := cmd.Args[pairStart:]
	if len(remaining) == 0 || len(remaining)%redisPairWidth != 0 {
		conn.WriteError("ERR syntax error")
		return
	}
	pairs, err := parseZAddPairs(remaining)
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var added int
	if err := r.retryRedisWrite(ctx, func() error {
		var err error
		added, err = r.zaddTxn(ctx, cmd.Args[1], flags, pairs)
		return err
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(added)
}

// buildZSetMigrationView extracts member→score from ZSet migration Put elems
// so that applyZAddPair can see migrated members without a store round-trip.
// Returns a map from member name to score; absent members were not migrated.
func buildZSetMigrationView(migrationElems []*kv.Elem[kv.OP], key []byte) map[string]float64 {
	view := make(map[string]float64)
	for _, elem := range migrationElems {
		if elem.Op != kv.Put {
			continue
		}
		m := store.ExtractZSetMemberName(elem.Key, key)
		if m == nil {
			continue
		}
		score, err := store.UnmarshalZSetScore(elem.Value)
		if err == nil {
			view[string(m)] = score
		}
	}
	return view
}

// resolveZSetMemberScore returns the current score and existence for a ZSet
// member. It checks inTxnView first (covers migration elems and earlier pairs
// in the same ZADD call), then falls back to a store GetAt.
func (r *RedisServer) resolveZSetMemberScore(ctx context.Context, memberKey []byte, member string, readTS uint64, inTxnView map[string]float64) (score float64, exists bool, err error) {
	if s, ok := inTxnView[member]; ok {
		return s, true, nil
	}
	raw, getErr := r.store.GetAt(ctx, memberKey, readTS)
	if getErr == nil {
		s, unmarshalErr := store.UnmarshalZSetScore(raw)
		if unmarshalErr != nil {
			return 0, false, cockerrors.WithStack(unmarshalErr)
		}
		return s, true, nil
	}
	if !cockerrors.Is(getErr, store.ErrKeyNotFound) {
		return 0, false, cockerrors.WithStack(getErr)
	}
	return 0, false, nil
}

// applyZAddPair processes one ZADD pair against the wide-column store: reads the
// existing member score (if any), checks the ZADD flags, emits del-old-score /
// put-member / put-score-index ops, and returns the updated elems, the add count
// (0 or 1), and the length delta (0 or +1).
// inTxnView provides an in-transaction view of member→score for members written
// in the same transaction (migration or earlier pairs); checked before GetAt so
// migrated and duplicate members are handled correctly.
func (r *RedisServer) applyZAddPair(ctx context.Context, key []byte, p zaddPair, flags zaddFlags, readTS uint64, elems []*kv.Elem[kv.OP], inTxnView map[string]float64) ([]*kv.Elem[kv.OP], int, int64, error) {
	memberKey := store.ZSetMemberKey(key, []byte(p.member))
	oldScore, memberExists, err := r.resolveZSetMemberScore(ctx, memberKey, p.member, readTS, inTxnView)
	if err != nil {
		return nil, 0, 0, err
	}
	if !flags.allows(memberExists, oldScore, p.score) {
		return elems, 0, 0, nil
	}
	if memberExists {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(p.member))})
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: memberKey, Value: store.MarshalZSetScore(p.score)},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, p.score, []byte(p.member)), Value: []byte{}},
	)
	// Update inTxnView so subsequent pairs (duplicates) see this write.
	inTxnView[p.member] = p.score
	if memberExists {
		return elems, 0, 0, nil
	}
	return elems, 1, 1, nil
}

func (r *RedisServer) zaddTxn(ctx context.Context, key []byte, flags zaddFlags, pairs []zaddPair) (int, error) {
	readTS := r.readTS()
	if err := r.requireKeyTypeOrEmpty(ctx, key, readTS, redisTypeZSet); err != nil {
		return 0, err
	}

	commitTS, err := r.coordinator.Clock().NextFenced()
	if err != nil {
		return 0, cockerrors.Wrap(err, "zaddTxn: allocate commitTS")
	}

	migrationElems, err := r.buildZSetLegacyMigrationElems(ctx, key, readTS)
	if err != nil {
		return 0, err
	}
	// Capacity: each pair may produce 3 ops (del old score + put member + put score index),
	// plus migration elems and a delta key.
	elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+len(pairs)*3+setWideColOverhead) //nolint:mnd // 3 ops per pair
	elems = append(elems, migrationElems...)

	// Seed the in-transaction view from migration elems so that migrated
	// members are not incorrectly counted as new by applyZAddPair.
	inTxnView := buildZSetMigrationView(migrationElems, key)

	// For large batches, mergeZSetBulkScores performs one prefix scan that
	// eliminates O(N) GetAt calls inside applyZAddPair; it is a no-op for
	// batches below wideColumnBulkScanThreshold.
	inTxnView, err = r.mergeZSetBulkScores(ctx, key, readTS, len(pairs), inTxnView)
	if err != nil {
		return 0, err
	}

	added := 0
	lenDelta := int64(0)
	for _, p := range pairs {
		var c int
		var d int64
		elems, c, d, err = r.applyZAddPair(ctx, key, p, flags, readTS, elems, inTxnView)
		if err != nil {
			return 0, err
		}
		added += c
		lenDelta += d
	}

	if len(elems) == 0 {
		return 0, nil
	}

	if lenDelta != 0 {
		deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: lenDelta})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ZSetMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}

	return added, r.dispatchAndSignalZSet(ctx, readTS, commitTS, elems, key)
}

// dispatchAndSignalZSet dispatches the elems through the coordinator
// and, on success, wakes any BZPOPMIN waiter on the same node.
// coordinator.Dispatch blocks until the FSM applies locally, so by
// the time Signal fires the new members are visible at the readTS
// the woken waiter will pick on its next iteration. Pulled out of
// zaddTxn / zincrbyTxn so the parents stay under the cyclop budget
// — the signal step would otherwise add an extra branch on the
// dispatch error path.
func (r *RedisServer) dispatchAndSignalZSet(
	ctx context.Context,
	readTS, commitTS uint64,
	elems []*kv.Elem[kv.OP],
	zsetKey []byte,
) error {
	_, err := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	if err != nil {
		return cockerrors.WithStack(err)
	}
	r.zsetWaiters.Signal(zsetKey)
	return nil
}

// zincrbyTxn performs one attempt of ZINCRBY in wide-column format.
// Returns the new score after applying increment.
func (r *RedisServer) zincrbyTxn(ctx context.Context, key []byte, member string, increment float64) (float64, error) {
	readTS := r.readTS()
	if err := r.requireKeyTypeOrEmpty(ctx, key, readTS, redisTypeZSet); err != nil {
		return 0, err
	}

	memberKey := store.ZSetMemberKey(key, []byte(member))
	commitTS, err := r.coordinator.Clock().NextFenced()
	if err != nil {
		return 0, cockerrors.Wrap(err, "zincrbyTxn: allocate commitTS")
	}

	migrationElems, migErr := r.buildZSetLegacyMigrationElems(ctx, key, readTS)
	if migErr != nil {
		return 0, migErr
	}

	// Check in-txn migration view before falling back to the store
	// (migrated keys are not yet visible at readTS).
	inTxnView := buildZSetMigrationView(migrationElems, key)
	oldScore, memberExists, err := r.resolveZSetMemberScore(ctx, memberKey, member, readTS, inTxnView)
	if err != nil {
		return 0, err
	}

	newScore := oldScore + increment
	if math.IsNaN(newScore) {
		return 0, errors.New("ERR resulting score is not a number (NaN)")
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+3) //nolint:mnd // del old score + put member + put score index
	elems = append(elems, migrationElems...)
	if memberExists {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(member))})
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: memberKey, Value: store.MarshalZSetScore(newScore)},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, newScore, []byte(member)), Value: []byte{}},
	)
	if !memberExists {
		deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ZSetMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}
	if err := r.dispatchAndSignalZSet(ctx, readTS, commitTS, elems, key); err != nil {
		return 0, err
	}
	return newScore, nil
}

func (r *RedisServer) zincrby(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	increment, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var newScore float64
	if err := r.retryRedisWrite(ctx, func() error {
		var txnErr error
		newScore, txnErr = r.zincrbyTxn(ctx, cmd.Args[1], string(cmd.Args[3]), increment)
		return txnErr
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteBulkString(formatRedisFloat(newScore))
}

func parseZRangeOptions(args [][]byte) (zrangeOptions, error) {
	opts := zrangeOptions{}
	for _, arg := range args {
		switch strings.ToUpper(string(arg)) {
		case "WITHSCORES":
			opts.withScores = true
		case "REV":
			opts.reverse = true
		default:
			return zrangeOptions{}, errors.New("ERR syntax error")
		}
	}
	return opts, nil
}

func reverseZSetEntries(entries []redisZSetEntry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}

func writeZRangeReply(conn redcon.Conn, entries []redisZSetEntry, withScores bool) {
	if withScores {
		conn.WriteArray(len(entries) * redisPairWidth)
		for _, entry := range entries {
			conn.WriteBulkString(entry.Member)
			conn.WriteBulkString(formatRedisFloat(entry.Score))
		}
		return
	}

	conn.WriteArray(len(entries))
	for _, entry := range entries {
		conn.WriteBulkString(entry.Member)
	}
}

func removeZSetMembers(members map[string]float64, rawMembers [][]byte) []redisZSetEntry {
	removed := make([]redisZSetEntry, 0, len(rawMembers))
	for _, member := range rawMembers {
		memberKey := string(member)
		if score, ok := members[memberKey]; ok {
			delete(members, memberKey)
			removed = append(removed, redisZSetEntry{Member: memberKey, Score: score})
		}
	}
	return removed
}

func (r *RedisServer) persistZSetEntriesTxn(ctx context.Context, key []byte, readTS uint64, entries []redisZSetEntry) error {
	if len(entries) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}

	memberPrefix := store.ZSetMemberScanPrefix(key)
	memberEnd := store.PrefixScanEnd(memberPrefix)
	probeKVs, probeErr := r.store.ScanAt(ctx, memberPrefix, memberEnd, 1, readTS)
	if probeErr != nil {
		return cockerrors.WithStack(probeErr)
	}
	if len(probeKVs) > 0 {
		current, _, err := r.loadZSetAt(ctx, key, readTS)
		if err != nil {
			return err
		}
		st := &zsetTxnState{
			members:     zsetEntriesToMap(entries),
			origMembers: zsetEntriesToMap(current.Entries),
			isWide:      true,
			exists:      true,
			dirty:       true,
		}
		elems, lenDelta := buildZSetWideElems(key, st)
		if lenDelta != 0 {
			commitTS, err := r.coordinator.Clock().NextFenced()
			if err != nil {
				return cockerrors.Wrap(err, "persistZSetEntriesTxn: allocate commitTS")
			}
			deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: lenDelta})
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetMetaDeltaKey(key, commitTS, 0),
				Value: deltaVal,
			})
			_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
				IsTxn:    true,
				StartTS:  normalizeStartTS(readTS),
				CommitTS: commitTS,
				Elems:    elems,
			})
			return cockerrors.WithStack(dispatchErr)
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}

	payload, err := marshalZSetValue(redisZSetValue{Entries: entries})
	if err != nil {
		return err
	}
	return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisZSetKey(key), Value: payload},
	})
}

func (r *RedisServer) persistZSetRemovalsTxn(ctx context.Context, key []byte, readTS uint64, removed, remaining []redisZSetEntry) error {
	if len(remaining) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	memberPrefix := store.ZSetMemberScanPrefix(key)
	memberEnd := store.PrefixScanEnd(memberPrefix)
	probeKVs, err := r.store.ScanAt(ctx, memberPrefix, memberEnd, 1, readTS)
	if err != nil {
		return cockerrors.WithStack(err)
	}
	if len(probeKVs) == 0 {
		return r.persistZSetEntriesTxn(ctx, key, readTS, remaining)
	}
	commitTS, err := r.coordinator.Clock().NextFenced()
	if err != nil {
		return cockerrors.Wrap(err, "persistZSetRemovalsTxn: allocate commitTS")
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(removed)*zsetOpsPerEntry+1)
	for _, entry := range removed {
		member := []byte(entry.Member)
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetMemberKey(key, member)},
			&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, entry.Score, member)},
		)
	}
	deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: -int64(len(removed))})
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.ZSetMetaDeltaKey(key, commitTS, 0),
		Value: deltaVal,
	})
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return cockerrors.WithStack(dispatchErr)
}

func (r *RedisServer) zrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	start, err := parseInt(cmd.Args[2])
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	stop, err := parseInt(cmd.Args[3])
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	opts, err := parseZRangeOptions(cmd.Args[4:])
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	r.zrangeRead(conn, cmd.Args[1], start, stop, opts)
}

func (r *RedisServer) zrangeRead(conn redcon.Conn, key []byte, start, stop int, opts zrangeOptions) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), key, readTS, redisTypeZSet)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeZSet {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadZSetAt(context.Background(), key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	entries := append([]redisZSetEntry(nil), value.Entries...)
	if opts.reverse {
		reverseZSetEntries(entries)
	}
	s, e := normalizeRankRange(start, stop, len(entries))
	if e < s {
		conn.WriteArray(0)
		return
	}
	writeZRangeReply(conn, entries[s:e+1], opts.withScores)
}

func (r *RedisServer) zrem(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAtExpect(ctx, cmd.Args[1], readTS, redisTypeZSet)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			removed = 0
			return nil
		}
		if typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(ctx, cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		members := zsetEntriesToMap(value.Entries)
		removedEntries := removeZSetMembers(members, cmd.Args[2:])
		removed = len(removedEntries)
		if removed == 0 {
			return nil
		}
		return r.persistZSetRemovalsTxn(ctx, cmd.Args[1], readTS, removedEntries, zsetMapToEntries(members))
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) zremrangebyrank(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	start, err := parseInt(cmd.Args[2])
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	stop, err := parseInt(cmd.Args[3])
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAtExpect(ctx, cmd.Args[1], readTS, redisTypeZSet)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			removed = 0
			return nil
		}
		if typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(ctx, cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		s, e := normalizeRankRange(start, stop, len(value.Entries))
		if e < s {
			removed = 0
			return nil
		}
		remaining := append([]redisZSetEntry{}, value.Entries[:s]...)
		remaining = append(remaining, value.Entries[e+1:]...)
		removedEntries := append([]redisZSetEntry(nil), value.Entries[s:e+1]...)
		removed = len(removedEntries)
		return r.persistZSetRemovalsTxn(ctx, cmd.Args[1], readTS, removedEntries, remaining)
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(removed)
}

// tryBZPopMinWithMode runs one BZPOPMIN attempt against key. The
// fast flag selects keyTypeAtExpectFast (no slow-path fallback, no
// wrongType detection) when true; the caller MUST guarantee that the
// only mutations since the previous full check are signalling writes
// (ZADD/ZINCRBY for zsetWaiters). bzpopminWaitLoop enforces this by
// running fast=false on the first iteration and after every
// fallback-timer wake or wall-time-bounded re-arm.
func (r *RedisServer) tryBZPopMinWithMode(key []byte, fast bool) (*bzpopminResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var result *bzpopminResult
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		var typ redisValueType
		var err error
		if fast {
			typ, err = r.keyTypeAtExpectFast(ctx, key, readTS, redisTypeZSet)
		} else {
			typ, err = r.keyTypeAtExpect(ctx, key, readTS, redisTypeZSet)
		}
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			result = nil
			return nil
		}
		if typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(ctx, key, readTS)
		if err != nil {
			return err
		}
		if len(value.Entries) == 0 {
			result = nil
			return nil
		}
		popped := value.Entries[0]
		remaining := append([]redisZSetEntry(nil), value.Entries[1:]...)

		// Detect wide-column storage.
		memberPrefix := store.ZSetMemberScanPrefix(key)
		memberEnd := store.PrefixScanEnd(memberPrefix)
		probeKVs, probeErr := r.store.ScanAt(ctx, memberPrefix, memberEnd, 1, readTS)
		if probeErr != nil {
			return cockerrors.WithStack(probeErr)
		}
		isWide := len(probeKVs) > 0

		if err := r.persistBZPopMinResult(ctx, key, readTS, popped, remaining, isWide); err != nil {
			return err
		}
		result = &bzpopminResult{key: key, entry: popped}
		return nil
	})
	return result, err
}

func (r *RedisServer) persistBZPopMinResult(ctx context.Context, key []byte, readTS uint64, popped redisZSetEntry, remaining []redisZSetEntry, isWide bool) error {
	if len(remaining) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	if isWide {
		// Wide-column: delete the popped member key + score index, emit delta -1.
		commitTS, err := r.coordinator.Clock().NextFenced()
		if err != nil {
			return cockerrors.Wrap(err, "persistBZPopMinResult: allocate commitTS")
		}
		deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: -1})
		elems := []*kv.Elem[kv.OP]{
			{Op: kv.Del, Key: store.ZSetMemberKey(key, []byte(popped.Member))},
			{Op: kv.Del, Key: store.ZSetScoreKey(key, popped.Score, []byte(popped.Member))},
			{Op: kv.Put, Key: store.ZSetMetaDeltaKey(key, commitTS, 0), Value: deltaVal},
		}
		_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  normalizeStartTS(readTS),
			CommitTS: commitTS,
			Elems:    elems,
		})
		return cockerrors.WithStack(dispatchErr)
	}
	// Legacy blob: write back all remaining entries.
	payload, err := marshalZSetValue(redisZSetValue{Entries: remaining})
	if err != nil {
		return err
	}
	return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisZSetKey(key), Value: payload},
	})
}

func (r *RedisServer) bzpopmin(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyBlockingToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	timeoutSeconds, err := strconv.ParseFloat(string(cmd.Args[len(cmd.Args)-1]), 64)
	if err != nil || timeoutSeconds < 0 {
		conn.WriteError("ERR timeout is not a float or out of range")
		return
	}

	// timeout=0 means infinite wait in Redis; cap at redisDispatchTimeout to prevent goroutine leak.
	if timeoutSeconds == 0 {
		timeoutSeconds = redisDispatchTimeout.Seconds()
	}
	deadline := time.Now().Add(time.Duration(timeoutSeconds * float64(time.Second)))

	keys := cmd.Args[1 : len(cmd.Args)-1]
	r.bzpopminWaitLoop(conn, keys, deadline)
}

// bzpopminWaitLoop runs the BLOCK-window wait loop. Extracted from
// bzpopmin so the parent function stays under the cyclop budget.
// Uses an event-driven signal from the in-process ZADD / ZINCRBY
// path with a fallback timer for paths that bypass the signal.
//
// Registration happens BEFORE the first tryBZPopMin so a signal that
// fires between the check and the wait cannot be lost: the buffered
// channel holds it, and the next select wakes immediately.
func (r *RedisServer) bzpopminWaitLoop(conn redcon.Conn, keys [][]byte, deadline time.Time) {
	handlerCtx := r.handlerContext()
	w, release := r.zsetWaiters.Register(keys)
	defer release()
	// fast tracks whether the next iteration may skip the wrongType
	// slow probe. The first iteration is always full so an existing
	// wrongType key surfaces an immediate WRONGTYPE; subsequent
	// iterations after a signal-driven wake skip the wrongType
	// detection because zsetWaiters.Signal only fires for ZADD /
	// ZINCRBY (neither of which can introduce a wrongType).
	//
	// lastFullCheck wall-time-bounds how long the fast mode can stay
	// active under sustained signal pressure. Without this gate, a
	// hot key whose zsetWaiters.Signal fires faster than each
	// bzpopminTryAllKeys round finishes can keep waiterC perpetually
	// full, starving the fallback timer and letting a wrongType
	// write on a co-registered key (multi-key BZPOPMIN) go
	// undetected for the entire BLOCK window. Demoting `fast` back
	// to false after redisBlockWaitFallback elapses since the last
	// full check restores the #666 ceiling: WRONGTYPE on any
	// registered key surfaces within ~one fallback interval (100 ms)
	// regardless of signal rate. See
	// TestRedis_BZPopMinDetectsWrongTypeUnderSignalLoad for the
	// regression scenario.
	fast := false
	lastFullCheck := time.Now()
	for {
		if handlerCtx.Err() != nil {
			conn.WriteNull()
			return
		}
		var done bool
		if ok := r.runWithHeavyCommandSlot(func() {
			done = r.bzpopminTryAllKeys(conn, keys, fast)
		}); !ok {
			conn.WriteError(errRedisHeavyCommandPoolFull.Error())
			return
		}
		if done {
			return
		}
		if !fast {
			lastFullCheck = time.Now()
		}
		if !time.Now().Before(deadline) {
			conn.WriteNull()
			return
		}
		signaled := waitForBlockedCommandUpdate(handlerCtx, w.C, deadline)
		fast = signaled && time.Since(lastFullCheck) < redisBlockWaitFallback
	}
}

// bzpopminTryAllKeys runs one tryBZPopMinWithMode pass across keys.
// Returns true when a result was written (success or terminal error)
// and the caller should stop the loop, false to continue waiting.
// The fast flag is forwarded to tryBZPopMinWithMode: true selects
// the signal-driven-wake path (skips wrongType detection); false
// selects the full check.
func (r *RedisServer) bzpopminTryAllKeys(conn redcon.Conn, keys [][]byte, fast bool) bool {
	for _, key := range keys {
		result, err := r.tryBZPopMinWithMode(key, fast)
		if err != nil {
			writeRedisError(conn, err)
			return true
		}
		if result == nil {
			continue
		}
		conn.WriteArray(redisTripletWidth)
		conn.WriteBulk(result.key)
		conn.WriteBulkString(result.entry.Member)
		conn.WriteBulkString(formatRedisFloat(result.entry.Score))
		return true
	}
	return false
}

// waitForBlockedCommandUpdate blocks until one of: a write signal
// arrives, the fallback poll tick fires, the parent handlerCtx is
// cancelled, or the BLOCK deadline elapses — whichever happens first.
// The fallback bounds latency for write paths that do not signal (Lua
// flush, follower-applied entries); it cannot exceed the remaining
// BLOCK window so the deadline branch in the caller's loop top always
// gets a chance to fire when the BLOCK expires. Shared by every
// blocking-command wait loop (XREAD BLOCK, BZPOPMIN today; BLPOP /
// BRPOP / BLMOVE in follow-ups) — the keyWaiterRegistry that produces
// waiterC is per-domain (streamWaiters vs zsetWaiters), but the
// timer-and-select shape is identical.
//
// Returns true iff the wake came from waiterC (i.e., a producer
// Signal). False on fallback-timer fire or handlerCtx cancellation.
// Callers that have a signal-implied invariant (e.g., "only ZADD /
// ZINCRBY fires zsetWaiters.Signal") can use the return value to
// pick a faster re-check on the next iteration; fallback wakes
// always need the full check because writes that bypass Signal
// (Lua flush, follower-applied entries, wrongType-introducing
// commands) only become observable through the timer branch.
func waitForBlockedCommandUpdate(handlerCtx context.Context, waiterC <-chan struct{}, deadline time.Time) bool {
	fallback := redisBlockWaitFallback
	if remaining := time.Until(deadline); remaining < fallback {
		fallback = remaining
	}
	timer := time.NewTimer(fallback)
	defer func() {
		if !timer.Stop() {
			// The timer either fired (its case won and the channel
			// was drained inline by select) or is still buffering
			// the tick (waiter / handlerCtx won the race); drain
			// the channel non-blocking so timer GC is clean.
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-waiterC:
		return true
	case <-timer.C:
		return false
	case <-handlerCtx.Done():
		return false
	}
}

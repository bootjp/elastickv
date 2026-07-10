package adapter

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	cockerrors "github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

func (r *RedisServer) hset(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	if r.runStandaloneDedup(conn, cmd) {
		return
	}
	added, err := r.applyHashFieldPairs(cmd.Args[1], cmd.Args[2:])
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(added)
}

func (r *RedisServer) hmset(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	if r.runStandaloneDedup(conn, cmd) {
		return
	}
	if _, err := r.applyHashFieldPairs(cmd.Args[1], cmd.Args[2:]); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteString("OK")
}

// buildHashLegacyMigrationElems returns ops that atomically migrate a legacy
// !redis|hash| blob to wide-column !hs|fld| keys.  Returns nil if no legacy
// blob exists.  The base meta key is also written with the migrated count so
// that resolveHashMeta works correctly after migration.
func (r *RedisServer) buildHashLegacyMigrationElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := r.store.GetAt(ctx, redisHashKey(key), readTS)
	if cockerrors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	value, err := unmarshalHashValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value)+setWideColOverhead)
	for field, val := range value {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey(key, []byte(field)),
			Value: []byte(val),
		})
	}
	ttlMs, err := legacyTTLMillisForRecreateAt(ctx, r.store, key, readTS)
	if err != nil {
		return nil, err
	}
	// Delete the legacy blob.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisHashKey(key)})
	// Write a base meta so that resolveHashMeta starts from an accurate count.
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaKey(key),
		Value: store.MarshalHashMeta(store.HashMeta{Len: int64(len(value)), ExpireAt: ttlMs}),
	})
	return elems, nil
}

// addLegacyHashFieldsToMap adds field names from migration Put elems (fields
// being migrated in the current transaction, not yet visible at readTS) into
// existsMap so that buildHashFieldElems does not count them as new fields.
func addLegacyHashFieldsToMap(migrationElems []*kv.Elem[kv.OP], key []byte, existsMap map[string]struct{}) {
	for _, elem := range migrationElems {
		if elem.Op == kv.Put {
			if f := store.ExtractHashFieldName(elem.Key, key); f != nil {
				existsMap[string(f)] = struct{}{}
			}
		}
	}
}

// buildHashFieldElems iterates over field-value pairs in args, checks each
// field against existsMap to determine if it is new, appends Put operations
// to elems, and returns the updated elems and new-field count.
// existsMap is built by scanHashFieldExistsMap before this call so that
// existence checks are a single bulk scan rather than N ExistsAt round-trips.
func (r *RedisServer) buildHashFieldElems(key []byte, args [][]byte, existsMap map[string]struct{}, elems []*kv.Elem[kv.OP]) ([]*kv.Elem[kv.OP], int) {
	newFields := 0
	for i := 0; i < len(args); i += redisPairWidth {
		field := args[i]
		value := args[i+1]
		fieldStr := string(field)
		fieldKey := store.HashFieldKey(key, field)
		if _, exists := existsMap[fieldStr]; !exists {
			newFields++
			// Mark as seen so duplicate field names in one HSET call are not
			// counted as additional new fields (Redis deduplication semantics).
			existsMap[fieldStr] = struct{}{}
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: value})
	}
	return elems, newFields
}

func (r *RedisServer) applyHashFieldPairs(key []byte, args [][]byte) (int, error) {
	if len(args) == 0 || len(args)%redisPairWidth != 0 {
		return 0, errors.New("ERR wrong number of arguments for hash command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var added int
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.requireKeyTypeOrEmpty(ctx, key, readTS, redisTypeHash); err != nil {
			return err
		}

		startTS := normalizeStartTS(readTS)
		commitTS, err := r.nextCommitTSAfter(ctx, startTS, "applyHashFieldPairs: allocate commitTS")
		if err != nil {
			return cockerrors.WithStack(err)
		}

		// Atomically migrate any legacy blob on first wide-column write.
		// Fetch migration elems before allocating the main elems slice so that
		// the initial capacity accounts for both migration and field Put ops,
		// avoiding a reallocation when a legacy blob is present.
		migrationElems, err := r.buildHashLegacyMigrationElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+len(args)/redisPairWidth+setWideColOverhead)
		elems = append(elems, migrationElems...)

		// Bulk-scan existing fields once so buildHashFieldElems can check
		// existence via a map lookup instead of per-field ExistsAt.
		existsMap, err := r.scanHashFieldExistsMap(ctx, key, readTS)
		if err != nil {
			return err
		}
		// Fields from the legacy blob are being migrated in this same transaction,
		// so they are not yet visible at readTS. Add them to existsMap so that
		// buildHashFieldElems does not count already-existing fields as new.
		addLegacyHashFieldsToMap(migrationElems, key, existsMap)

		var newFields int
		elems, newFields = r.buildHashFieldElems(key, args, existsMap, elems)
		added = newFields

		// Emit a single delta key for all newly-added fields.
		if newFields != 0 {
			deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: int64(newFields)})
			elems = append(elems,
				redisTxnWideHashFenceElem(key),
				&kv.Elem[kv.OP]{
					Op:    kv.Put,
					Key:   store.HashMetaDeltaKey(key, commitTS, 0),
					Value: deltaVal,
				},
			)
		}

		if len(elems) == 0 {
			return nil
		}

		_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  startTS,
			CommitTS: commitTS,
			Elems:    elems,
		})
		return cockerrors.WithStack(dispatchErr)
	})
	return added, err
}

func (r *RedisServer) hget(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]
	field := cmd.Args[2]
	readTS := r.readTS()
	ctx := context.Background()

	// Fast path: look the wide-column field up directly. Live
	// wide-column hashes resolve here in 1 seek + TTL probe versus
	// the ~17 seeks rawKeyTypeAt issues through keyTypeAt. Legacy-
	// blob hashes miss the wide-column key and fall through.
	raw, hit, alive, err := r.hashFieldFastLookup(ctx, key, field, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if hit {
		if !alive {
			conn.WriteNull()
			return
		}
		// WriteBulk sends the payload directly from the []byte backing
		// store; WriteBulkString(string(raw)) would force a []byte →
		// string copy on every fast-path hit.
		conn.WriteBulk(raw)
		return
	}
	r.hgetSlow(conn, ctx, key, field, readTS)
}

// hashFieldFastLookup probes the wide-column field entry directly and
// reports whether it is present and TTL-alive. Returns hit=false when
// the wide-column key is absent, or when the narrow string-encoding
// guard in hasHigherPriorityStringEncoding fires, so the caller
// falls through to hgetSlow.
//
// Priority-alignment scope: this fast path does NOT fully mirror
// rawKeyTypeAt / keyTypeAt's priority checks. The guard only probes
// redisStrKey (the common SET-over-previous-hash corruption case);
// rarer dual-encoding corruption involving HLL, legacy bare keys, or
// list meta / delta entries is NOT caught here and will surface the
// wide-column hash answer instead of the WRONGTYPE / nil response
// keyTypeAt would produce. In normal operation at most one encoding
// exists per user key, so the guard is a guaranteed miss and the
// priority-alignment gap is invisible; pre-existing writers already
// clean up the old encoding before switching types. A full check
// would cost ~3-5 extra seeks per fast-path hit, which would negate
// most of the gain over the ~17-seek keyTypeAt slow path.
func (r *RedisServer) hashFieldFastLookup(ctx context.Context, key, field []byte, readTS uint64) (raw []byte, hit, alive bool, err error) {
	// Probe the wide-column field FIRST so the priority guard only
	// runs on a hit. Placing the guard before the probe made every
	// miss (nonexistent key, legacy-blob hash, or wrong-type) pay an
	// unnecessary ExistsAt on redisStrKey -- pure overhead for the
	// common negative-lookup case and for any workload that still
	// carries legacy-blob encodings. See the PR #565 independent
	// review for the Medium-severity regression this addresses.
	raw, err = r.store.GetAt(ctx, store.HashFieldKey(key, field), readTS)
	if err != nil {
		if cockerrors.Is(err, store.ErrKeyNotFound) {
			return nil, false, false, nil
		}
		return nil, false, false, cockerrors.WithStack(err)
	}
	// Only pay the guard seek when we actually have a hit to defer.
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return nil, false, false, hErr
	} else if higher {
		return nil, false, false, nil
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return nil, false, false, cockerrors.WithStack(expErr)
	}
	return raw, true, !expired, nil
}

// hasHigherPriorityStringEncoding returns true iff the new-format
// string encoding (redisStrKey) exists for key. This is NARROWER
// than rawKeyTypeAt's full string-wins tiebreaker, which also covers
// HyperLogLog (redisHLLKey) and the legacy bare key: those rarer
// dual-encoding corruption cases still reach the wide-column fast
// path and may return the collection-specific answer instead of
// WRONGTYPE / nil.
//
// The narrow scope is deliberate -- expanding the guard to every
// string-priority candidate (3 ExistsAt calls + the list-meta probe)
// would cost ~4-5 extra seeks per fast-path hit, regressing the
// negative case further than the ordering tweak in
// hashFieldFastLookup / setMemberFastExists / hashFieldFastExists
// already saved. Callers that require complete priority alignment
// must take the keyTypeAt slow path explicitly.
func (r *RedisServer) hasHigherPriorityStringEncoding(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	exists, err := r.store.ExistsAt(ctx, redisStrKey(key), readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	return exists, nil
}

// hgetSlow falls back to the type-probing path when hashFieldFastLookup
// misses. Handles legacy-blob hashes and nil / WRONGTYPE disambiguation.
func (r *RedisServer) hgetSlow(conn redcon.Conn, ctx context.Context, key, field []byte, readTS uint64) {
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeHash)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteNull()
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}
	value, err := r.loadHashAt(ctx, key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	fieldValue, ok := value[string(field)]
	if !ok {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(fieldValue)
}

func (r *RedisServer) hmget(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeHash)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	fields := cmd.Args[redisPairWidth:]
	if typ == redisTypeNone {
		conn.WriteArray(len(fields))
		for range cmd.Args[2:] {
			conn.WriteNull()
		}
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteArray(len(fields))
	for _, field := range fields {
		fieldValue, ok := value[string(field)]
		if !ok {
			conn.WriteNull()
			continue
		}
		conn.WriteBulkString(fieldValue)
	}
}

func (r *RedisServer) hdel(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		var err error
		removed, err = r.hdelTxn(ctx, cmd.Args[1], cmd.Args[2:])
		return err
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(removed)
}

// hdelWideColumn deletes the given fields from the wide-column hash and emits a negative delta.
func (r *RedisServer) hdelWideColumn(ctx context.Context, key []byte, fields [][]byte, readTS uint64) (int, error) {
	delElems, removed, err := r.resolveHashFieldDelElems(ctx, key, fields, readTS)
	if err != nil {
		return 0, err
	}
	if removed == 0 {
		return 0, nil
	}
	startTS := normalizeStartTS(readTS)
	commitTS, err := r.nextCommitTSAfter(ctx, startTS, "hdelWideColumn: allocate commitTS")
	if err != nil {
		return 0, cockerrors.WithStack(err)
	}
	elems := delElems
	deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: int64(-removed)})
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaDeltaKey(key, commitTS, 0),
		Value: deltaVal,
	})
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		Elems:    elems,
	})
	return removed, cockerrors.WithStack(dispatchErr)
}

// resolveHashFieldDelElems checks which fields exist using either a bulk scan
// (for large batches) or individual ExistsAt calls (for small batches), then
// returns Del elems for every field that exists and the count of deletions.
func (r *RedisServer) resolveHashFieldDelElems(ctx context.Context, key []byte, fields [][]byte, readTS uint64) ([]*kv.Elem[kv.OP], int, error) {
	var existsMap map[string]struct{}
	if len(fields) >= wideColumnBulkScanThreshold {
		var err error
		existsMap, err = r.scanHashFieldExistsMap(ctx, key, readTS)
		if err != nil {
			return nil, 0, err
		}
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(fields)+1)
	removed := 0
	for _, field := range fields {
		fieldKey := store.HashFieldKey(key, field)
		var exists bool
		if existsMap != nil {
			_, exists = existsMap[string(field)]
		} else {
			var err error
			exists, err = r.store.ExistsAt(ctx, fieldKey, readTS)
			if err != nil {
				return nil, 0, cockerrors.WithStack(err)
			}
		}
		if exists {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: fieldKey})
			removed++
		}
	}
	return elems, removed, nil
}

func (r *RedisServer) hdelTxn(ctx context.Context, key []byte, fields [][]byte) (int, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeHash)
	if err != nil {
		return 0, err
	}
	if typ == redisTypeNone {
		return 0, nil
	}
	if typ != redisTypeHash {
		return 0, wrongTypeError()
	}

	// Wide-column path: check if any !hs|fld| keys exist for this key.
	hashFieldPrefix := store.HashFieldScanPrefix(key)
	hashFieldEnd := store.PrefixScanEnd(hashFieldPrefix)
	wideKVs, err := r.store.ScanAt(ctx, hashFieldPrefix, hashFieldEnd, 1, readTS)
	if err != nil {
		return 0, cockerrors.WithStack(err)
	}
	if len(wideKVs) > 0 {
		return r.hdelWideColumn(ctx, key, fields, readTS)
	}

	// Legacy blob path.
	value, err := r.loadHashAt(ctx, key, readTS)
	if err != nil {
		return 0, err
	}
	removed := removeHashFields(value, fields)
	if removed == 0 {
		return 0, nil
	}
	return removed, r.persistHashTxn(ctx, key, readTS, value)
}

func removeHashFields(value redisHashValue, fields [][]byte) int {
	removed := 0
	for _, field := range fields {
		if _, ok := value[string(field)]; ok {
			delete(value, string(field))
			removed++
		}
	}
	return removed
}

func (r *RedisServer) persistHashTxn(ctx context.Context, key []byte, readTS uint64, value redisHashValue) error {
	if len(value) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	// Wide-column rewrite: write per-field keys and a new base meta.
	// deleteLogicalKeyElems (called by the caller when needed) clears old keys.
	elems := make([]*kv.Elem[kv.OP], 0, len(value)+1)
	for field, val := range value {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey(key, []byte(field)),
			Value: []byte(val),
		})
	}
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaKey(key),
		Value: store.MarshalHashMeta(store.HashMeta{Len: int64(len(value))}),
	})
	// Also remove the legacy blob if it was present.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisHashKey(key)})
	return r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) hexists(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]
	field := cmd.Args[2]
	readTS := r.readTS()
	ctx := context.Background()

	// Fast path: direct wide-column field existence check. ExistsAt
	// is cheaper than GetAt since we don't need the value payload.
	hit, alive, err := r.hashFieldFastExists(ctx, key, field, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if hit {
		if alive {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return
	}
	r.hexistsSlow(conn, ctx, key, field, readTS)
}

func (r *RedisServer) hashFieldFastExists(ctx context.Context, key, field []byte, readTS uint64) (hit, alive bool, err error) {
	// Probe FIRST; guard only on hit. See hashFieldFastLookup for the
	// regression rationale.
	exists, err := r.store.ExistsAt(ctx, store.HashFieldKey(key, field), readTS)
	if err != nil {
		return false, false, cockerrors.WithStack(err)
	}
	if !exists {
		return false, false, nil
	}
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return false, false, hErr
	} else if higher {
		return false, false, nil
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return false, false, cockerrors.WithStack(expErr)
	}
	return true, !expired, nil
}

func (r *RedisServer) hexistsSlow(conn redcon.Conn, ctx context.Context, key, field []byte, readTS uint64) {
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeHash)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}
	value, err := r.loadHashAt(ctx, key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if _, ok := value[string(field)]; ok {
		conn.WriteInt(1)
		return
	}
	conn.WriteInt(0)
}

func (r *RedisServer) hlen(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeHash)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	// Wide-column path: use delta-aggregated metadata for O(1) count.
	count, exists, err := r.resolveHashMeta(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if exists {
		conn.WriteInt64(count)
		return
	}
	// Legacy blob fallback: load all fields and count.
	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(len(value))
}

func (r *RedisServer) hincrby(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	increment, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var current int64
	if err := r.retryRedisWrite(ctx, func() error {
		var txnErr error
		current, txnErr = r.hincrbyTxn(ctx, cmd.Args[1], cmd.Args[2], increment)
		return txnErr
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt64(current)
}

// readHashFieldInt reads the current integer value of a hash field from wide-column or legacy storage.
// Returns (current, isNewField, legacyHashValue, error). legacyHashValue is non-nil only when
// the value came from a legacy JSON blob that needs to be migrated on the next write.
func (r *RedisServer) readHashFieldInt(ctx context.Context, key, field []byte, readTS uint64) (int64, bool, redisHashValue, error) {
	fieldKey := store.HashFieldKey(key, field)
	raw, readErr := r.store.GetAt(ctx, fieldKey, readTS)
	if readErr != nil && !cockerrors.Is(readErr, store.ErrKeyNotFound) {
		return 0, true, nil, cockerrors.WithStack(readErr)
	}
	if readErr == nil {
		current, parseErr := strconv.ParseInt(string(raw), 10, 64)
		if parseErr != nil {
			return 0, false, nil, errors.New("ERR hash value is not an integer")
		}
		return current, false, nil, nil
	}
	// Not in wide-column – check legacy blob.
	legacyValue, legacyErr := r.loadHashAt(ctx, key, readTS)
	if legacyErr != nil {
		return 0, true, nil, legacyErr
	}
	if rawLegacy, ok := legacyValue[string(field)]; ok {
		current, parseErr := strconv.ParseInt(rawLegacy, 10, 64)
		if parseErr != nil {
			return 0, false, nil, errors.New("ERR hash value is not an integer")
		}
		return current, false, legacyValue, nil
	}
	return 0, true, legacyValue, nil
}

// hincrbyWithMigration handles the HINCRBY case where a legacy JSON blob must be migrated
// atomically with the increment operation.
func (r *RedisServer) hincrbyWithMigration(ctx context.Context, key, fieldKey []byte, readTS, commitTS uint64, current int64, isNewField bool, typ redisValueType, increment int64) (int64, error) {
	migrationElems, migErr := r.buildHashLegacyMigrationElems(ctx, key, readTS)
	if migErr != nil {
		return 0, migErr
	}
	current += increment
	newVal := strconv.FormatInt(current, 10)
	elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+setWideColOverhead)
	elems = append(elems, migrationElems...)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: []byte(newVal)})
	if isNewField {
		deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
		elems = append(elems,
			redisTxnWideHashFenceElem(key),
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.HashMetaDeltaKey(key, commitTS, 0),
				Value: deltaVal,
			},
		)
	}
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		ReadKeys: redisTxnWideCreateReadKeys(key, typ, redisTxnWideHashFenceKey),
		Elems:    elems,
	})
	return current, cockerrors.WithStack(dispatchErr)
}

func (r *RedisServer) hincrbyTxn(ctx context.Context, key, field []byte, increment int64) (int64, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeOrEmptyAt(ctx, key, readTS, redisTypeHash)
	if err != nil {
		return 0, err
	}

	startTS := normalizeStartTS(readTS)
	commitTS, err := r.nextCommitTSAfter(ctx, startTS, "hincrbyTxn: allocate commitTS")
	if err != nil {
		return 0, cockerrors.WithStack(err)
	}
	fieldKey := store.HashFieldKey(key, field)

	current, isNewField, legacyValue, err := r.readHashFieldInt(ctx, key, field, readTS)
	if err != nil {
		return 0, err
	}

	// If a legacy blob exists, migrate it atomically with the increment.
	if len(legacyValue) > 0 {
		return r.hincrbyWithMigration(ctx, key, fieldKey, readTS, commitTS, current, isNewField, typ, increment)
	}

	current += increment
	newVal := strconv.FormatInt(current, 10)
	elems := make([]*kv.Elem[kv.OP], 0, setWideColOverhead)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: []byte(newVal)})
	if isNewField {
		deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
		elems = append(elems,
			redisTxnWideHashFenceElem(key),
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.HashMetaDeltaKey(key, commitTS, 0),
				Value: deltaVal,
			},
		)
	}
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		ReadKeys: redisTxnWideCreateReadKeys(key, typ, redisTxnWideHashFenceKey),
		Elems:    elems,
	})
	return current, cockerrors.WithStack(dispatchErr)
}

func (r *RedisServer) incr(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	if r.runStandaloneDedup(conn, cmd) {
		return
	}
	r.incrLegacy(conn, cmd)
}

func (r *RedisServer) incrLegacy(conn redcon.Conn, cmd redcon.Command) {
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var current int64
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(ctx, cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		if typ != redisTypeNone && typ != redisTypeString {
			return wrongTypeError()
		}

		current = 0
		var existingTTL *time.Time
		if typ == redisTypeString {
			raw, ttl, err := r.readRedisStringAt(cmd.Args[1], readTS)
			if err != nil {
				return err
			}
			existingTTL = ttl
			current, err = strconv.ParseInt(string(raw), 10, 64)
			if err != nil {
				return fmt.Errorf("ERR value is not an integer or out of range")
			}
		}
		current++

		// INCR preserves any existing TTL (Redis semantics).
		encoded := encodeRedisStr([]byte(strconv.FormatInt(current, 10)), existingTTL)
		elems := []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: redisStrKey(cmd.Args[1]), Value: encoded},
		}
		if existingTTL != nil {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(cmd.Args[1]), Value: encodeRedisTTL(*existingTTL)})
		} else {
			// Defensively clear any stale/legacy scan index entry so the sweeper
			// cannot later expire a now-persistent key.
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(cmd.Args[1])})
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt64(current)
}

func (r *RedisServer) hgetall(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeHash)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	fields := make([]string, 0, len(value))
	for field := range value {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	conn.WriteArray(len(fields) * redisPairWidth)
	for _, field := range fields {
		conn.WriteBulkString(field)
		conn.WriteBulkString(value[field])
	}
}

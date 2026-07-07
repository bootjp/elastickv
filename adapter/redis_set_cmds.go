package adapter

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	cockerrors "github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

func (r *RedisServer) sadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.mutateExactSet(conn, setKind, cmd.Args[1], cmd.Args[2:], true)
}

func (r *RedisServer) srem(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.mutateExactSet(conn, setKind, cmd.Args[1], cmd.Args[2:], false)
}

func (r *RedisServer) validateExactSetKind(kind string, key []byte, readTS uint64) error {
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return err
	}

	switch kind {
	case setKind:
		return r.validateExactSetType(typ, key, readTS)
	case hllKind:
		return r.validateExactHLLType(typ, key, readTS)
	default:
		return errors.New("ERR unsupported exact set kind")
	}
}

func (r *RedisServer) hllExistsAt(key []byte, readTS uint64) (bool, error) {
	exists, err := r.store.ExistsAt(context.Background(), redisHLLKey(key), readTS)
	if err != nil {
		return false, fmt.Errorf("exists hll: %w", err)
	}
	return exists, nil
}

// buildSetLegacyMigrationElems returns ops that atomically migrate a legacy
// !redis|set| blob to wide-column !st|mem| keys. Returns nil if no legacy
// blob exists.
func (r *RedisServer) buildSetLegacyMigrationElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := r.store.GetAt(ctx, redisSetKey(key), readTS)
	if cockerrors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	value, err := unmarshalSetValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value.Members)+setWideColOverhead)
	for _, member := range value.Members {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.SetMemberKey(key, []byte(member)),
			Value: []byte{},
		})
	}
	// Delete the legacy blob.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisSetKey(key)})
	// Write a base meta so that resolveSetMeta starts from an accurate count.
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.SetMetaKey(key),
		Value: store.MarshalSetMeta(store.SetMeta{Len: int64(len(value.Members))}),
	})
	return elems, nil
}

// buildLegacySetMemberBase extracts member names from migration Put elems
// (members being migrated in the current transaction, invisible at readTS)
// and returns them as a set. Returns nil when no migration is happening.
func buildLegacySetMemberBase(migrationElems []*kv.Elem[kv.OP], key []byte) map[string]struct{} {
	var base map[string]struct{}
	for _, elem := range migrationElems {
		if elem.Op == kv.Put {
			if m := store.ExtractSetMemberName(elem.Key, key); m != nil {
				if base == nil {
					base = make(map[string]struct{})
				}
				base[string(m)] = struct{}{}
			}
		}
	}
	return base
}

func (r *RedisServer) validateExactSetType(typ redisValueType, key []byte, readTS uint64) error {
	if typ == redisTypeSet {
		return nil
	}
	if typ != redisTypeNone {
		return wrongTypeError()
	}

	hllExists, err := r.hllExistsAt(key, readTS)
	if err != nil {
		return err
	}
	if hllExists {
		return wrongTypeError()
	}
	return nil
}

func (r *RedisServer) validateExactHLLType(typ redisValueType, key []byte, readTS uint64) error {
	if typ == redisTypeNone {
		return nil
	}

	hllExists, err := r.hllExistsAt(key, readTS)
	if err != nil {
		return err
	}
	if !hllExists {
		return wrongTypeError()
	}
	return nil
}

func exactSetMembers(value redisSetValue) map[string]struct{} {
	members := make(map[string]struct{}, len(value.Members))
	for _, member := range value.Members {
		members[member] = struct{}{}
	}
	return members
}

func applyExactSetMutation(existing map[string]struct{}, members [][]byte, add bool) int {
	changed := 0
	for _, member := range members {
		memberKey := string(member)
		_, ok := existing[memberKey]
		if add {
			if ok {
				continue
			}
			existing[memberKey] = struct{}{}
			changed++
			continue
		}
		if ok {
			delete(existing, memberKey)
			changed++
		}
	}
	return changed
}

func sortedExactSetMembers(existing map[string]struct{}) []string {
	out := make([]string, 0, len(existing))
	for member := range existing {
		out = append(out, member)
	}
	sort.Strings(out)
	return out
}

func (r *RedisServer) persistExactSetMembersTxn(ctx context.Context, kind string, key []byte, readTS uint64, members map[string]struct{}) error {
	if kind != setKind {
		// HLL and other non-set kinds keep using the legacy blob format.
		if len(members) == 0 {
			elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
			if err != nil {
				return err
			}
			return r.dispatchElems(ctx, true, readTS, elems)
		}
		payload, err := marshalSetValue(redisSetValue{Members: sortedExactSetMembers(members)})
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: redisExactSetStorageKey(kind, key), Value: payload},
		})
	}
	// Wide-column set: full rewrite (used when the whole state is available).
	if len(members) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(members)+setWideColOverhead)
	for member := range members {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.SetMemberKey(key, []byte(member)),
			Value: []byte{},
		})
	}
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.SetMetaKey(key),
		Value: store.MarshalSetMeta(store.SetMeta{Len: int64(len(members))}),
	})
	// Remove legacy blob if present.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisSetKey(key)})
	return r.dispatchElems(ctx, true, readTS, elems)
}

// applySetMemberMutation emits a Put or Del for one set member and returns the
// change count (1) and the signed length delta (+1 or -1), or (0, 0) if no change.
func applySetMemberMutation(elems []*kv.Elem[kv.OP], memberKey []byte, exists, add bool) ([]*kv.Elem[kv.OP], int, int64) {
	if add && !exists {
		return append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: memberKey, Value: []byte{}}), 1, 1
	}
	if !add && exists {
		return append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: memberKey}), 1, -1
	}
	return elems, 0, 0
}

// mutateExactSetLegacy handles SADD/SREM for non-set kinds (e.g. HLL) via the legacy blob path.
func (r *RedisServer) mutateExactSetLegacy(conn redcon.Conn, ctx context.Context, kind string, key []byte, members [][]byte, add bool) {
	var changed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.validateExactSetKind(kind, key, readTS); err != nil {
			return err
		}
		value, err := r.loadSetAt(ctx, kind, key, readTS)
		if err != nil {
			return err
		}
		existing := exactSetMembers(value)
		changed = applyExactSetMutation(existing, members, add)
		if changed == 0 {
			return nil
		}
		return r.persistExactSetMembersTxn(ctx, kind, key, readTS, existing)
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(changed)
}

// mutateExactSetWide handles SADD/SREM for the wide-column set path.
func (r *RedisServer) mutateExactSetWide(conn redcon.Conn, ctx context.Context, key []byte, members [][]byte, add bool) {
	var changed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.validateExactSetKind(setKind, key, readTS); err != nil {
			return err
		}

		commitTS, err := r.coordinator.Clock().NextFenced()
		if err != nil {
			return cockerrors.Wrap(err, "mutateExactSetWide: allocate commitTS")
		}

		migrationElems, migErr := r.buildSetLegacyMigrationElems(ctx, key, readTS)
		if migErr != nil {
			return migErr
		}
		elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+len(members)+setWideColOverhead)
		elems = append(elems, migrationElems...)

		// Extract legacy member names from migration ops so that applySetMemberMutations
		// can treat them as already-existing (they are not yet visible at readTS).
		legacyMemberBase := buildLegacySetMemberBase(migrationElems, key)

		var lenDelta int64
		var mutErr error
		elems, changed, lenDelta, mutErr = r.applySetMemberMutations(ctx, key, members, add, readTS, elems, legacyMemberBase)
		if mutErr != nil {
			return mutErr
		}

		if changed == 0 && len(migrationElems) == 0 {
			return nil
		}

		elems = appendSetDeltaElems(elems, key, lenDelta, commitTS)

		if len(elems) == 0 {
			return nil
		}

		_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  normalizeStartTS(readTS),
			CommitTS: commitTS,
			Elems:    elems,
		})
		return cockerrors.WithStack(dispatchErr)
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(changed)
}

func appendSetDeltaElems(elems []*kv.Elem[kv.OP], key []byte, lenDelta int64, commitTS uint64) []*kv.Elem[kv.OP] {
	if lenDelta == 0 {
		return elems
	}
	if lenDelta > 0 {
		elems = append(elems, redisTxnWideSetFenceElem(key))
	}
	deltaVal := store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: lenDelta})
	return append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.SetMetaDeltaKey(key, commitTS, 0),
		Value: deltaVal,
	})
}

// scanSetMemberExistsMap does a paginated prefix scan of all member keys for
// the given set and returns a map from member name to struct{}{}.
// Using a single prefix scan eliminates the per-member ExistsAt round-trip.
func (r *RedisServer) scanSetMemberExistsMap(ctx context.Context, key []byte, readTS uint64) (map[string]struct{}, error) {
	return r.scanKeyExistsMap(ctx, store.SetMemberScanPrefix(key), readTS,
		func(k []byte) []byte { return store.ExtractSetMemberName(k, key) })
}

// scanHashFieldExistsMap does a paginated prefix scan of all field keys for
// the given hash and returns a map from field name to struct{}{}.
// Using a single prefix scan eliminates per-field ExistsAt round-trips.
func (r *RedisServer) scanHashFieldExistsMap(ctx context.Context, key []byte, readTS uint64) (map[string]struct{}, error) {
	return r.scanKeyExistsMap(ctx, store.HashFieldScanPrefix(key), readTS,
		func(k []byte) []byte { return store.ExtractHashFieldName(k, key) })
}

// mergeZSetBulkScores performs a single prefix scan of ZSet member keys and
// merges the store scores into inTxnView when pairCount >= wideColumnBulkScanThreshold.
// This avoids O(pairCount) individual GetAt round-trips inside applyZAddPair.
// Members already in inTxnView (migration elems or earlier pairs) take precedence.
// Returns inTxnView unchanged when the batch is below the threshold.
func (r *RedisServer) mergeZSetBulkScores(ctx context.Context, key []byte, readTS uint64, pairCount int, inTxnView map[string]float64) (map[string]float64, error) {
	if pairCount < wideColumnBulkScanThreshold {
		return inTxnView, nil
	}
	bulkScores, err := r.scanZSetMemberScoreMap(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if inTxnView == nil {
		return bulkScores, nil
	}
	for m, s := range bulkScores {
		if _, alreadySeen := inTxnView[m]; !alreadySeen {
			inTxnView[m] = s
		}
	}
	return inTxnView, nil
}

// scanZSetMemberScoreMap does a paginated prefix scan of all member keys for
// the given ZSet and returns a map from member name to its current score.
// Using a single prefix scan eliminates O(N) GetAt round-trips in ZADD for
// large batches (>= wideColumnBulkScanThreshold pairs).
func (r *RedisServer) scanZSetMemberScoreMap(ctx context.Context, key []byte, readTS uint64) (map[string]float64, error) {
	scanPrefix := store.ZSetMemberScanPrefix(key)
	scanEnd := store.PrefixScanEnd(scanPrefix)
	scores := make(map[string]float64)
	cursor := scanPrefix
	for {
		scanKVs, err := r.store.ScanAt(ctx, cursor, scanEnd, store.MaxDeltaScanLimit, readTS)
		if err != nil {
			return nil, cockerrors.WithStack(err)
		}
		for _, pair := range scanKVs {
			m := store.ExtractZSetMemberName(pair.Key, key)
			if m == nil {
				continue
			}
			if s, decodeErr := store.UnmarshalZSetScore(pair.Value); decodeErr == nil {
				scores[string(m)] = s
			}
		}
		if len(scanKVs) < store.MaxDeltaScanLimit {
			break
		}
		lastKey := scanKVs[len(scanKVs)-1].Key
		next := make([]byte, len(lastKey)+1)
		copy(next, lastKey)
		cursor = next
	}
	return scores, nil
}

// scanKeyExistsMap paginates through all keys under scanPrefix, extracts a
// name from each key using extractName, and builds a set of existing names.
// It is used by scanSetMemberExistsMap and scanHashFieldExistsMap to eliminate
// per-key ExistsAt round-trips during SADD/SREM/HDEL operations.
func (r *RedisServer) scanKeyExistsMap(ctx context.Context, scanPrefix []byte, readTS uint64, extractName func([]byte) []byte) (map[string]struct{}, error) {
	scanEnd := store.PrefixScanEnd(scanPrefix)
	existsMap := make(map[string]struct{})
	cursor := scanPrefix
	for {
		scanKVs, err := r.store.ScanAt(ctx, cursor, scanEnd, store.MaxDeltaScanLimit, readTS)
		if err != nil {
			return nil, cockerrors.WithStack(err)
		}
		for _, pair := range scanKVs {
			if name := extractName(pair.Key); name != nil {
				existsMap[string(name)] = struct{}{}
			}
		}
		if len(scanKVs) < store.MaxDeltaScanLimit {
			break
		}
		lastKey := scanKVs[len(scanKVs)-1].Key
		next := make([]byte, len(lastKey)+1)
		copy(next, lastKey)
		cursor = next
	}
	return existsMap, nil
}

// initSetExistsMap builds the initial existence map for a set mutation batch.
// For large batches or when legacy members are present it does a bulk prefix
// scan; otherwise it returns an empty (non-nil) map for per-member ExistsAt
// fallback. Legacy members from migration elems are merged in so that members
// already in-flight in the same transaction are treated as existing.
func (r *RedisServer) initSetExistsMap(ctx context.Context, key []byte, members [][]byte, readTS uint64, legacyBase map[string]struct{}) (map[string]struct{}, error) {
	existsMap := make(map[string]struct{})
	if len(members) >= wideColumnBulkScanThreshold || len(legacyBase) > 0 {
		var err error
		existsMap, err = r.scanSetMemberExistsMap(ctx, key, readTS)
		if err != nil {
			return nil, cockerrors.WithStack(err)
		}
	}
	for m := range legacyBase {
		existsMap[m] = struct{}{}
	}
	return existsMap, nil
}

// lookupSetMemberExists reports whether memberStr is present, updating
// existsMap as a cache. For small clean batches (no bulk scan, no legacy
// migration) it falls back to an ExistsAt store read; otherwise it relies
// solely on the pre-built map.
func (r *RedisServer) lookupSetMemberExists(ctx context.Context, memberStr string, memberKey []byte, readTS uint64, existsMap map[string]struct{}, isSmallClean bool) (bool, error) {
	if _, ok := existsMap[memberStr]; ok {
		return true, nil
	}
	if !isSmallClean {
		return false, nil
	}
	exists, err := r.store.ExistsAt(ctx, memberKey, readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	if exists {
		existsMap[memberStr] = struct{}{}
	}
	return exists, nil
}

// applySetMemberMutations resolves existence for each member using either a
// pre-built bulk scan (for large batches) or individual ExistsAt calls (for
// small batches), then applies the mutation to elems.
// The bulk scan threshold is wideColumnBulkScanThreshold.
// legacyBase contains members from a legacy blob being migrated in the same
// transaction; they are not visible at readTS and must be treated as existing.
func (r *RedisServer) applySetMemberMutations(ctx context.Context, key []byte, members [][]byte, add bool, readTS uint64, elems []*kv.Elem[kv.OP], legacyBase map[string]struct{}) ([]*kv.Elem[kv.OP], int, int64, error) {
	existsMap, err := r.initSetExistsMap(ctx, key, members, readTS, legacyBase)
	if err != nil {
		return nil, 0, 0, err
	}
	isSmallClean := len(members) < wideColumnBulkScanThreshold && len(legacyBase) == 0
	changed := 0
	lenDelta := int64(0)
	for _, member := range members {
		memberStr := string(member)
		memberKey := store.SetMemberKey(key, member)
		exists, lookupErr := r.lookupSetMemberExists(ctx, memberStr, memberKey, readTS, existsMap, isSmallClean)
		if lookupErr != nil {
			return nil, 0, 0, lookupErr
		}
		var cnt int
		var d int64
		elems, cnt, d = applySetMemberMutation(elems, memberKey, exists, add)
		changed += cnt
		lenDelta += d
		// Update existsMap to reflect this mutation so that subsequent
		// duplicate members in this call observe the correct in-txn state.
		if add {
			existsMap[memberStr] = struct{}{}
		} else {
			delete(existsMap, memberStr)
		}
	}
	return elems, changed, lenDelta, nil
}

func (r *RedisServer) mutateExactSet(conn redcon.Conn, kind string, key []byte, members [][]byte, add bool) {
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	if kind != setKind {
		r.mutateExactSetLegacy(conn, ctx, kind, key, members, add)
		return
	}
	r.mutateExactSetWide(conn, ctx, key, members, add)
}

func (r *RedisServer) sismember(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]
	member := cmd.Args[2]
	readTS := r.readTS()
	ctx := context.Background()

	hit, alive, err := r.setMemberFastExists(ctx, key, member, readTS)
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
	r.sismemberSlow(conn, ctx, key, member, readTS)
}

func (r *RedisServer) setMemberFastExists(ctx context.Context, key, member []byte, readTS uint64) (hit, alive bool, err error) {
	// Probe FIRST; guard only on hit. See hashFieldFastLookup for the
	// regression rationale.
	exists, err := r.store.ExistsAt(ctx, store.SetMemberKey(key, member), readTS)
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

func (r *RedisServer) sismemberSlow(conn redcon.Conn, ctx context.Context, key, member []byte, readTS uint64) {
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeSet {
		conn.WriteError(wrongTypeMessage)
		return
	}
	value, err := r.loadSetAt(ctx, setKind, key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if slices.Contains(value.Members, string(member)) {
		conn.WriteInt(1)
		return
	}
	conn.WriteInt(0)
}

func (r *RedisServer) smembers(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeSet {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadSetAt(context.Background(), setKind, cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteArray(len(value.Members))
	for _, member := range value.Members {
		conn.WriteBulkString(member)
	}
}

func (r *RedisServer) pfadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var changed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.validateExactSetKind(hllKind, cmd.Args[1], readTS); err != nil {
			return err
		}

		value, err := r.loadSetAt(context.Background(), hllKind, cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		existing := exactSetMembers(value)
		changed = applyExactSetMutation(existing, cmd.Args[2:], true)
		if changed == 0 {
			return nil
		}

		return r.persistExactSetMembersTxn(ctx, hllKind, cmd.Args[1], readTS, existing)
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	if changed == 0 {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (r *RedisServer) pfcount(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	readTS := r.readTS()
	union := map[string]struct{}{}
	for _, key := range cmd.Args[1:] {
		typ, err := r.keyTypeAt(ctx, key, readTS)
		if err != nil {
			writeRedisError(conn, err)
			return
		}
		if typ != redisTypeNone {
			hllExists, err := r.store.ExistsAt(ctx, redisHLLKey(key), readTS)
			if err != nil {
				writeRedisError(conn, err)
				return
			}
			if !hllExists {
				conn.WriteError(wrongTypeMessage)
				return
			}
		}
		value, err := r.loadSetAt(ctx, hllKind, key, readTS)
		if err != nil {
			writeRedisError(conn, err)
			return
		}
		for _, member := range value.Members {
			union[member] = struct{}{}
		}
	}
	conn.WriteInt(len(union))
}

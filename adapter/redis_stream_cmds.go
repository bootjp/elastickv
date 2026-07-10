package adapter

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	cockerrors "github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type xreadRequest struct {
	block    time.Duration
	count    int
	keys     [][]byte
	afterIDs []string
}

type xreadOptions struct {
	block        time.Duration
	count        int
	streamsIndex int
}

type xreadResult struct {
	key     []byte
	entries []redisStreamEntry
}

type xaddRequest struct {
	// maxLen is -1 when no MAXLEN clause was given, 0 for explicit MAXLEN 0,
	// or a positive value for MAXLEN <n>.
	maxLen int
	id     string
	fields []string
}

func parseXAddMaxLen(args [][]byte) (int, int, error) {
	argIndex := redisPairWidth
	if len(args) < 5 || !strings.EqualFold(string(args[argIndex]), "MAXLEN") {
		return -1, argIndex, nil
	}

	argIndex++
	if argIndex < len(args) && string(args[argIndex]) == "~" {
		argIndex++
	}
	if argIndex >= len(args) {
		return 0, 0, errors.New("ERR syntax error")
	}

	maxLen, err := strconv.Atoi(string(args[argIndex]))
	if err != nil || maxLen < 0 {
		return 0, 0, errors.New("ERR syntax error")
	}
	return maxLen, argIndex + 1, nil
}

func parseXAddFields(args [][]byte, argIndex int) ([]string, error) {
	if argIndex >= len(args) {
		return nil, errors.New("ERR syntax error")
	}
	if (len(args)-argIndex)%redisPairWidth != 0 {
		return nil, errors.New("ERR wrong number of arguments for 'XADD' command")
	}

	fields := make([]string, 0, len(args)-argIndex)
	for _, arg := range args[argIndex:] {
		fields = append(fields, string(arg))
	}
	return fields, nil
}

func parseXAddRequest(args [][]byte) (xaddRequest, error) {
	maxLen, argIndex, err := parseXAddMaxLen(args)
	if err != nil {
		return xaddRequest{}, err
	}
	if argIndex >= len(args) {
		return xaddRequest{}, errors.New("ERR syntax error")
	}
	fields, err := parseXAddFields(args, argIndex+1)
	if err != nil {
		return xaddRequest{}, err
	}
	return xaddRequest{maxLen: maxLen, id: string(args[argIndex]), fields: fields}, nil
}

// nextXAddID computes the ID the next XADD should assign.
//
// hasLast reports whether the stream currently tracks a "last" ID (i.e. at
// least one XADD has ever succeeded). last{Ms,Seq} must be the highest ID
// the stream has ever seen — not merely the current tail — so that XADD '*'
// stays strictly monotonic even after XTRIM removes the current tail.
func nextXAddID(hasLast bool, lastMs, lastSeq uint64, requested string) (string, error) {
	if requested != "*" {
		requestedID, requestedValid := tryParseRedisStreamID(requested)
		if !requestedValid {
			return "", errors.New("ERR Invalid stream ID specified as stream command argument")
		}
		// Redis rejects IDs <= 0-0 unconditionally; a stream entry with
		// ID "0-0" is unreachable via XREAD ... 0 (which means "after 0-0").
		if requestedID.ms == 0 && requestedID.seq == 0 {
			return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
		}
		if hasLast && compareStreamIDs(requestedID.ms, requestedID.seq, lastMs, lastSeq) <= 0 {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		return requested, nil
	}
	return autoXAddID(safeUnixMilliToUint64(time.Now().UnixMilli()), hasLast, lastMs, lastSeq)
}

// autoXAddID resolves XADD '*' to a concrete stream ID given a wall-clock
// nowMs. Pulled out of nextXAddID so the auto-ID branch is testable
// without depending on time.Now() — the only un-injectable dependency is
// already isolated in the caller.
//
// Two corner cases the caller cannot rely on the wall clock to avoid:
//
//   - nowMs == 0 on a fresh stream (!hasLast). A naive "<nowMs>-0" reply
//     yields "0-0", which Redis explicitly rejects as a stream ID and
//     which XREAD ... 0 would treat as the empty after-marker. Bump the
//     seq to 1 so the first auto-generated entry is "0-1" — strictly
//     greater than 0-0 and reachable via XREAD ... 0. (This case fires
//     only when safeUnixMilliToUint64 clamped a pre-epoch clock to 0;
//     under any sane clock, nowMs is well above 0.)
//
//   - nowMs <= lastMs. Advance past lastMs/lastSeq via bumpStreamID so
//     the stream stays strictly monotonic even across a backwards clock
//     step or a corrupted meta where lastMs is far in the future.
func autoXAddID(nowMs uint64, hasLast bool, lastMs, lastSeq uint64) (string, error) {
	if !hasLast || nowMs > lastMs {
		seq := uint64(0)
		if nowMs == 0 {
			seq = 1
		}
		return strconv.FormatUint(nowMs, 10) + "-" + strconv.FormatUint(seq, 10), nil
	}
	// Either nowMs == lastMs (same millisecond), or lastMs is in the future
	// (monotonic guarantee across a backwards clock step or a corrupted
	// meta). Advance past lastMs-lastSeq via bumpStreamID; if the ID space
	// is exhausted, surface an error rather than wrap to 0.
	ms, seq, err := bumpStreamID(lastMs, lastSeq)
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(ms, 10) + "-" + strconv.FormatUint(seq, 10), nil
}

// safeUnixMilliToUint64 returns ms as uint64, clamping any negative value
// (caused by a system clock set before the Unix epoch) to 0. Without this
// clamp, a direct uint64 cast of a negative int64 would yield a value
// near math.MaxUint64, which would then make nextXAddID's "future-ms"
// branch chase that pathological value forever — effectively wedging
// every subsequent XADD '*' on the stream until the clock recovers.
// The lastMs/lastSeq monotonic guarantee carries the stream forward
// from there via bumpStreamID.
func safeUnixMilliToUint64(ms int64) uint64 {
	if ms < 0 {
		return 0
	}
	return uint64(ms) //nolint:gosec // negative values handled above
}

// bumpStreamID returns the strictly-greater successor of (ms, seq) within
// the uint64-uint64 stream ID space. Bumps seq; on seq overflow carries
// to ms+1, seq=0; on ms overflow returns an error (no representable
// successor) instead of wrapping to 0-0, which would produce a duplicate
// or non-monotonic ID.
func bumpStreamID(ms, seq uint64) (uint64, uint64, error) {
	switch {
	case seq < ^uint64(0):
		return ms, seq + 1, nil
	case ms < ^uint64(0):
		return ms + 1, 0, nil
	default:
		return 0, 0, errors.New("ERR The stream has exhausted the ID space")
	}
}

func compareStreamIDs(lms, lseq, rms, rseq uint64) int {
	switch {
	case lms < rms:
		return -1
	case lms > rms:
		return 1
	case lseq < rseq:
		return -1
	case lseq > rseq:
		return 1
	default:
		return 0
	}
}

func (r *RedisServer) xadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	req, err := parseXAddRequest(cmd.Args)
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	var id string
	if err := r.retryRedisWrite(ctx, func() error {
		id, err = r.xaddTxn(ctx, cmd.Args[1], req)
		return err
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteBulkString(id)
}

func (r *RedisServer) xaddTxn(ctx context.Context, key []byte, req xaddRequest) (string, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeStream)
	if err != nil {
		return "", err
	}
	if typ != redisTypeNone && typ != redisTypeStream {
		return "", wrongTypeError()
	}

	legacyCleanup, meta, metaFound, err := r.streamWriteBase(ctx, key, readTS)
	if err != nil {
		return "", err
	}

	id, parsedID, err := resolveXAddID(meta, metaFound, req.id)
	if err != nil {
		return "", err
	}

	if err := xaddEnforceMaxWideColumn(key, meta.Length, req.maxLen); err != nil {
		return "", err
	}

	entryValue, err := marshalStreamEntry(newRedisStreamEntry(id, req.fields))
	if err != nil {
		return "", err
	}

	// Capacity hint covers: optional legacy-cleanup Del + one entry Put +
	// one meta Put + the trim Dels. legacyCleanup is at most one element,
	// and only non-empty on the very first write against a stream whose
	// pre-migration blob is still on disk.
	const xaddFixedElemCount = 2
	elems := make([]*kv.Elem[kv.OP], 0,
		len(legacyCleanup)+xaddFixedElemCount+estimateXAddTrimCount(req.maxLen, meta.Length))
	elems = append(elems, legacyCleanup...)
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.StreamEntryKey(key, parsedID.ms, parsedID.seq),
		Value: entryValue,
	})

	nextLen, trim, err := r.xaddTrimIfNeeded(ctx, key, readTS, req.maxLen, meta.Length+1)
	if err != nil {
		return "", err
	}
	elems = append(elems, trim...)
	elems = appendMaxLenZeroSelfDel(elems, req.maxLen, key, parsedID)

	metaBytes, err := store.MarshalStreamMeta(store.StreamMeta{
		Length:  nextLen,
		LastMs:  parsedID.ms,
		LastSeq: parsedID.seq,
	})
	if err != nil {
		return "", cockerrors.WithStack(err)
	}
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.StreamMetaKey(key), Value: metaBytes})

	return id, r.dispatchAndSignalStream(ctx, true, readTS, elems, key)
}

// dispatchAndSignalStream dispatches the elems through the coordinator
// and, on success, wakes any XREAD BLOCK waiter on the same node.
// dispatchElems blocks until the FSM applies locally, so by the time
// Signal fires the new entries are visible at the readTS the woken
// waiter will pick on its next iteration. Pulled out of xaddTxn so the
// parent function stays under the cyclop budget — the signal step
// would otherwise add an extra branch on the dispatch error path.
func (r *RedisServer) dispatchAndSignalStream(
	ctx context.Context,
	isTxn bool,
	startTS uint64,
	elems []*kv.Elem[kv.OP],
	streamKey []byte,
) error {
	if err := r.dispatchElems(ctx, isTxn, startTS, elems); err != nil {
		return err
	}
	r.streamWaiters.Signal(streamKey)
	return nil
}

// appendMaxLenZeroSelfDel handles the MAXLEN 0 edge case. The trim loop
// runs scans at readTS and therefore cannot see the entry we just queued,
// so without this follow-up Del the freshly-added entry would survive
// while meta.Length said 0. The coordinator applies elems in order at a
// single commitTS, so appending Del after the Put tombstones it cleanly.
func appendMaxLenZeroSelfDel(elems []*kv.Elem[kv.OP], maxLen int, key []byte, parsedID redisStreamID) []*kv.Elem[kv.OP] {
	if maxLen != 0 {
		return elems
	}
	return append(elems, &kv.Elem[kv.OP]{
		Op:  kv.Del,
		Key: store.StreamEntryKey(key, parsedID.ms, parsedID.seq),
	})
}

// xaddEnforceMaxWideColumn rejects an XADD that would push the stream past
// maxWideColumnItems when no MAXLEN clause could rescue it. A MAXLEN >= 0
// and <= the cap keeps the committed length bounded even when meta.Length is
// already at the ceiling, so we only reject on the ungated path.
func xaddEnforceMaxWideColumn(key []byte, currentLength int64, maxLen int) error {
	if maxLen >= 0 && maxLen <= maxWideColumnItems {
		return nil
	}
	if currentLength < int64(maxWideColumnItems) {
		return nil
	}
	return cockerrors.Wrapf(ErrCollectionTooLarge,
		"stream %q would exceed %d entries", key, maxWideColumnItems)
}

// xaddTrimIfNeeded returns (finalLength, trimElems, err) for an XADD.
// estimateXAddTrimCount returns how many entries the XADD's MAXLEN trim
// will remove, or 0 when maxLen is unset or the current length fits under
// it. Used only as a capacity hint for the elems slice; the actual trim
// list is computed by xaddTrimIfNeeded.
func estimateXAddTrimCount(maxLen int, currentLength int64) int {
	if maxLen < 0 {
		return 0
	}
	nextLen := currentLength + 1
	if nextLen <= int64(maxLen) {
		return 0
	}
	// Compute in int64 and clamp at maxWideColumnItems. A capacity hint
	// of math.MaxInt would let make() try to allocate ~16 EiB on 64-bit
	// targets and either panic or OOM; capping at the wide-column ceiling
	// keeps the hint useful (saves slice growth in the common case) while
	// preventing pathological allocation when meta.Length is corrupted.
	// xaddTrimIfNeeded enforces the same cap on the actual trim count;
	// this hint just sizes the elems slice.
	diff := nextLen - int64(maxLen)
	if diff <= 0 {
		return 0
	}
	if diff > int64(maxWideColumnItems) {
		return maxWideColumnItems
	}
	return int(diff)
}

// When maxLen < 0 (unset) or the new length fits under it, no trim is
// emitted and trimElems is nil; otherwise Del operations for the oldest
// entries are returned and finalLength equals maxLen. All scans use the
// caller's ctx and readTS so the trim happens at the same MVCC snapshot
// as the write.
func (r *RedisServer) xaddTrimIfNeeded(
	ctx context.Context,
	key []byte,
	readTS uint64,
	maxLen int,
	candidateLen int64,
) (int64, []*kv.Elem[kv.OP], error) {
	if maxLen < 0 || candidateLen <= int64(maxLen) {
		return candidateLen, nil, nil
	}
	// int64 arithmetic + clamp at maxWideColumnItems. A single XADD must
	// not emit more than maxWideColumnItems Del operations: it would risk
	// exceeding the Raft message-size limit and would force a single
	// commit to materialise an unbounded list of keys. The cap is loose
	// enough that it never bites in normal operation (xaddEnforceMaxWideColumn
	// rejects streams whose committed length is already at the ceiling),
	// but defends against a corrupted meta.Length feeding the trim path.
	diff := candidateLen - int64(maxLen)
	if diff <= 0 {
		return candidateLen, nil, nil
	}
	count := maxWideColumnItems
	if diff <= int64(maxWideColumnItems) {
		count = int(diff)
	}
	trim, err := r.buildXTrimHeadElems(ctx, key, readTS, count)
	if err != nil {
		return 0, nil, err
	}
	// Final length must reflect the trim that actually committed, not
	// the requested maxLen, so that meta.Length stays consistent with
	// the entries on disk when the cap kicks in or the scan returns
	// fewer rows than requested. MAXLEN 0 is a special case: the
	// freshly-added entry is removed by appendMaxLenZeroSelfDel in the
	// caller, so the post-commit length is 0 regardless of what trim
	// did to the pre-existing rows.
	if maxLen == 0 {
		return 0, trim, nil
	}
	return candidateLen - int64(len(trim)), trim, nil
}

// streamWriteBase prepares a write to a stream. Returns the loaded meta
// (zero value when the stream has never been written) and, when a legacy
// single-blob key is still present on disk, a Del elem that the caller
// must include in the write transaction. No migration is performed:
// legacy entries are discarded, not re-materialised into the new layout.
// This matches the PR #620 operator directive that pre-migration data is
// expendable and is cleared explicitly rather than saved.
func (r *RedisServer) streamWriteBase(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], store.StreamMeta, bool, error) {
	meta, metaFound, err := r.loadStreamMetaAt(ctx, key, readTS)
	if err != nil {
		return nil, store.StreamMeta{}, false, err
	}
	if metaFound {
		return nil, meta, true, nil
	}
	legacyCleanup, err := r.legacyStreamCleanupElems(ctx, key, readTS)
	if err != nil {
		return nil, store.StreamMeta{}, false, err
	}
	return legacyCleanup, store.StreamMeta{}, false, nil
}

// legacyStreamCleanupElems returns a Del elem for the legacy single-blob
// key if one is still present on disk, or nil otherwise. Called by
// streamWriteBase and deleteStreamWideColumnElems so every write or delete
// that touches a stream also evicts any stale legacy data.
func (r *RedisServer) legacyStreamCleanupElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	legacyKey := redisStreamKey(key)
	exists, err := r.store.ExistsAt(ctx, legacyKey, readTS)
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	if !exists {
		return nil, nil
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Del, Key: legacyKey}}, nil
}

// resolveXAddID resolves the requested ID (possibly '*') against the current
// stream meta and returns the assigned string ID plus its parsed form.
func resolveXAddID(meta store.StreamMeta, hasMeta bool, requested string) (string, redisStreamID, error) {
	var (
		hasLast         bool
		lastMs, lastSeq uint64
	)
	if hasMeta {
		// LastMs/LastSeq carry the highest ID ever assigned even when the
		// stream was trimmed to empty, so auto-ID generation stays
		// monotonic across MAXLEN=0 / XDEL-all cycles.
		hasLast = meta.Length > 0 || meta.LastMs != 0 || meta.LastSeq != 0
		lastMs, lastSeq = meta.LastMs, meta.LastSeq
	}
	id, err := nextXAddID(hasLast, lastMs, lastSeq, requested)
	if err != nil {
		return "", redisStreamID{}, err
	}
	parsed, ok := tryParseRedisStreamID(id)
	if !ok {
		return "", redisStreamID{}, errors.New("ERR Invalid stream ID specified as stream command argument")
	}
	return id, parsed, nil
}

// buildXTrimHeadElems emits Del operations for the oldest `count` entries
// in the entry-per-key layout via a bounded range scan at the caller's
// MVCC snapshot (ctx, readTS). Mixing a later timestamp here would let us
// tombstone keys the caller's view never saw.
func (r *RedisServer) buildXTrimHeadElems(
	ctx context.Context,
	key []byte,
	readTS uint64,
	count int,
) ([]*kv.Elem[kv.OP], error) {
	if count <= 0 {
		return nil, nil
	}
	// Defense-in-depth cap on the per-trim scan so a caller that asked
	// for math.MaxInt (corrupted meta upstream) cannot try to materialise
	// an unbounded list of Del elems in a single transaction. Callers
	// (xaddTrimIfNeeded, xtrimTxn) already cap; this is a belt-and-braces
	// guard on the boundary that actually allocates.
	if count > maxWideColumnItems {
		count = maxWideColumnItems
	}
	prefix := store.StreamEntryScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	kvs, err := r.store.ScanAt(ctx, prefix, end, count, readTS)
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(kvs))
	for _, pair := range kvs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: append([]byte(nil), pair.Key...)})
	}
	return elems, nil
}

func parseXTrimMaxLen(args [][]byte) (int, error) {
	if !strings.EqualFold(string(args[2]), "MAXLEN") {
		return 0, errors.New("ERR syntax error")
	}

	argIndex := 3
	if argIndex < len(args) && (string(args[argIndex]) == "~" || string(args[argIndex]) == "=") {
		argIndex++
	}
	if argIndex != len(args)-1 {
		return 0, errors.New("ERR syntax error")
	}

	maxLen, err := strconv.Atoi(string(args[argIndex]))
	if err != nil || maxLen < 0 {
		return 0, errors.New("ERR syntax error")
	}
	return maxLen, nil
}

func (r *RedisServer) xtrim(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	maxLen, err := parseXTrimMaxLen(cmd.Args)
	if err != nil {
		conn.WriteError("ERR syntax error")
		return
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		var err error
		removed, err = r.xtrimTxn(ctx, cmd.Args[1], maxLen)
		return err
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(removed)
}

// streamTypeForWrite returns (true, nil) when the key is either absent
// (no-op write) or already a stream, (false, nil) when the caller should
// short-circuit with "no stream here", and (_, err) for wrong-type or
// store errors. Extracted from xtrimTxn so the outer function stays
// within the cyclop budget.
func (r *RedisServer) streamTypeForWrite(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeStream)
	if err != nil {
		return false, err
	}
	switch typ {
	case redisTypeNone:
		return false, nil
	case redisTypeStream:
		return true, nil
	case redisTypeString, redisTypeList, redisTypeHash, redisTypeSet, redisTypeZSet:
		return false, wrongTypeError()
	default:
		return false, wrongTypeError()
	}
}

// flushLegacyCleanupOnTrimNoOp commits the legacy-blob Del + meta Put
// for an XTRIM whose length is already under maxLen. Without this
// flush a subsequent read would still find the stale legacy blob.
// Returns 0 removed entries; callers use that directly.
func (r *RedisServer) flushLegacyCleanupOnTrimNoOp(
	ctx context.Context, readTS uint64, key []byte,
	meta store.StreamMeta, legacyCleanup []*kv.Elem[kv.OP],
) (int, error) {
	if len(legacyCleanup) == 0 {
		return 0, nil
	}
	metaBytes, err := store.MarshalStreamMeta(meta)
	if err != nil {
		return 0, cockerrors.WithStack(err)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(legacyCleanup)+1)
	elems = append(elems, legacyCleanup...)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.StreamMetaKey(key), Value: metaBytes})
	return 0, r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) xtrimTxn(ctx context.Context, key []byte, maxLen int) (int, error) {
	readTS := r.readTS()
	proceed, err := r.streamTypeForWrite(ctx, key, readTS)
	if err != nil || !proceed {
		return 0, err
	}

	legacyCleanup, meta, _, err := r.streamWriteBase(ctx, key, readTS)
	if err != nil {
		return 0, err
	}

	if meta.Length <= int64(maxLen) {
		return r.flushLegacyCleanupOnTrimNoOp(ctx, readTS, key, meta, legacyCleanup)
	}

	// Cap the trim request at maxWideColumnItems so a single XTRIM cannot
	// emit an unbounded list of Del operations in one Raft commit. int64
	// arithmetic upfront also keeps a corrupted meta.Length (>MaxInt)
	// from wrapping into a negative scan count.
	diff := meta.Length - int64(maxLen)
	requestedRemoved := maxWideColumnItems
	if diff <= int64(maxWideColumnItems) {
		requestedRemoved = int(diff)
	}
	trim, err := r.buildXTrimHeadElems(ctx, key, readTS, requestedRemoved)
	if err != nil {
		return 0, err
	}

	// Use len(trim) — the actual entries we are about to delete — for
	// both the meta.Length update and the XTRIM return value. The
	// requested count and the actual count can diverge when the trim
	// hits the per-txn cap or the underlying scan returns fewer rows
	// than requested (concurrent writes / partial consistency); using
	// the actual count keeps meta.Length consistent with on-disk state
	// and reports the truth back to the client.
	actualRemoved := len(trim)
	elems := make([]*kv.Elem[kv.OP], 0, len(legacyCleanup)+actualRemoved+1)
	elems = append(elems, legacyCleanup...)
	elems = append(elems, trim...)
	meta.Length -= int64(actualRemoved)
	metaBytes, err := store.MarshalStreamMeta(meta)
	if err != nil {
		return 0, cockerrors.WithStack(err)
	}
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.StreamMetaKey(key), Value: metaBytes})
	return actualRemoved, r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) xrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.rangeStream(conn, cmd, false)
}

func (r *RedisServer) xrevrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.rangeStream(conn, cmd, true)
}

func parseXReadCountArg(args [][]byte, index int) (int, error) {
	if index+1 >= len(args) {
		return 0, errors.New("ERR syntax error")
	}
	count, err := strconv.Atoi(string(args[index+1]))
	if err != nil || count <= 0 {
		return 0, errors.New("ERR syntax error")
	}
	// Clamp client-supplied COUNT to the wide-column ceiling so a single
	// XREAD cannot pre-allocate a maxInt-sized []redisStreamEntry slice or
	// pull more entries than the store will return for the equivalent
	// uncapped scan. Cap is silent (Redis-compatible): the client always
	// sees at most maxWideColumnItems entries per stream per call.
	if count > maxWideColumnItems {
		count = maxWideColumnItems
	}
	return count, nil
}

func parseXReadBlockArg(args [][]byte, index int) (time.Duration, error) {
	if index+1 >= len(args) {
		return 0, errors.New("ERR syntax error")
	}
	ms, err := strconv.Atoi(string(args[index+1]))
	if err != nil || ms < 0 {
		return 0, errors.New("ERR syntax error")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func parseXReadOptions(args [][]byte) (xreadOptions, error) {
	opts := xreadOptions{count: -1, streamsIndex: -1}
	for i := 1; i < len(args); {
		next, done, err := parseXReadOption(&opts, args, i)
		if err != nil {
			return xreadOptions{}, err
		}
		if done {
			return opts, nil
		}
		i = next
	}
	return opts, nil
}

func parseXReadOption(opts *xreadOptions, args [][]byte, i int) (int, bool, error) {
	switch strings.ToUpper(string(args[i])) {
	case redisKeywordCount:
		count, err := parseXReadCountArg(args, i)
		if err != nil {
			return 0, false, err
		}
		opts.count = count
		return i + redisPairWidth, false, nil
	case "BLOCK":
		block, err := parseXReadBlockArg(args, i)
		if err != nil {
			return 0, false, err
		}
		opts.block = block
		return i + redisPairWidth, false, nil
	case "STREAMS":
		opts.streamsIndex = i + 1
		return len(args), true, nil
	default:
		return 0, false, errors.New("ERR syntax error")
	}
}

func splitXReadStreams(args [][]byte, streamsIndex int) ([][]byte, []string, error) {
	if streamsIndex < 0 || streamsIndex >= len(args) {
		return nil, nil, errors.New("ERR syntax error")
	}
	remaining := len(args) - streamsIndex
	if remaining%redisPairWidth != 0 {
		return nil, nil, errors.New("ERR syntax error")
	}

	streamCount := remaining / redisPairWidth
	keys := make([][]byte, streamCount)
	afterIDs := make([]string, streamCount)
	for i := range streamCount {
		keys[i] = args[streamsIndex+i]
		afterIDs[i] = string(args[streamsIndex+streamCount+i])
	}
	return keys, afterIDs, nil
}

func parseXReadRequest(args [][]byte) (xreadRequest, error) {
	opts, err := parseXReadOptions(args)
	if err != nil {
		return xreadRequest{}, err
	}
	keys, afterIDs, err := splitXReadStreams(args, opts.streamsIndex)
	if err != nil {
		return xreadRequest{}, err
	}
	return xreadRequest{block: opts.block, count: opts.count, keys: keys, afterIDs: afterIDs}, nil
}

func (r *RedisServer) resolveXReadAfterIDs(ctx context.Context, req *xreadRequest) error {
	for i, afterID := range req.afterIDs {
		if afterID != "$" {
			continue
		}
		resolved, err := r.resolveXReadDollarID(ctx, req.keys[i])
		if err != nil {
			return err
		}
		req.afterIDs[i] = resolved
	}
	return nil
}

// resolveXReadDollarID resolves the "$" after-ID for a single stream by
// asking the store for the highest ID ever assigned. The new-layout meta
// answers in one read; when meta is absent the stream is treated as
// empty — legacy single-blob data is intentionally ignored under the
// "discard-on-read, delete-on-write" contract documented on
// dollarIDFromState (and matching loadStreamAt). Returns streamZeroID
// for non-existent and empty-never-written streams. ctx threads through
// the caller's cancellation/deadline so the resolve step doesn't survive
// past a BLOCK-window cancel.
func (r *RedisServer) resolveXReadDollarID(ctx context.Context, key []byte) (string, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeStream)
	if err != nil {
		return "", err
	}
	if typ == redisTypeNone {
		return streamZeroID, nil
	}
	if typ != redisTypeStream {
		return "", wrongTypeError()
	}
	return r.dollarIDFromState(ctx, key, readTS)
}

// dollarIDFromState returns the highest-ever-assigned stream ID as a string.
// Reads the new-layout meta record (O(1)); when meta is absent the stream
// is treated as empty — legacy single-blob data is intentionally ignored
// under the "discard-on-read, delete-on-write" contract (see loadStreamAt
// and the PR #620 writeup), so $ resolves to streamZeroID for any stream
// that has never been written in the new layout.
func (r *RedisServer) dollarIDFromState(ctx context.Context, key []byte, readTS uint64) (string, error) {
	meta, found, err := r.loadStreamMetaAt(ctx, key, readTS)
	if err != nil {
		return "", err
	}
	if !found {
		return streamZeroID, nil
	}
	if meta.Length == 0 && meta.LastMs == 0 && meta.LastSeq == 0 {
		return streamZeroID, nil
	}
	return strconv.FormatUint(meta.LastMs, 10) + "-" + strconv.FormatUint(meta.LastSeq, 10), nil
}

func selectXReadEntries(entries []redisStreamEntry, afterID string, count int) []redisStreamEntry {
	afterParsedID, afterParsedValid := tryParseRedisStreamID(afterID)
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].compareID(afterID, afterParsedID, afterParsedValid) > 0
	})
	if start >= len(entries) {
		return nil
	}
	end := len(entries)
	if count > 0 && start+count < end {
		end = start + count
	}
	return entries[start:end]
}

func (r *RedisServer) xreadOnce(ctx context.Context, req xreadRequest) ([]xreadResult, error) {
	results := make([]xreadResult, 0, len(req.keys))
	for i, key := range req.keys {
		readTS := r.readTS()
		typ, err := r.keyTypeAtExpect(ctx, key, readTS, redisTypeStream)
		if err != nil {
			return nil, err
		}
		if typ == redisTypeNone {
			continue
		}
		if typ != redisTypeStream {
			return nil, wrongTypeError()
		}

		entries, err := r.readStreamAfter(ctx, key, readTS, req.afterIDs[i], req.count)
		if err != nil {
			return nil, err
		}
		if len(entries) > 0 {
			results = append(results, xreadResult{key: key, entries: entries})
		}
	}
	return results, nil
}

// readStreamAfter returns up to `count` entries with ID strictly greater
// than afterID via the entry-per-key range scan. When the meta key is
// absent the stream is treated as empty; legacy single-blob data is
// intentionally ignored under the "discard-on-read, delete-on-write"
// contract documented on loadStreamAt. A subsequent XADD or XTRIM will
// delete any lingering legacy blob in the same transaction, so a stream
// whose meta is still missing here cannot have live legacy data from the
// caller's perspective.
func (r *RedisServer) readStreamAfter(ctx context.Context, key []byte, readTS uint64, afterID string, count int) ([]redisStreamEntry, error) {
	_, found, err := r.loadStreamMetaAt(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return r.scanStreamEntriesAfter(ctx, key, readTS, afterID, count)
}

// scanStreamEntriesAfter runs a [strictly-after(afterID), ∞) range scan over
// entry keys, capped by count (when positive) or maxWideScanLimit otherwise.
// When count is non-positive, we mirror scanStreamEntriesAt's guard: request
// maxWideScanLimit (which is maxWideColumnItems+1) and reject if the scan
// filled, so an XREAD without COUNT cannot OOM the server on a pathological
// stream.
//
// afterID must be a parseable stream ID in either the strict "ms-seq" form or
// the shorthand "ms" form (no dash), which Redis normalises to "ms-0".
// Genuinely malformed IDs are rejected immediately so the caller never
// receives a full-stream result set for invalid input.
func (r *RedisServer) scanStreamEntriesAfter(ctx context.Context, key []byte, readTS uint64, afterID string, count int) ([]redisStreamEntry, error) {
	afterID, ok := normalizeStreamAfterID(afterID)
	if !ok {
		return nil, errors.New("ERR Invalid stream ID specified as stream command argument")
	}
	prefix := store.StreamEntryScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	start := streamScanStartForAfter(prefix, afterID)
	limit := count
	unbounded := limit <= 0
	if unbounded {
		limit = maxWideScanLimit
	}
	kvs, err := r.store.ScanAt(ctx, start, end, limit, readTS)
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	if unbounded && len(kvs) > maxWideColumnItems {
		return nil, cockerrors.Wrapf(ErrCollectionTooLarge, "stream %q exceeds %d entries", key, maxWideColumnItems)
	}
	entries := make([]redisStreamEntry, 0, len(kvs))
	for _, pair := range kvs {
		entry, err := unmarshalStreamEntry(pair.Value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// streamScanStartForAfter returns the inclusive start key to use for an
// XREAD-style "after afterID" range scan. If afterID parses cleanly we
// start at ID+1 so the scan is exclusive of afterID. Callers must validate
// afterID before calling this function; if afterID is unparseable, the
// returned prefix is the entry-prefix start, which gives a full scan.
//
// Edge case: if afterID is (math.MaxUint64-math.MaxUint64), there is no
// successor ID inside the entry-prefix keyspace, so the correct start is
// one past the prefix (empty scan). Returning the afterID key itself
// would make the inclusive scan include it, which is the opposite of
// "strictly after."
func streamScanStartForAfter(prefix []byte, afterID string) []byte {
	parsed, ok := tryParseRedisStreamID(afterID)
	if !ok {
		return prefix
	}
	ms, seq := parsed.ms, parsed.seq
	switch {
	case seq < ^uint64(0):
		seq++
	case ms < ^uint64(0):
		ms++
		seq = 0
	default:
		// afterID is the largest representable stream ID. No entry can be
		// strictly after it; return the scan-end sentinel so the scan is
		// empty instead of silently inclusive.
		return store.PrefixScanEnd(prefix)
	}
	start := make([]byte, 0, len(prefix)+store.StreamIDBytes)
	start = append(start, prefix...)
	start = append(start, store.EncodeStreamID(ms, seq)...)
	return start
}

// normalizeStreamAfterID normalises an XREAD afterID to the strict "ms-seq"
// form used by tryParseRedisStreamID. Redis accepts a shorthand "ms" form
// (no dash) as meaning "ms-0". Truly invalid IDs — those that are neither
// valid "ms-seq" strings nor parseable as a bare uint64 — return ("", false).
func normalizeStreamAfterID(id string) (string, bool) {
	if strings.IndexByte(id, '-') >= 0 {
		_, ok := tryParseRedisStreamID(id)
		return id, ok
	}
	// Shorthand: bare millisecond component only. Redis treats "ms" as "ms-0"
	// for XREAD after-IDs (entries strictly after ms-0).
	if _, err := strconv.ParseUint(id, 10, 64); err != nil {
		return "", false
	}
	return id + "-0", true
}

func writeStreamEntry(conn redcon.Conn, entry redisStreamEntry) {
	conn.WriteArray(redisPairWidth)
	conn.WriteBulkString(entry.ID)
	conn.WriteArray(len(entry.Fields))
	for _, field := range entry.Fields {
		conn.WriteBulkString(field)
	}
}

func writeStreamEntries(conn redcon.Conn, entries []redisStreamEntry) {
	conn.WriteArray(len(entries))
	for _, entry := range entries {
		writeStreamEntry(conn, entry)
	}
}

func writeXReadResults(conn redcon.Conn, results []xreadResult) {
	conn.WriteArray(len(results))
	for _, result := range results {
		conn.WriteArray(redisPairWidth)
		conn.WriteBulk(result.key)
		writeStreamEntries(conn, result.entries)
	}
}

// isXReadIterCtxError reports whether err originates from the per-iteration
// XREAD context firing (BLOCK budget consumed mid-call). The check covers
// the bare context sentinels, cockroachdb/errors-wrapped variants, and
// gRPC's status.Error(codes.DeadlineExceeded / codes.Canceled, ...) which
// is what bubbles up through coordinator.Dispatch when the iter ctx fires
// during a Raft-mediated read. Hits on this path must be silently
// translated to "empty iteration" so the BLOCK-window null contract holds.
func isXReadIterCtxError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if cockerrors.Is(err, context.DeadlineExceeded) || cockerrors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) { //nolint:exhaustive // only the two ctx-related codes matter; the rest must propagate as real errors
	case codes.DeadlineExceeded, codes.Canceled:
		return true
	default:
		return false
	}
}

func (r *RedisServer) xread(conn redcon.Conn, cmd redcon.Command) {
	req, err := parseXReadRequest(cmd.Args)
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	blockDuration := req.block
	// block=0 means infinite wait in Redis; cap at redisDispatchTimeout to prevent goroutine leak.
	if blockDuration == 0 {
		blockDuration = redisDispatchTimeout
	}
	deadline := time.Now().Add(blockDuration)

	// $ resolution uses a short fixed timeout rather than the BLOCK
	// window: it's a single bounded read per key, not a wait. A tight
	// BLOCK (e.g. `BLOCK 1`) used to turn any slow $-resolve into a
	// protocol-level error on this path; use redisDispatchTimeout so
	// the resolve either succeeds quickly or fails cleanly, leaving
	// the BLOCK-window timeout semantics (null on expiry) to the
	// busy-poll below.
	//
	// Parent on r.handlerContext() (not context.Background()) so an
	// in-flight resolve aborts promptly when the server is shutting
	// down — otherwise the per-resolve ScanAt could survive past
	// graceful-shutdown's drain window.
	resolveCtx, resolveCancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	if ok := r.runWithHeavyCommandSlot(func() {
		err = r.resolveXReadAfterIDs(resolveCtx, &req)
	}); !ok {
		resolveCancel()
		conn.WriteError(errRedisHeavyCommandPoolFull.Error())
		return
	}
	resolveCancel()
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	r.xreadBusyPoll(conn, req, deadline)
}

// xreadBusyPoll runs the BLOCK-window wait loop. Extracted from xread so
// the parent function stays under the cyclop budget. Uses an event-driven
// signal from the in-process XADD path with a fallback timer for paths
// that bypass the signal (Lua flush, follower-side FSM apply).
//
// Registration happens BEFORE the first xreadOnce so a signal that fires
// between the check and the wait cannot be lost: the buffered channel
// holds it, and the next select wakes immediately.
func (r *RedisServer) xreadBusyPoll(conn redcon.Conn, req xreadRequest, deadline time.Time) {
	handlerCtx := r.handlerContext()
	w, release := r.streamWaiters.Register(req.keys)
	defer release()
	for {
		// Server-shutdown short-circuit: if the parent handlerContext
		// has been cancelled, abandon the wait loop immediately rather
		// than block until the BLOCK deadline. iterCtx below is rooted
		// in handlerCtx, so it would cancel-on-call too — but routing
		// through isXReadIterCtxError silently translates that into an
		// empty iteration and the loop would otherwise wait at
		// redisBlockWaitFallback cadence until the deadline.
		if handlerCtx.Err() != nil {
			conn.WriteNull()
			return
		}
		// BLOCK-expired before the loop body: respect the Redis contract
		// that a BLOCK timeout returns null, not an error. If we fell
		// through here without remaining time (very small BLOCK, or
		// $-resolution consumed the budget) creating an
		// already-expired context.WithTimeout would make xreadOnce
		// return DeadlineExceeded, which we'd then surface as an error.
		iterTimeout := time.Until(deadline)
		if iterTimeout <= 0 {
			conn.WriteNull()
			return
		}
		// Cap each iteration at redisDispatchTimeout to avoid holding
		// storage resources longer than a single dispatch.
		if iterTimeout > redisDispatchTimeout {
			iterTimeout = redisDispatchTimeout
		}
		// iterCtx is rooted in handlerCtx so its underlying storage
		// scans abort promptly on server shutdown rather than running
		// until iterTimeout fires. The handlerCtx.Err() guard at the
		// top of each iteration prevents the loop from spinning once
		// the parent ctx is cancelled.
		iterCtx, iterCancel := context.WithTimeout(handlerCtx, iterTimeout)
		var results []xreadResult
		var err error
		if ok := r.runWithHeavyCommandSlot(func() {
			results, err = r.xreadOnce(iterCtx, req)
		}); !ok {
			iterCancel()
			conn.WriteError(errRedisHeavyCommandPoolFull.Error())
			return
		}
		iterCancel()
		// Per-iteration ctx hitting its deadline (or being cancelled by
		// the upstream BLOCK timeout) is not a client-facing error — it
		// just means this poll round did not see any new entries. Treat
		// it as an empty iteration so the loop continues to the next
		// round (or falls through to the null-on-deadline branch below).
		// Without this, a tight BLOCK (e.g. BLOCK 10 against a busy /
		// slow node) leaks the iteration ctx-deadline into a -ERR reply,
		// which violates the Redis BLOCK-timeout contract (null on
		// timeout). xreadOnce returns nil results on any error, so
		// suppressing iter-ctx errors here is sound.
		if err != nil && !isXReadIterCtxError(err) {
			writeRedisError(conn, err)
			return
		}
		if len(results) > 0 {
			writeXReadResults(conn, results)
			return
		}

		if !time.Now().Before(deadline) {
			conn.WriteNull()
			return
		}
		waitForBlockedCommandUpdate(handlerCtx, w, deadline)
	}
}

func (r *RedisServer) xlen(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeStream)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeStream {
		conn.WriteError(wrongTypeMessage)
		return
	}
	meta, found, err := r.loadStreamMetaAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if found {
		conn.WriteInt64(meta.Length)
		return
	}
	stream, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt64(int64(len(stream.Entries)))
}

func parseRangeStreamCount(args [][]byte) (int, error) {
	count := -1
	for i := 4; i < len(args); i += redisPairWidth {
		// args[i] is safe: the for-loop guard `i < len(args)` ensures it.
		// gosec G602 false-positives here under flow analysis.
		if i+1 >= len(args) || !strings.EqualFold(string(args[i]), redisKeywordCount) { //nolint:gosec
			return 0, errors.New("ERR syntax error")
		}
		nextCount, err := strconv.Atoi(string(args[i+1]))
		if err != nil || nextCount < 0 {
			return 0, errors.New("ERR syntax error")
		}
		count = nextCount
	}
	// Clamp client-supplied COUNT for XRANGE / XREVRANGE the same way XREAD
	// clamps it (parseXReadCountArg). The negative sentinel -1 (no COUNT)
	// is preserved unchanged so the unbounded path still trips
	// maxWideColumnItems guard inside rangeStreamNewLayout.
	if count > maxWideColumnItems {
		count = maxWideColumnItems
	}
	return count, nil
}

func streamEntryMatchesRange(entryID, startRaw, endRaw string, reverse bool) bool {
	if reverse {
		return streamWithinUpper(entryID, startRaw) && streamWithinLower(entryID, endRaw)
	}
	return streamWithinLower(entryID, startRaw) && streamWithinUpper(entryID, endRaw)
}

func selectForwardStreamRangeEntries(entries []redisStreamEntry, startRaw, endRaw string, count int) []redisStreamEntry {
	selected := make([]redisStreamEntry, 0, len(entries))
	for _, entry := range entries {
		if !streamEntryMatchesRange(entry.ID, startRaw, endRaw, false) {
			continue
		}
		selected = append(selected, entry)
		if count >= 0 && len(selected) >= count {
			break
		}
	}
	return selected
}

func selectReverseStreamRangeEntries(entries []redisStreamEntry, startRaw, endRaw string, count int) []redisStreamEntry {
	selected := make([]redisStreamEntry, 0, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		if !streamEntryMatchesRange(entries[i].ID, startRaw, endRaw, true) {
			continue
		}
		selected = append(selected, entries[i])
		if count >= 0 && len(selected) >= count {
			break
		}
	}
	return selected
}

func selectStreamRangeEntries(entries []redisStreamEntry, startRaw, endRaw string, reverse bool, count int) []redisStreamEntry {
	if reverse {
		return selectReverseStreamRangeEntries(entries, startRaw, endRaw, count)
	}
	return selectForwardStreamRangeEntries(entries, startRaw, endRaw, count)
}

func (r *RedisServer) rangeStream(conn redcon.Conn, cmd redcon.Command, reverse bool) {
	count, err := parseRangeStreamCount(cmd.Args)
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAtExpect(context.Background(), cmd.Args[1], readTS, redisTypeStream)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeStream {
		conn.WriteError(wrongTypeMessage)
		return
	}

	startRaw, endRaw := string(cmd.Args[2]), string(cmd.Args[3])

	_, metaFound, err := r.loadStreamMetaAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if metaFound {
		selected, err := r.rangeStreamNewLayout(context.Background(), cmd.Args[1], readTS, startRaw, endRaw, reverse, count)
		if err != nil {
			writeRedisError(conn, err)
			return
		}
		writeStreamEntries(conn, selected)
		return
	}

	stream, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	selected := selectStreamRangeEntries(stream.Entries, startRaw, endRaw, reverse, count)
	writeStreamEntries(conn, selected)
}

// rangeStreamNewLayout serves XRANGE / XREVRANGE from the entry-per-key
// layout via a bounded range scan. The (start, end) inputs are the raw
// command bounds — "-", "+", "(1000-0", or "1000-0" — and are converted to
// binary scan bounds so only the selected entries are unmarshaled.
func (r *RedisServer) rangeStreamNewLayout(
	ctx context.Context, key []byte, readTS uint64,
	startRaw, endRaw string, reverse bool, count int,
) ([]redisStreamEntry, error) {
	prefix := store.StreamEntryScanPrefix(key)
	scanStart, scanEnd, ok, err := streamScanBounds(prefix, startRaw, endRaw, reverse)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	limit := count
	unbounded := limit <= 0
	if unbounded {
		limit = maxWideScanLimit
	}
	var kvs []*store.KVPair
	if reverse {
		kvs, err = r.store.ReverseScanAt(ctx, scanStart, scanEnd, limit, readTS)
	} else {
		kvs, err = r.store.ScanAt(ctx, scanStart, scanEnd, limit, readTS)
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	// An XRANGE/XREVRANGE without COUNT on a pathological stream must
	// not be able to pull maxWideScanLimit entries into a single reply.
	// Mirror scanStreamEntriesAt's guard.
	if unbounded && len(kvs) > maxWideColumnItems {
		return nil, cockerrors.Wrapf(ErrCollectionTooLarge, "stream %q exceeds %d entries", key, maxWideColumnItems)
	}
	entries := make([]redisStreamEntry, 0, len(kvs))
	for _, pair := range kvs {
		entry, err := unmarshalStreamEntry(pair.Value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// streamScanBounds maps the raw XRANGE / XREVRANGE bounds to half-open
// [start, end) scan bounds over the entry prefix. For reverse scans,
// the ReverseScanAt convention is still [start, end) with results in
// descending order starting from just-before(end).
//
// Returns ok=false when the bounds define an empty range (e.g. start > end),
// in which case the caller should emit an empty array.
func streamScanBounds(prefix []byte, startRaw, endRaw string, reverse bool) ([]byte, []byte, bool, error) {
	var lowRaw, highRaw string
	if reverse {
		// XREVRANGE takes (high, low).
		highRaw, lowRaw = startRaw, endRaw
	} else {
		lowRaw, highRaw = startRaw, endRaw
	}

	start, err := streamBoundLow(prefix, lowRaw)
	if err != nil {
		return nil, nil, false, err
	}
	end, err := streamBoundHigh(prefix, highRaw)
	if err != nil {
		return nil, nil, false, err
	}
	if bytes.Compare(start, end) >= 0 {
		return nil, nil, false, nil
	}
	return start, end, true, nil
}

// streamBoundLow returns the inclusive lower bound of the scan in binary form.
// When the bound is "(ID" (exclusive) and ID is the largest representable
// stream ID, the scan-end sentinel is returned so streamScanBounds'
// start >= end check collapses the range to empty; otherwise the scan
// would silently include the exclusive bound entry.
func streamBoundLow(prefix []byte, raw string) ([]byte, error) {
	if raw == "-" {
		return prefix, nil
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	ms, seq, ok := parseStreamBoundID(raw, false, exclusive)
	if !ok {
		return nil, errors.New("ERR Invalid stream ID specified as stream command argument")
	}
	if exclusive {
		switch {
		case seq < ^uint64(0):
			seq++
		case ms < ^uint64(0):
			ms++
			seq = 0
		default:
			return store.PrefixScanEnd(prefix), nil
		}
	}
	return appendStreamKey(prefix, ms, seq), nil
}

// streamBoundHigh returns the exclusive upper bound of the scan in binary form.
func streamBoundHigh(prefix []byte, raw string) ([]byte, error) {
	if raw == "+" {
		return store.PrefixScanEnd(prefix), nil
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	ms, seq, ok := parseStreamBoundID(raw, true, exclusive)
	if !ok {
		return nil, errors.New("ERR Invalid stream ID specified as stream command argument")
	}
	if !exclusive {
		switch {
		case seq < ^uint64(0):
			seq++
		case ms < ^uint64(0):
			ms++
			seq = 0
		default:
			return store.PrefixScanEnd(prefix), nil
		}
	}
	return appendStreamKey(prefix, ms, seq), nil
}

// parseStreamBoundID accepts both the strict ms-seq form and the shorthand
// "ms" form that Redis XRANGE/XREVRANGE allow. Redis interprets a shorthand
// ID differently depending on position and exclusivity:
//
//   - Lower bound inclusive ("5"): expand to 5-0; scan starts at 5-0.
//   - Lower bound exclusive ("(5"): expand to 5-0; caller shifts +1 → 5-1.
//   - Upper bound inclusive ("5"): expand to 5-MaxUint64; caller shifts +1 → 6-0 (exclusive upper).
//   - Upper bound exclusive ("(5"): expand to 5-0; scan stops at 5-0 (excludes all ms=5 entries).
//
// The rule is: seq = MaxUint64 when upper && !exclusive (need to include the
// full ms row before the caller's inclusive→exclusive shift), seq = 0
// otherwise. Full ms-seq IDs pass through unchanged.
func parseStreamBoundID(raw string, upper, exclusive bool) (uint64, uint64, bool) {
	if strings.IndexByte(raw, '-') >= 0 {
		parsed, ok := tryParseRedisStreamID(raw)
		if !ok {
			return 0, 0, false
		}
		return parsed.ms, parsed.seq, true
	}
	ms, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	// Upper inclusive bounds need seq=MaxUint64 so the caller's +1 shift
	// produces (ms+1)-0, covering the entire ms row. All other
	// combinations use seq=0: lower inclusive starts at ms-0, lower
	// exclusive starts at ms-0 then the caller shifts to ms-1, and upper
	// exclusive stops before ms-0 (excluding the whole ms).
	if upper && !exclusive {
		return ms, ^uint64(0), true
	}
	return ms, 0, true
}

func appendStreamKey(prefix []byte, ms, seq uint64) []byte {
	out := make([]byte, 0, len(prefix)+store.StreamIDBytes)
	out = append(out, prefix...)
	out = append(out, store.EncodeStreamID(ms, seq)...)
	return out
}

func streamWithinLower(entryID, raw string) bool {
	if raw == "-" {
		return true
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	cmp := compareRedisStreamID(entryID, raw)
	if exclusive {
		return cmp > 0
	}
	return cmp >= 0
}

func streamWithinUpper(entryID, raw string) bool {
	if raw == "+" {
		return true
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	cmp := compareRedisStreamID(entryID, raw)
	if exclusive {
		return cmp < 0
	}
	return cmp <= 0
}

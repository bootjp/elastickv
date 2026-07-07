package backup

import (
	"bytes"
	"io"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/errors"
)

// decode.go orchestrates the per-prefix dispatch from a raw `.fsm`
// stream into the four adapter encoders (DynamoDB, S3, Redis, SQS).
// It is the Phase 0a glue documented in §"Pipeline" of
// docs/design/2026_04_29_implemented_snapshot_logical_decoder.md.
//
// The dispatcher is intentionally thin: every per-record decision
// (manifest reassembly, TTL inlining, side-record exclusion, ...) is
// owned by the per-adapter encoder. This file is just the routing
// table that maps "leading bytes of an entry's key" to the adapter
// method that owns that prefix. MANIFEST.json and CHECKSUMS emission
// live in cmd/elastickv-snapshot-decode (not here) because Phase 0a
// callers want to drive both from CLI flags and from in-process
// integration tests, and stamping wall-time metadata in this package
// would couple it to the wrong layer.

// AdapterSet selects which adapter encoders DecodeSnapshot
// instantiates. Disabled adapters are silently dropped at dispatch
// time (counted as Internal); they do NOT create empty output
// directories.
type AdapterSet struct {
	DynamoDB bool
	S3       bool
	Redis    bool
	SQS      bool
}

// AllAdapters enables every adapter. Convenience for the CLI's
// default (`--adapter` flag absent) and for end-to-end tests.
func AllAdapters() AdapterSet {
	return AdapterSet{DynamoDB: true, S3: true, Redis: true, SQS: true}
}

// DecodeOptions configures DecodeSnapshot. Zero values are
// documented as "off" for booleans; OutRoot is required.
type DecodeOptions struct {
	// OutRoot is the destination directory tree root. Must be set.
	OutRoot string
	// ScratchRoot is where the S3 encoder buffers blob chunks during
	// assembly. Defaults to <OutRoot>/.scratch when empty (NEVER to
	// OutRoot itself — sharing would let S3Encoder.Finalize's
	// os.RemoveAll wipe the final dump). Callers backing the CLI's
	// --scratch-root flag override it (e.g. to a tmpfs).
	//
	// DecodeSnapshot returns ErrDecodeOptionsInvalid if ScratchRoot
	// cleaned-equals OutRoot: a CLI misconfig that passes
	// --scratch-root=$OUT_ROOT would otherwise be the silent
	// data-loss path the default-target check is designed to prevent.
	ScratchRoot string
	// Adapters selects which adapter encoders to construct. Entries
	// for disabled adapters are dropped silently and counted as
	// Internal.
	Adapters AdapterSet
	// RedisDBIndex selects the <n> in redis/db_<n>/. Phase 0a always
	// emits db_0 but the parameter is plumbed so Phase 1 can dump
	// multiple DBs.
	RedisDBIndex int

	// IncludeIncompleteUploads enables `--include-incomplete-uploads`
	// for the S3 encoder (in-flight multipart uploads).
	IncludeIncompleteUploads bool
	// IncludeOrphans enables `--include-orphans` for the S3 encoder
	// (pre-generation orphan blob chunks).
	IncludeOrphans bool
	// RenameS3Collisions enables `--rename-collisions` for the S3
	// encoder (rename user keys ending in `.elastickv-meta.json`).
	RenameS3Collisions bool

	// PreserveSQSVisibility enables `--preserve-sqs-visibility` for
	// the SQS encoder (carry live visibility-state fields into the
	// dump rather than zeroing them).
	PreserveSQSVisibility bool
	// IncludeSQSSideRecords enables `--include-sqs-side-records` for
	// the SQS encoder (`_internals/dedup.jsonl` etc.).
	IncludeSQSSideRecords bool

	// DynamoDBBundleJSONL switches the DynamoDB encoder to the JSONL
	// bundle layout (`items/data-<part>.jsonl`).
	DynamoDBBundleJSONL bool

	// WarnSink, when non-nil, receives structured warnings from the
	// per-adapter encoders ("redis_orphan_ttl", "ddb_orphan_items",
	// ...). The dispatcher itself does not emit warnings.
	WarnSink func(event string, fields ...any)
}

// DecodeCounters reports per-class entry counts after a successful
// DecodeSnapshot call.
//
// The breakdown is:
//
//	Total      - every snapshot entry that reached the dispatcher
//	Tombstone  - entries whose value-header flagged a delete; skipped
//	             before adapter dispatch
//	DynamoDB   - entries routed to a DDBEncoder method
//	S3         - entries routed to an S3Encoder method
//	Redis      - entries routed to a RedisDB method
//	SQS        - entries routed to a SQSEncoder method
//	Internal   - entries matched by a known internal-only prefix
//	             (e.g. !txn|, !s3route|) AND entries that matched an
//	             adapter prefix whose adapter was excluded by
//	             DecodeOptions.Adapters; both are intentional drops
//	Unknown    - entries whose key prefix matched no route — surfaced
//	             so a malformed or version-skewed snapshot does not
//	             silently round-trip into an empty dump
//
// Total = Tombstone + DynamoDB + S3 + Redis + SQS + Internal + Unknown.
type DecodeCounters struct {
	Total     uint64
	Tombstone uint64
	DynamoDB  uint64
	S3        uint64
	Redis     uint64
	SQS       uint64
	Internal  uint64
	Unknown   uint64
}

// DecodeResult is returned by DecodeSnapshot after a successful
// run. Header carries the snapshot's `last_commit_ts` for the
// caller's MANIFEST.json.
type DecodeResult struct {
	Header   SnapshotHeader
	Counters DecodeCounters
}

// ErrDecodeOptionsInvalid is returned when DecodeSnapshot is called
// with an under-populated DecodeOptions (missing OutRoot, etc.).
var ErrDecodeOptionsInvalid = errors.New("backup: DecodeOptions invalid")

// DecodeSnapshot reads a `.fsm`-format stream from r, dispatches
// every non-tombstone entry to the appropriate adapter encoder
// rooted under opts.OutRoot, and runs Finalize on each enabled
// adapter at end of stream.
//
// Caller closes r. DecodeSnapshot does NOT write MANIFEST.json or
// CHECKSUMS — both are emitted by the cmd/ wrapper from the returned
// DecodeResult plus its own wall-time / source-path metadata.
func DecodeSnapshot(r io.Reader, opts DecodeOptions) (DecodeResult, error) {
	d, err := newDispatcher(opts)
	if err != nil {
		return DecodeResult{}, err
	}
	hdr, err := ReadSnapshotWithHeader(r, func(_ SnapshotHeader, e SnapshotEntry) error {
		return d.handleEntry(e)
	})
	// finalize() runs unconditionally so encoders (notably the S3
	// encoder's scratch tree) get cleanup even when the stream is
	// truncated. The original read error wins over a finalize error
	// when both occur — the read failure is the root cause and the
	// finalize error is most likely a downstream symptom (gemini r1
	// medium on PR #806).
	fErr := d.finalize()
	if err == nil {
		err = fErr
	}
	if err != nil {
		return DecodeResult{}, err
	}
	return DecodeResult{Header: hdr, Counters: d.counters}, nil
}

// dispatcher is the in-memory state carried across the ReadSnapshot
// callback invocations. One per DecodeSnapshot call.
type dispatcher struct {
	opts     DecodeOptions
	ddb      *DDBEncoder
	s3       *S3Encoder
	redis    *RedisDB
	sqs      *SQSEncoder
	counters DecodeCounters
}

// newDispatcher constructs the enabled per-adapter encoders. The
// validation it runs is the *parameter* validation; filesystem
// existence / permission errors surface later, on first write.
func newDispatcher(opts DecodeOptions) (*dispatcher, error) {
	if opts.OutRoot == "" {
		return nil, errors.Wrap(ErrDecodeOptionsInvalid, "OutRoot required")
	}
	d := &dispatcher{opts: opts}
	if opts.Adapters.DynamoDB {
		d.ddb = NewDDBEncoder(opts.OutRoot).
			WithBundleJSONL(opts.DynamoDBBundleJSONL).
			WithWarnSink(opts.WarnSink)
	}
	if opts.Adapters.S3 {
		scratch := opts.ScratchRoot
		if scratch == "" {
			// NEVER default scratch to OutRoot — S3Encoder.Finalize
			// runs os.RemoveAll(scratchRoot/s3/) and OutRoot also
			// holds the *final* assembled bodies at <OutRoot>/s3/.
			// Sharing the root would wipe the dump immediately after
			// it lands (gemini r1 security-high on PR #806). The
			// dedicated `.scratch` subtree keeps the two trees
			// disjoint regardless of where OutRoot points.
			scratch = filepath.Join(opts.OutRoot, ".scratch")
		}
		// Belt-and-braces on the operator-supplied path: the
		// default-path fix above only protects empty ScratchRoot;
		// a CLI invocation that passes --scratch-root=$OUT explicitly
		// would still let Finalize wipe the dump (Codex r3 P1 on
		// PR #806). Fail fast on cleaned-equal paths so the
		// misconfiguration surfaces as ErrDecodeOptionsInvalid
		// rather than as silent data loss after a long decode.
		if filepath.Clean(scratch) == filepath.Clean(opts.OutRoot) {
			return nil, errors.Wrap(ErrDecodeOptionsInvalid,
				"ScratchRoot must not equal OutRoot: S3Encoder.Finalize "+
					"would os.RemoveAll the final <OutRoot>/s3/ tree")
		}
		d.s3 = NewS3Encoder(opts.OutRoot, scratch).
			WithIncludeIncompleteUploads(opts.IncludeIncompleteUploads).
			WithIncludeOrphans(opts.IncludeOrphans).
			WithRenameCollisions(opts.RenameS3Collisions).
			WithWarnSink(opts.WarnSink)
	}
	if opts.Adapters.Redis {
		d.redis = NewRedisDB(opts.OutRoot, opts.RedisDBIndex).
			WithWarnSink(opts.WarnSink)
	}
	if opts.Adapters.SQS {
		d.sqs = NewSQSEncoder(opts.OutRoot).
			WithIncludeSideRecords(opts.IncludeSQSSideRecords).
			WithPreserveVisibility(opts.PreserveSQSVisibility).
			WithWarnSink(opts.WarnSink)
	}
	return d, nil
}

// handleEntry is the per-entry hook ReadSnapshot calls. Tombstones
// are counted and dropped before any prefix matching runs — Phase 0a
// dumps reflect the *current* user-visible state, not the
// crash-consistent MVCC tombstone fan.
func (d *dispatcher) handleEntry(e SnapshotEntry) error {
	d.counters.Total++
	if e.Tombstone {
		d.counters.Tombstone++
		return nil
	}
	return d.route(e.UserKey, e.UserValue)
}

// route does the leading-prefix match against the global routing
// table. Entries with no matching prefix increment Unknown — these
// are the format-skew / corruption signal a Phase 0a operator wants
// to surface, not swallow.
func (d *dispatcher) route(key, value []byte) error {
	for _, r := range prefixRoutes {
		if bytes.HasPrefix(key, r.prefix) {
			return r.handler(d, key, value)
		}
	}
	d.counters.Unknown++
	return nil
}

// finalize runs Finalize on every constructed adapter encoder. The
// order is fixed and matches the dump-tree order
// (dynamodb/, s3/, redis/, sqs/) so an operator inspecting a partial
// dump after a finalize-time error sees the adapters in the
// expected sequence.
//
// Each encoder is finalized best-effort: an error in one does NOT
// short-circuit the rest, so later encoders still get to clean up
// their scratch directories. The first error encountered wins
// (mirrors the ReadSnapshotWithHeader / finalize ordering in
// DecodeSnapshot — gemini r1 medium on PR #806).
func (d *dispatcher) finalize() error {
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if d.ddb != nil {
		record(d.ddb.Finalize())
	}
	if d.s3 != nil {
		record(d.s3.Finalize())
	}
	if d.redis != nil {
		record(d.redis.Finalize())
	}
	if d.sqs != nil {
		record(d.sqs.Finalize())
	}
	return firstErr
}

// prefixRoute is one row in the global dispatch table. handler is a
// method-style function so the table itself stays a pure data
// declaration (no closures-over-state).
type prefixRoute struct {
	prefix  []byte
	handler func(d *dispatcher, key, value []byte) error
}

// prefixRoutes is the canonical leading-prefix → handler table.
// Initialized once via package-init sort so that LONGER prefixes
// win over shorter ones (e.g. "!hs|meta|d|" must match before
// "!hs|meta|"). Tests pin the ordering so a future addition that
// introduces an ambiguity surfaces as a test failure.
var prefixRoutes = buildPrefixRoutes()

func buildPrefixRoutes() []prefixRoute {
	r := []prefixRoute{
		// DynamoDB
		{[]byte(DDBTableMetaPrefix), routeDDBTableMeta},
		{[]byte(DDBTableGenPrefix), routeDDBTableGen},
		{[]byte(DDBItemPrefix), routeDDBItem},
		{[]byte(DDBGSIPrefix), routeDDBGSI},
		// S3
		{[]byte(S3BucketMetaPrefix), routeS3BucketMeta},
		{[]byte(S3BucketGenPrefix), routeS3BucketGen},
		{[]byte(S3ObjectManifestPrefix), routeS3ObjectManifest},
		{[]byte(S3UploadMetaPrefix), routeS3UploadMeta(S3UploadMetaPrefix)},
		{[]byte(S3UploadPartPrefix), routeS3UploadMeta(S3UploadPartPrefix)},
		{[]byte(S3BlobPrefix), routeS3Blob},
		{[]byte(S3GCUploadPrefix), routeInternalDrop},
		{[]byte(S3RoutePrefix), routeInternalDrop},
		// SQS
		{[]byte(SQSQueueMetaPrefix), routeSQSQueueMeta},
		{[]byte(SQSQueueGenPrefix), routeSQSQueueGen},
		{[]byte(SQSQueueSeqPrefix), routeSQSSide(SQSQueueSeqPrefix)},
		{[]byte(SQSQueueTombstonePrefix), routeSQSSide(SQSQueueTombstonePrefix)},
		{[]byte(SQSMsgDataPrefix), routeSQSMessageData},
		{[]byte(SQSMsgVisPrefix), routeSQSSide(SQSMsgVisPrefix)},
		{[]byte(SQSMsgByAgePrefix), routeSQSSide(SQSMsgByAgePrefix)},
		{[]byte(SQSMsgDedupPrefix), routeSQSSide(SQSMsgDedupPrefix)},
		{[]byte(SQSMsgGroupPrefix), routeSQSSide(SQSMsgGroupPrefix)},
		// Redis hash
		{[]byte(RedisHashMetaDeltaPrefix), routeRedisHashMeta},
		{[]byte(RedisHashMetaPrefix), routeRedisHashMeta},
		{[]byte(RedisHashFieldPrefix), routeRedisHashField},
		// Redis list
		{[]byte(ListMetaDeltaPrefix), routeRedisListMetaDelta},
		{[]byte(ListMetaPrefix), routeRedisListMeta},
		{[]byte(ListItemPrefix), routeRedisListItem},
		{[]byte(ListClaimPrefix), routeRedisListClaim},
		// Redis set
		{[]byte(RedisSetMetaDeltaPrefix), routeRedisSetMetaDelta},
		{[]byte(RedisSetMetaPrefix), routeRedisSetMeta},
		{[]byte(RedisSetMemberPrefix), routeRedisSetMember},
		// Redis zset
		{[]byte(RedisZSetMetaDeltaPrefix), routeRedisZSetMetaDelta},
		{[]byte(RedisZSetMetaPrefix), routeRedisZSetMeta},
		{[]byte(RedisZSetMemberPrefix), routeRedisZSetMember},
		{[]byte(RedisZSetScorePrefix), routeRedisZSetScore},
		{[]byte(RedisZSetLegacyBlobPrefix), routeRedisZSetLegacy},
		// Redis stream
		{[]byte(RedisStreamMetaPrefix), routeRedisStreamMeta},
		{[]byte(RedisStreamEntryPrefix), routeRedisStreamEntry},
		// Redis simple — handlers take the userKey AFTER the prefix.
		{[]byte(RedisStringPrefix), routeRedisString},
		{[]byte(RedisHLLPrefix), routeRedisHLL},
		{[]byte(RedisTTLPrefix), routeRedisTTL},
		// Internal-only: transactional intents / locks / resolver
		// records. Phase 0a drops these; they belong to the running
		// cluster, not the data.
		{[]byte("!txn|"), routeInternalDrop},
		// Internal-only: distribution catalog (route table + version
		// metadata persisted under distribution/catalog.go's
		// `!dist|meta|` / `!dist|route|` prefixes). These ride the
		// default Raft group's Pebble and so appear in every
		// clustered snapshot. Without this route they would land
		// in Counters.Unknown — a false corruption signal on real
		// production dumps (Codex r1 P2 + claude-bot r1 on PR #806).
		{[]byte("!dist|"), routeInternalDrop},
		// Internal-only: encryption writer-registry rows persisted
		// under internal/encryption/registry.go's
		// `!encryption|writers|` prefix. These are §4.1 writer
		// registry rows (one per (dek_id, uint16 node_id) pair) that
		// the FSM apply path writes through the default Raft group's
		// Pebble. They ride snapshots like any other 0x03 entry.
		// Using the broader `!encryption|` prefix so future encryption
		// metadata under the same namespace is also classified as
		// Internal rather than Unknown (Codex r3 P2 on PR #806).
		{[]byte("!encryption|"), routeInternalDrop},
	}
	sort.SliceStable(r, func(i, j int) bool {
		return len(r[i].prefix) > len(r[j].prefix)
	})
	return r
}

// =====================
// DynamoDB route adapters
// =====================

func routeDDBTableMeta(d *dispatcher, k, v []byte) error {
	if d.ddb == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.DynamoDB++
	return d.ddb.HandleTableMeta(k, v)
}

func routeDDBTableGen(d *dispatcher, k, v []byte) error {
	if d.ddb == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.DynamoDB++
	return d.ddb.HandleTableGen(k, v)
}

func routeDDBItem(d *dispatcher, k, v []byte) error {
	if d.ddb == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.DynamoDB++
	return d.ddb.HandleItem(k, v)
}

func routeDDBGSI(d *dispatcher, k, v []byte) error {
	if d.ddb == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.DynamoDB++
	return d.ddb.HandleGSIRow(k, v)
}

// ===============
// S3 route adapters
// ===============

func routeS3BucketMeta(d *dispatcher, k, v []byte) error {
	if d.s3 == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.S3++
	return d.s3.HandleBucketMeta(k, v)
}

func routeS3BucketGen(d *dispatcher, _, _ []byte) error {
	// !s3|bucket|gen| is the per-bucket generation counter. The
	// encoder reads the generation from the manifest key itself; the
	// counter record is not needed at dump time. Drop with Internal.
	d.counters.Internal++
	return nil
}

func routeS3ObjectManifest(d *dispatcher, k, v []byte) error {
	if d.s3 == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.S3++
	return d.s3.HandleObjectManifest(k, v)
}

func routeS3Blob(d *dispatcher, k, v []byte) error {
	if d.s3 == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.S3++
	return d.s3.HandleBlob(k, v)
}

// routeS3UploadMeta returns a handler that forwards a specific
// in-flight-multipart prefix into HandleIncompleteUpload. The
// indirection captures the prefix label the S3 encoder uses to
// distinguish the family at parse time.
func routeS3UploadMeta(prefix string) func(d *dispatcher, k, v []byte) error {
	return func(d *dispatcher, k, v []byte) error {
		if d.s3 == nil {
			d.counters.Internal++
			return nil
		}
		d.counters.S3++
		return d.s3.HandleIncompleteUpload(prefix, k, v)
	}
}

// ===============
// SQS route adapters
// ===============

func routeSQSQueueMeta(d *dispatcher, k, v []byte) error {
	if d.sqs == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.SQS++
	return d.sqs.HandleQueueMeta(k, v)
}

func routeSQSQueueGen(d *dispatcher, k, v []byte) error {
	if d.sqs == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.SQS++
	return d.sqs.HandleQueueGen(k, v)
}

func routeSQSMessageData(d *dispatcher, k, v []byte) error {
	if d.sqs == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.SQS++
	return d.sqs.HandleMessageData(k, v)
}

// routeSQSSide returns a handler that forwards a specific side-
// record prefix into HandleSideRecord. The encoder uses the prefix
// label to dispatch to the right family (dedup, group, vis, ...).
func routeSQSSide(prefix string) func(d *dispatcher, k, v []byte) error {
	return func(d *dispatcher, k, v []byte) error {
		if d.sqs == nil {
			d.counters.Internal++
			return nil
		}
		d.counters.SQS++
		return d.sqs.HandleSideRecord(prefix, k, v)
	}
}

// =================
// Redis route adapters
// =================

func routeRedisHashMeta(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleHashMeta(k, v)
}

func routeRedisHashField(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleHashField(k, v)
}

func routeRedisListMeta(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleListMeta(k, v)
}

func routeRedisListMetaDelta(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleListMetaDelta(k, v)
}

func routeRedisListItem(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleListItem(k, v)
}

func routeRedisListClaim(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleListClaim(k, v)
}

func routeRedisSetMeta(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleSetMeta(k, v)
}

func routeRedisSetMetaDelta(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleSetMetaDelta(k, v)
}

func routeRedisSetMember(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleSetMember(k, v)
}

func routeRedisZSetMeta(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleZSetMeta(k, v)
}

func routeRedisZSetMetaDelta(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleZSetMetaDelta(k, v)
}

func routeRedisZSetMember(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleZSetMember(k, v)
}

func routeRedisZSetScore(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleZSetScore(k, v)
}

func routeRedisZSetLegacy(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleZSetLegacyBlob(k, v)
}

func routeRedisStreamMeta(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleStreamMeta(k, v)
}

func routeRedisStreamEntry(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	d.counters.Redis++
	return d.redis.HandleStreamEntry(k, v)
}

// Redis simple-type handlers expect the userKey AFTER the prefix —
// they take the slice that lives between the leading "!redis|<x>|"
// segment and the inverted-TS suffix the snapshot reader already
// stripped.
func routeRedisString(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	userKey := k[len(RedisStringPrefix):]
	d.counters.Redis++
	return d.redis.HandleString(userKey, v)
}

func routeRedisHLL(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	userKey := k[len(RedisHLLPrefix):]
	d.counters.Redis++
	return d.redis.HandleHLL(userKey, v)
}

func routeRedisTTL(d *dispatcher, k, v []byte) error {
	if d.redis == nil {
		d.counters.Internal++
		return nil
	}
	userKey := k[len(RedisTTLPrefix):]
	d.counters.Redis++
	return d.redis.HandleTTL(userKey, v)
}

// routeInternalDrop is the no-op handler for prefixes Phase 0a
// deliberately discards (transactional intents, route-catalog
// entries, garbage-collection metadata, ...).
func routeInternalDrop(d *dispatcher, _, _ []byte) error {
	d.counters.Internal++
	return nil
}

package adapter

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// leaseReadKeyless performs a keyless quorum-freshness lease check for
// multi-shard read handlers (Scan, GSI/whole-table Query fallback). These
// reads visit every shard the range intersects, so the check fences EVERY
// group the coordinator owns via LeaseReadAllGroupsThrough — a default-group-
// only lease would let a non-default group serve a stale snapshot. A
// single-group coordinator falls back to one LeaseRead, so single-group
// deployments still issue exactly one lease read. It bounds the wait with
// dynamoLeaseReadTimeout so a stalled Raft cannot hang the handler when the
// client never cancels, and writes the same InternalServerError that getItem
// produces on lease failure. Returns false after writing an error response;
// the caller should simply return.
// leaseReadKeyless fences every group via the keyless all-groups lease check.
// `leaseCtx` MUST be the SAME context the pre-pass armed (it bounds the entire
// pre-pass — schema read + the lease that lands here — by dynamoLeaseReadTimeout
// total; coderabbit Major on PR #952 round-4). Creating a fresh
// context.WithTimeout(r.Context(), dynamoLeaseReadTimeout) here would re-arm
// the 5s budget per call, so a slow schema read followed by the keyless
// fallback could consume close to 10s end-to-end. Callers that do NOT have a
// pre-pass context must pass their own bounded ctx; r.Context() with the
// handler's own timeout-on-the-roundabout is the conservative choice.
func (d *DynamoDBServer) leaseReadKeyless(w http.ResponseWriter, leaseCtx context.Context) bool {
	if err := kv.LeaseReadAllGroupsThrough(d.coordinator, leaseCtx); err != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
		return false
	}
	return true
}

// leaseCheckScan runs the Scan lease pre-pass. A Scan reads the whole table, so
// a VALID scan must fence EVERY group via the keyless all-groups check. But a
// scan against a missing table, an unknown index, or a GSI with
// ConsistentRead=true never touches data: the read path rejects it with a
// deterministic 4xx, so establishing freshness is unnecessary and a failed
// all-groups fence on a degraded deployment would mask that 4xx with a 500
// (codex #952 P2-A). leaseCheckScan therefore cheaply pre-validates the request
// (schema load + the same GSI read-option checks scanItems re-runs) at a
// tentative timestamp and skips the lease on a client-side validation error,
// while still failing closed (fencing every group) on a transient schema-read
// failure. Returns false after writing an error response; the caller returns.
func (d *DynamoDBServer) leaseCheckScan(w http.ResponseWriter, r *http.Request, in scanInput) bool {
	// leaseCtx bounds the pre-validation schema read AND the lease read so a
	// stalled schema read cannot block the handler past dynamoLeaseReadTimeout
	// before the lease phase begins. leaseReadKeyless creates its own bounded
	// context for the actual lease read.
	leaseCtx, leaseCancel := context.WithTimeout(r.Context(), dynamoLeaseReadTimeout)
	defer leaseCancel()
	schema, plan, err := d.multiShardReadLeasePlan(leaseCtx, in.TableName, in.IndexName, in.Select, in.ProjectionExpression, in.ExpressionAttributeNames, in.ConsistentRead)
	if err != nil {
		// Transient/internal schema-read failure: fail closed by fencing every
		// group. leaseReadKeyless writes the same InternalServerError on its
		// own failure.
		return d.leaseReadKeyless(w, leaseCtx)
	}
	if plan == queryLeaseSkip {
		// Client-side validation problem (table not found, unknown index,
		// unsupported ConsistentRead): the read path re-runs the identical
		// validation and surfaces the deterministic 4xx, so skip the lease so a
		// degraded-lease failure cannot mask it with a 500 (codex #952 P2-A).
		return true
	}
	// Malformed ExclusiveStartKey is a deterministic ValidationException the
	// read path rejects in resolveTableReadBounds / resolveGSIReadBounds —
	// before the iterator is constructed and before any store read. If we let
	// the lease run first, a degraded shard's 500 would mask that 4xx
	// (codex #952 P2 round-3). Pre-validate against the loaded schema and skip
	// leasing on failure; the read path will surface the identical error.
	if scanExclusiveStartKeyInvalid(schema, in) {
		return true
	}
	// Same logic for a malformed ProjectionExpression: newReadPageState runs
	// resolveProjectionAttributes before the iterator reads from the store, so a
	// parse failure is a deterministic ValidationException the lease pre-pass
	// must not mask (codex #952 P2 round-4 line 2346). validateGSIReadOptions
	// already covers the GSI case; this catches the base-table path that the
	// earlier ExclusiveStartKey check left exposed.
	if projectionInvalid(in.ProjectionExpression, in.ExpressionAttributeNames) {
		return true
	}
	// Valid whole-table read: fence every group (fail closed).
	return d.leaseReadKeyless(w, leaseCtx)
}

// multiShardReadLeasePlan cheaply classifies whether a multi-shard read (Scan or
// a GSI/whole-table Query) is a VALID data read that must fence every group
// (queryLeaseAllGroups) or a CLIENT-side validation problem the read path
// rejects identically without touching data (queryLeaseSkip). It performs the
// same table-existence and GSI read-option checks prepareReadSchema runs, at a
// tentative timestamp (schema only, no readTS sampling), so the lease pre-pass
// never masks a deterministic 4xx with a degraded-lease 500. A transient/internal
// schema-read failure is returned as an error so the caller fails closed.
//
// The loaded schema is returned (nil when the table is missing or on error) so
// callers that need a further deterministic validation (the GSI Query
// KeyConditionExpression check) can reuse it without a second schema load.
//
// Validation failures are reported via queryLeaseSkip rather than an error: the
// read path re-runs the same resolution and reports the identical validation
// error, so error mapping is unchanged.
func (d *DynamoDBServer) multiShardReadLeasePlan(
	ctx context.Context,
	tableName string,
	indexName string,
	selectValue string,
	projectionExpression string,
	names map[string]string,
	consistentRead *bool,
) (*dynamoTableSchema, queryLeasePlan, error) {
	tentativeTS := snapshotTS(d.coordinator.Clock(), d.store)
	schema, exists, err := d.loadTableSchemaAt(ctx, tableName, tentativeTS)
	if err != nil {
		// loadTableSchemaAt maps ErrKeyNotFound to (_, false, nil); any error
		// reaching here is a transient store/context/decode failure, so fail
		// closed.
		return nil, queryLeaseAllGroups, errors.WithStack(err)
	}
	if !exists {
		// Table not found is a deterministic ResourceNotFoundException the read
		// path produces without touching data; skip the lease.
		return nil, queryLeaseSkip, nil
	}
	// validateGSIReadOptions runs the identical unknown-index / GSI
	// ConsistentRead / projection checks prepareReadSchema performs. Any failure
	// is a *dynamoAPIError (a deterministic ValidationException), so classify it
	// as a skip; a transient failure is impossible here (no store access).
	if err := validateGSIReadOptions(schema, indexName, selectValue, projectionExpression, names, consistentRead); err != nil {
		if dynamoErrIsTransient(err) {
			// Defensive: validateGSIReadOptions returns only *dynamoAPIError, so
			// this is unreachable. Fail closed if a future change adds a
			// transient path.
			return nil, queryLeaseAllGroups, errors.WithStack(err)
		}
		return schema, queryLeaseSkip, nil
	}
	return schema, queryLeaseAllGroups, nil
}

// leaseCheckQuery lease-checks the shard a Query reads with a bounded
// timeout, writing the same InternalServerError getItem produces on
// failure. When the request resolves to a single base-table hash-key
// prefix (the common case), the check is routed to that prefix's owning
// group via LeaseReadForKey so a multi-group deployment confirms the
// shard that actually holds the data — not the default group. GSI
// queries and any request whose prefix cannot be resolved here fall back
// to the keyless check, which establishes freshness across every shard
// the range can touch. Returns false after writing an error response;
// the caller should simply return.
func (d *DynamoDBServer) leaseCheckQuery(w http.ResponseWriter, r *http.Request, in queryInput) bool {
	// leaseCtx bounds the entire pre-pass — the schema read that resolves
	// the lease key and the lease read itself — so a stalled schema read
	// cannot block the handler past dynamoLeaseReadTimeout before the lease
	// phase begins. The keyless fallback creates its own bounded context.
	leaseCtx, leaseCancel := context.WithTimeout(r.Context(), dynamoLeaseReadTimeout)
	defer leaseCancel()
	leaseKey, plan, err := d.queryLeaseKey(leaseCtx, in)
	if err != nil {
		// Transient/internal schema read failure: the routing key could
		// not be resolved, so fail closed by fencing EVERY group via the
		// keyless check (a strict superset of the single group this query
		// would have routed to). leaseReadKeyless writes the same
		// InternalServerError on its own failure.
		return d.leaseReadKeyless(w, leaseCtx)
	}
	switch plan {
	case queryLeaseSkip:
		// Client-side validation problem (table not found, malformed/
		// unsupported KeyConditionExpression): the request touches no data,
		// so establishing freshness is unnecessary. Skip the lease entirely
		// and let queryItems re-run the identical resolution and surface the
		// deterministic ResourceNotFoundException/ValidationException — a
		// lease failure on the fallback must not mask that 4xx with a 500 in
		// a degraded deployment (codex #952 P2). This matches getItem, which
		// writes the 4xx before any lease read.
		return true
	case queryLeaseAllGroups:
		// GSI / whole-table query: a VALID read that spans multiple shards,
		// so the keyless all-groups check is the correct fence (fail closed).
		return d.leaseReadKeyless(w, leaseCtx)
	case queryLeaseSingleGroup:
		if _, err := kv.LeaseReadForKeyThrough(d.coordinator, leaseCtx, leaseKey); err != nil {
			writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
			return false
		}
		return true
	default:
		// Unreachable: queryLeaseKey only returns the three plans above. Fail
		// closed via the all-groups fence rather than silently proceeding.
		return d.leaseReadKeyless(w, leaseCtx)
	}
}

// queryLeasePlan classifies how a Query lease pre-pass must fence the read.
type queryLeasePlan int

const (
	// queryLeaseSingleGroup: the query routes to exactly one shard group;
	// fence that group via the resolved leaseKey.
	queryLeaseSingleGroup queryLeasePlan = iota
	// queryLeaseAllGroups: a VALID multi-shard read (GSI query or whole-table
	// prefix); fence every group via the keyless all-groups check.
	queryLeaseAllGroups
	// queryLeaseSkip: a CLIENT-side validation problem (table not found,
	// malformed/unsupported KeyConditionExpression) that the read path rejects
	// deterministically without touching data; skip the lease so the handler's
	// 4xx is never masked by a transient lease failure.
	queryLeaseSkip
)

// queryLeaseKey resolves the single hash-key prefix a base-table Query reads,
// at a tentative timestamp (schema only, no readTS sampling), so the lease
// check can be routed to the owning shard group. It returns:
//   - (prefix, queryLeaseSingleGroup, nil) when the query routes to exactly
//     one shard group;
//   - (nil, queryLeaseAllGroups, nil) for a VALID multi-shard read (GSI query
//     or whole-table prefix) the caller must fence across every group;
//   - (nil, queryLeaseSkip, nil) for a CLIENT-side validation problem (table
//     not found, unknown index, GSI ConsistentRead, malformed/unsupported
//     KeyConditionExpression) the read path rejects identically — the caller
//     skips the lease so the deterministic 4xx is not masked by a transient
//     lease failure (codex #952 P2). GSI queries are validated against the
//     table schema here before being classified as a multi-shard read so an
//     invalid index can never trigger the all-groups fence (codex #952 P2-B);
//   - (nil, _, err) for a TRANSIENT/INTERNAL schema-read failure (leaseCtx
//     deadline, Pebble error) so the caller fails closed.
//
// Validation failures are reported via queryLeaseSkip rather than an error: the
// read path re-runs the same resolution and reports the identical validation
// error, so error mapping is unchanged.
func (d *DynamoDBServer) queryLeaseKey(ctx context.Context, in queryInput) ([]byte, queryLeasePlan, error) {
	if strings.TrimSpace(in.IndexName) != "" {
		// A GSI query is a multi-shard read, but only when it passes the same
		// validation the read path runs: a query against a missing table,
		// unknown index, GSI ConsistentRead, or malformed KeyConditionExpression
		// touches no data and the read path rejects it with a deterministic 4xx.
		// Fencing every group before that validation would mask the 4xx with a
		// degraded-lease 500, so classify those as a skip (codex #952 P2-B).
		schema, plan, err := d.multiShardReadLeasePlan(ctx, in.TableName, in.IndexName, in.Select, in.ProjectionExpression, in.ExpressionAttributeNames, in.ConsistentRead)
		if err != nil {
			return nil, queryLeaseAllGroups, errors.WithStack(err)
		}
		if plan == queryLeaseSkip {
			return nil, queryLeaseSkip, nil
		}
		// Malformed ExclusiveStartKey is a deterministic ValidationException the
		// read path rejects before the iterator is constructed (codex #952 P2
		// round-3). Skip leasing on failure so a degraded shard cannot mask
		// that 4xx with a 500.
		if queryExclusiveStartKeyInvalid(schema, in) {
			return nil, queryLeaseSkip, nil
		}
		// Malformed ProjectionExpression is the same kind of deterministic
		// ValidationException newReadPageState raises before the iterator
		// touches data (codex #952 P2 round-4 line 2492); skip the lease so a
		// degraded shard cannot mask it.
		if projectionInvalid(in.ProjectionExpression, in.ExpressionAttributeNames) {
			return nil, queryLeaseSkip, nil
		}
		// Schema + GSI options are valid; the KeyConditionExpression is the last
		// deterministic validation the read path runs before touching data.
		return nil, gsiQueryLeasePlan(in, schema), nil
	}
	tentativeTS := snapshotTS(d.coordinator.Clock(), d.store)
	schema, exists, err := d.loadTableSchemaAt(ctx, in.TableName, tentativeTS)
	if err != nil {
		// loadTableSchemaAt maps ErrKeyNotFound to (_, false, nil); any
		// error reaching here is a transient store/context/decode failure,
		// so fail closed.
		return nil, queryLeaseSingleGroup, errors.WithStack(err)
	}
	if !exists {
		// Table not found is a deterministic ResourceNotFoundException the
		// read path produces without touching data; skip the lease.
		return nil, queryLeaseSkip, nil
	}
	// Same ExclusiveStartKey pre-check as the GSI branch above (base table).
	if queryExclusiveStartKeyInvalid(schema, in) {
		return nil, queryLeaseSkip, nil
	}
	// Same ProjectionExpression pre-check (base-table path; codex #952 P2 round-4
	// line 2492).
	if projectionInvalid(in.ProjectionExpression, in.ExpressionAttributeNames) {
		return nil, queryLeaseSkip, nil
	}
	prefix, plan := queryLeasePrefix(in, schema)
	return prefix, plan, nil
}

// queryLeasePrefix resolves the single hash-key prefix a base-table Query
// reads, classifying the read into queryLeaseSingleGroup (resolved prefix),
// queryLeaseAllGroups (whole-table prefix: a valid multi-shard read), or
// queryLeaseSkip (malformed KeyConditionExpression: a validation error the
// read path rejects identically). The validation error is deliberately not
// surfaced — only the routing classification matters here, and the read path
// reports the identical error downstream.
func queryLeasePrefix(in queryInput, schema *dynamoTableSchema) ([]byte, queryLeasePlan) {
	keySchema, cond, err := resolveQueryCondition(in, schema)
	if err != nil {
		// Malformed/unsupported KeyConditionExpression: a deterministic
		// ValidationException the read path produces without touching data.
		return nil, queryLeaseSkip
	}
	// A query whose key schema hash key differs from the primary hash
	// key reads the whole-table prefix (see queryScanPrefix), which can
	// span multiple shards; let the all-groups check cover them.
	if keySchema.HashKey != schema.PrimaryKey.HashKey {
		return nil, queryLeaseAllGroups
	}
	prefix, err := queryScanPrefix(schema, in, keySchema, cond.hashValue)
	if err != nil {
		// queryScanPrefix only fails on an unparseable hash-key value — a
		// ValidationException the read path rejects identically.
		return nil, queryLeaseSkip
	}
	return prefix, queryLeaseSingleGroup
}

// gsiQueryLeasePlan classifies a GSI Query (already known to name a valid index
// on an existing table) as the multi-shard all-groups read it is, unless its
// KeyConditionExpression is malformed — the last deterministic validation the
// read path runs before touching data. resolveQueryCondition does no store
// access and returns only *dynamoAPIError, so a failure is a ValidationException
// the read path rejects identically; classify it as a skip so the lease pre-pass
// cannot mask that 4xx with a degraded-lease 500 (codex #952 P2-B). Like
// queryLeasePrefix, the validation error is deliberately not surfaced — only the
// routing classification matters here.
func gsiQueryLeasePlan(in queryInput, schema *dynamoTableSchema) queryLeasePlan {
	if _, _, err := resolveQueryCondition(in, schema); err != nil {
		return queryLeaseSkip
	}
	return queryLeaseAllGroups
}

// leaseCheckTransactGetItems performs a quorum-freshness lease check on every
// shard the TransactGetItems request will read, with a bounded timeout, BEFORE
// the caller resolves the single snapshot timestamp. Item keys are resolved at a
// tentative timestamp (schemas change rarely, so a slight pre-lease stale schema
// is acceptable) used only to route the lease check; the actual snapshot
// timestamp is sampled by the caller afterwards. Items whose schema or key
// cannot be resolved here are skipped: they never reach a store read, and
// buildTransactGetItemsResponses surfaces the identical validation error
// downstream so error mapping is unchanged. When every item is skipped no
// shard is touched, so the function returns true without a lease read.
//
// Keys are first deduplicated by value, then collapsed to one representative key
// per owning Raft group, so a transaction touching up to transactGetItemsMaxItems
// keys that share a group issues a single lease read instead of one per key.
// Each group maintains its own lease, so checking one key per group still
// establishes freshness for every shard the transaction reads. Returns false
// after writing the same InternalServerError getItem produces on lease failure;
// the caller should simply return.
func (d *DynamoDBServer) leaseCheckTransactGetItems(w http.ResponseWriter, r *http.Request, in transactGetItemsInput) bool {
	// leaseCtx bounds the entire pre-pass — both the per-item schema reads
	// that resolve keys and the lease reads themselves — so a stalled
	// schema read (Pebble backpressure, iterator leak) cannot block the
	// handler past dynamoLeaseReadTimeout before the lease phase begins.
	leaseCtx, leaseCancel := context.WithTimeout(r.Context(), dynamoLeaseReadTimeout)
	defer leaseCancel()
	uniqueKeys, skipLease, transientErr := d.resolveTransactGetItemKeys(leaseCtx, in)
	if transientErr != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, transientErr.Error())
		return false
	}
	if skipLease {
		return true
	}
	groupKeys := kv.LeaseReadGroupKeys(d.coordinator, uniqueKeys)
	if leaseErr := d.leaseReadGroupKeys(leaseCtx, groupKeys); leaseErr != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, leaseErr.Error())
		return false
	}
	return true
}

// resolveTransactGetItemKeys runs the per-item schema resolution that the
// lease pre-pass needs. Returns (uniqueKeys, skipLease, transientErr) where
// skipLease is true when the read path will surface a deterministic 4xx via
// buildTransactGetItemsResponses without touching any store — leasing the
// valid items in that case only risks masking that 4xx with a degraded-shard
// 500 (codex P2 #952). skipLease covers three cases:
//   - (a) every item was malformed (nothing to fence)
//   - (b) at least one item was malformed and at least one was valid
//     (buildTransactGetItemsResponses returns a ValidationException for the
//     malformed item; the valid items never reach a store read)
//   - (c) the request contains a duplicate (table, key) pair — DynamoDB
//     rejects this with `Transaction request cannot include multiple
//     operations on one item`, a deterministic ValidationException the read
//     path produces before touching data, so the lease must be skipped for
//     the same reason malformed-mixed-with-valid is skipped.
//
// transientErr is the schema-read failure the caller MUST fail closed on
// (CLAUDE.md: the slow conditions the fence targets are exactly when a
// silently-dropped item would let a stale snapshot through).
func (d *DynamoDBServer) resolveTransactGetItemKeys(ctx context.Context, in transactGetItemsInput) ([][]byte, bool, error) {
	tentativeTS := snapshotTS(d.coordinator.Clock(), d.store)
	schemaCache := make(map[string]*dynamoTableSchema)
	seenKeys := make(map[string]struct{}, len(in.TransactItems))
	uniqueKeys := make([][]byte, 0, len(in.TransactItems))
	hasMalformed := false
	hasDuplicate := false
	for _, item := range in.TransactItems {
		itemKey, ok, err := d.transactGetItemKey(ctx, item, schemaCache, tentativeTS)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			hasMalformed = true
			continue
		}
		if _, dup := seenKeys[string(itemKey)]; dup {
			hasDuplicate = true
			continue
		}
		seenKeys[string(itemKey)] = struct{}{}
		uniqueKeys = append(uniqueKeys, itemKey)
	}
	if hasMalformed || hasDuplicate || len(uniqueKeys) == 0 {
		return nil, true, nil
	}
	return uniqueKeys, false, nil
}

// leaseReadGroupKeys fences every group whose key appears in groupKeys. The
// single-group case stays on the calling goroutine; multi-group fan-out is
// concurrent so a 100-item TransactGetItems does not serialize into 100 Raft
// round-trips and blow dynamoLeaseReadTimeout (gemini HIGH on PR #952). The
// fan-out is bounded by len(groupKeys) ≤ transactGetItemsMaxItems (100), so a
// per-call goroutine pool is unnecessary. Returns the first error seen across
// all goroutines (the rest are dropped to preserve the single-response
// contract at the HTTP layer).
func (d *DynamoDBServer) leaseReadGroupKeys(ctx context.Context, groupKeys [][]byte) error {
	if len(groupKeys) == 0 {
		return nil
	}
	if len(groupKeys) == 1 {
		_, err := kv.LeaseReadForKeyThrough(d.coordinator, ctx, groupKeys[0])
		return errors.WithStack(err)
	}
	// Derive a cancellable child so the first error cancels the sibling lease
	// reads instead of letting them run out the full dynamoLeaseReadTimeout
	// budget (coderabbit Major on PR #952 round-4). The siblings observe the
	// cancellation via the LeaseReadForKeyThrough's own context check.
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(groupKeys))
	var wg sync.WaitGroup
	for _, itemKey := range groupKeys {
		wg.Add(1)
		go func(k []byte) {
			defer wg.Done()
			if _, err := kv.LeaseReadForKeyThrough(d.coordinator, cancelCtx, k); err != nil {
				select {
				case errCh <- err:
					cancel() // unwind the remaining goroutines on the first error.
				default:
				}
			}
		}(itemKey)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// transactGetItemKey resolves the storage key for one TransactGetItems Get at
// the tentative timestamp. It returns (key, true, nil) on success,
// (nil, false, nil) when the item is MALFORMED (nil Get, empty/unknown table,
// or an invalid key) — the read path rejects those identically, so the lease
// pre-pass may safely skip them — and (nil, false, err) for a TRANSIENT or
// INTERNAL schema-read failure (leaseCtx deadline, Pebble error) that the
// caller MUST fail closed on rather than skip, otherwise the item's shard goes
// unfenced and a stale read can slip through. The malformed/transient split
// keys off dynamoErrIsTransient: validation errors are *dynamoAPIError,
// everything else is treated as transient. It never writes a response:
// validation is left to the read path so error mapping stays identical.
func (d *DynamoDBServer) transactGetItemKey(ctx context.Context, item transactGetItem, schemaCache map[string]*dynamoTableSchema, tentativeTS uint64) ([]byte, bool, error) {
	if item.Get == nil || strings.TrimSpace(item.Get.TableName) == "" {
		return nil, false, nil
	}
	schema, err := d.resolveTransactTableSchema(ctx, schemaCache, item.Get.TableName, tentativeTS)
	if err != nil {
		if dynamoErrIsTransient(err) {
			return nil, false, errors.WithStack(err)
		}
		// Validation error (table not found): the read path rejects it
		// identically, so skip rather than fail closed.
		return nil, false, nil
	}
	// itemKeyFromAttributes only fails on malformed key attributes
	// (missing/unparseable hash or range key), a pure validation error the
	// read path rejects identically; transactGetItemKeyFromSchema swallows
	// it to ok=false so the item is skipped, not failed closed.
	itemKey, ok := transactGetItemKeyFromSchema(schema, item.Get.Key)
	return itemKey, ok, nil
}

// transactGetItemKeyFromSchema computes the storage key for a TransactGetItems
// Get, returning ok=false when the key attributes are malformed. The error is
// deliberately discarded: it is a validation failure the read path reports
// downstream, and the lease pre-pass only needs the routing key.
func transactGetItemKeyFromSchema(schema *dynamoTableSchema, key map[string]attributeValue) ([]byte, bool) {
	itemKey, err := schema.itemKeyFromAttributes(key)
	if err != nil {
		return nil, false
	}
	return itemKey, true
}

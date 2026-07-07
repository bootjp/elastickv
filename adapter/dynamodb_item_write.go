package adapter

import (
	"bytes"
	"context"
	"io"
	"maps"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

type updateItemInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	UpdateExpression          string                    `json:"UpdateExpression"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ReturnValues              string                    `json:"ReturnValues"`
}

type putItemInput struct {
	TableName                 string                    `json:"TableName"`
	Item                      map[string]attributeValue `json:"Item"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ReturnValues              string                    `json:"ReturnValues"`
}

type deleteItemInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ReturnValues              string                    `json:"ReturnValues"`
}

func (d *DynamoDBServer) putItem(w http.ResponseWriter, r *http.Request) {
	in, err := decodePutItemInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if err := d.ensureLegacyTableMigration(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	plan, err := d.putItemWithRetry(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	d.observeWrittenItems(r.Context(), in.TableName, 1)
	resp := map[string]any{}
	if attrs := putItemReturnAttributes(in.ReturnValues, plan.current); len(attrs) > 0 {
		resp["Attributes"] = attrs
	}
	writeDynamoJSON(w, resp)
}

func decodePutItemInput(bodyReader io.Reader) (putItemInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return putItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in putItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		return putItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return putItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	if err := validatePutItemReturnValues(in.ReturnValues); err != nil {
		return putItemInput{}, err
	}
	return in, nil
}

func validatePutItemReturnValues(returnValues string) error {
	switch strings.TrimSpace(returnValues) {
	case "", dynamoReturnValueNone, dynamoReturnValueAllOld:
		return nil
	default:
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported ReturnValues")
	}
}

func (d *DynamoDBServer) putItemWithRetry(ctx context.Context, in putItemInput) (*itemWritePlan, error) {
	return d.retryItemWriteWithGeneration(
		ctx,
		in.TableName,
		"put item retry attempts exhausted",
		func(readTS uint64) (*itemWritePlan, error) {
			return d.preparePutItemWrite(ctx, in, readTS)
		},
	)
}

type itemWritePlan struct {
	req        *kv.OperationGroup[kv.OP]
	generation uint64
	cleanup    [][]byte
	current    map[string]attributeValue
	next       map[string]attributeValue
}

func (d *DynamoDBServer) retryItemWriteWithGeneration(
	ctx context.Context,
	tableName string,
	exhaustedMessage string,
	prepare func(readTS uint64) (*itemWritePlan, error),
) (*itemWritePlan, error) {
	// Option-2 one-phase dedup (default on, with an explicit rollback switch):
	// on a retryable write error, reuse the failed attempt's write set under a
	// fresh commit_ts + prev_commit_ts so the FSM no-ops a commit that already
	// landed under leadership churn, instead of re-reading and re-appending (the
	// :duplicate-elements anomaly). See
	// docs/design/2026_06_03_partial_dynamodb_onephase_dedup.md.
	//
	// Leader-only (codex P1, PR #920): the dedup path allocates commit_ts from
	// the LOCAL HLC and carries it as prev_commit_ts, so that timestamp MUST be
	// leader-issued to stay globally unique — otherwise two frontends could mint
	// the same commit_ts in one millisecond and the exact-ts probe would dedup
	// against the wrong writer's version, losing an update. On the leader the
	// single HLC issues monotonic unique values, and NextFenced's physical-ceiling
	// fence keeps a deposed leader's window disjoint from its successor's. A
	// non-leader (reachable only when no leaderMap HTTP proxy forwards follower
	// ingress) falls back to the legacy path, where Coordinator.Dispatch redirects
	// to the leader and the LEADER allocates commit_ts — never this follower's HLC.
	if d.onePhaseTxnDedup && d.coordinator.IsLeader() {
		return d.retryItemWriteWithGenerationDedup(ctx, tableName, exhaustedMessage, prepare)
	}
	return d.retryItemWriteWithGenerationLegacy(ctx, tableName, exhaustedMessage, prepare)
}

// retryItemWriteWithGenerationLegacy is the pre-dedup retry loop: it recomputes
// the write set from a fresh read on every retryable error. It is the active
// path whenever dedup is explicitly off or this node is not the leader, so it
// stays byte-identical to the pre-feature behavior.
func (d *DynamoDBServer) retryItemWriteWithGenerationLegacy(
	ctx context.Context,
	tableName string,
	exhaustedMessage string,
	prepare func(readTS uint64) (*itemWritePlan, error),
) (*itemWritePlan, error) {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTS := d.nextTxnReadTS()
		plan, err := prepare(readTS)
		if err != nil {
			return nil, err
		}
		if plan.req == nil {
			return plan, nil
		}
		plan.req.StartTS = readTS
		if err = d.commitItemWrite(ctx, plan.req); err != nil {
			if !isRetryableTransactWriteError(err) {
				return nil, errors.WithStack(err)
			}
		} else {
			retry, verifyErr := d.handleGenerationFenceResult(
				ctx,
				d.verifyTableGeneration(ctx, tableName, plan.generation),
				plan.cleanup,
			)
			if verifyErr != nil {
				return nil, verifyErr
			}
			if !retry {
				return plan, nil
			}
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return nil, errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return nil, newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, exhaustedMessage)
}

// reusableItemWrite captures a dispatched single-item write attempt so a
// subsequent retry can REUSE its exact write set (the same Put/Del elems) under
// a fresh commit_ts and probe whether it already landed, instead of re-reading
// and recomputing the item. Recomputing is what duplicates a list_append under
// leadership churn: attempt 1 commits at C1 but returns a WriteConflict, the
// retry re-reads the now-larger list and appends again. Reuse + the FSM's
// exact-ts dedup probe close that. See option 2 in
// docs/design/2026_06_03_partial_dynamodb_onephase_dedup.md.
type reusableItemWrite struct {
	// plan holds the reused OperationGroup (plan.req: Elems + fixed StartTS) and
	// the captured current/next item. The client-visible result
	// (updateItemReturnAttributes over current/next) is invariant across reuse
	// — the write set was built once from attempt 1's read — so plan is also the
	// correct value to return when the FSM dedup no-ops the apply (R1).
	plan *itemWritePlan
	// commitTS is the most recent dispatched commit_ts for this write set; the
	// next retry passes it as PrevCommitTS so the FSM probes exactly the attempt
	// that might have landed.
	commitTS uint64
	// probeKey is kv.PrimaryKeyForElems(plan.req.Elems) — the same key the FSM
	// uses as meta.PrimaryKey — so the adapter-side self-inflicted-conflict guard
	// and the FSM dedup probe agree on the point they query (R4).
	probeKey []byte
}

// retryItemWriteWithGenerationDedup is the option-2 retry loop. The first
// attempt computes the write set from a fresh read; any retryable failure makes
// the next iteration REUSE that write set under a fresh commit_ts carrying
// prev_commit_ts, so the FSM no-ops if the prior attempt already landed. A
// genuine WriteConflict on a reuse (the self-conflict probe missed) drops the
// pending attempt and recomputes from a fresh read.
func (d *DynamoDBServer) retryItemWriteWithGenerationDedup(
	ctx context.Context,
	tableName string,
	exhaustedMessage string,
	prepare func(readTS uint64) (*itemWritePlan, error),
) (*itemWritePlan, error) {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	var pending *reusableItemWrite
	for range transactRetryMaxAttempts {
		var (
			plan *itemWritePlan
			err  error
		)
		if pending != nil {
			plan, pending, err = d.itemWriteReuseAttempt(ctx, tableName, pending)
		} else {
			plan, pending, err = d.itemWriteFirstAttempt(ctx, tableName, prepare)
		}
		if err != nil {
			// commitItemWrite already wraps dispatch errors; the attempt helpers
			// return them raw, so return raw here too (no double WithStack).
			if !isRetryableTransactWriteError(err) {
				return nil, err
			}
		} else if plan != nil {
			return plan, nil
		}
		if waitErr := waitRetryWithDeadline(ctx, deadline, backoff); waitErr != nil {
			return nil, errors.WithStack(waitErr)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return nil, newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, exhaustedMessage)
}

// itemWriteFirstAttempt runs the recompute branch of the dedup loop: a fresh
// read snapshot, a locally-allocated commit_ts, and a dispatch. On a retryable
// write error it returns a reusableItemWrite so the next iteration reuses this
// write set. Return shapes match itemWriteReuseAttempt (see
// retryItemWriteWithGenerationDedup).
func (d *DynamoDBServer) itemWriteFirstAttempt(
	ctx context.Context,
	tableName string,
	prepare func(readTS uint64) (*itemWritePlan, error),
) (*itemWritePlan, *reusableItemWrite, error) {
	readTS := d.nextTxnReadTS()
	plan, err := prepare(readTS)
	if err != nil {
		return nil, nil, err
	}
	if plan.req == nil {
		return plan, nil, nil
	}
	// Allocate through the coordinator so TSO-enabled deployments use the
	// same timestamp source as Dispatch. The fallback still honors the HLC
	// physical-ceiling fence.
	commitTS, err := kv.NextTimestampThrough(ctx, d.coordinator, "dynamodb item-write first attempt: allocate commitTS")
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	plan.req.StartTS = readTS
	plan.req.CommitTS = commitTS
	if dispErr := d.commitItemWrite(ctx, plan.req); dispErr != nil {
		// dispErr is already wrapped by commitItemWrite; return it raw.
		if isRetryableTransactWriteError(dispErr) {
			return nil, &reusableItemWrite{
				plan:     plan,
				commitTS: commitTS,
				probeKey: kv.PrimaryKeyForElems(plan.req.Elems),
			}, dispErr
		}
		return nil, nil, dispErr
	}
	return d.finishItemWriteAttempt(ctx, tableName, plan)
}

// itemWriteReuseAttempt runs one reuse iteration: re-dispatch the captured write
// set under a fresh commit_ts carrying pending.commitTS as PrevCommitTS, so the
// FSM probes whether the prior attempt landed.
func (d *DynamoDBServer) itemWriteReuseAttempt(
	ctx context.Context,
	tableName string,
	pending *reusableItemWrite,
) (*itemWritePlan, *reusableItemWrite, error) {
	commitTS, err := kv.NextTimestampThrough(ctx, d.coordinator, "dynamodb item-write reuse: allocate commitTS")
	if err != nil {
		return nil, pending, errors.WithStack(err)
	}
	pending.plan.req.CommitTS = commitTS
	pending.plan.req.PrevCommitTS = pending.commitTS
	dispErr := d.commitItemWrite(ctx, pending.plan.req)
	if dispErr == nil {
		return d.finishItemWriteAttempt(ctx, tableName, pending.plan)
	}
	if errors.Is(dispErr, store.ErrWriteConflict) {
		return d.resolveReuseWriteConflict(ctx, tableName, pending, commitTS, dispErr)
	}
	if isRetryableTransactWriteError(dispErr) {
		// Still ambiguous (e.g. TxnLocked): this reuse may itself have landed,
		// so the next retry must probe THIS commit_ts. dispErr is already
		// wrapped by commitItemWrite; return it raw.
		pending.commitTS = commitTS
		return nil, pending, dispErr
	}
	return nil, nil, dispErr
}

// resolveReuseWriteConflict handles a WriteConflict from a reuse dispatch via
// the self-inflicted-conflict guard: probe whether THIS reuse's commit_ts
// actually landed (the apply may have committed but surfaced WriteConflict under
// churn). On a hit the conflict is against our own commit — return the cached
// plan, no double-apply. On a miss the write key is genuinely held by another
// txn — drop pending so the next iteration recomputes from a fresh read.
func (d *DynamoDBServer) resolveReuseWriteConflict(
	ctx context.Context,
	tableName string,
	pending *reusableItemWrite,
	commitTS uint64,
	dispErr error,
) (*itemWritePlan, *reusableItemWrite, error) {
	if len(pending.probeKey) > 0 {
		landed, perr := d.store.CommittedVersionAt(ctx, pending.probeKey, commitTS)
		if perr != nil {
			// Fail closed: a probe read error makes "did our reuse land?"
			// unknowable, and a blind recompute would double-append if it HAD
			// landed. Surface the probe error instead of silently recomputing,
			// matching the FSM-side dedupProbeOnePhase (kv/fsm.go) which also
			// propagates probe errors. The wrapped error is non-retryable, so
			// the loop returns it to the client rather than re-applying.
			return nil, nil, errors.Wrap(perr, "dynamodb item-write: self-conflict probe")
		}
		if landed {
			// The reuse landed at commitTS. Run the SAME generation fence +
			// cleanup the normal success path runs (finishItemWriteAttempt), so
			// a table dropped/recreated under us cleans up the landed write and
			// recomputes instead of returning a stale plan (coderabbit major).
			return d.finishItemWriteAttempt(ctx, tableName, pending.plan)
		}
	}
	// Probe missed (or no probe key): a genuine cross-writer conflict. dispErr
	// is already wrapped by commitItemWrite; return it raw so the loop recomputes.
	return nil, nil, dispErr
}

// finishItemWriteAttempt runs the table-generation fence after a successful
// commit. Returns (plan, nil, nil) when the write is durable; (nil, nil, nil)
// when the generation changed and the caller must recompute from a fresh read;
// (nil, nil, err) on a fence error.
func (d *DynamoDBServer) finishItemWriteAttempt(
	ctx context.Context,
	tableName string,
	plan *itemWritePlan,
) (*itemWritePlan, *reusableItemWrite, error) {
	retry, verifyErr := d.handleGenerationFenceResult(
		ctx,
		d.verifyTableGeneration(ctx, tableName, plan.generation),
		plan.cleanup,
	)
	if verifyErr != nil {
		return nil, nil, verifyErr
	}
	if retry {
		return nil, nil, nil
	}
	return plan, nil, nil
}

func (d *DynamoDBServer) preparePutItemWrite(ctx context.Context, in putItemInput, readTS uint64) (*itemWritePlan, error) {
	schema, exists, err := d.loadTableSchemaAt(ctx, in.TableName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	itemKey, err := schema.itemKeyFromAttributes(in.Item)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	keyAttrs, err := primaryKeyAttributes(schema.PrimaryKey, in.Item)
	if err != nil {
		return nil, err
	}
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, keyAttrs, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var current map[string]attributeValue
	if found {
		current = currentLocation.item
	}
	if err := validateConditionOnItem(
		in.ConditionExpression,
		in.ExpressionAttributeNames,
		in.ExpressionAttributeValues,
		valueOrEmptyMap(current, found),
	); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	req, cleanup, err := buildItemWriteRequestWithSource(schema, itemKey, in.Item, currentLocation)
	if err != nil {
		return nil, err
	}
	return &itemWritePlan{
		req:        req,
		generation: schema.Generation,
		cleanup:    cleanup,
		current:    cloneAttributeValueMap(current),
		next:       cloneAttributeValueMap(in.Item),
	}, nil
}

func (d *DynamoDBServer) commitItemWrite(ctx context.Context, req *kv.OperationGroup[kv.OP]) error {
	_, err := d.coordinator.Dispatch(ctx, req)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *DynamoDBServer) deleteItem(w http.ResponseWriter, r *http.Request) {
	in, shouldReturnOld, err := decodeDeleteItemInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if err := d.ensureLegacyTableMigration(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	lockKey, err := dynamoItemUpdateLockKey(in.TableName, in.Key)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	unlock := d.lockItemUpdate(lockKey)
	defer unlock()
	plan, err := d.deleteItemWithRetry(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if len(plan.current) > 0 {
		d.observeWrittenItems(r.Context(), in.TableName, 1)
	}
	resp := map[string]any{}
	if shouldReturnOld && len(plan.current) > 0 {
		resp["Attributes"] = plan.current
	}
	writeDynamoJSON(w, resp)
}

func decodeDeleteItemInput(bodyReader io.Reader) (deleteItemInput, bool, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return deleteItemInput{}, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in deleteItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		return deleteItemInput{}, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return deleteItemInput{}, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	shouldReturnOld, err := parseDeleteItemReturnValues(in.ReturnValues)
	if err != nil {
		return deleteItemInput{}, false, err
	}
	return in, shouldReturnOld, nil
}

func parseDeleteItemReturnValues(returnValues string) (bool, error) {
	switch strings.TrimSpace(returnValues) {
	case "", dynamoReturnValueNone:
		return false, nil
	case dynamoReturnValueAllOld:
		return true, nil
	default:
		return false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported ReturnValues")
	}
}

type deleteItemPlan struct {
	req        *kv.OperationGroup[kv.OP]
	generation uint64
	current    map[string]attributeValue
}

func (d *DynamoDBServer) deleteItemWithRetry(ctx context.Context, in deleteItemInput) (*deleteItemPlan, error) {
	var deletePlan *deleteItemPlan
	_, err := d.retryItemWriteWithGeneration(
		ctx,
		in.TableName,
		"delete retry attempts exhausted",
		func(readTS uint64) (*itemWritePlan, error) {
			var err error
			deletePlan, err = d.prepareDeleteItemWrite(ctx, in, readTS)
			if err != nil {
				return nil, err
			}
			return &itemWritePlan{
				req:        deletePlan.req,
				generation: deletePlan.generation,
			}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return deletePlan, nil
}

func (d *DynamoDBServer) prepareDeleteItemWrite(ctx context.Context, in deleteItemInput, readTS uint64) (*deleteItemPlan, error) {
	schema, exists, err := d.loadTableSchemaAt(ctx, in.TableName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, in.Key, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	current := map[string]attributeValue(nil)
	if found {
		current = currentLocation.item
	}
	if err := validateConditionOnItem(
		in.ConditionExpression,
		in.ExpressionAttributeNames,
		in.ExpressionAttributeValues,
		valueOrEmptyMap(current, found),
	); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	if !found {
		return &deleteItemPlan{current: nil}, nil
	}
	req, err := buildItemDeleteRequestWithSource(currentLocation)
	if err != nil {
		return nil, err
	}
	return &deleteItemPlan{
		req:        req,
		generation: schema.Generation,
		current:    cloneAttributeValueMap(current),
	}, nil
}

func (d *DynamoDBServer) updateItem(w http.ResponseWriter, r *http.Request) {
	in, err := decodeUpdateItemInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if err := d.ensureLegacyTableMigration(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	lockKey, err := dynamoItemUpdateLockKey(in.TableName, in.Key)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	unlock := d.lockItemUpdate(lockKey)
	defer unlock()
	plan, err := d.updateItemWithRetry(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	d.observeWrittenItems(r.Context(), in.TableName, 1)
	resp := map[string]any{}
	if attrs := updateItemReturnAttributes(in.ReturnValues, plan.current, plan.next); len(attrs) > 0 {
		resp["Attributes"] = attrs
	}
	writeDynamoJSON(w, resp)
}

func decodeUpdateItemInput(bodyReader io.Reader) (updateItemInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return updateItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in updateItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		return updateItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return updateItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	if err := validateUpdateItemReturnValues(in.ReturnValues); err != nil {
		return updateItemInput{}, err
	}
	return in, nil
}

func validateUpdateItemReturnValues(returnValues string) error {
	switch strings.TrimSpace(returnValues) {
	case "",
		dynamoReturnValueNone,
		dynamoReturnValueAllOld,
		dynamoReturnValueUpdatedOld,
		dynamoReturnValueAllNew,
		dynamoReturnValueUpdatedNew:
		return nil
	default:
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported ReturnValues")
	}
}

func (d *DynamoDBServer) updateItemWithRetry(ctx context.Context, in updateItemInput) (*itemWritePlan, error) {
	return d.retryItemWriteWithGeneration(
		ctx,
		in.TableName,
		"update retry attempts exhausted",
		func(readTS uint64) (*itemWritePlan, error) {
			return d.prepareUpdateItemWrite(ctx, in, readTS)
		},
	)
}

func (d *DynamoDBServer) prepareUpdateItemWrite(ctx context.Context, in updateItemInput, readTS uint64) (*itemWritePlan, error) {
	schema, exists, err := d.loadTableSchemaAt(ctx, in.TableName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	itemKey, err := schema.itemKeyFromAttributes(in.Key)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, in.Key, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var current map[string]attributeValue
	if !found {
		current = map[string]attributeValue{}
	} else {
		current = currentLocation.item
	}
	nextItem, err := buildUpdatedItem(schema, in, current)
	if err != nil {
		return nil, err
	}
	req, cleanup, err := buildItemWriteRequestWithSource(schema, itemKey, nextItem, currentLocation)
	if err != nil {
		return nil, err
	}
	return &itemWritePlan{
		req:        req,
		generation: schema.Generation,
		cleanup:    cleanup,
		current:    cloneAttributeValueMap(current),
		next:       cloneAttributeValueMap(nextItem),
	}, nil
}

func buildUpdatedItem(schema *dynamoTableSchema, in updateItemInput, current map[string]attributeValue) (map[string]attributeValue, error) {
	if err := validateConditionOnItem(in.ConditionExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, current); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	nextItem := cloneAttributeValueMap(current)
	maps.Copy(nextItem, in.Key)
	if err := applyUpdateExpression(in.UpdateExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, nextItem); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if err := ensurePrimaryKeyUnchanged(schema.PrimaryKey, in.Key, nextItem); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return nextItem, nil
}

func ensurePrimaryKeyUnchanged(keySchema dynamoKeySchema, originalKey map[string]attributeValue, nextItem map[string]attributeValue) error {
	if err := ensureSinglePrimaryKeyUnchanged(keySchema.HashKey, originalKey, nextItem); err != nil {
		return err
	}
	if keySchema.RangeKey != "" {
		if err := ensureSinglePrimaryKeyUnchanged(keySchema.RangeKey, originalKey, nextItem); err != nil {
			return err
		}
	}
	return nil
}

func ensureSinglePrimaryKeyUnchanged(attrName string, originalKey map[string]attributeValue, nextItem map[string]attributeValue) error {
	keyVal, ok := originalKey[attrName]
	if !ok {
		return errors.New("missing key attribute")
	}
	nextVal, ok := nextItem[attrName]
	if !ok {
		return errors.New("cannot remove key attribute")
	}
	if !attributeValueEqual(keyVal, nextVal) {
		return errors.New("cannot update primary key attribute")
	}
	return nil
}

type dynamoItemLocation struct {
	schema *dynamoTableSchema
	key    []byte
	item   map[string]attributeValue
}

func buildItemWriteRequestWithSource(
	targetSchema *dynamoTableSchema,
	targetKey []byte,
	nextItem map[string]attributeValue,
	current *dynamoItemLocation,
) (*kv.OperationGroup[kv.OP], [][]byte, error) {
	payload, err := encodeStoredDynamoItem(nextItem)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: targetKey, Value: payload}}
	cleanup := [][]byte{targetKey}
	delKeys, putKeys, err := itemStorageDelta(targetSchema, targetKey, nextItem, current)
	if err != nil {
		return nil, nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	for _, key := range delKeys {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
	}
	for _, key := range putKeys {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: targetKey})
		cleanup = append(cleanup, key)
	}
	return &kv.OperationGroup[kv.OP]{
		IsTxn:   true,
		StartTS: 0,
		Elems:   elems,
	}, cleanup, nil
}

func itemStorageDelta(
	targetSchema *dynamoTableSchema,
	targetKey []byte,
	nextItem map[string]attributeValue,
	current *dynamoItemLocation,
) ([][]byte, [][]byte, error) {
	oldKeys, err := itemStorageKeys(current)
	if err != nil {
		return nil, nil, err
	}
	newKeys, err := targetSchema.gsiEntryKeysForItem(nextItem)
	if err != nil {
		return nil, nil, err
	}
	newSet := bytesToSet(newKeys)
	oldSet := bytesToSet(oldKeys)
	delete(oldSet, string(targetKey))
	delKeys := make([][]byte, 0, len(oldKeys))
	for key, raw := range oldSet {
		if _, ok := newSet[key]; ok {
			continue
		}
		delKeys = append(delKeys, raw)
	}
	putKeys := make([][]byte, 0, len(newKeys))
	for key, raw := range newSet {
		if _, ok := oldSet[key]; ok {
			continue
		}
		putKeys = append(putKeys, raw)
	}
	return delKeys, putKeys, nil
}

func itemStorageKeys(current *dynamoItemLocation) ([][]byte, error) {
	if current == nil || len(current.item) == 0 {
		return nil, nil
	}
	gsiKeys, err := current.schema.gsiEntryKeysForItem(current.item)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(gsiKeys)+1)
	out = append(out, bytes.Clone(current.key))
	out = append(out, gsiKeys...)
	return out, nil
}

func bytesToSet(keys [][]byte) map[string][]byte {
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		out[string(key)] = key
	}
	return out
}

func putItemReturnAttributes(returnValues string, current map[string]attributeValue) map[string]attributeValue {
	if !strings.EqualFold(strings.TrimSpace(returnValues), dynamoReturnValueAllOld) || len(current) == 0 {
		return nil
	}
	return cloneAttributeValueMap(current)
}

func updateItemReturnAttributes(returnValues string, current map[string]attributeValue, next map[string]attributeValue) map[string]attributeValue {
	switch strings.TrimSpace(returnValues) {
	case "", dynamoReturnValueNone:
		return nil
	case dynamoReturnValueAllOld:
		if len(current) == 0 {
			return nil
		}
		return cloneAttributeValueMap(current)
	case dynamoReturnValueAllNew:
		return cloneAttributeValueMap(next)
	case dynamoReturnValueUpdatedOld:
		return selectUpdatedAttributes(current, next, true)
	case dynamoReturnValueUpdatedNew:
		return selectUpdatedAttributes(current, next, false)
	default:
		return nil
	}
}

func selectUpdatedAttributes(current map[string]attributeValue, next map[string]attributeValue, oldValues bool) map[string]attributeValue {
	keys := updatedAttributeNames(current, next)
	if len(keys) == 0 {
		return nil
	}
	out := make(map[string]attributeValue, len(keys))
	for _, key := range keys {
		if oldValues {
			if value, ok := current[key]; ok {
				out[key] = value
			}
			continue
		}
		if value, ok := next[key]; ok {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func updatedAttributeNames(current map[string]attributeValue, next map[string]attributeValue) []string {
	seen := make(map[string]struct{}, len(current)+len(next))
	for name := range current {
		seen[name] = struct{}{}
	}
	for name := range next {
		seen[name] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, name := range names {
		oldVal, oldOK := current[name]
		newVal, newOK := next[name]
		if !oldOK && !newOK {
			continue
		}
		if oldOK && newOK && attributeValueEqual(oldVal, newVal) {
			continue
		}
		out = append(out, name)
	}
	return out
}

func buildItemDeleteRequestWithSource(current *dynamoItemLocation) (*kv.OperationGroup[kv.OP], error) {
	if current == nil {
		return &kv.OperationGroup[kv.OP]{
			IsTxn:   true,
			StartTS: 0,
			Elems:   nil,
		}, nil
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Del, Key: current.key}}
	delKeys, err := itemStorageKeys(current)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	for _, key := range delKeys {
		if bytes.Equal(key, current.key) {
			continue
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
	}
	return &kv.OperationGroup[kv.OP]{
		IsTxn:   true,
		StartTS: 0,
		Elems:   elems,
	}, nil
}

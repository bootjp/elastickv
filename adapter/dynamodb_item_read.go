package adapter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

type getItemInput struct {
	TableName                string                    `json:"TableName"`
	Key                      map[string]attributeValue `json:"Key"`
	ProjectionExpression     string                    `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string         `json:"ExpressionAttributeNames"`
	ConsistentRead           *bool                     `json:"ConsistentRead"`
}

func (d *DynamoDBServer) parseGetItemInput(w http.ResponseWriter, r *http.Request) (getItemInput, bool) {
	body, err := io.ReadAll(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return getItemInput{}, false
	}
	var in getItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return getItemInput{}, false
	}
	if strings.TrimSpace(in.TableName) == "" {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, "missing table name")
		return getItemInput{}, false
	}
	if err := d.ensureLegacyTableMigration(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return getItemInput{}, false
	}
	return in, true
}

func (d *DynamoDBServer) getItem(w http.ResponseWriter, r *http.Request) {
	in, ok := d.parseGetItemInput(w, r)
	if !ok {
		return
	}
	// Tentative TS for schema resolution only; schemas change rarely
	// so a slight pre-lease stale is acceptable. The item read below
	// is sampled AFTER the lease check.
	tentativeTS := d.resolveDynamoReadTS(in.ConsistentRead)
	_, itemKey, ok := d.resolveGetItemTarget(w, r, in, tentativeTS)
	if !ok {
		return
	}
	// Lease-check the shard that actually owns the ITEM key with a
	// bounded timeout so a stalled Raft cannot hang this handler
	// indefinitely if the client never cancels. Use defer so the
	// cancel runs even if LeaseReadForKey panics or a future
	// refactor inserts an early return; the cost of keeping ctx
	// alive until handler exit is negligible because the next
	// in-handler calls are local store reads.
	leaseCtx, leaseCancel := context.WithTimeout(r.Context(), dynamoLeaseReadTimeout)
	defer leaseCancel()
	if _, err := kv.LeaseReadForKeyThrough(d.coordinator, leaseCtx, itemKey); err != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
		return
	}
	// Re-sample readTS AFTER the lease confirmation so that any write
	// that completed on the same shard BEFORE the confirmation is
	// visible. Sampling earlier would violate linearizability for
	// ConsistentRead=false reads by returning a snapshot from before
	// the most recent confirmed commit.
	readTS := d.resolveDynamoReadTS(in.ConsistentRead)
	// Pin readTS so concurrent MVCC GC cannot reclaim versions
	// between the schema revalidation and the item read below;
	// matches the pattern already used by queryItems / scanItems /
	// transactGetItems.
	readPin := d.pinReadTS(readTS)
	defer readPin.Release()

	// Re-resolve schema + itemKey at readTS and verify that the key
	// we lease-checked is STILL the key that will be read. A table
	// migration that commits between the tentative schema load and
	// the lease confirmation may shift the item to a different shard
	// even if the request parameters are unchanged, so comparing the
	// computed item keys (not just generation) catches any future
	// schema change that alters item routing.
	finalSchema, freshItemKey, ok := d.resolveGetItemTarget(w, r, in, readTS)
	if !ok {
		return
	}
	if !bytes.Equal(freshItemKey, itemKey) {
		writeDynamoError(w, http.StatusServiceUnavailable, dynamoErrInternal,
			"table routing changed during read; please retry")
		return
	}

	current, found, err := d.readLogicalItemAt(r.Context(), finalSchema, in.Key, readTS)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	if !found {
		writeDynamoJSON(w, map[string]any{})
		return
	}
	d.observeReadMetrics(r.Context(), in.TableName, 1, 1)
	projected, err := projectItem(current.item, in.ProjectionExpression, in.ExpressionAttributeNames)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	writeDynamoJSON(w, map[string]any{"Item": projected})
}

// resolveGetItemTarget loads the schema and computes the item key whose
// shard must be lease-checked before the read. Returns false after
// writing an error response; the caller should simply return.
func (d *DynamoDBServer) resolveGetItemTarget(w http.ResponseWriter, r *http.Request, in getItemInput, readTS uint64) (*dynamoTableSchema, []byte, bool) {
	schema, exists, err := d.loadTableSchemaAt(r.Context(), in.TableName, readTS)
	if err != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
		return nil, nil, false
	}
	if !exists {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
		return nil, nil, false
	}
	itemKey, err := schema.itemKeyFromAttributes(in.Key)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return nil, nil, false
	}
	return schema, itemKey, true
}

func (d *DynamoDBServer) readItemAtKeyAt(ctx context.Context, key []byte, ts uint64) (map[string]attributeValue, bool, error) {
	b, err := d.store.GetAt(ctx, key, ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	item, err := decodeStoredDynamoItem(b)
	if err != nil {
		return nil, false, err
	}
	return item, true, nil
}

func (d *DynamoDBServer) readLogicalItemAt(
	ctx context.Context,
	schema *dynamoTableSchema,
	key map[string]attributeValue,
	ts uint64,
) (*dynamoItemLocation, bool, error) {
	itemKey, err := schema.itemKeyFromAttributes(key)
	if err != nil {
		return nil, false, err
	}
	item, found, err := d.readItemAtKeyAt(ctx, itemKey, ts)
	if err != nil {
		return nil, false, err
	}
	if found {
		return &dynamoItemLocation{schema: schema, key: itemKey, item: item}, true, nil
	}
	sourceSchema := schema.migrationSourceSchema()
	if sourceSchema == nil {
		return nil, false, nil
	}
	sourceKey, err := sourceSchema.itemKeyFromAttributes(key)
	if err != nil {
		return nil, false, err
	}
	item, found, err = d.readItemAtKeyAt(ctx, sourceKey, ts)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return &dynamoItemLocation{schema: sourceSchema, key: sourceKey, item: item}, true, nil
}

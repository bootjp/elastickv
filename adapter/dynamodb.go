package adapter

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	targetPrefix             = "DynamoDB_20120810."
	putItemTarget            = targetPrefix + "PutItem"
	getItemTarget            = targetPrefix + "GetItem"
	updateItemTarget         = targetPrefix + "UpdateItem"
	transactWriteItemsTarget = targetPrefix + "TransactWriteItems"
)

const (
	updateSplitCount            = 2
	transactRetryMaxAttempts    = 128
	transactRetryMaxDuration    = 2 * time.Second
	transactRetryInitialBackoff = 1 * time.Millisecond
	transactRetryMaxBackoff     = 10 * time.Millisecond
	transactRetryBackoffFactor  = 2
)

type DynamoDBServer struct {
	listen           net.Listener
	store            store.MVCCStore
	coordinator      kv.Coordinator
	dynamoTranscoder *dynamodbTranscoder
	httpServer       *http.Server
}

func NewDynamoDBServer(listen net.Listener, st store.MVCCStore, coordinate kv.Coordinator) *DynamoDBServer {
	d := &DynamoDBServer{
		listen:           listen,
		store:            st,
		coordinator:      coordinate,
		dynamoTranscoder: newDynamoDBTranscoder(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", d.handle)
	d.httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	return d
}

func (d *DynamoDBServer) Run() error {
	if err := d.httpServer.Serve(d.listen); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.WithStack(err)
	}
	return nil
}

func (d *DynamoDBServer) Stop() {
	if d.httpServer != nil {
		_ = d.httpServer.Shutdown(context.Background())
	}
}

func (d *DynamoDBServer) handle(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Amz-Target")
	switch target {
	case putItemTarget:
		d.putItem(w, r)
	case getItemTarget:
		d.getItem(w, r)
	case updateItemTarget:
		d.updateItem(w, r)
	case transactWriteItemsTarget:
		d.transactWriteItems(w, r)
	default:
		http.Error(w, "unsupported operation", http.StatusBadRequest)
	}
}

func (d *DynamoDBServer) putItem(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reqs, err := d.dynamoTranscoder.PutItemToRequest(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err = d.coordinator.Dispatch(r.Context(), reqs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	_, _ = w.Write([]byte("{}"))
}

type getItemInput struct {
	TableName string                    `json:"TableName"`
	Key       map[string]attributeValue `json:"Key"`
}

func (d *DynamoDBServer) getItem(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var in getItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	keyAttr, ok := in.Key["key"]
	if !ok {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	readTS := snapshotTS(d.coordinator.Clock(), d.store)
	v, err := d.store.GetAt(r.Context(), []byte(keyAttr.S), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			w.Header().Set("Content-Type", "application/x-amz-json-1.0")
			_, _ = w.Write([]byte("{}"))
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := map[string]map[string]attributeValue{
		"Item": {
			"key":   {S: keyAttr.S},
			"value": {S: string(v)},
		},
	}
	out, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	_, _ = w.Write(out)
}

type updateItemInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	UpdateExpression          string                    `json:"UpdateExpression"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

func (d *DynamoDBServer) updateItem(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var in updateItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	keyAttr, ok := in.Key["key"]
	if !ok {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	key := []byte(keyAttr.S)

	if err := d.validateCondition(r.Context(), in.ConditionExpression, in.ExpressionAttributeNames, key); err != nil {
		w.Header().Set("x-amzn-ErrorType", "ConditionalCheckFailedException")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	updExpr := replaceNames(in.UpdateExpression, in.ExpressionAttributeNames)
	parts := strings.SplitN(updExpr, "=", updateSplitCount)
	if len(parts) != updateSplitCount {
		http.Error(w, "invalid update expression", http.StatusBadRequest)
		return
	}
	valPlaceholder := strings.TrimSpace(parts[1])
	valAttr, ok := in.ExpressionAttributeValues[valPlaceholder]
	if !ok {
		http.Error(w, "missing value attribute", http.StatusBadRequest)
		return
	}

	elem := &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   key,
		Value: []byte(valAttr.S),
	}
	req := &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{elem},
	}
	if _, err = d.coordinator.Dispatch(r.Context(), req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	_, _ = w.Write([]byte("{}"))
}

func (d *DynamoDBServer) transactWriteItems(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	reqs, err := d.dynamoTranscoder.TransactWriteItemsToRequest(body)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err = d.dispatchTransactWriteItemsWithRetry(r.Context(), reqs); err != nil {
		writeDynamoError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	_, _ = w.Write([]byte("{}"))
}

func (d *DynamoDBServer) dispatchTransactWriteItemsWithRetry(ctx context.Context, reqs *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	backoff := transactRetryInitialBackoff
	var lastErr error
	startedAt := time.Now()

	for attempt := 0; attempt < transactRetryMaxAttempts; attempt++ {
		resp, err := d.coordinator.Dispatch(ctx, reqs)
		if err == nil {
			return resp, nil
		}
		lastErr = errors.WithStack(err)
		if !isRetryableTransactWriteError(err) {
			return nil, lastErr
		}
		if time.Since(startedAt) >= transactRetryMaxDuration {
			return nil, errors.Wrapf(lastErr, "transact write retry timeout after %s (attempts: %d)", transactRetryMaxDuration, attempt+1)
		}
		if err := waitTransactRetryBackoff(ctx, backoff); err != nil {
			if lastErr == nil {
				return nil, err
			}
			return nil, errors.Wrapf(err, "transact write retry canceled: %v", lastErr)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}

	return nil, lastErr
}

func isRetryableTransactWriteError(err error) bool {
	return errors.Is(err, store.ErrWriteConflict) || errors.Is(err, kv.ErrTxnLocked)
}

func waitTransactRetryBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-timer.C:
		return nil
	}
}

func nextTransactRetryBackoff(current time.Duration) time.Duration {
	if current >= transactRetryMaxBackoff {
		return transactRetryMaxBackoff
	}
	next := current * transactRetryBackoffFactor
	if next > transactRetryMaxBackoff {
		return transactRetryMaxBackoff
	}
	return next
}

func writeDynamoError(w http.ResponseWriter, status int, errorType string, message string) {
	if message == "" {
		message = http.StatusText(status)
	}

	resp := map[string]string{
		"message": message,
	}
	if errorType != "" {
		resp["__type"] = errorType
		w.Header().Set("x-amzn-ErrorType", errorType)
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func replaceNames(expr string, names map[string]string) string {
	for k, v := range names {
		expr = strings.ReplaceAll(expr, k, v)
	}
	return expr
}

func (d *DynamoDBServer) validateCondition(ctx context.Context, expr string, names map[string]string, key []byte) error {
	expr = replaceNames(expr, names)
	if expr == "" {
		return nil
	}
	readTS := snapshotTS(d.coordinator.Clock(), d.store)
	exists, err := d.store.ExistsAt(ctx, key, readTS)
	if err != nil {
		return errors.WithStack(err)
	}
	switch {
	case strings.HasPrefix(expr, "attribute_exists("):
		if !exists {
			return errors.New("conditional check failed")
		}
	case strings.HasPrefix(expr, "attribute_not_exists("):
		if exists {
			return errors.New("conditional check failed")
		}
	}
	return nil
}

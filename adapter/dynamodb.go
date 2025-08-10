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

const updateSplitCount = 2

type DynamoDBServer struct {
	listen           net.Listener
	store            store.ScanStore
	coordinator      kv.Coordinator
	dynamoTranscoder *dynamodbTranscoder
	httpServer       *http.Server
}

func NewDynamoDBServer(listen net.Listener, st store.ScanStore, coordinate *kv.Coordinate) *DynamoDBServer {
	return &DynamoDBServer{
		listen:           listen,
		store:            st,
		coordinator:      coordinate,
		dynamoTranscoder: newDynamoDBTranscoder(),
	}
}

func (d *DynamoDBServer) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", d.handle)
	d.httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
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
	if _, err = d.coordinator.Dispatch(reqs); err != nil {
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
	v, err := d.store.Get(r.Context(), []byte(keyAttr.S))
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

	req := &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{
			{
				Op:    kv.Put,
				Key:   key,
				Value: []byte(valAttr.S),
			},

	// Parse UpdateExpression for SET clause(s)
	setPrefix := "SET "
	setIdx := strings.Index(strings.ToUpper(updExpr), setPrefix)
	if setIdx == -1 {
		http.Error(w, "only SET clause is supported in update expression", http.StatusBadRequest)
		return
	}
	setExpr := updExpr[setIdx+len(setPrefix):]
	// Remove any trailing clauses (REMOVE, ADD, DELETE)
	for _, clause := range []string{" REMOVE ", " ADD ", " DELETE "} {
		if idx := strings.Index(strings.ToUpper(setExpr), clause); idx != -1 {
			setExpr = setExpr[:idx]
		}
	}
	assignments := strings.Split(setExpr, ",")
	elems := make([]*kv.Elem[kv.OP], 0, len(assignments))
	for _, assign := range assignments {
		parts := strings.SplitN(assign, "=", 2)
		if len(parts) != 2 {
			http.Error(w, "invalid assignment in update expression", http.StatusBadRequest)
			return
		}
		field := strings.TrimSpace(parts[0])
		valPlaceholder := strings.TrimSpace(parts[1])
		valAttr, ok := in.ExpressionAttributeValues[valPlaceholder]
		if !ok {
			http.Error(w, "missing value attribute: "+valPlaceholder, http.StatusBadRequest)
			return
		}
		// For this example, we only support updating the "value" field
		if field != "value" {
			http.Error(w, "only 'value' field can be updated", http.StatusBadRequest)
			return
		}
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   key,
			Value: []byte(valAttr.S),
		})
	}

	req := &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: elems,
	}
	if _, err = d.coordinator.Dispatch(req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	_, _ = w.Write([]byte("{}"))
}

func (d *DynamoDBServer) transactWriteItems(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reqs, err := d.dynamoTranscoder.TransactWriteItemsToRequest(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err = d.coordinator.Dispatch(reqs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	_, _ = w.Write([]byte("{}"))
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
	exists, err := d.store.Exists(ctx, key)
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

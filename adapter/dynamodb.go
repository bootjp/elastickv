package adapter

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	targetPrefix  = "DynamoDB_20120810."
	putItemTarget = targetPrefix + "PutItem"
	getItemTarget = targetPrefix + "GetItem"
)

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

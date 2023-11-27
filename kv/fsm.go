package kv

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/hashicorp/raft"

	pb "github.com/bootjp/elastickv/proto"
)

type kv struct {
	mtx sync.RWMutex
	m   map[*[]byte][]byte
}

var _ raft.FSM = &kv{}

func (f *kv) Apply(l *raft.Log) interface{} {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var req pb.PutRequest
	if err := proto.Unmarshal(l.Data, &req); err != nil {
		return err
	}

	f.mtx.Lock()
	defer f.mtx.Unlock()

	f.m[&req.Key] = req.Value

	return nil
}

var ErrNotImplemented = errors.New("not implemented")

func (f *kv) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, ErrNotImplemented
}

func (f *kv) Restore(r io.ReadCloser) error {
	return ErrNotImplemented
}

type snapshot struct {
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	return ErrNotImplemented
}

func (s *snapshot) Release() {
}

type rpcInterface struct {
	store *kv
	raft  *raft.Raft
}

var ErrRetrieable = errors.New("retrievable error")

func (r rpcInterface) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		return nil, ErrRetrieable
	}
	return &pb.PutResponse{
		CommitIndex: f.Index(),
	}, nil
}

func (r rpcInterface) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	r.store.mtx.RLock()
	defer r.store.mtx.RUnlock()
	return &pb.GetResponse{
		ReadAtIndex: r.raft.AppliedIndex(),
		Value:       r.store.m[&req.Key],
	}, nil
}

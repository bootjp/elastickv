package kv

import (
	"errors"
	"io"
	"log"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/hashicorp/raft"

	pb "github.com/bootjp/elastickv/proto"
)

type store struct {
	mtx sync.RWMutex
	m   map[*[]byte][]byte
}

func NewStore() *store {
	return &store{
		mtx: sync.RWMutex{},
		m:   make(map[*[]byte][]byte),
	}
}

var _ raft.FSM = &store{}

func (f *store) Apply(l *raft.Log) interface{} {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var req pb.PutRequest
	if err := proto.Unmarshal(l.Data, &req); err != nil {
		return err
	}

	f.m[&req.Key] = req.Value
	log.Println("Put", req.Key, req.Value)
	log.Println("map", f.m)

	return nil
}

var ErrNotImplemented = errors.New("not implemented")

func (f *store) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, ErrNotImplemented
}

func (f *store) Restore(r io.ReadCloser) error {
	return ErrNotImplemented
}

type snapshot struct {
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	return ErrNotImplemented
}

func (s *snapshot) Release() {
}

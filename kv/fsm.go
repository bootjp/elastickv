package kv

import (
	"context"

	"github.com/cockroachdb/errors"

	"io"
	"log/slog"
	"os"
	"sync"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
	"github.com/spaolacci/murmur3"
	"google.golang.org/protobuf/proto"
)

type store struct {
	mtx sync.RWMutex
	m   map[uint64][]byte
	log *slog.Logger
}

type Store interface {
	raft.FSM
}

// ignore lint
var _ = Store(&store{})

func NewStore() *store {
	return &store{
		mtx: sync.RWMutex{},
		m:   make(map[uint64][]byte),
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{})),
	}
}

var _ raft.FSM = &store{}

func (f *store) Apply(l *raft.Log) interface{} {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var req pb.PutRequest
	if err := proto.Unmarshal(l.Data, &req); err != nil {
		return errors.WithStack(err)
	}

	h := murmur3.New64()
	_, err := h.Write(req.Key)
	if err != nil {
		return errors.WithStack(err)
	}
	key := h.Sum64()

	f.m[key] = req.Value

	f.log.InfoContext(context.Background(), "applied raft log",
		slog.String("key", string(req.Key)),
		slog.String("value", string(req.Value)),
		slog.Uint64("hash", key),
	)

	return nil
}

var ErrNotImplemented = errors.New("not implemented")

func (f *store) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, ErrNotImplemented
}

func (f *store) Restore(_ io.ReadCloser) error {
	return ErrNotImplemented
}

type snapshot struct {
}

func (s *snapshot) Persist(_ raft.SnapshotSink) error {
	return ErrNotImplemented
}

func (s *snapshot) Release() {
}

package kv

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type kvFSM struct {
	mtx   sync.RWMutex
	store Store
	log   *slog.Logger
}

type FSM interface {
	raft.FSM
}

func NewKvFSM(store Store) FSM {
	return &kvFSM{
		mtx:   sync.RWMutex{},
		store: store,
		log:   slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{})),
	}
}

var _ FSM = &kvFSM{}
var _ raft.FSM = &kvFSM{}

var ErrUnknownRequestType = errors.New("unknown request type")

func (f *kvFSM) Apply(l *raft.Log) interface{} {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	ctx := context.TODO()

	req := pb.PutRequest{}
	if err := proto.Unmarshal(l.Data, &req); err != nil {
		return errors.WithStack(err)
	}

	f.log.InfoContext(ctx, "applied raft log", slog.String("type", "PutRequest"))
	return errors.WithStack(f.store.Put(ctx, req.Key, req.Value))
}

var ErrNotImplemented = errors.New("not implemented")

func (f *kvFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, ErrNotImplemented
}

func (f *kvFSM) Restore(_ io.ReadCloser) error {
	return ErrNotImplemented
}

type snapshot struct {
}

func (s *snapshot) Persist(_ raft.SnapshotSink) error {
	return ErrNotImplemented
}

func (s *snapshot) Release() {
}

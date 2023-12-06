package kv

import (
	"context"
	"fmt"
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

	req, err := f.Unmarshal(l.Data)

	if err != nil {
		return errors.WithStack(err)
	}

	switch v := req.(type) {
	case *pb.PutRequest:
		return errors.WithStack(f.store.Put(ctx, v.Key, v.Value))
	case *pb.DeleteRequest:
		return errors.WithStack(f.store.Delete(ctx, v.Key))
	case *pb.GetRequest:
		return errors.WithStack(ErrUnknownRequestType)
	default:
		return errors.WithStack(ErrUnknownRequestType)
	}
}

func (f *kvFSM) Unmarshal(b []byte) (proto.Message, error) {
	// todo パフォーマンス上の問題があるので、もっと良い方法を考える

	putReq := &pb.PutRequest{}
	if err := proto.Unmarshal(b, putReq); err == nil {
		fmt.Println("Unmarshaled as PutRequest:", putReq)
		return putReq, nil
	}

	delReq := &pb.DeleteRequest{}
	if err := proto.Unmarshal(b, delReq); err == nil {
		fmt.Println("Unmarshaled as DeleteRequest:", delReq)
		return delReq, nil
	}

	getReq := &pb.GetRequest{}
	if err := proto.Unmarshal(b, getReq); err == nil {
		fmt.Println("Unmarshaled as GetRequest:", getReq)
		return getReq, nil
	}

	return nil, errors.WithStack(ErrUnknownRequestType)
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

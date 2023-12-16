package kv

import (
	"context"
	"io"
	"log/slog"
	"os"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type kvFSM struct {
	store Store
	log   *slog.Logger
}

type FSM interface {
	raft.FSM
}

func NewKvFSM(store Store) FSM {
	return &kvFSM{
		store: store,
		log:   slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{})),
	}
}

var _ FSM = (*kvFSM)(nil)
var _ raft.FSM = (*kvFSM)(nil)

var ErrUnknownRequestType = errors.New("unknown request type")

func (f *kvFSM) Apply(l *raft.Log) interface{} {
	ctx := context.TODO()

	m := &pb.Mutation{}

	err := proto.Unmarshal(l.Data, m)
	if err != nil {
		return errors.WithStack(err)
	}

	switch m.Op {
	case pb.Mutation_PUT:
		err := f.store.Put(ctx, m.Key, m.Value)
		if err != nil {
			return errors.WithStack(err)
		}
	case pb.Mutation_DELETE:
		err := f.store.Delete(ctx, m.Key)
		if err != nil {
			return errors.WithStack(err)
		}
	case pb.Mutation_UNKNOWN,
		pb.Mutation_GET: // GETはレプリケーションされない
		return errors.WithStack(ErrUnknownRequestType)
	}

	return errors.WithStack(ErrUnknownRequestType)
}

func (f *kvFSM) Unmarshal(b []byte) (proto.Message, error) {
	// todo パフォーマンス上の問題があるので、もっと良い方法を考える

	putReq := &pb.PutRequest{}
	if err := proto.Unmarshal(b, putReq); err == nil {
		return putReq, nil
	}

	delReq := &pb.DeleteRequest{}
	if err := proto.Unmarshal(b, delReq); err == nil {
		return delReq, nil
	}

	getReq := &pb.GetRequest{}
	if err := proto.Unmarshal(b, getReq); err == nil {
		return getReq, nil
	}

	return nil, errors.WithStack(ErrUnknownRequestType)
}

var ErrNotImplemented = errors.New("not implemented")

func (f *kvFSM) Snapshot() (raft.FSMSnapshot, error) {
	buf, err := f.store.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &kvFSMSnapshot{
		buf,
	}, nil
}

func (f *kvFSM) Restore(r io.ReadCloser) error {
	defer r.Close()

	return errors.WithStack(f.store.Restore(r))
}

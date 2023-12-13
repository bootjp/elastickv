package kv

import (
	"bytes"
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

var _ FSM = &kvFSM{}
var _ raft.FSM = &kvFSM{}

var ErrUnknownRequestType = errors.New("unknown request type")

func (f *kvFSM) Apply(l *raft.Log) interface{} {
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

	return &kvFSMSnapshotSink{
		writer: buf,
	}, nil
}

func (f *kvFSM) Restore(r io.ReadCloser) error {
	defer r.Close()

	return errors.WithStack(f.store.Restore(r))
}

var _ raft.FSMSnapshot = &kvFSMSnapshotSink{}

type kvFSMSnapshotSink struct {
	writer io.ReadWriter
}

type FSMSnapshotSink struct {
	io.WriteCloser

	buf bytes.Buffer
}

func (f *kvFSMSnapshotSink) ID() string {
	return ""
}

func (f *kvFSMSnapshotSink) Cancel() error {
	return nil
}

func (f *kvFSMSnapshotSink) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	_, err := io.Copy(sink, f.writer)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (f *kvFSMSnapshotSink) Release() {
}

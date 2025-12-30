package kv

import (
	"context"
	"io"
	"log/slog"
	"os"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type kvFSM struct {
	store store.MVCCStore
	log   *slog.Logger
}

type FSM interface {
	raft.FSM
}

func NewKvFSM(store store.MVCCStore) FSM {
	return &kvFSM{
		store: store,
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}
}

var _ FSM = (*kvFSM)(nil)
var _ raft.FSM = (*kvFSM)(nil)

var ErrUnknownRequestType = errors.New("unknown request type")

func (f *kvFSM) Apply(l *raft.Log) interface{} {
	ctx := context.TODO()

	r := &pb.Request{}
	err := proto.Unmarshal(l.Data, r)
	if err != nil {
		return errors.WithStack(err)
	}

	err = f.handleRequest(ctx, r, r.Ts)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (f *kvFSM) handleRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	switch {
	case r.IsTxn:
		return f.handleTxnRequest(ctx, r, commitTS)
	default:
		return f.handleRawRequest(ctx, r, commitTS)
	}
}

func (f *kvFSM) handleRawRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	muts, err := toStoreMutations(r.Mutations)
	if err != nil {
		return errors.WithStack(err)
	}
	// Raw requests always commit against the latest state; use commitTS as both
	// the validation snapshot and the commit timestamp.
	return errors.WithStack(f.store.ApplyMutations(ctx, muts, commitTS, commitTS))
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

func (f *kvFSM) handleTxnRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	switch r.Phase {
	case pb.Phase_PREPARE:
		return f.handlePrepareRequest(ctx, r)
	case pb.Phase_COMMIT:
		return f.handleCommitRequest(ctx, r, commitTS)
	case pb.Phase_ABORT:
		return f.handleAbortRequest(ctx, r)
	case pb.Phase_NONE:
		// not reached
		return errors.WithStack(ErrUnknownRequestType)
	default:
		return errors.WithStack(ErrUnknownRequestType)
	}
}

func (f *kvFSM) validateConflicts(ctx context.Context, muts []*pb.Mutation, startTS uint64) error {
	// Debug guard only: real OCC runs at the leader/storage layer, so conflicts
	// should already be resolved before log application. Keep this stub to make
	// any unexpected violations visible during development.
	return nil
}

func (f *kvFSM) handlePrepareRequest(ctx context.Context, r *pb.Request) error {
	err := f.validateConflicts(ctx, r.Mutations, r.Ts)
	f.log.InfoContext(ctx, "handlePrepareRequest finish")
	return errors.WithStack(err)
}

func (f *kvFSM) handleCommitRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	muts, err := toStoreMutations(r.Mutations)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := f.validateConflicts(ctx, r.Mutations, r.Ts); err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(f.store.ApplyMutations(ctx, muts, r.Ts, commitTS))
}

func (f *kvFSM) handleAbortRequest(_ context.Context, _ *pb.Request) error {
	// OCC does not rely on locks; abort is a no-op.
	return nil
}

func toStoreMutations(muts []*pb.Mutation) ([]*store.KVPairMutation, error) {
	out := make([]*store.KVPairMutation, 0, len(muts))
	for _, mut := range muts {
		switch mut.Op {
		case pb.Op_PUT:
			out = append(out, &store.KVPairMutation{
				Op:    store.OpTypePut,
				Key:   mut.Key,
				Value: mut.Value,
			})
		case pb.Op_DEL:
			out = append(out, &store.KVPairMutation{
				Op:  store.OpTypeDelete,
				Key: mut.Key,
			})
		default:
			return nil, ErrUnknownRequestType
		}
	}
	return out, nil
}

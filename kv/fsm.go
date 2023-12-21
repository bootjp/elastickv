package kv

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"os"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type kvFSM struct {
	store     Store
	lockStore Store
	log       *slog.Logger
}

type FSM interface {
	raft.FSM
}

func NewKvFSM(store Store, lockStore Store) FSM {
	return &kvFSM{
		store:     store,
		lockStore: lockStore,
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
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

	err = f.handleRequest(ctx, r)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (f *kvFSM) handleRequest(ctx context.Context, r *pb.Request) error {
	switch {
	case r.IsTxn:
		return f.handleTxnRequest(ctx, r)
	default:
		return f.handleRawRequest(ctx, r)
	}
}

func (f *kvFSM) handleRawRequest(ctx context.Context, r *pb.Request) error {
	for _, mut := range r.Mutations {
		switch mut.Op {
		case pb.Op_PUT:
			err := f.store.Put(ctx, mut.Key, mut.Value)
			if err != nil {
				return errors.WithStack(err)
			}
		case pb.Op_DEL:
			err := f.store.Delete(ctx, mut.Key)
			if err != nil {
				return errors.WithStack(err)
			}
		default:
			return errors.WithStack(ErrUnknownRequestType)
		}
	}

	return nil
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

func (f *kvFSM) handleTxnRequest(ctx context.Context, r *pb.Request) error {
	switch r.Phase {
	case pb.Phase_PREPARE:
		return f.handlePrepareRequest(ctx, r)
	case pb.Phase_COMMIT:
		return f.handleCommitRequest(ctx, r)
	case pb.Phase_ABORT:
		return f.handleAbortRequest(ctx, r)
	case pb.Phase_NONE:
		// not reached
		return errors.WithStack(ErrUnknownRequestType)
	default:
		return errors.WithStack(ErrUnknownRequestType)
	}
}

var ErrKeyAlreadyLocked = errors.New("key already locked")

func (f *kvFSM) handlePrepareRequest(ctx context.Context, r *pb.Request) error {
	err := f.lockStore.Txn(ctx, func(ctx context.Context, txn Txn) error {
		for _, mut := range r.Mutations {
			if exist, _ := txn.Exists(ctx, mut.Key); exist {
				return errors.WithStack(ErrKeyAlreadyLocked)
			}
			//nolint:gomnd
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, r.Ts)
			err := txn.Put(ctx, mut.Key, b)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	})
	f.log.InfoContext(ctx, "handlePrepareRequest finish")

	return errors.WithStack(err)
}

func (f *kvFSM) handleCommitRequest(ctx context.Context, r *pb.Request) error {
	err := f.lockStore.Txn(ctx, func(ctx context.Context, lockTxn Txn) error {
		err := f.store.Txn(ctx, func(ctx context.Context, txn Txn) error {
			// commit
			for _, mut := range r.Mutations {
				switch mut.Op {
				case pb.Op_PUT:
					err := txn.Put(ctx, mut.Key, mut.Value)
					if err != nil {
						return errors.WithStack(err)
					}
				case pb.Op_DEL:
					err := txn.Delete(ctx, mut.Key)
					if err != nil {
						return errors.WithStack(err)
					}
				}
			}
			return nil
		})

		return errors.WithStack(err)
	})

	if err != nil {
		return errors.WithStack(err)
	}

	// delete lock
	err = f.lockStore.Txn(ctx, func(ctx context.Context, txn Txn) error {
		for _, mut := range r.Mutations {
			err := txn.Delete(ctx, mut.Key)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	})

	return errors.WithStack(err)
}

func (f *kvFSM) handleAbortRequest(ctx context.Context, r *pb.Request) error {
	err := f.lockStore.Txn(ctx, func(ctx context.Context, txn Txn) error {
		for _, mut := range r.Mutations {
			err := txn.Delete(ctx, mut.Key)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		return nil
	})

	return errors.WithStack(err)
}

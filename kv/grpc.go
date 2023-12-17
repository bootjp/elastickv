package kv

import (
	"context"
	"log/slog"
	"os"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/spaolacci/murmur3"
	"google.golang.org/protobuf/proto"
)

var _ pb.RawKVServer = (*GRPCServer)(nil)
var _ pb.TransactionalKVServer = (*GRPCServer)(nil)

type GRPCServer struct {
	pb.UnimplementedRawKVServer
	pb.UnimplementedTransactionalKVServer
	fsm     FSM
	store   Store
	raft    *raft.Raft
	log     *slog.Logger
	convert Convert
}

var ErrRetryable = errors.New("retryable error")

func NewGRPCServer(fsm FSM, store Store, raft *raft.Raft) *GRPCServer {
	return &GRPCServer{
		fsm:   fsm,
		store: store,
		raft:  raft,
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
		convert: Convert{},
	}
}

func (r GRPCServer) RawGet(ctx context.Context, req *pb.RawGetRequest) (*pb.RawGetResponse, error) {
	v, err := r.store.Get(ctx, req.Key)
	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return &pb.RawGetResponse{
				ReadAtIndex: r.raft.AppliedIndex(),
				Value:       nil,
			}, nil
		default:
			return nil, errors.WithStack(err)
		}
	}

	r.log.InfoContext(ctx, "Get",
		slog.String("key", string(req.Key)),
		slog.String("value", string(v)))

	return &pb.RawGetResponse{
		ReadAtIndex: r.raft.AppliedIndex(),
		Value:       v,
	}, nil
}

func (r GRPCServer) RawPut(ctx context.Context, req *pb.RawPutRequest) (*pb.RawPutResponse, error) {
	m, err := r.convert.RawPutToRequest(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	b, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		r.log.ErrorContext(ctx, "failed to apply raft log", slog.String("error", err.Error()))
		return nil, ErrRetryable
	}

	return &pb.RawPutResponse{
		CommitIndex: f.Index(),
		Success:     true,
	}, nil
}

func (r GRPCServer) RawDelete(ctx context.Context, req *pb.RawDeleteRequest) (*pb.RawDeleteResponse, error) {
	m, err := r.convert.RawDeleteToRequest(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		r.log.ErrorContext(ctx, "failed to apply raft log", slog.String("error", err.Error()))
		return nil, ErrRetryable
	}

	return &pb.RawDeleteResponse{
		CommitIndex: f.Index(),
	}, nil
}

func (r GRPCServer) PreWrite(ctx context.Context, req *pb.PreWriteRequest) (*pb.PreCommitResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		r.log.ErrorContext(ctx, "failed to apply raft log", slog.String("error", err.Error()))
		return nil, ErrRetryable
	}

	return &pb.PreCommitResponse{
		//CommitIndex: f.Index(),
	}, nil
}

func (r GRPCServer) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		r.log.ErrorContext(ctx, "failed to apply raft log", slog.String("error", err.Error()))
		return nil, ErrRetryable
	}

	return &pb.CommitResponse{
		//CommitIndex: f.Index(),
	}, nil
}

func (r GRPCServer) Rollback(ctx context.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		r.log.ErrorContext(ctx, "failed to apply raft log", slog.String("error", err.Error()))
		return nil, ErrRetryable
	}

	return &pb.RollbackResponse{
		//CommitIndex: f.Index(),
	}, nil
}

func (r GRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		r.log.ErrorContext(ctx, "failed to apply raft log", slog.String("error", err.Error()))
		return nil, ErrRetryable
	}

	return &pb.PutResponse{
		//CommitIndex: f.Index(),
	}, nil
}

func (r GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	h := murmur3.New64()
	if _, err := h.Write(req.Key); err != nil {
		return nil, errors.WithStack(err)
	}

	v, err := r.store.Get(ctx, req.Key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.log.InfoContext(ctx, "Get",
		slog.String("key", string(req.Key)),
		slog.String("value", string(v)))

	return &pb.GetResponse{
		//ReadAtIndex: r.raft.AppliedIndex(),
		Value: v,
	}, nil
}

func (r GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		r.log.ErrorContext(ctx, "failed to apply raft log", slog.String("error", err.Error()))
		return nil, ErrRetryable
	}

	return &pb.DeleteResponse{
		//CommitIndex: f.Index(),
	}, nil
}

func (r GRPCServer) Scan(_ context.Context, _ *pb.ScanRequest) (*pb.ScanResponse, error) {
	return nil, nil
}

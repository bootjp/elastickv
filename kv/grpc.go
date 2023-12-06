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

var _ pb.RawKVServer = &GRPCServer{}

type GRPCServer struct {
	pb.UnimplementedRawKVServer
	fsm   FSM
	store Store
	raft  *raft.Raft
	log   *slog.Logger
}

var ErrRetryable = errors.New("retryable error")

func NewGRPCServer(fsm FSM, store Store, raft *raft.Raft) *GRPCServer {
	return &GRPCServer{
		fsm:   fsm,
		store: store,
		raft:  raft,
		log:   slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{})),
	}
}

func (r GRPCServer) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		r.log.ErrorContext(context.Background(), "failed to apply raft log", slog.String("error", err.Error()))
		return nil, ErrRetryable
	}

	return &pb.PutResponse{
		CommitIndex: f.Index(),
		Success:     true,
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
		ReadAtIndex: r.raft.AppliedIndex(),
		Value:       v,
	}, nil
}

func (r GRPCServer) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		r.log.ErrorContext(context.Background(), "failed to apply raft log", slog.String("error", err.Error()))
		return nil, ErrRetryable
	}

	return &pb.DeleteResponse{
		CommitIndex: f.Index(),
	}, nil
}

package kv

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
	"github.com/spaolacci/murmur3"
	"google.golang.org/protobuf/proto"
)

var _ pb.RawKVServer = &GRPCServer{}

type GRPCServer struct {
	pb.UnimplementedRawKVServer

	store *store
	raft  *raft.Raft
	log   *slog.Logger
}

var ErrRetryable = errors.New("retryable error")

func NewGRPCServer(store *store, raft *raft.Raft) *GRPCServer {
	return &GRPCServer{
		store: store,
		raft:  raft,
		log:   slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{})),
	}
}

func (r GRPCServer) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		return nil, ErrRetryable
	}

	return &pb.PutResponse{
		CommitIndex: f.Index(),
	}, nil
}

func (r GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	r.store.mtx.RLock()
	defer r.store.mtx.RUnlock()

	h := murmur3.New64()
	_, err := h.Write(req.Key)
	if err != nil {
		return nil, err
	}

	key := h.Sum64()
	r.log.InfoContext(ctx, "Get", slog.String("key", string(req.Key)), slog.Uint64("hash", key), slog.String("value", string(r.store.m[key])))

	return &pb.GetResponse{
		ReadAtIndex: r.raft.AppliedIndex(),
		Value:       r.store.m[key],
	}, nil
}

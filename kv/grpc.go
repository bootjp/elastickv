package kv

import (
	"context"
	"errors"
	"log"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

var _ pb.RawKVServer = &GRPCServer{}

type GRPCServer struct {
	pb.UnimplementedRawKVServer

	store *store
	raft  *raft.Raft
}

var ErrRetrieable = errors.New("retrievable error")

func NewGRPCServer(store *store, raft *raft.Raft) *GRPCServer {
	return &GRPCServer{
		store: store,
		raft:  raft,
	}
}

func (r GRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	f := r.raft.Apply(b, time.Second)
	if err := f.Error(); err != nil {
		return nil, ErrRetrieable
	}

	return &pb.PutResponse{
		CommitIndex: f.Index(),
	}, nil
}

func (r GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	r.store.mtx.RLock()
	defer r.store.mtx.RUnlock()
	log.Println("Get", req.Key, r.store.m[&req.Key])
	//log.Println("map", r.store.m)

	return &pb.GetResponse{
		ReadAtIndex: r.raft.AppliedIndex(),
		Value:       r.store.m[&req.Key],
	}, nil
}

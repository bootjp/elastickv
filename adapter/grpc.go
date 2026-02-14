package adapter

import (
	"context"
	"log/slog"
	"os"

	"github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/spaolacci/murmur3"
)

var _ pb.RawKVServer = (*GRPCServer)(nil)
var _ pb.TransactionalKVServer = (*GRPCServer)(nil)

type GRPCServer struct {
	log            *slog.Logger
	grpcTranscoder *grpcTranscoder
	coordinator    kv.Coordinator
	store          store.MVCCStore

	pb.UnimplementedRawKVServer
	pb.UnimplementedTransactionalKVServer
}

func NewGRPCServer(store store.MVCCStore, coordinate kv.Coordinator) *GRPCServer {
	return &GRPCServer{
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
		grpcTranscoder: newGrpcGrpcTranscoder(),
		coordinator:    coordinate,
		store:          store,
	}
}

func (r *GRPCServer) Close() error {
	return nil
}

func (r *GRPCServer) clock() *kv.HLC {
	if r == nil || r.coordinator == nil {
		return nil
	}
	return r.coordinator.Clock()
}

func (r *GRPCServer) RawGet(ctx context.Context, req *pb.RawGetRequest) (*pb.RawGetResponse, error) {
	readTS := req.GetTs()
	if readTS == 0 {
		readTS = snapshotTS(r.clock(), r.store)
	}

	v, err := r.store.GetAt(ctx, req.Key, readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return &pb.RawGetResponse{Value: nil}, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.log.InfoContext(ctx, "Get",
		slog.String("key", string(req.Key)),
		slog.String("value", string(v)))

	return &pb.RawGetResponse{Value: v}, nil
}

func (r *GRPCServer) RawLatestCommitTS(ctx context.Context, req *pb.RawLatestCommitTSRequest) (*pb.RawLatestCommitTSResponse, error) {
	key := req.GetKey()
	if len(key) == 0 {
		return nil, errors.WithStack(kv.ErrInvalidRequest)
	}

	ts, exists, err := r.store.LatestCommitTS(ctx, key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.RawLatestCommitTSResponse{
		Ts:     ts,
		Exists: exists,
	}, nil
}

func (r *GRPCServer) RawScanAt(ctx context.Context, req *pb.RawScanAtRequest) (*pb.RawScanAtResponse, error) {
	limit64 := req.GetLimit()
	limit, err := rawScanLimit(limit64)
	if err != nil {
		return &pb.RawScanAtResponse{Kv: nil}, err
	}

	readTS := req.GetTs()
	if readTS == 0 {
		readTS = snapshotTS(r.clock(), r.store)
	}

	res, err := r.store.ScanAt(ctx, req.StartKey, req.EndKey, limit, readTS)
	if err != nil {
		return &pb.RawScanAtResponse{Kv: nil}, errors.WithStack(err)
	}

	return &pb.RawScanAtResponse{Kv: rawKvPairs(res)}, nil
}

func rawScanLimit(limit64 int64) (int, error) {
	if limit64 < 0 {
		return 0, errors.WithStack(kv.ErrInvalidRequest)
	}
	maxInt64 := int64(^uint(0) >> 1)
	if limit64 > maxInt64 {
		return 0, errors.WithStack(internal.ErrIntOverflow)
	}
	return int(limit64), nil
}

func rawKvPairs(res []*store.KVPair) []*pb.RawKvPair {
	out := make([]*pb.RawKvPair, 0, len(res))
	for _, kvp := range res {
		if kvp == nil {
			continue
		}
		out = append(out, &pb.RawKvPair{
			Key:   kvp.Key,
			Value: kvp.Value,
		})
	}
	return out
}

func (r *GRPCServer) RawPut(ctx context.Context, req *pb.RawPutRequest) (*pb.RawPutResponse, error) {
	m, err := r.grpcTranscoder.RawPutToRequest(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	res, err := r.coordinator.Dispatch(ctx, m)
	if err != nil {
		return &pb.RawPutResponse{
			CommitIndex: uint64(0),
			Success:     false,
		}, errors.WithStack(err)
	}

	return &pb.RawPutResponse{
		CommitIndex: res.CommitIndex,
		Success:     true,
	}, nil
}

func (r *GRPCServer) RawDelete(ctx context.Context, req *pb.RawDeleteRequest) (*pb.RawDeleteResponse, error) {
	m, err := r.grpcTranscoder.RawDeleteToRequest(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	res, err := r.coordinator.Dispatch(ctx, m)
	if err != nil {
		return &pb.RawDeleteResponse{
			CommitIndex: uint64(0),
			Success:     false,
		}, errors.WithStack(err)
	}

	return &pb.RawDeleteResponse{
		CommitIndex: res.CommitIndex,
		Success:     true,
	}, nil
}

func (r *GRPCServer) PreWrite(ctx context.Context, req *pb.PreWriteRequest) (*pb.PreCommitResponse, error) {
	return nil, kv.ErrNotImplemented
}

func (r *GRPCServer) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	return nil, kv.ErrNotImplemented
}

func (r *GRPCServer) Rollback(ctx context.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	return nil, kv.ErrNotImplemented
}

func (r *GRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	reqs, err := r.grpcTranscoder.TransactionalPutToRequests(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.log.InfoContext(ctx, "Put", slog.Any("reqs", reqs))

	res, err := r.coordinator.Dispatch(ctx, reqs)
	if err != nil {
		return &pb.PutResponse{
			CommitIndex: uint64(0),
		}, errors.WithStack(err)
	}

	return &pb.PutResponse{
		CommitIndex: res.CommitIndex,
		Success:     true,
	}, nil
}

func (r *GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	h := murmur3.New64()
	if _, err := h.Write(req.Key); err != nil {
		return nil, errors.WithStack(err)
	}

	readTS := snapshotTS(r.clock(), r.store)
	v, err := r.store.GetAt(ctx, req.Key, readTS)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrKeyNotFound):
			return &pb.GetResponse{Value: nil}, nil
		default:
			return nil, errors.WithStack(err)
		}
	}

	r.log.InfoContext(ctx, "Get",
		slog.String("key", string(req.Key)),
		slog.String("value", string(v)))

	return &pb.GetResponse{
		Value: v,
	}, nil
}

func (r *GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	reqs, err := r.grpcTranscoder.TransactionalDeleteToRequests(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.log.InfoContext(ctx, "Delete", slog.Any("reqs", reqs))

	res, err := r.coordinator.Dispatch(ctx, reqs)
	if err != nil {
		return &pb.DeleteResponse{
			CommitIndex: uint64(0),
		}, errors.WithStack(err)
	}

	return &pb.DeleteResponse{
		CommitIndex: res.CommitIndex,
		Success:     true,
	}, nil
}

func (r *GRPCServer) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	limit, err := internal.Uint64ToInt(req.Limit)
	if err != nil {
		return &pb.ScanResponse{
			Kv: nil,
		}, errors.WithStack(err)
	}
	readTS := snapshotTS(r.clock(), r.store)
	res, err := r.store.ScanAt(ctx, req.StartKey, req.EndKey, limit, readTS)
	if err != nil {
		return &pb.ScanResponse{
			Kv: nil,
		}, errors.WithStack(err)
	}

	r.log.InfoContext(ctx, "Scan",
		slog.String("startKey", string(req.StartKey)),
		slog.String("endKey", string(req.EndKey)),
		slog.Uint64("limit", req.Limit),
	)

	var kvs []*pb.Kv
	for _, v := range res {
		kvs = append(kvs, &pb.Kv{
			Key:   v.Key,
			Value: v.Value,
		})
	}

	return &pb.ScanResponse{
		Kv: kvs,
	}, nil
}

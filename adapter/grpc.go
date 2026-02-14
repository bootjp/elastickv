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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func (r GRPCServer) RawGet(ctx context.Context, req *pb.RawGetRequest) (*pb.RawGetResponse, error) {
	readTS := req.GetTs()
	if readTS == 0 {
		readTS = snapshotTS(r.coordinator.Clock(), r.store)
	}

	if r.coordinator.IsLeaderForKey(req.Key) {
		v, err := r.store.GetAt(ctx, req.Key, readTS)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrKeyNotFound):
				return &pb.RawGetResponse{
					Value: nil,
				}, nil
			default:
				return nil, errors.WithStack(err)
			}
		}
		r.log.InfoContext(ctx, "Get",
			slog.String("key", string(req.Key)),
			slog.String("value", string(v)))

		return &pb.RawGetResponse{
			Value: v,
		}, nil
	}

	v, err := r.tryLeaderGet(req.Key)
	if err != nil {
		return &pb.RawGetResponse{
			Value: nil,
		}, err
	}

	r.log.InfoContext(ctx, "Get",
		slog.String("key", string(req.Key)),
		slog.String("value", string(v)))

	return &pb.RawGetResponse{
		Value: v,
	}, nil
}

func (r GRPCServer) RawLatestCommitTS(ctx context.Context, req *pb.RawLatestCommitTSRequest) (*pb.RawLatestCommitTSResponse, error) {
	key := req.GetKey()
	if len(key) == 0 {
		return nil, errors.WithStack(kv.ErrInvalidRequest)
	}

	if r.coordinator.IsLeaderForKey(key) {
		// Ensure we are still leader before serving from local state.
		if err := r.coordinator.VerifyLeaderForKey(key); err != nil {
			return nil, errors.WithStack(err)
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

	ts, exists, err := r.tryLeaderLatestCommitTS(ctx, key)
	if err != nil {
		return &pb.RawLatestCommitTSResponse{
			Ts:     0,
			Exists: false,
		}, err
	}

	return &pb.RawLatestCommitTSResponse{
		Ts:     ts,
		Exists: exists,
	}, nil
}

func (r GRPCServer) tryLeaderGet(key []byte) ([]byte, error) {
	addr := r.coordinator.RaftLeaderForKey(key)
	if addr == "" {
		return nil, ErrLeaderNotFound
	}

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer conn.Close()

	cli := pb.NewRawKVClient(conn)
	ts := snapshotTS(r.coordinator.Clock(), r.store)
	resp, err := cli.RawGet(context.Background(), &pb.RawGetRequest{Key: key, Ts: ts})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return resp.Value, nil
}

func (r GRPCServer) tryLeaderLatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	addr := r.coordinator.RaftLeaderForKey(key)
	if addr == "" {
		return 0, false, ErrLeaderNotFound
	}

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	defer conn.Close()

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: key})
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	return resp.Ts, resp.Exists, nil
}

func (r GRPCServer) RawPut(_ context.Context, req *pb.RawPutRequest) (*pb.RawPutResponse, error) {
	m, err := r.grpcTranscoder.RawPutToRequest(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	res, err := r.coordinator.Dispatch(m)
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

func (r GRPCServer) RawDelete(ctx context.Context, req *pb.RawDeleteRequest) (*pb.RawDeleteResponse, error) {
	m, err := r.grpcTranscoder.RawDeleteToRequest(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	res, err := r.coordinator.Dispatch(m)
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

func (r GRPCServer) PreWrite(ctx context.Context, req *pb.PreWriteRequest) (*pb.PreCommitResponse, error) {
	return nil, kv.ErrNotImplemented
}

func (r GRPCServer) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	return nil, kv.ErrNotImplemented
}

func (r GRPCServer) Rollback(ctx context.Context, req *pb.RollbackRequest) (*pb.RollbackResponse, error) {
	return nil, kv.ErrNotImplemented
}

func (r GRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	reqs, err := r.grpcTranscoder.TransactionalPutToRequests(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.log.InfoContext(ctx, "Put", slog.Any("reqs", reqs))

	res, err := r.coordinator.Dispatch(reqs)
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

func (r GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	h := murmur3.New64()
	if _, err := h.Write(req.Key); err != nil {
		return nil, errors.WithStack(err)
	}

	readTS := snapshotTS(r.coordinator.Clock(), r.store)
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

func (r GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	reqs, err := r.grpcTranscoder.TransactionalDeleteToRequests(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.log.InfoContext(ctx, "Delete", slog.Any("reqs", reqs))

	res, err := r.coordinator.Dispatch(reqs)
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

func (r GRPCServer) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	limit, err := internal.Uint64ToInt(req.Limit)
	if err != nil {
		return &pb.ScanResponse{
			Kv: nil,
		}, errors.WithStack(err)
	}
	readTS := snapshotTS(r.coordinator.Clock(), r.store)
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

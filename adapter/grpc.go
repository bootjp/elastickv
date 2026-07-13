package adapter

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/spaolacci/murmur3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ pb.RawKVServer = (*GRPCServer)(nil)
var _ pb.TransactionalKVServer = (*GRPCServer)(nil)

type GRPCServer struct {
	log            *slog.Logger
	grpcTranscoder *grpcTranscoder
	coordinator    kv.Coordinator
	store          store.MVCCStore

	closeStore bool
	closeOnce  sync.Once
	closeErr   error

	pb.UnimplementedRawKVServer
	pb.UnimplementedTransactionalKVServer
}

type rawReadFenceGetter interface {
	GetAtWithReadFence(ctx context.Context, key []byte, ts uint64, groupID uint64, readRouteVersion uint64) ([]byte, error)
}

type rawReadFenceCommitTSReader interface {
	LatestCommitTSWithReadFence(ctx context.Context, key []byte, readRouteVersion uint64) (uint64, bool, error)
}

type rawReadFenceScanner interface {
	ScanAtWithReadFence(ctx context.Context, start []byte, end []byte, limit int, ts uint64, reverse bool, groupID uint64, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error)
}

type rawReadFenceVersioner interface {
	ReadRouteVersion() uint64
}

type GRPCServerOption func(*GRPCServer)

type rawGroupGetter interface {
	GetGroupAt(ctx context.Context, groupID uint64, key []byte, ts uint64) ([]byte, error)
}

type rawGroupScanner interface {
	ScanGroupAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error)
}

func WithCloseStore() GRPCServerOption {
	return func(s *GRPCServer) {
		s.closeStore = true
	}
}

func NewGRPCServer(store store.MVCCStore, coordinate kv.Coordinator, opts ...GRPCServerOption) *GRPCServer {
	s := &GRPCServer{
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
		grpcTranscoder: newGrpcGrpcTranscoder(),
		coordinator:    kv.WithKeyVizLabel(coordinate, keyviz.LabelRawKV),
		store:          store,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(s)
	}
	return s
}

func (r *GRPCServer) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if !r.closeStore || r.store == nil {
			return
		}
		if err := r.store.Close(); err != nil {
			r.closeErr = errors.WithStack(err)
		}
	})
	return r.closeErr
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
		readTS = globalSnapshotTS(ctx, r.clock(), r.store)
	}

	var v []byte
	var err error
	if fenceGetter, ok := r.store.(rawReadFenceGetter); ok {
		v, err = fenceGetter.GetAtWithReadFence(ctx, req.Key, readTS, req.GetGroupId(), r.readRouteVersion(req.GetReadRouteVersion()))
	} else if groupID := req.GetGroupId(); groupID != 0 {
		groupGetter, ok := r.store.(rawGroupGetter)
		if !ok {
			return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "raw get with explicit group requires a group-aware store"))
		}
		v, err = groupGetter.GetGroupAt(ctx, groupID, req.Key, readTS)
	} else {
		v, err = r.store.GetAt(ctx, req.Key, readTS)
	}
	if errors.Is(err, store.ErrKeyNotFound) {
		return &pb.RawGetResponse{Value: nil, Exists: false}, nil
	}
	if errors.Is(err, store.ErrReadTSCompacted) {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, store.ErrReadTSCompacted.Error()))
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.log.InfoContext(ctx, "Get",
		slog.String("key", string(req.Key)),
		slog.String("value", string(v)))

	return &pb.RawGetResponse{Value: v, Exists: true}, nil
}

func (r *GRPCServer) RawLatestCommitTS(ctx context.Context, req *pb.RawLatestCommitTSRequest) (*pb.RawLatestCommitTSResponse, error) {
	key := req.GetKey()
	if len(key) == 0 {
		// No key: return the store's global last-committed watermark.
		// Used by followers to obtain the leader's authoritative LastCommitTS
		// without per-key overhead, enabling consistent-read snapshot alignment.
		ts := r.store.LastCommitTS()
		return &pb.RawLatestCommitTSResponse{
			Ts:     ts,
			Exists: ts > 0,
		}, nil
	}

	var ts uint64
	var exists bool
	var err error
	if fenceReader, ok := r.store.(rawReadFenceCommitTSReader); ok {
		ts, exists, err = fenceReader.LatestCommitTSWithReadFence(ctx, key, r.readRouteVersion(req.GetReadRouteVersion()))
	} else {
		ts, exists, err = r.store.LatestCommitTS(ctx, key)
	}
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
		readTS = globalSnapshotTS(ctx, r.clock(), r.store)
	}

	res, err := r.rawScanAt(ctx, req, limit, readTS)
	if err != nil {
		if errors.Is(err, store.ErrReadTSCompacted) {
			return &pb.RawScanAtResponse{Kv: nil}, errors.WithStack(status.Error(codes.FailedPrecondition, store.ErrReadTSCompacted.Error()))
		}
		return &pb.RawScanAtResponse{Kv: nil}, errors.WithStack(err)
	}

	return &pb.RawScanAtResponse{Kv: rawKvPairs(res)}, nil
}

func (r *GRPCServer) rawScanAt(ctx context.Context, req *pb.RawScanAtRequest, limit int, readTS uint64) ([]*store.KVPair, error) {
	if fenceScanner, ok := r.store.(rawReadFenceScanner); ok {
		res, err := fenceScanner.ScanAtWithReadFence(ctx, req.StartKey, req.EndKey, limit, readTS, req.GetReverse(), req.GetGroupId(), r.readRouteVersion(req.GetReadRouteVersion()), req.GetRouteStart(), req.GetRouteEnd())
		return res, errors.WithStack(err)
	}
	if groupID := req.GetGroupId(); groupID != 0 {
		if req.GetReverse() {
			return nil, errors.WithStack(status.Error(codes.InvalidArgument, "raw scan with explicit group does not support reverse scans"))
		}
		groupScanner, ok := r.store.(rawGroupScanner)
		if !ok {
			return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "raw scan with explicit group requires a group-aware store"))
		}
		res, err := groupScanner.ScanGroupAt(ctx, groupID, req.StartKey, req.EndKey, limit, readTS)
		return res, errors.WithStack(err)
	}
	if req.GetReverse() {
		res, err := r.store.ReverseScanAt(ctx, req.StartKey, req.EndKey, limit, readTS)
		return res, errors.WithStack(err)
	}
	res, err := r.store.ScanAt(ctx, req.StartKey, req.EndKey, limit, readTS)
	return res, errors.WithStack(err)
}

func (r *GRPCServer) readRouteVersion(requested uint64) uint64 {
	if requested != 0 {
		return requested
	}
	versioner, ok := r.store.(rawReadFenceVersioner)
	if !ok {
		return 0
	}
	return versioner.ReadRouteVersion()
}

func rawScanLimit(limit64 int64) (int, error) {
	if limit64 < 0 {
		return 0, errors.WithStack(kv.ErrInvalidRequest)
	}
	maxInt64 := int64(^uint(0) >> 1)
	if limit64 > maxInt64 {
		return 0, errors.WithStack(internal.ErrIntOverflow)
	}
	return grpcScanLimit(int(limit64))
}

// maxGRPCScanLimit caps the number of results per RawScanAt call.
// It is set to MaxDeltaScanLimit+1 so that aggregateLenDeltas can request one
// extra item beyond the documented delta limit to distinguish "exactly
// MaxDeltaScanLimit results" from "more than MaxDeltaScanLimit results".
const maxGRPCScanLimit = store.MaxDeltaScanLimit + 1

func grpcScanLimit(limit int) (int, error) {
	if limit < 0 {
		return 0, errors.WithStack(kv.ErrInvalidRequest)
	}
	if limit > maxGRPCScanLimit {
		return 0, errors.WithStack(kv.ErrInvalidRequest)
	}
	return limit, nil
}

func rawKvPairs(res []*store.KVPair) []*pb.RawKVPair {
	out := make([]*pb.RawKVPair, 0, len(res))
	for _, kvp := range res {
		if kvp == nil {
			continue
		}
		out = append(out, &pb.RawKVPair{
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

	readTS := globalSnapshotTS(ctx, r.clock(), r.store)
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
	limit, err = grpcScanLimit(limit)
	if err != nil {
		return &pb.ScanResponse{
			Kv: nil,
		}, err
	}
	readTS := globalSnapshotTS(ctx, r.clock(), r.store)
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

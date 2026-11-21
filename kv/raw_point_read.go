package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

func readRawPoint(ctx context.Context, conn grpc.ClientConnInterface, req *pb.RawGetRequest) (store.VersionedValue, error) {
	resp, err := pb.NewRawKVClient(conn).RawGet(ctx, req)
	if err != nil {
		return store.VersionedValue{}, errors.WithStack(err)
	}
	if req.NeedCommitTs {
		return decodeRawPointVersion(resp)
	}
	// Older nodes may omit Exists when returning a value.
	if !resp.GetExists() && resp.GetValue() == nil {
		return store.VersionedValue{}, store.ErrKeyNotFound
	}
	return store.VersionedValue{Value: resp.GetValue()}, nil
}

func decodeRawPointVersion(resp *pb.RawGetResponse) (store.VersionedValue, error) {
	switch version := resp.GetVersion().(type) {
	case *pb.RawGetResponse_CommitTs:
		if version.CommitTs > 0 && resp.GetExists() {
			return store.VersionedValue{TS: version.CommitTs, Value: resp.GetValue()}, nil
		}
	case *pb.RawGetResponse_Missing:
		if version.Missing && !resp.GetExists() && len(resp.GetValue()) == 0 {
			return store.VersionedValue{}, store.ErrKeyNotFound
		}
	}
	return store.VersionedValue{}, store.ErrNotSupported
}

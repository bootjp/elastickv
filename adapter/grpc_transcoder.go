package adapter

import (
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
)

type grpcTranscoder struct {
}

func newGrpcGrpcTranscoder() *grpcTranscoder {
	return &grpcTranscoder{}
}

func (c *grpcTranscoder) RawPutToRequest(m *pb.RawPutRequest) (*kv.OperationGroup[kv.OP], error) {
	return &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{
			{
				Op:    kv.Put,
				Key:   m.Key,
				Value: m.Value,
			},
		},
	}, nil
}

func (c *grpcTranscoder) RawDeleteToRequest(m *pb.RawDeleteRequest) (*kv.OperationGroup[kv.OP], error) {
	return &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: []*kv.Elem[kv.OP]{
			{
				Op:  kv.Del,
				Key: m.Key,
			},
		},
	}, nil
}

func (c *grpcTranscoder) TransactionalPutToRequests(m *pb.PutRequest) (*kv.OperationGroup[kv.OP], error) {
	return &kv.OperationGroup[kv.OP]{
		IsTxn: true,
		Elems: []*kv.Elem[kv.OP]{
			{
				Op:    kv.Put,
				Key:   m.Key,
				Value: m.Value,
			},
		},
	}, nil
}

func (c *grpcTranscoder) TransactionalDeleteToRequests(m *pb.DeleteRequest) (*kv.OperationGroup[kv.OP], error) {
	return &kv.OperationGroup[kv.OP]{
		IsTxn: true,
		Elems: []*kv.Elem[kv.OP]{
			{
				Op:  kv.Del,
				Key: m.Key,
			},
		},
	}, nil
}

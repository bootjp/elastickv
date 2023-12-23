package transport

import (
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
)

type GrpcTranscoder struct {
}

func NewGrpcGrpcTranscoder() *GrpcTranscoder {
	return &GrpcTranscoder{}
}

func (c *GrpcTranscoder) RawPutToRequest(m *pb.RawPutRequest) (*kv.OperationGroup[kv.OP], error) {
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

func (c *GrpcTranscoder) RawDeleteToRequest(m *pb.RawDeleteRequest) (*kv.OperationGroup[kv.OP], error) {
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

func (c *GrpcTranscoder) TransactionalPutToRequests(m *pb.PutRequest) (*kv.OperationGroup[kv.OP], error) {
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

func (c *GrpcTranscoder) TransactionalDeleteToRequests(m *pb.DeleteRequest) (*kv.OperationGroup[kv.OP], error) {
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

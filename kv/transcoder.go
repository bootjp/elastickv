package kv

import pb "github.com/bootjp/elastickv/proto"

func NewTranscoder() *Transcoder {
	return &Transcoder{}
}

type OP int

const (
	Put OP = iota
	Del
)

type Elem[T OP] struct {
	Op    T
	Key   []byte
	Value []byte
}

type OperationGroup[T OP] struct {
	Elems []*Elem[T]
	IsTxn bool
}

type Transcoder struct {
}

func (c *Transcoder) RawPutToRequest(m *pb.RawPutRequest) (*pb.Request, error) {
	return &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   m.Key,
				Value: m.Value,
			},
		},
	}, nil
}

func (c *Transcoder) RawDeleteToRequest(m *pb.RawDeleteRequest) (*pb.Request, error) {
	return &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{
			{
				Op:  pb.Op_DEL,
				Key: m.Key,
			},
		},
	}, nil
}

func (c *Transcoder) TransactionalPutToRequests(m *pb.PutRequest) ([]*pb.Request, error) {
	return []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_PREPARE,
			Mutations: []*pb.Mutation{
				{
					Key:   m.Key,
					Value: m.Value,
				},
			},
		},
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{
				{
					Key:   m.Key,
					Value: m.Value,
				},
			},
		},
	}, nil
}

func (c *Transcoder) TransactionalDeleteToRequests(m *pb.DeleteRequest) ([]*pb.Request, error) {
	return []*pb.Request{
		{
			IsTxn: true,
			Phase: pb.Phase_PREPARE,
			Mutations: []*pb.Mutation{
				{
					Key: m.Key,
				},
			},
		},
		{
			IsTxn: true,
			Phase: pb.Phase_COMMIT,
			Mutations: []*pb.Mutation{
				{
					Key: m.Key,
				},
			},
		},
	}, nil
}

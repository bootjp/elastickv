package kv

import pb "github.com/bootjp/elastickv/proto"

type Convert struct {
}

func (c *Convert) RawPutToRequest(m *pb.RawPutRequest) (*pb.Request, error) {
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

func (c *Convert) RawDeleteToRequest(m *pb.RawDeleteRequest) (*pb.Request, error) {
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

func (c *Convert) PutToRequests(m *pb.PutRequest) ([]*pb.Request, error) {
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

func (c *Convert) DeleteToRequests(m *pb.DeleteRequest) ([]*pb.Request, error) {
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

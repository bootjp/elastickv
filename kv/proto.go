package kv

import pb "github.com/bootjp/elastickv/proto"

type Convert struct {
}

func (c *Convert) RawPutToMutation(m *pb.RawPutRequest) (*pb.Mutation, error) {
	return &pb.Mutation{
		Op:    pb.Mutation_PUT,
		Key:   m.Key,
		Value: m.Value,
	}, nil

}

func (c *Convert) RawDeleteToMutation(m *pb.RawDeleteRequest) (*pb.Mutation, error) {
	return &pb.Mutation{
		Op:  pb.Mutation_DELETE,
		Key: m.Key,
	}, nil
}

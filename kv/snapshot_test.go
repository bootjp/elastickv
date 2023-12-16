package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestSnapshot(t *testing.T) {
	store := NewMemoryStore()
	fsm := NewKvFSM(store)

	mutation := pb.Mutation{
		Op:    pb.Mutation_PUT,
		Key:   []byte("hoge"),
		Value: []byte("fuga"),
	}

	b, err := proto.Marshal(&mutation)
	assert.NoError(t, err)

	fsm.Apply(&raft.Log{
		Type: raft.LogCommand,
		Data: b,
	})
	fsm.Apply(&raft.Log{
		Type: raft.LogBarrier,
	})

	ctx := context.Background()
	v, err := store.Get(ctx, []byte("hoge"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("fuga"), v)

	snapshot, err := fsm.Snapshot()
	assert.NoError(t, err)

	store2 := NewMemoryStore()
	fsm2 := NewKvFSM(store2)

	kvFSMSnap, ok := snapshot.(*kvFSMSnapshot)
	assert.True(t, ok)
	assert.NoError(t, err)

	err = fsm2.Restore(kvFSMSnap)
	assert.NoError(t, err)

	v, err = store2.Get(ctx, []byte("hoge"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("fuga"), v)

}

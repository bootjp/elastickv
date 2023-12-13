package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"google.golang.org/protobuf/proto"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
)

func TestSnapshot(t *testing.T) {
	store := NewMemoryStore()
	fsm := NewKvFSM(store)

	put := pb.PutRequest{
		Key:   []byte("hoge"),
		Value: []byte("fuga"),
	}

	b, err := proto.Marshal(&put)
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

	kvFSMSnap := snapshot.(*kvFSMSnapshot)
	assert.NoError(t, err)

	err = fsm2.Restore(kvFSMSnap)
	assert.NoError(t, err)

	v, err = store2.Get(ctx, []byte("hoge"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("fuga"), v)

}

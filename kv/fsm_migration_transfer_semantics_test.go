package kv

import (
	"context"
	"testing"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestMigrationTransferDoesNotChangeLargeWriteAdmission(t *testing.T) {
	value := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	ctx := context.Background()
	for _, test := range []struct {
		transactional bool
		phase         pb.Phase
	}{{false, pb.Phase_NONE}, {true, pb.Phase_NONE}, {true, pb.Phase_PREPARE}} {
		fsm := newMigrationWriteTrackerFSM(t)
		key := []byte("n")
		mutations := []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: value}}
		if test.transactional {
			mutations = append([]*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, CommitTS: 60, LockTTLms: defaultTxnLockTTLms})}}, mutations...)
		}
		request := &pb.Request{IsTxn: test.transactional, Phase: test.phase, Ts: 50, Mutations: mutations}
		require.Less(t, proto.Size(request), internalutil.GRPCMaxMessageBytes)
		require.NoError(t, fsm.applyRequestErr(ctx, request))
		if test.phase == pb.Phase_NONE {
			actual, err := fsm.store.GetAt(ctx, key, 100)
			require.NoError(t, err)
			require.Len(t, actual, len(value))
		}
	}
}

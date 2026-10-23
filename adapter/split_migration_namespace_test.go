package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func migrationNamespacePeerContext(t *testing.T, authorized bool) context.Context {
	t.Helper()
	ctx := peer.NewContext(t.Context(), &peer.Peer{AuthInfo: credentials.TLSInfo{}})
	if authorized {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+t.Name()))
	}
	return ctx
}

func TestSplitMigrationNamespaceSendFirstAuthentication(t *testing.T) {
	for _, key := range [][]byte{[]byte("!migstage|ready|"), []byte("!migstage|ownership"), []byte("!migstage|retired"),
		[]byte("!migstage|ack|"), []byte("!migwrite|source"), []byte("!migfence|source"),
		[]byte("!dist|migstage|row"), store.MigrationConfigurationIndexKey(),
		[]byte("!txn|int|!migstage|ready|"), []byte("!txn|lock|!migstage|ready|")} {
		t.Run(string(key), func(t *testing.T) {
			auth, _ := SplitMigrationTokenAuth(t.Name(), false, false)
			request := &pb.ForwardRequest{Requests: []*pb.Request{{IsTxn: true,
				Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key}}}}}
			for _, authorized := range []bool{false, true} {
				called := false
				_, err := auth(migrationNamespacePeerContext(t, authorized), request,
					&grpc.UnaryServerInfo{FullMethod: pb.Internal_Forward_FullMethodName}, func(context.Context, any) (any, error) {
						called = true
						return nil, nil
					})
				require.Equal(t, authorized, called)
				if authorized {
					require.NoError(t, err)
				} else {
					require.Equal(t, codes.Unauthenticated, status.Code(err))
				}
			}
		})
	}
}

func TestSplitMigrationNamespaceAuthorizedForwardStillRejectsInternalWrites(t *testing.T) {
	st := store.NewMVCCStore()
	fsm := kv.NewKvFSMWithHLC(st, kv.NewHLC())
	proposer := &applyingMigrationProposer{fsm: fsm}
	tx := kv.NewTransactionWithProposer(proposer, kv.WithMigrationRequestAdmission(st, 1))
	t.Cleanup(tx.Close)
	internal := NewInternalWithEngine(tx, forwardObserverLeader{}, nil, nil, WithInternalStore(st))
	auth, _ := SplitMigrationTokenAuth(t.Name(), false, false)
	request := &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{{IsTxn: true, Ts: 10, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("!migstage|ready|"), CommitTS: 20})},
		{Op: pb.Op_PUT, Key: []byte("!migstage|ready|"), Value: []byte("poison")},
	}}}}
	_, err := auth(migrationNamespacePeerContext(t, true), request,
		&grpc.UnaryServerInfo{FullMethod: pb.Internal_Forward_FullMethodName}, func(ctx context.Context, req any) (any, error) {
			forward, ok := req.(*pb.ForwardRequest)
			require.True(t, ok)
			return internal.Forward(ctx, forward)
		})
	require.ErrorIs(t, err, kv.ErrInvalidRequest)
	require.Zero(t, proposer.calls, "privileged transport cannot bypass preproposal namespace admission")
}

func TestSplitMigrationNamespacePreservesOrdinaryPeerAliases(t *testing.T) {
	auth, _ := SplitMigrationTokenAuth(t.Name(), false, false)
	for _, key := range []string{"customer", "_migack", "_mighlc", "_migpromote", "!txn|meta", "!txn|foo",
		"!txn|backup|customer", "!txn|int|customer", "!migstage", "!migstage-user", "!hs|!migstage|ready||field"} {
		request := &pb.ForwardRequest{Requests: []*pb.Request{{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(key)}}}}}
		called := false
		_, err := auth(t.Context(), request, &grpc.UnaryServerInfo{FullMethod: pb.Internal_Forward_FullMethodName}, func(context.Context, any) (any, error) {
			called = true
			return nil, nil
		})
		require.NoError(t, err, key)
		require.True(t, called, key)
	}
}

func TestSplitMigrationNamespaceProtectsPrefixDeletes(t *testing.T) {
	auth, _ := SplitMigrationTokenAuth(t.Name(), false, false)
	for _, key := range []string{"!migs", "!migstage|", "!migwrite|", "!migfence|", "!dist|migstage|"} {
		request := &pb.ForwardRequest{Requests: []*pb.Request{{Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte(key)}}}}}
		_, err := auth(migrationNamespacePeerContext(t, false), request,
			&grpc.UnaryServerInfo{FullMethod: pb.Internal_Forward_FullMethodName}, func(context.Context, any) (any, error) {
				t.Fatal("unprivileged prefix delete reached its handler")
				return nil, nil
			})
		require.Equal(t, codes.Unauthenticated, status.Code(err), key)
	}
}

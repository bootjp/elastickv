package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSplitPartitionLedgerRequiresMigrationAuthentication(t *testing.T) {
	key, err := distribution.CatalogOwnershipPartitionKey(3, make([]byte, 32))
	require.NoError(t, err)
	for _, prefix := range []string{"", "!txn|int|", "!txn|lock|"} {
		for _, operation := range []pb.Op{pb.Op_PUT, pb.Op_DEL, pb.Op_DEL_PREFIX} {
			auth, _ := SplitMigrationTokenAuth(t.Name(), false, false)
			request := &pb.ForwardRequest{Requests: []*pb.Request{{Mutations: []*pb.Mutation{
				{Op: operation, Key: append([]byte(prefix), key...)},
			}}}}
			for _, authorized := range []bool{false, true} {
				called := false
				_, err := auth(migrationNamespacePeerContext(t, authorized), request,
					&grpc.UnaryServerInfo{FullMethod: pb.Internal_Forward_FullMethodName},
					func(context.Context, any) (any, error) { called = true; return nil, nil })
				require.Equal(t, authorized, called)
				if authorized {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			}
		}
	}
}

func TestSplitPartitionLedgerPrefixAuthorization(t *testing.T) {
	for _, key := range []string{"!dist|meta|ownership_partition/", "!dist|meta|ownership_partition",
		"!txn|int|!dist|meta|ownership_partition/", "!txn|lock|!dist|meta|ownership_partition"} {
		require.True(t, splitJobControlMutation(&pb.Mutation{Op: pb.Op_DEL_PREFIX, Key: []byte(key)}), key)
	}
	for _, key := range []string{"customer", "!dist|meta|ownership_partition_user", "!txn|int|customer"} {
		require.False(t, splitJobControlMutation(&pb.Mutation{Op: pb.Op_DEL_PREFIX, Key: []byte(key)}), key)
	}
}

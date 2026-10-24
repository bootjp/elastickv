package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCPublicMutationsRejectControlKeys(t *testing.T) {
	t.Parallel()
	// No coordinator is configured: rejected writes must never reach dispatch.
	server := NewGRPCServer(nil, nil)
	ctx := context.Background()
	for _, key := range [][]byte{
		distribution.CatalogSplitJobKey(7), distribution.CatalogRouteKey(1), distribution.CatalogVersionKey(),
		[]byte("!txn|int|!dist|job|forged"), []byte("!txn|meta|primary"), []byte("!dist|migstage|forged"),
	} {
		t.Run(string(key), func(t *testing.T) {
			_, err := server.RawPut(ctx, &pb.RawPutRequest{Key: key})
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = server.RawDelete(ctx, &pb.RawDeleteRequest{Key: key})
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = server.Put(ctx, &pb.PutRequest{Key: key})
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			_, err = server.Delete(ctx, &pb.DeleteRequest{Key: key})
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		})
	}
	for _, key := range []string{"user", "!txn|foo", "!txn|backup|customer", "!ddb|customer", "!s3|customer"} {
		require.NoError(t, validatePublicMutationKey([]byte(key)))
	}
}

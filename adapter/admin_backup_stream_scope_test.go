package adapter

import (
	"context"
	"encoding/base64"
	"testing"

	logicalbackup "github.com/bootjp/elastickv/internal/backup"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newScopeFilterTestServer(t *testing.T) (*AdminServer, []byte) {
	t.Helper()
	ordersKey := []byte(logicalbackup.DDBTableMetaPrefix + base64.RawURLEncoding.EncodeToString([]byte("orders")))
	// A second scope inside the same adapter, so the adapter filter cannot do
	// the separating that only the scope selection should do.
	itemsKey := []byte(logicalbackup.DDBTableMetaPrefix + base64.RawURLEncoding.EncodeToString([]byte("items")))
	redisKey := []byte(logicalbackup.RedisStringPrefix + "key")
	store := &backupTestStore{keys: [][]byte{itemsKey, ordersKey, redisKey}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store,
		map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)
	return srv, ordersKey
}

// BeginBackup's scope selection is what expected_keys and the preflight
// retained-count scan were computed over. A stream that reaches outside it
// sends data the integrity baseline never covered, so a scope the Begin
// selection did not include is refused rather than silently served.
func TestStreamBackupRejectsScopesOutsideTheBeginSelection(t *testing.T) {
	t.Parallel()

	srv, _ := newScopeFilterTestServer(t)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{
		Scopes: []*pb.BackupScope{{Adapter: "redis", Scope: "db_0"}},
	})
	require.NoError(t, err)

	stream := &backupTestStream{ctx: context.Background()}
	err = srv.StreamBackup(&pb.StreamBackupRequest{
		PinToken: begin.GetPinToken(),
		Scopes:   []*pb.BackupScope{{Adapter: "dynamodb", Scope: "orders"}},
	}, stream)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Empty(t, stream.got, "no record from an unselected scope may be sent")
}

// A stream that names no scopes inherits the Begin selection rather than
// widening to everything. Both fixtures live in the same adapter, so the
// adapter filter cannot separate them -- only the scope selection can.
func TestStreamBackupInheritsTheBeginScopeSelection(t *testing.T) {
	t.Parallel()

	srv, ordersKey := newScopeFilterTestServer(t)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{
		Scopes: []*pb.BackupScope{{Adapter: "dynamodb", Scope: "orders"}},
	})
	require.NoError(t, err)

	stream := &backupTestStream{ctx: context.Background()}
	require.NoError(t, srv.StreamBackup(&pb.StreamBackupRequest{PinToken: begin.GetPinToken()}, stream))
	require.Len(t, stream.got, 1, "an empty stream scope list inherits Begin, it does not widen")
	require.Equal(t, ordersKey, stream.got[0].GetKey())
}

// Narrowing within the Begin selection stays legal: the baseline covers a
// superset of what the stream asks for.
func TestStreamBackupAllowsNarrowingWithinTheBeginSelection(t *testing.T) {
	t.Parallel()

	srv, ordersKey := newScopeFilterTestServer(t)
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{
		Scopes: []*pb.BackupScope{
			{Adapter: "dynamodb", Scope: "orders"},
			{Adapter: "dynamodb", Scope: "items"},
		},
	})
	require.NoError(t, err)

	stream := &backupTestStream{ctx: context.Background()}
	require.NoError(t, srv.StreamBackup(&pb.StreamBackupRequest{
		PinToken: begin.GetPinToken(),
		Scopes:   []*pb.BackupScope{{Adapter: "dynamodb", Scope: "orders"}},
	}, stream))
	require.Len(t, stream.got, 1)
	require.Equal(t, ordersKey, stream.got[0].GetKey())
}

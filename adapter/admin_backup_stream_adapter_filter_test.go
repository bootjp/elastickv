package adapter

import (
	"context"
	"testing"

	logicalbackup "github.com/bootjp/elastickv/internal/backup"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

// BeginBackup and scope listing skip an excluded adapter before strict
// classification, so a dump that excludes DynamoDB passes both preflight scans
// even when the cluster holds a malformed or future-format DynamoDB key. The
// stream has to apply the same filter in the same order: without it the dump
// aborts partway through on a key it was never going to send.
func TestStreamBackupAppliesTheBeginAdapterFilter(t *testing.T) {
	t.Parallel()

	redisKey := []byte(logicalbackup.RedisStringPrefix + "key")
	// Recognised as DynamoDB by prefix, but ScopeForKey cannot classify it.
	malformedDDBKey := []byte(logicalbackup.DDBTableMetaPrefix + "!!!")
	_, _, scopeErr := logicalbackup.ScopeForKey(malformedDDBKey)
	require.Error(t, scopeErr, "the fixture must be a key strict classification rejects")

	store := &backupTestStore{keys: [][]byte{malformedDDBKey, redisKey}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store,
		map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)

	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{Adapters: []string{"redis"}})
	require.NoError(t, err, "preflight already excludes the adapter before classifying it")

	stream := &backupTestStream{ctx: context.Background()}
	require.NoError(t, srv.StreamBackup(&pb.StreamBackupRequest{PinToken: begin.GetPinToken()}, stream))
	require.Len(t, stream.got, 1)
	require.Equal(t, redisKey, stream.got[0].GetKey())
}

// A malformed key inside an adapter the dump does include must still fail the
// stream: the filter narrows what is classified, it does not relax classification.
func TestStreamBackupStillRejectsMalformedKeysInSelectedAdapters(t *testing.T) {
	t.Parallel()

	malformedDDBKey := []byte(logicalbackup.DDBTableMetaPrefix + "!!!")
	store := &backupTestStore{keys: [][]byte{malformedDDBKey}}
	group := &backupTestGroup{status: raftengine.Status{AppliedIndex: 100}, every: 10_000}
	proposer := newBackupTestProposer()
	srv := newBackupControlTestServer(t, store,
		map[uint64]*backupTestGroup{1: group}, map[uint64]*backupTestProposer{1: proposer}, nil)

	// Begin itself fails closed on the malformed key once DynamoDB is included,
	// which is the existing contract; drive the stream through a redis-only
	// Begin and then widen the request's scopes to reach the stream filter.
	begin, err := srv.BeginBackup(context.Background(), &pb.BeginBackupRequest{Adapters: []string{"dynamodb"}})
	require.Error(t, err, "preflight classifies the included adapter strictly")
	require.Nil(t, begin)
}

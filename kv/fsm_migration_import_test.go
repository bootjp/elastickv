package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestMigrationStoreVersionsFromProtoStagesKeys(t *testing.T) {
	t.Parallel()

	rawKey := []byte("user|k")
	value := []byte("value")
	got := migrationStoreVersionsFromProto(7, []*pb.MVCCVersion{
		nil,
		{
			Key:       rawKey,
			CommitTs:  11,
			Value:     value,
			KeyFamily: distribution.MigrationFamilyUser,
			ExpireAt:  123,
		},
	})

	require.Len(t, got, 1)
	require.Equal(t, distribution.MigrationStagedDataKey(7, []byte("user|k")), got[0].Key)
	require.Equal(t, uint64(11), got[0].CommitTS)
	require.Equal(t, []byte("value"), got[0].Value)
	require.Equal(t, distribution.MigrationFamilyUser, got[0].KeyFamily)
	require.Equal(t, uint64(123), got[0].ExpireAt)

	rawKey[0] = 'X'
	value[0] = 'X'
	require.Equal(t, distribution.MigrationStagedDataKey(7, []byte("user|k")), got[0].Key)
	require.Equal(t, []byte("value"), got[0].Value)
}

func TestApplyMigrationImportWritesOnlyStagedKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	hlc := NewHLC()
	fsm := &kvFSM{store: st, hlc: hlc}
	req := &pb.ImportRangeVersionsRequest{
		JobId:     9,
		BracketId: 1,
		BatchSeq:  1,
		Cursor:    []byte("cursor"),
		Versions: []*pb.MVCCVersion{
			{Key: []byte("user|k"), CommitTs: 10, Value: []byte("v")},
		},
	}
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	applied := fsm.applyMigrationImport(ctx, data)
	result, ok := applied.(store.ImportVersionsResult)
	require.True(t, ok, "got %T: %v", applied, applied)
	require.Equal(t, []byte("cursor"), result.AckedCursor)
	require.Equal(t, uint64(10), result.MaxImportedTS)
	require.GreaterOrEqual(t, hlc.Current(), uint64(10))

	staged := distribution.MigrationStagedDataKey(9, []byte("user|k"))
	got, err := st.GetAt(ctx, staged, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	_, err = st.GetAt(ctx, []byte("user|k"), 10)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

type captureMigrationImportStore struct {
	store.MVCCStore
	opts store.ImportVersionsOptions
}

func (s *captureMigrationImportStore) ImportVersionsRaft(_ context.Context, opts store.ImportVersionsOptions) (store.ImportVersionsResult, error) {
	s.opts = opts
	return store.ImportVersionsResult{AckedCursor: opts.Cursor, MaxImportedTS: 10}, nil
}

func TestApplyMigrationImportThreadsPendingApplyIndex(t *testing.T) {
	t.Parallel()

	capturing := &captureMigrationImportStore{}
	fsm := &kvFSM{store: capturing, pendingApplyIdx: 1234}
	req := &pb.ImportRangeVersionsRequest{
		JobId:     9,
		BracketId: 1,
		BatchSeq:  1,
		Cursor:    []byte("cursor"),
		Versions: []*pb.MVCCVersion{
			{Key: []byte("user|k"), CommitTs: 10, Value: []byte("v")},
		},
	}
	data, err := proto.Marshal(req)
	require.NoError(t, err)

	applied := fsm.applyMigrationImport(context.Background(), data)
	result, ok := applied.(store.ImportVersionsResult)
	require.True(t, ok, "got %T: %v", applied, applied)
	require.Equal(t, []byte("cursor"), result.AckedCursor)
	require.Equal(t, uint64(1234), capturing.opts.AppliedIndex)
	require.Equal(t, uint64(9), capturing.opts.JobID)
	require.Len(t, capturing.opts.Versions, 1)
	require.Equal(t, distribution.MigrationStagedDataKey(9, []byte("user|k")), capturing.opts.Versions[0].Key)
}

// failingMigrationImportStore fails ImportVersionsRaft with a fixed error so
// the FSM's ordinary-vs-halt classification can be exercised directly.
type failingMigrationImportStore struct {
	store.MVCCStore
	importErr error
	floorErr  error
	maxTS     uint64
}

func (s *failingMigrationImportStore) ImportVersionsRaft(_ context.Context, opts store.ImportVersionsOptions) (store.ImportVersionsResult, error) {
	if s.importErr != nil {
		return store.ImportVersionsResult{}, s.importErr
	}
	return store.ImportVersionsResult{AckedCursor: opts.Cursor, MaxImportedTS: s.maxTS}, nil
}

func (s *failingMigrationImportStore) MigrationHLCFloor(context.Context, uint64) (uint64, error) {
	if s.floorErr != nil {
		return 0, s.floorErr
	}
	return 0, nil
}

func migrationImportCommandPayload(t *testing.T) []byte {
	t.Helper()

	data, err := proto.Marshal(&pb.ImportRangeVersionsRequest{
		JobId:     9,
		BracketId: 1,
		BatchSeq:  1,
		Cursor:    []byte("cursor"),
		Versions: []*pb.MVCCVersion{
			{Key: []byte("user|k"), CommitTs: 10, Value: []byte("v")},
		},
	})
	require.NoError(t, err)

	return data
}

// A store-side import failure is per-replica: the leader can apply the batch
// and ack the RPC while this voter skips the imported versions for good. The
// response must therefore halt the apply loop instead of letting the engine
// advance setApplied past the entry.
func TestApplyMigrationImportHaltsOnStoreFailure(t *testing.T) {
	t.Parallel()

	pebbleIOErr := errors.New("pebble: background error")
	fsm := &kvFSM{store: &failingMigrationImportStore{importErr: pebbleIOErr}}

	applied := fsm.applyMigrationImport(context.Background(), migrationImportCommandPayload(t))

	err := haltApplyOf(applied)
	require.Error(t, err, "store failure must halt apply, got %T: %v", applied, applied)
	require.True(t, errors.Is(err, ErrMigrationImportApply), "got %v", err)
	require.ErrorIs(t, err, pebbleIOErr)
}

// A failed HLC-floor read is a store read on this replica only, so it halts
// for the same reason. Replay after restart is safe: the import batch already
// committed, and the replayed entry is recognised as a duplicate.
func TestApplyMigrationImportHaltsOnHLCFloorFailure(t *testing.T) {
	t.Parallel()

	floorErr := errors.New("pebble: read failed")
	fsm := &kvFSM{store: &failingMigrationImportStore{maxTS: 0, floorErr: floorErr}}

	applied := fsm.applyMigrationImport(context.Background(), migrationImportCommandPayload(t))

	err := haltApplyOf(applied)
	require.Error(t, err, "hlc floor failure must halt apply, got %T: %v", applied, applied)
	require.True(t, errors.Is(err, ErrMigrationImportApply), "got %v", err)
	require.ErrorIs(t, err, floorErr)
}

func TestApplyMigrationImportHaltsOnUndecodablePayload(t *testing.T) {
	t.Parallel()

	fsm := &kvFSM{store: &failingMigrationImportStore{}}

	applied := fsm.applyMigrationImport(context.Background(), []byte{0xff, 0xff, 0xff, 0xff})

	err := haltApplyOf(applied)
	require.Error(t, err, "undecodable payload must halt apply, got %T: %v", applied, applied)
	require.True(t, errors.Is(err, ErrMigrationImportApply), "got %v", err)
}

// Verdicts on the request bytes are reached identically by every replica, so
// they stay ordinary errors: the RPC caller sees them and the group advances
// setApplied in step. Halting on these would turn a malformed request into a
// cluster-wide outage.
func TestApplyMigrationImportOrdinaryErrorsDoNotHalt(t *testing.T) {
	t.Parallel()

	tests := map[string]error{
		"batch gap":           store.ErrImportBatchGap,
		"invalid version":     store.ErrInvalidImportVersion,
		"value too large":     store.ErrValueTooLarge,
		"wrapped batch gap":   errors.Wrap(store.ErrImportBatchGap, "context"),
		"wrapped bad version": errors.Wrap(store.ErrInvalidImportVersion, "context"),
	}
	for name, importErr := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fsm := &kvFSM{store: &failingMigrationImportStore{importErr: importErr}}

			applied := fsm.applyMigrationImport(context.Background(), migrationImportCommandPayload(t))

			require.NoError(t, haltApplyOf(applied), "request-shaped error must not halt apply")
			err, ok := applied.(error)
			require.True(t, ok, "got %T: %v", applied, applied)
			require.ErrorIs(t, err, importErr)
			require.False(t, errors.Is(err, ErrMigrationImportApply))
		})
	}
}

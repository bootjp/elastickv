package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMigrationPromoteTargetKeyRestoresRawKey(t *testing.T) {
	t.Parallel()

	targetKey := migrationPromoteTargetKey(9)
	raw, ok := targetKey(distribution.MigrationStagedDataKey(9, []byte("user|k")))
	require.True(t, ok)
	require.Equal(t, []byte("user|k"), raw)

	_, ok = targetKey(distribution.MigrationStagedDataKey(10, []byte("user|k")))
	require.False(t, ok)
}

func TestApplyMigrationPromoteMovesStagedVersions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	hlc := NewHLC()
	fsm := &kvFSM{store: st, hlc: hlc}
	staged := distribution.MigrationStagedDataKey(9, []byte("user|k"))
	require.NoError(t, st.PutAt(ctx, staged, []byte("v10"), 10, 0))
	require.NoError(t, st.DeleteAt(ctx, staged, 20))

	cmd, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{
		JobId:       9,
		MaxVersions: 10,
	})
	require.NoError(t, err)
	applied := fsm.Apply(cmd)
	result, ok := applied.(store.PromoteVersionsResult)
	require.True(t, ok, "got %T: %v", applied, applied)
	require.True(t, result.Done)
	require.Equal(t, uint64(2), result.PromotedRows)
	require.Equal(t, uint64(2), result.TotalPromotedRows)
	require.Equal(t, uint64(20), result.MaxPromotedTS)
	require.GreaterOrEqual(t, hlc.Current(), uint64(20))
	stateReader, ok := st.(store.MigrationPromotionStateReader)
	require.True(t, ok)
	state, ok, err := stateReader.MigrationPromotionState(ctx, 9)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, state.Done)
	require.Equal(t, uint64(2), state.PromotedRows)
	require.Equal(t, uint64(20), state.MaxPromotedTS)

	got, err := st.GetAt(ctx, []byte("user|k"), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("v10"), got)
	_, err = st.GetAt(ctx, []byte("user|k"), 20)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	left, err := st.ExportVersions(ctx, store.ExportVersionsOptions{
		StartKey:    distribution.MigrationStagedDataKeyPrefix(9),
		EndKey:      store.PrefixScanEnd(distribution.MigrationStagedDataKeyPrefix(9)),
		MaxVersions: 10,
	})
	require.NoError(t, err)
	require.Empty(t, left.Versions)
}

func TestApplyMigrationPromoteMalformedPayloadHalts(t *testing.T) {
	t.Parallel()

	fsm := &kvFSM{store: store.NewMVCCStore()}
	err := haltApplyOf(fsm.Apply([]byte{raftEncodeMigrationPromote, 0xff, 0xff}))
	require.True(t, errors.Is(err, ErrMigrationPromoteApply), "got %v", err)
}

func TestApplyMigrationPromoteInvalidCursorReturnsOrdinaryError(t *testing.T) {
	t.Parallel()

	fsm := &kvFSM{store: store.NewMVCCStore()}
	cmd, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{
		Cursor:      []byte{0xff},
		MaxVersions: 10,
	})
	require.NoError(t, err)
	resp := fsm.Apply(cmd)
	require.Nil(t, haltApplyOf(resp))
	err, ok := resp.(error)
	require.True(t, ok, "got %T: %v", resp, resp)
	require.ErrorIs(t, err, store.ErrInvalidExportCursor)
	require.False(t, errors.Is(err, ErrMigrationPromoteApply))
}

// PromoteStagedVersions runs inside FSM apply, synchronously, on every voter.
// The per-chunk bounds arrive in the Raft command, so a request that names an
// oversized batch makes one apply load, re-encrypt, and commit that much staged
// data in a single Pebble batch on every replica at once. The defaults only fill
// in unset bounds, so they are not limits; the hard ceilings are.
func TestMigrationPromoteOptionsClampOversizedBounds(t *testing.T) {
	t.Parallel()

	opts := migrationPromoteOptionsFromProto(&pb.PromoteStagedVersionsRequest{
		JobId:           9,
		MaxVersions:     1 << 30,
		MaxBytes:        1 << 40,
		MaxScannedBytes: 1 << 42,
	}, 7)

	require.Equal(t, maxMigrationPromoteMaxVersions, opts.MaxVersions)
	require.Equal(t, uint64(maxMigrationPromoteMaxBytes), opts.MaxBytes)
	require.Equal(t, uint64(maxMigrationPromoteMaxScannedBytes), opts.MaxScannedBytes)
	require.Equal(t, uint64(7), opts.AppliedIndex)
}

// Unset bounds still take the defaults, and a request under the ceiling is
// passed through unchanged so a caller can still ask for smaller chunks.
func TestMigrationPromoteOptionsKeepDefaultsAndSmallerRequests(t *testing.T) {
	t.Parallel()

	defaults := migrationPromoteOptionsFromProto(&pb.PromoteStagedVersionsRequest{JobId: 9}, 0)
	require.Equal(t, defaultMigrationPromoteMaxVersions, defaults.MaxVersions)
	require.Equal(t, uint64(defaultMigrationPromoteMaxBytes), defaults.MaxBytes)
	require.Equal(t, uint64(defaultMigrationPromoteMaxScannedBytes), defaults.MaxScannedBytes)

	smaller := migrationPromoteOptionsFromProto(&pb.PromoteStagedVersionsRequest{
		JobId:           9,
		MaxVersions:     16,
		MaxBytes:        1024,
		MaxScannedBytes: 4096,
	}, 0)
	require.Equal(t, 16, smaller.MaxVersions)
	require.Equal(t, uint64(1024), smaller.MaxBytes)
	require.Equal(t, uint64(4096), smaller.MaxScannedBytes)
}

// The clamp has to be a pure function of the command so every replica derives
// the same bounds from the same entry and apply stays deterministic.
func TestMigrationPromoteOptionsAreDeterministicPerCommand(t *testing.T) {
	t.Parallel()

	req := &pb.PromoteStagedVersionsRequest{JobId: 9, MaxVersions: 1 << 30, MaxBytes: 1 << 40}
	first := migrationPromoteOptionsFromProto(req, 11)
	second := migrationPromoteOptionsFromProto(req, 11)
	// PromoteVersionsOptions carries a closure, which never compares equal, so
	// the bounds this clamp owns are compared directly.
	require.Equal(t, first.MaxVersions, second.MaxVersions)
	require.Equal(t, first.MaxBytes, second.MaxBytes)
	require.Equal(t, first.MaxScannedBytes, second.MaxScannedBytes)
	require.Equal(t, first.StartKey, second.StartKey)
	require.Equal(t, first.EndKey, second.EndKey)
}

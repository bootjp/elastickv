package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPartitionControlStrictCodec(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte(`{}`), []byte(`{"Transition":{},"unknown":1}`), []byte(`{} {}`), []byte(`{"Transition":{},"Transition":{}}`)} {
		_, err := DecodeMigrationPartitionControl(raw)
		require.Error(t, err)
	}
}

func partitionControlFixture(t *testing.T, st store.MVCCStore, local uint64) (*kvFSM, MigrationPartitionControl) {
	t.Helper()
	f := &kvFSM{store: st, shardGroupID: local, routes: forbiddenApplyWatcher{}}
	require.NoError(t, f.RecordAppliedConfiguration(9))
	parent := distribution.RouteDescriptor{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1, State: distribution.RouteStateActive, MinWriteTSExclusive: 100}
	hash, err := distribution.OwnershipPartitionDescriptorHash(parent)
	require.NoError(t, err)
	role := store.MigrationOwner
	if local != 1 {
		role = store.MigrationNonOwner
	}
	state := store.MigrationOwnershipState{JobID: 11, Generation: "0123456789abcdef0123456789abcdef", LocalGroupID: local, OwnerGroupID: 1,
		CatalogVersion: 1, ConfigurationIndex: 9, RouteID: 1, RouteStart: parent.Start, RouteEnd: parent.End,
		Role: role, MinWriteTSExclusive: 100, WriteFloorKnown: true, DescriptorHash: hash}
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	require.NoError(t, writer.ClearMigrationStateWithOwnership(t.Context(), state, 10))
	view, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	left, right := parent, parent
	left.RouteID, left.ParentRouteID, left.End, left.SplitAtHLC = 2, 1, []byte("m"), 20
	right.RouteID, right.ParentRouteID, right.Start, right.SplitAtHLC = 3, 1, []byte("m"), 20
	manifest, err := distribution.EncodeOwnershipPartitionManifest(distribution.OwnershipPartitionManifest{PreviousVersion: 1, Version: 2, Parent: parent, Children: []distribution.RouteDescriptor{left, right}})
	require.NoError(t, err)
	c, err := BuildMigrationPartitionControl(nil, view.Ownership, manifest, nil, 40, 9)
	require.NoError(t, err)
	return f, c
}

func partitionControlCommand(t *testing.T, c MigrationPartitionControl) []byte {
	t.Helper()
	raw, err := EncodeMigrationPartitionControl(c)
	require.NoError(t, err)
	cmd, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{JobId: 11, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_PARTITION,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: c.Transition.ConfigurationIndex, PartitionTransition: raw})
	require.NoError(t, err)
	return cmd
}

func TestPartitionControlApplyAndExactReplay(t *testing.T) {
	for _, local := range []uint64{1, 2} {
		t.Run(map[uint64]string{1: "owner", 2: "nonowner"}[local], func(t *testing.T) {
			st := store.NewMVCCStore()
			f, c := partitionControlFixture(t, st, local)
			cmd := partitionControlCommand(t, c)
			f.SetApplyIndex(20)
			result, ok := f.Apply(cmd).(store.CleanupVersionsResult)
			require.True(t, ok)
			require.True(t, result.Done)
			reader, ok := st.(store.MigrationOwnershipPartitionStore)
			require.True(t, ok)
			before, err := reader.MigrationOwnershipPartitions(t.Context())
			require.NoError(t, err)
			require.Len(t, before.Partitions, 2)
			require.Len(t, before.Receipts, 1)
			for _, p := range before.Partitions {
				require.Equal(t, c.Transition.After[0].Role, p.Role)
				require.EqualValues(t, 100, p.MinWriteTSExclusive)
				require.True(t, store.MigrationOwnershipPartitionEvidenceValid(before, p))
				require.EqualValues(t, 40, p.CatalogVersion)
				require.EqualValues(t, 20, p.AppliedIndex)
			}
			f.SetApplyIndex(21)
			_, ok = f.Apply(cmd).(store.CleanupVersionsResult)
			require.True(t, ok)
			after, err := reader.MigrationOwnershipPartitions(t.Context())
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestPartitionControlRefreshStableIdentityAndNegativeRole(t *testing.T) {
	st := store.NewMVCCStore()
	f, c := partitionControlFixture(t, st, 2)
	f.SetApplyIndex(20)
	require.IsType(t, store.CleanupVersionsResult{}, f.Apply(partitionControlCommand(t, c)))
	reader, ok := st.(store.MigrationOwnershipPartitionStore)
	require.True(t, ok)
	before, err := reader.MigrationOwnershipPartitions(t.Context())
	require.NoError(t, err)
	m, err := distribution.DecodeOwnershipPartitionManifest(c.Manifest)
	require.NoError(t, err)
	refresh, err := BuildMigrationPartitionControl(before.Partitions[:1], nil, nil, &m.Children[0], 41, 9)
	require.NoError(t, err)
	f.SetApplyIndex(21)
	require.IsType(t, store.CleanupVersionsResult{}, f.Apply(partitionControlCommand(t, refresh)))
	after, err := reader.MigrationOwnershipPartitions(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Partitions[0].PartitionID, after.Partitions[0].PartitionID)
	require.Equal(t, store.MigrationNonOwner, after.Partitions[0].Role)
	require.Equal(t, before.Partitions[0].Origins, after.Partitions[0].Origins)
	require.EqualValues(t, 41, after.Partitions[0].CatalogVersion)
	refresh.Transition.After[0].Role = store.MigrationOwner
	_, err = EncodeMigrationPartitionControl(refresh)
	require.Error(t, err)
}

func TestPartitionControlRejectsChangedPriorAndConfiguration(t *testing.T) {
	st := store.NewMVCCStore()
	f, c := partitionControlFixture(t, st, 1)
	// A complete valid-looking alternate legacy CAS must still match durable state.
	legacy := append([]store.MigrationOwnershipState(nil), c.PriorOwnership...)
	legacy[0].AppliedIndex++
	conflict, err := BuildMigrationPartitionControl(nil, legacy, c.Manifest, nil, 40, 9)
	require.NoError(t, err)
	f.SetApplyIndex(20)
	response := f.Apply(partitionControlCommand(t, conflict))
	require.NoError(t, haltApplyOf(response))
	applyErr, ok := response.(error)
	require.True(t, ok)
	require.ErrorIs(t, applyErr, store.ErrMigrationOwnership)
	require.NoError(t, f.RecordAppliedConfiguration(21))
	f.SetApplyIndex(22)
	response = f.Apply(partitionControlCommand(t, c))
	require.NoError(t, haltApplyOf(response))
	applyErr, ok = response.(error)
	require.True(t, ok)
	require.ErrorIs(t, applyErr, ErrMigrationConfigurationChanged)
}

type partitionFailureStore struct {
	store.MVCCStore
	err error
}

func (s *partitionFailureStore) MigrationOwnershipPartitions(context.Context) (store.MigrationOwnershipPartitionState, error) {
	return store.MigrationOwnershipPartitionState{}, nil
}
func (s *partitionFailureStore) ApplyMigrationOwnershipPartition(context.Context, store.MigrationOwnershipPartitionTransition, uint64) error {
	return s.err
}

func TestPartitionControlIOHaltsButSemanticRefusalDoesNot(t *testing.T) {
	base := store.NewMVCCStore()
	_, c := partitionControlFixture(t, base, 1)
	for _, failure := range []error{errors.New("partition batch IO failure"), store.ErrMigrationOwnership, store.ErrNotSupported} {
		f := &kvFSM{store: &partitionFailureStore{MVCCStore: base, err: failure}, shardGroupID: 1}
		require.NoError(t, f.reloadMigrationConfiguration(t.Context()))
		f.SetApplyIndex(20)
		response := f.Apply(partitionControlCommand(t, c))
		if errors.Is(failure, store.ErrMigrationOwnership) || errors.Is(failure, store.ErrNotSupported) {
			require.NoError(t, haltApplyOf(response))
		} else {
			require.Error(t, haltApplyOf(response))
		}
	}
}

func TestPartitionControlCanonicalAndBoundedPayload(t *testing.T) {
	_, c := partitionControlFixture(t, store.NewMVCCStore(), 1)
	raw, err := EncodeMigrationPartitionControl(c)
	require.NoError(t, err)
	_, err = DecodeMigrationPartitionControl(raw)
	require.NoError(t, err)
	_, err = DecodeMigrationPartitionControl(append([]byte(" "), raw...))
	require.Error(t, err)
	_, err = DecodeMigrationPartitionControl(bytes.Replace(raw, []byte(`"Transition":`), []byte(`"transition":`), 1))
	require.Error(t, err)
	_, err = DecodeMigrationPartitionControl(bytes.Repeat([]byte("x"), MaxMigrationPartitionControlBytes+1))
	require.Error(t, err)
	c.Transition.Before[0].AttestationHash[0] ^= 1
	_, err = EncodeMigrationPartitionControl(c)
	require.Error(t, err)
}

func TestPartitionControlRequiresBoundConfiguration(t *testing.T) {
	err := ValidateMigrationPartitionRequest(&pb.CleanupMigrationRequest{Mode: pb.MigrationCleanupMode(7)})
	require.Error(t, err)
}

func TestPartitionControlPebbleApplyPersistsReceiptAndIndex(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	f, c := partitionControlFixture(t, st, 1)
	f.SetApplyIndex(20)
	require.IsType(t, store.CleanupVersionsResult{}, f.Apply(partitionControlCommand(t, c)), "typed control must dispatch before the raw batch scope")
	reader, ok := st.(store.MigrationOwnershipPartitionStore)
	require.True(t, ok)
	before, err := reader.MigrationOwnershipPartitions(t.Context())
	require.NoError(t, err)
	require.NoError(t, st.Close())
	reopened, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	reader, ok = reopened.(store.MigrationOwnershipPartitionStore)
	require.True(t, ok)
	after, err := reader.MigrationOwnershipPartitions(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after)
	applied, ok := reopened.(interface{ LastAppliedIndex() (uint64, bool, error) })
	require.True(t, ok)
	index, present, err := applied.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.EqualValues(t, 20, index)
}

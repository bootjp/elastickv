package adapter

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitWorkingSourceRejectsHistoricalTargetControlBudget(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	r.migrate(t, 1, 2, "m")
	r.migrate(t, r.ownership(t, 2)[0].RouteID, 3, "t")
	require.NoError(t, r.server.RunMigrationOwnershipRefreshOnce(t.Context()))
	snapshot, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	parentID := r.ownership(t, 3)[0].RouteID
	parent, found := findRouteByID(snapshot.Routes, parentID)
	require.True(t, found)
	job := distribution.SplitJob{JobID: 100, SourceRouteID: parentID, SourceGroupID: 3,
		TargetGroupID: 1, Phase: distribution.SplitJobPhasePlanned}
	source := targetBudgetNativePriors(t, r.groups[3].Store, parent, store.MigrationOwner)
	target := targetBudgetNativePriors(t, r.groups[1].Store, parent, store.MigrationNonOwner)
	leftID, rightID, err := r.server.allocateChildRouteIDs(t.Context(), snapshot.ReadTS, snapshot.Routes)
	require.NoError(t, err)
	nextRouteID, err := r.server.catalog.NextRouteIDAt(t.Context(), snapshot.ReadTS)
	require.NoError(t, err)
	measure := func(size int) ([]byte, kv.MigrationPartitionControl, kv.MigrationPartitionControl, int, int) {
		t.Helper()
		key := append([]byte("x"), bytes.Repeat([]byte("k"), size)...)
		left, right := splitCatalogRoutes(parent, key, leftID, rightID, parent.SplitAtHLC+1)
		manifest, encodeErr := distribution.EncodeOwnershipPartitionManifest(distribution.OwnershipPartitionManifest{
			PreviousVersion: snapshot.Version, Version: snapshot.Version + 1, JobID: job.JobID,
			Parent: parent, Children: []distribution.RouteDescriptor{left, right}})
		require.NoError(t, encodeErr)
		sourceControl := targetBudgetControl(t, source, manifest, snapshot.Version+1)
		targetControl := targetBudgetControl(t, target, manifest, snapshot.Version+1)
		sourceJSON, marshalErr := json.Marshal(sourceControl)
		require.NoError(t, marshalErr)
		targetJSON, marshalErr := json.Marshal(targetControl)
		require.NoError(t, marshalErr)
		sourceMaximum, sizeErr := maximumSplitPartitionControlSize(sourceJSON)
		require.NoError(t, sizeErr)
		targetMaximum, sizeErr := maximumSplitPartitionControlSize(targetJSON)
		require.NoError(t, sizeErr)
		return key, sourceControl, targetControl, sourceMaximum, targetMaximum
	}
	_, _, _, sourceSize, targetSize := measure(0)
	require.Greater(t, targetSize, sourceSize, "historical target must have the larger valid native envelope")
	limit := kv.MaxMigrationPartitionControlBytes
	lo, hi := 0, 700<<10
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		_, _, _, size, _ := measure(mid)
		if size <= limit {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	key, sourceControl, targetControl, sourceSize, targetSize := measure(lo)
	job.SplitKey = key
	require.LessOrEqual(t, sourceSize, limit)
	require.Greater(t, targetSize, limit)
	sourceJSON, err := kv.EncodeMigrationPartitionControl(sourceControl)
	require.NoError(t, err)
	targetJSON, err := kv.EncodeMigrationPartitionControl(targetControl)
	require.NoError(t, err, "target rejection must concern the growth reserve, not malformed evidence")
	require.NoError(t, validateSplitPartitionControlSize(sourceControl))
	require.ErrorIs(t, validateSplitPartitionControlSize(targetControl), kv.ErrInvalidRequest)
	t.Logf("split key bytes=%d source encoded=%d maximum=%d target encoded=%d maximum=%d limit=%d",
		len(job.SplitKey), len(sourceJSON), sourceSize, len(targetJSON), targetSize, limit)
	require.NoError(t, r.server.catalog.CreateSplitJob(t.Context(), job))
	_, err = r.server.ensureSplitWorkingSource(t.Context(), job)
	assert.ErrorIs(t, err, kv.ErrInvalidRequest, "the required historical target control must be budgeted before root geometry commits")
	after, loadErr := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, loadErr)
	assert.Equal(t, snapshot.Version, after.Version, "target budget rejection must not advance the catalog")
	assert.True(t, reflect.DeepEqual(snapshot.Routes, after.Routes), "target budget rejection must not publish children")
	afterNextRouteID, loadErr := r.server.catalog.NextRouteIDAt(t.Context(), after.ReadTS)
	require.NoError(t, loadErr)
	assert.Equal(t, nextRouteID, afterNextRouteID, "target budget rejection must not allocate route IDs")
	stored, exists, loadErr := r.server.catalog.SplitJob(t.Context(), job.JobID)
	require.NoError(t, loadErr)
	require.True(t, exists)
	assert.Zero(t, stored.WorkingSourceRouteID, "target budget rejection must not publish working-source allocation")
}

type targetBudgetPriors struct {
	partitions    []store.MigrationOwnershipPartition
	legacy        []store.MigrationOwnershipState
	configuration uint64
}

func targetBudgetNativePriors(t *testing.T, st store.MVCCStore, parent distribution.RouteDescriptor, role store.MigrationOwnershipRole) targetBudgetPriors {
	t.Helper()
	reader, ok := st.(store.MigrationServingAdmissionReader)
	require.True(t, ok)
	view, err := reader.MigrationServingAdmissionState(t.Context())
	require.NoError(t, err)
	partitions, legacy := splitPartitionBudgetPriors(view, parent)
	require.NotZero(t, len(partitions)+len(legacy))
	for _, partition := range partitions {
		require.True(t, store.MigrationOwnershipPartitionEvidenceValid(view.Partitions, partition))
		require.Equal(t, role, partition.Role)
	}
	for _, old := range legacy {
		require.True(t, store.MigrationOwnershipEvidenceValid(old))
		require.Equal(t, role, old.Role)
	}
	configuration, err := kv.MigrationConfigurationIndex(t.Context(), st)
	require.NoError(t, err)
	return targetBudgetPriors{partitions: partitions, legacy: legacy, configuration: configuration}
}

func targetBudgetControl(t *testing.T, priors targetBudgetPriors, manifest []byte, version uint64) kv.MigrationPartitionControl {
	t.Helper()
	control, err := kv.BuildMigrationPartitionControl(priors.partitions, priors.legacy, manifest, nil, version, priors.configuration)
	require.NoError(t, err, "both controls must have complete valid native prior evidence")
	return control
}

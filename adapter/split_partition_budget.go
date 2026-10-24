package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const splitPartitionMaxUintDigits = 20

// Check the full downstream envelope before publishing geometry. The manifest
// alone is smaller than the control, which repeats bounds in priors and tiles.
func (s *DistributionServer) validateSplitPartitionBudget(ctx context.Context, parent distribution.RouteDescriptor, manifest []byte, version uint64, job *distribution.SplitJob) error {
	if s.ownershipRefresh == nil {
		return nil
	}
	groups := []uint64{parent.GroupID}
	if job != nil && job.TargetGroupID != parent.GroupID {
		groups = append(groups, job.TargetGroupID)
	}
	for _, id := range groups {
		if err := s.validateSplitGroupPartitionBudget(ctx, id, parent, manifest, version); err != nil {
			return err
		}
	}
	return nil
}

func (s *DistributionServer) validateSplitGroupPartitionBudget(ctx context.Context, id uint64, parent distribution.RouteDescriptor, manifest []byte, version uint64) error {
	group := s.ownershipRefresh.groups[id]
	view, err := s.splitPartitionAdmissionView(ctx, id, group)
	if err != nil {
		return errors.WithStack(err)
	}
	partitions, legacy := splitPartitionBudgetPriors(view, parent)
	if len(partitions)+len(legacy) == 0 {
		return nil
	}
	configuration, err := kv.MigrationConfigurationIndex(ctx, group.Store)
	if err != nil {
		return errors.WithStack(err)
	}
	control, err := kv.BuildMigrationPartitionControl(partitions, legacy, manifest, nil, version, configuration)
	if err != nil {
		return errors.WithStack(err)
	}
	return validateSplitPartitionControlSize(control)
}

func validateSplitPartitionControlSize(control kv.MigrationPartitionControl) error {
	encoded, err := kv.EncodeMigrationPartitionControl(control)
	if err != nil {
		return errors.WithStack(err)
	}
	maximum, err := maximumSplitPartitionControlSize(encoded)
	if err != nil {
		return err
	}
	if maximum > kv.MaxMigrationPartitionControlBytes {
		return errors.Wrap(kv.ErrInvalidRequest, "split ownership control exceeds transfer budget")
	}
	return nil
}

// Refresh and membership changes may grow every numeric stamp after this
// preflight. Reserve the full uint64 decimal width at every numeric position,
// not a fixed margin per request. Geometry, origins and digest widths remain
// immutable during those operations. This is sizing only, never an attestation.
func maximumSplitPartitionControlSize(encoded []byte) (int, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	maximum := len(encoded)
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return maximum, nil
		}
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if number, ok := token.(json.Number); ok {
			maximum += splitPartitionMaxUintDigits - len(number.String())
		}
	}
}

func splitPartitionBudgetPriors(view store.MigrationServingState, parent distribution.RouteDescriptor) ([]store.MigrationOwnershipPartition, []store.MigrationOwnershipState) {
	var partitions []store.MigrationOwnershipPartition
	var legacy []store.MigrationOwnershipState
	for _, candidate := range migrationPartitionCandidates(view, view.Partitions) {
		proof := candidate.geometry()
		if !routeRangeIntersects(proof.RouteStart, proof.RouteEnd, parent.Start, parent.End) {
			continue
		}
		if candidate.partition != nil {
			partitions = append(partitions, *candidate.partition)
		} else {
			legacy = append(legacy, *candidate.legacy)
		}
	}
	return partitions, legacy
}

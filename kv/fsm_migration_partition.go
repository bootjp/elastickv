package kv

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"slices"
	"sort"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	MaxMigrationPartitionControlBytes = 4 << 20
	maxMigrationPartitionPriors       = 256
)

// MigrationPartitionControl carries root evidence and exact native prior CASes.
// Neither catalog watchers nor immutable job proofs authorize this operation.
type MigrationPartitionControl struct {
	Transition          store.MigrationOwnershipPartitionTransition
	PriorPartitions     []store.MigrationOwnershipPartition
	PriorOwnership      []store.MigrationOwnershipState
	Manifest            []byte
	UnchangedDescriptor *distribution.RouteDescriptor
}

// DecodeMigrationPartitionJSON accepts only the encoder's exact byte format.
// Re-encoding rejects duplicate keys, case aliases, unknown fields, trailing
// input, insignificant whitespace and alternate number/base64 representations.
func DecodeMigrationPartitionJSON(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > MaxMigrationPartitionControlBytes {
		return errors.WithStack(ErrInvalidRequest)
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return errors.Wrap(ErrInvalidRequest, "invalid partition JSON")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.WithStack(ErrInvalidRequest)
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(raw, canonical) {
		return errors.WithStack(ErrInvalidRequest)
	}
	return nil
}

func EncodeMigrationPartitionJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(raw) > MaxMigrationPartitionControlBytes {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	return raw, nil
}

func DecodeMigrationPartitionControl(raw []byte) (MigrationPartitionControl, error) {
	var c MigrationPartitionControl
	if err := DecodeMigrationPartitionJSON(raw, &c); err != nil {
		return c, errors.WithStack(err)
	}
	expected, err := BuildMigrationPartitionControl(c.PriorPartitions, c.PriorOwnership, c.Manifest,
		c.UnchangedDescriptor, c.Transition.CatalogVersion, c.Transition.ConfigurationIndex)
	if err != nil {
		return c, errors.WithStack(err)
	}
	want, err := EncodeMigrationPartitionJSON(expected)
	if err != nil || !bytes.Equal(want, raw) {
		return c, errors.WithStack(store.ErrMigrationOwnership)
	}
	return c, nil
}

func EncodeMigrationPartitionControl(c MigrationPartitionControl) ([]byte, error) {
	raw, err := EncodeMigrationPartitionJSON(c)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	_, err = DecodeMigrationPartitionControl(raw)
	return raw, err
}

// BuildMigrationPartitionControl is shared by the committed-root producer and
// deterministic data apply. Store apply independently resolves complete prior
// coverage against its coherent durable frontier, including negative records.
func BuildMigrationPartitionControl(partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState,
	manifest []byte, unchanged *distribution.RouteDescriptor, version, configuration uint64) (MigrationPartitionControl, error) {
	c := MigrationPartitionControl{PriorPartitions: partitions, PriorOwnership: legacy, Manifest: manifest, UnchangedDescriptor: unchanged}
	if !migrationPartitionControlEnvelopeValid(version, configuration, len(partitions)+len(legacy)) {
		return c, errors.WithStack(ErrInvalidRequest)
	}
	before, refs, err := migrationPartitionPriorEvidence(partitions, legacy)
	if err != nil {
		return c, errors.WithStack(err)
	}
	parent, routes, hash, operation, err := migrationPartitionRootEvidence(manifest, unchanged, version)
	if err != nil {
		return c, errors.WithStack(err)
	}
	t := store.MigrationOwnershipPartitionTransition{Operation: operation, RootManifestHash: hash,
		Generation: before[0].Generation, LocalGroupID: before[0].LocalGroupID, CatalogVersion: version,
		ConfigurationIndex: configuration, Before: refs}
	if operation == store.MigrationOwnershipPartitionRefresh && len(legacy) != 0 {
		return c, errors.WithStack(store.ErrNotSupported)
	}
	t.After, err = migrationPartitionTiles(before, parent, routes, version, configuration)
	if err != nil {
		return c, errors.WithStack(err)
	}
	if err := stampMigrationPartitionIdentity(&t); err != nil {
		return c, errors.WithStack(err)
	}
	c.Transition = t
	return c, nil
}

func migrationPartitionControlEnvelopeValid(version, configuration uint64, count int) bool {
	return version != 0 && configuration != 0 && migrationPartitionCountValid(count)
}

func migrationPartitionCountValid(count int) bool {
	return count > 0 && count <= maxMigrationPartitionPriors
}

func stampMigrationPartitionIdentity(t *store.MigrationOwnershipPartitionTransition) error {
	identity, err := EncodeMigrationPartitionJSON(struct {
		Root    []byte
		Before  []store.MigrationOwnershipPartitionReference
		Version uint64
	}{t.RootManifestHash, t.Before, t.CatalogVersion})
	if err != nil {
		return errors.WithStack(err)
	}
	digest := sha256.Sum256(identity)
	t.OperationID = "partition/" + hex.EncodeToString(digest[:])
	for i := range t.After {
		if t.Operation == store.MigrationOwnershipPartitionSplit {
			t.After[i].PartitionID, err = store.MigrationOwnershipPartitionIDFor(*t, i)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func migrationPartitionPriorEvidence(partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState) ([]store.MigrationOwnershipPartition, []store.MigrationOwnershipPartitionReference, error) {
	before := slices.Clone(partitions)
	refs := make([]store.MigrationOwnershipPartitionReference, 0, len(partitions)+len(legacy))
	for _, p := range partitions {
		ref, err := store.MigrationOwnershipPartitionReferenceFor(p)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		refs = append(refs, ref)
	}
	for _, p := range legacy {
		ref, err := store.MigrationOwnershipLegacyReferenceFor(p)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		refs = append(refs, ref)
		before = append(before, MigrationPartitionFromLegacy(p))
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].LegacyJobID != refs[j].LegacyJobID {
			return refs[i].LegacyJobID < refs[j].LegacyJobID
		}
		return bytes.Compare(refs[i].PartitionID, refs[j].PartitionID) < 0
	})
	for i := 1; i < len(refs); i++ {
		if refs[i].LegacyJobID == refs[i-1].LegacyJobID && bytes.Equal(refs[i].PartitionID, refs[i-1].PartitionID) {
			return nil, nil, errors.WithStack(store.ErrMigrationOwnership)
		}
	}
	return before, refs, nil
}

// MigrationPartitionFromLegacy preserves the complete operational lineage.
// The original record, not this projection, is used for the legacy CAS hash.
func MigrationPartitionFromLegacy(s store.MigrationOwnershipState) store.MigrationOwnershipPartition {
	p := store.MigrationOwnershipPartition{Generation: s.Generation, LocalGroupID: s.LocalGroupID, OwnerGroupID: s.OwnerGroupID,
		CatalogVersion: s.CatalogVersion, ConfigurationIndex: s.ConfigurationIndex, AppliedIndex: s.AppliedIndex,
		RouteID: s.RouteID, ParentRouteID: s.ParentRouteID, RouteStart: s.RouteStart, RouteEnd: s.RouteEnd,
		DescriptorHash: s.DescriptorHash, Role: s.Role, MinWriteTSExclusive: s.MinWriteTSExclusive,
		WriteFloorKnown: s.WriteFloorKnown || s.MinWriteTSExclusive != 0, Staged: s.Staged, MigrationJobID: s.JobID,
		SourceTrackerVersion: s.SourceTrackerVersion, SourceAppliedIndex: s.SourceAppliedIndex}
	for _, r := range s.Receipts {
		p.Origins = append(p.Origins, store.MigrationOwnershipOperationReference{JobID: s.JobID, Operation: r.Operation,
			CatalogVersion: r.CatalogVersion, ConfigurationIndex: r.ConfigurationIndex, AppliedIndex: r.AppliedIndex,
			DescriptorHash: r.DescriptorHash, Staged: r.Staged})
	}
	return p
}

func migrationPartitionRootEvidence(raw []byte, unchanged *distribution.RouteDescriptor, version uint64) (distribution.RouteDescriptor, []distribution.RouteDescriptor, []byte, store.MigrationOwnershipPartitionOperation, error) {
	var zero distribution.RouteDescriptor
	if len(raw) != 0 && unchanged == nil {
		return migrationPartitionSplitEvidence(raw, version)
	}
	if len(raw) != 0 || unchanged == nil || unchanged.State != distribution.RouteStateActive {
		return zero, nil, nil, 0, errors.WithStack(store.ErrMigrationOwnership)
	}
	descriptor, err := distribution.EncodeRouteDescriptorForCatalogWrite(*unchanged, true)
	if err != nil {
		return zero, nil, nil, 0, errors.WithStack(err)
	}
	identity, err := EncodeMigrationPartitionJSON(struct {
		Domain     string
		Version    uint64
		Descriptor []byte
	}{"elastickv/ownership-partition/unchanged/v1", version, descriptor})
	if err != nil {
		return zero, nil, nil, 0, err
	}
	digest := sha256.Sum256(identity)
	return *unchanged, []distribution.RouteDescriptor{*unchanged}, digest[:], store.MigrationOwnershipPartitionRefresh, nil
}

func migrationPartitionSplitEvidence(raw []byte, version uint64) (distribution.RouteDescriptor, []distribution.RouteDescriptor, []byte, store.MigrationOwnershipPartitionOperation, error) {
	var zero distribution.RouteDescriptor
	m, err := distribution.DecodeOwnershipPartitionManifest(raw)
	if err != nil {
		return zero, nil, nil, 0, errors.WithStack(err)
	}
	canonical, err := distribution.EncodeOwnershipPartitionManifest(m)
	if err != nil || !bytes.Equal(canonical, raw) || m.Version > version {
		return zero, nil, nil, 0, errors.WithStack(store.ErrMigrationOwnership)
	}
	hash, err := distribution.OwnershipPartitionManifestHash(m)
	return m.Parent, m.Children, hash, store.MigrationOwnershipPartitionSplit, errors.WithStack(err)
}

func migrationPartitionTiles(before []store.MigrationOwnershipPartition, parent distribution.RouteDescriptor, routes []distribution.RouteDescriptor, version, configuration uint64) ([]store.MigrationOwnershipPartition, error) {
	parentHash, err := distribution.OwnershipPartitionDescriptorHash(parent)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	negativeOnly := migrationPartitionNegativeBeforeMatchesParent(before, parent) && migrationPartitionNegativeGeometryAllowed(before, routes, parentHash)
	boundaries, err := migrationPartitionBoundaries(before, parent, routes, version, configuration)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var after []store.MigrationOwnershipPartition
	for i, start := range boundaries {
		if len(parent.End) != 0 && bytes.Compare(start, parent.End) >= 0 {
			break
		}
		var end []byte
		if i+1 < len(boundaries) {
			end = boundaries[i+1]
		}
		p, found, err := migrationPartitionIntervalTile(before, parent, parentHash, routes, start, end, version, configuration, negativeOnly)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if !found {
			continue
		}
		after = append(after, p)
	}
	if !migrationPartitionCountValid(len(after)) {
		return nil, errors.WithStack(store.ErrMigrationOwnership)
	}
	return after, nil
}

func migrationPartitionIntervalTile(before []store.MigrationOwnershipPartition, parent distribution.RouteDescriptor, hash []byte, routes []distribution.RouteDescriptor, start, end []byte, version, configuration uint64, negativeOnly bool) (store.MigrationOwnershipPartition, bool, error) {
	p, found, err := migrationPartitionCovering(before, start, end)
	if err != nil || !found {
		return p, found, err
	}
	if !migrationPartitionParentMatches(p, parent, hash, negativeOnly) {
		return p, false, errors.WithStack(store.ErrMigrationOwnership)
	}
	p, err = projectMigrationPartitionTile(p, routes, start, end, version, configuration)
	return p, true, err
}

func migrationPartitionBoundaries(before []store.MigrationOwnershipPartition, parent distribution.RouteDescriptor, routes []distribution.RouteDescriptor, version, configuration uint64) ([][]byte, error) {
	boundaries := [][]byte{parent.Start}
	if len(parent.End) != 0 {
		boundaries = append(boundaries, parent.End)
	}
	for _, p := range before {
		if !migrationPartitionPriorEpochMatches(p, before[0], version, configuration) || !migrationPartitionRangeContained(p, parent.Start, parent.End) {
			return nil, errors.WithStack(store.ErrMigrationOwnership)
		}
		boundaries = append(boundaries, p.RouteStart)
		if len(p.RouteEnd) != 0 {
			boundaries = append(boundaries, p.RouteEnd)
		}
	}
	for _, route := range routes {
		boundaries = append(boundaries, route.Start)
	}
	sort.Slice(boundaries, func(i, j int) bool { return bytes.Compare(boundaries[i], boundaries[j]) < 0 })
	boundaries = slices.CompactFunc(boundaries, bytes.Equal)
	return boundaries, nil
}

func migrationPartitionPriorEpochMatches(p, first store.MigrationOwnershipPartition, version, configuration uint64) bool {
	return p.Generation == first.Generation && p.LocalGroupID == first.LocalGroupID && p.ConfigurationIndex <= configuration && p.CatalogVersion <= version
}

func migrationPartitionRangeContained(p store.MigrationOwnershipPartition, start, end []byte) bool {
	return bytes.Compare(p.RouteStart, start) >= 0 && (len(end) == 0 || (len(p.RouteEnd) != 0 && bytes.Compare(p.RouteEnd, end) <= 0))
}

func migrationPartitionParentMatches(p store.MigrationOwnershipPartition, parent distribution.RouteDescriptor, hash []byte, negativeOnly bool) bool {
	strict := p.RouteID == parent.RouteID && p.OwnerGroupID == parent.GroupID && bytes.Equal(p.DescriptorHash, hash) &&
		p.MinWriteTSExclusive == parent.MinWriteTSExclusive && (p.Role != store.MigrationOwner || p.Staged == parent.StagedVisibilityActive)
	return strict || (negativeOnly && migrationPartitionNegativeParentMatches(p, parent))
}

// MigrationPartitionNegativePriors validates complete native references before
// permitting geometry discovery or a non-authorizing background deferral.
func MigrationPartitionNegativePriors(partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState) bool {
	if !migrationPartitionCountValid(len(partitions) + len(legacy)) {
		return false
	}
	before, _, err := migrationPartitionPriorEvidence(partitions, legacy)
	return err == nil && migrationPartitionNegativeBefore(before)
}

func MigrationPartitionNegativePriorsMatchParent(partitions []store.MigrationOwnershipPartition, legacy []store.MigrationOwnershipState, parent distribution.RouteDescriptor) bool {
	if !migrationPartitionCountValid(len(partitions) + len(legacy)) {
		return false
	}
	before, _, err := migrationPartitionPriorEvidence(partitions, legacy)
	return err == nil && migrationPartitionNegativeBeforeMatchesParent(before, parent)
}

func migrationPartitionNegativeBefore(before []store.MigrationOwnershipPartition) bool {
	if len(before) == 0 {
		return false
	}
	for _, p := range before {
		if !migrationPartitionNegativeRole(p) || p.Generation != before[0].Generation || p.LocalGroupID != before[0].LocalGroupID {
			return false
		}
	}
	return true
}

func migrationPartitionNegativeRole(p store.MigrationOwnershipPartition) bool {
	return p.Role == store.MigrationNonOwner && p.SourceTrackerVersion == 0 && p.SourceAppliedIndex == 0 && !p.Staged && p.WriteFloorKnown
}

func migrationPartitionNegativeBeforeMatchesParent(before []store.MigrationOwnershipPartition, parent distribution.RouteDescriptor) bool {
	if !migrationPartitionNegativeBefore(before) {
		return false
	}
	for _, p := range before {
		if !migrationPartitionNegativeParentMatches(p, parent) {
			return false
		}
	}
	return true
}

func migrationPartitionNegativeParentMatches(p store.MigrationOwnershipPartition, parent distribution.RouteDescriptor) bool {
	return migrationPartitionNegativeRole(p) && parent.GroupID != p.LocalGroupID && parent.MinWriteTSExclusive >= p.MinWriteTSExclusive &&
		p.RouteID == parent.RouteID && p.ParentRouteID == parent.ParentRouteID && bytes.Equal(p.RouteStart, parent.Start) && bytes.Equal(p.RouteEnd, parent.End)
}

func migrationPartitionNegativeGeometryAllowed(before []store.MigrationOwnershipPartition, routes []distribution.RouteDescriptor, parentHash []byte) bool {
	if len(routes) > 1 {
		return true
	}
	// An unchanged refresh may retain a historical owner/floor, but cannot
	// invent a new descriptor hash without a committed geometry manifest.
	for _, p := range before {
		if !bytes.Equal(p.DescriptorHash, parentHash) {
			return false
		}
	}
	return true
}

func projectMigrationPartitionTile(p store.MigrationOwnershipPartition, routes []distribution.RouteDescriptor, start, end []byte, version, configuration uint64) (store.MigrationOwnershipPartition, error) {
	for _, route := range routes {
		if bytes.Compare(start, route.Start) < 0 || (len(route.End) != 0 && (len(end) == 0 || bytes.Compare(end, route.End) > 0)) {
			continue
		}
		p.RouteID, p.ParentRouteID = route.RouteID, route.ParentRouteID
		p.RouteStart, p.RouteEnd = start, end
		p.CatalogVersion, p.ConfigurationIndex, p.AppliedIndex, p.ReceiptID = version, configuration, 0, nil
		hash, err := distribution.OwnershipPartitionDescriptorHash(route)
		p.DescriptorHash = hash
		return p, errors.WithStack(err)
	}
	return p, errors.WithStack(store.ErrMigrationOwnership)
}

func migrationPartitionCovering(before []store.MigrationOwnershipPartition, start, end []byte) (store.MigrationOwnershipPartition, bool, error) {
	var chosen store.MigrationOwnershipPartition
	found := false
	for _, p := range before {
		if bytes.Compare(p.RouteStart, start) > 0 || (len(p.RouteEnd) != 0 && (len(end) == 0 || bytes.Compare(p.RouteEnd, end) < 0)) {
			continue
		}
		if found && p.CatalogVersion == chosen.CatalogVersion {
			return chosen, false, errors.WithStack(store.ErrMigrationOwnership)
		}
		if !found || p.CatalogVersion > chosen.CatalogVersion {
			chosen, found = p, true
		}
	}
	return chosen, found, nil
}

func ValidateMigrationPartitionRequest(req *pb.CleanupMigrationRequest) error {
	if req.GetMode() != pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_PARTITION ||
		!req.GetRequireConfigurationIndex() || req.GetExpectedConfigurationIndex() == 0 || req.GetOwnership() != nil {
		return errors.WithStack(ErrInvalidRequest)
	}
	c, err := DecodeMigrationPartitionControl(req.GetPartitionTransition())
	if err != nil {
		return errors.WithStack(err)
	}
	if c.Transition.ConfigurationIndex != req.GetExpectedConfigurationIndex() {
		return errors.WithStack(ErrMigrationConfigurationChanged)
	}
	return nil
}

func (f *kvFSM) applyMigrationPartitionControl(ctx context.Context, req *pb.CleanupMigrationRequest) any {
	if err := ValidateMigrationPartitionRequest(req); err != nil {
		return errors.WithStack(err)
	}
	c, err := DecodeMigrationPartitionControl(req.GetPartitionTransition())
	if err != nil {
		return errors.WithStack(err)
	}
	if f.shardGroupID == 0 || c.Transition.LocalGroupID != f.shardGroupID {
		return errors.WithStack(ErrInvalidRequest)
	}
	if err := f.verifyMigrationPartitionApplyConfiguration(ctx, c.Transition.ConfigurationIndex); err != nil {
		return err
	}
	writer, ok := f.store.(store.MigrationOwnershipPartitionStore)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	err = writer.ApplyMigrationOwnershipPartition(ctx, c.Transition, f.pendingApplyIdx)
	if err == nil {
		return store.CleanupVersionsResult{Done: true}
	}
	if migrationOwnershipOrdinaryError(err) {
		return errors.WithStack(err)
	}
	return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationCleanupApply), "kv/fsm: partition control"))
}

func (f *kvFSM) verifyMigrationPartitionApplyConfiguration(ctx context.Context, expected uint64) any {
	durable, err := MigrationConfigurationIndex(ctx, f.store)
	if err != nil {
		return haltErr(errors.Mark(err, ErrMigrationCleanupApply))
	}
	if durable == 0 || durable != expected || durable != f.AppliedConfigurationIndex() {
		return errors.WithStack(ErrMigrationConfigurationChanged)
	}
	return nil
}

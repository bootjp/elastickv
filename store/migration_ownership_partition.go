package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"sort"

	"github.com/cockroachdb/errors"
)

func partitionHasEvidence(s MigrationOwnershipPartitionState) bool {
	return len(s.Partitions)+len(s.Receipts) != 0
}

func validPartition(p MigrationOwnershipPartition) bool {
	return validPartitionIdentity(p) && validPartitionBoundary(p) && validPartitionRole(p) && validPartitionSource(p) && validPartitionOrigins(p)
}

func validPartitionIdentity(p MigrationOwnershipPartition) bool {
	if len(p.PartitionID) != sha256.Size || len(p.ReceiptID) != sha256.Size ||
		len(p.Generation) < 16 || len(p.Generation) > 128 || p.LocalGroupID == 0 || p.OwnerGroupID == 0 {
		return false
	}
	return p.RouteID != 0 && p.MigrationJobID != 0 && len(p.DescriptorHash) == sha256.Size
}

func validPartitionBoundary(p MigrationOwnershipPartition) bool {
	return p.CatalogVersion != 0 && p.ConfigurationIndex != 0 && p.AppliedIndex != 0 && p.WriteFloorKnown &&
		(len(p.RouteEnd) == 0 || bytes.Compare(p.RouteStart, p.RouteEnd) < 0)
}

func validPartitionRole(p MigrationOwnershipPartition) bool {
	if (p.Role == MigrationOwner && p.LocalGroupID != p.OwnerGroupID) ||
		(p.Role == MigrationNonOwner && (p.LocalGroupID == p.OwnerGroupID || p.Staged)) ||
		(p.Role != MigrationOwner && p.Role != MigrationNonOwner) {
		return false
	}
	return true
}

func validPartitionSource(p MigrationOwnershipPartition) bool {
	if !validPartitionSourceIdentity(p) {
		return false
	}
	if p.SourceTrackerVersion == 0 {
		return true
	}
	for _, origin := range p.Origins {
		if origin.JobID == p.MigrationJobID && origin.Operation == MigrationOwnershipAcquire && origin.AppliedIndex == p.SourceAppliedIndex {
			return true
		}
	}
	return false
}

func validPartitionSourceIdentity(p MigrationOwnershipPartition) bool {
	return (p.SourceTrackerVersion == 0) == (p.SourceAppliedIndex == 0) && p.SourceAppliedIndex <= p.AppliedIndex &&
		(p.SourceTrackerVersion == 0 || (p.Role == MigrationOwner && !p.Staged))
}

func validPartitionOrigins(p MigrationOwnershipPartition) bool {
	if len(p.Origins) == 0 {
		return false
	}
	seen := make(map[string]bool)
	for _, origin := range p.Origins {
		if !validPartitionOrigin(p, origin) {
			return false
		}
		data, _ := json.Marshal(origin)
		if seen[string(data)] {
			return false
		}
		seen[string(data)] = true
	}
	return true
}

func validPartitionOrigin(p MigrationOwnershipPartition, origin MigrationOwnershipOperationReference) bool {
	return validPartitionOriginIdentity(origin) && origin.CatalogVersion <= p.CatalogVersion &&
		origin.ConfigurationIndex <= p.ConfigurationIndex && origin.AppliedIndex != 0 && origin.AppliedIndex <= p.AppliedIndex &&
		len(origin.DescriptorHash) == sha256.Size
}

func validPartitionOriginIdentity(origin MigrationOwnershipOperationReference) bool {
	return origin.JobID != 0 && (origin.Operation == MigrationOwnershipAcquire || origin.Operation == MigrationOwnershipClear) &&
		origin.CatalogVersion != 0 && origin.ConfigurationIndex != 0
}

func samePartition(a, b MigrationOwnershipPartition) bool {
	x, _ := json.Marshal(clonePartition(a))
	y, _ := json.Marshal(clonePartition(b))
	return bytes.Equal(x, y)
}

func MigrationOwnershipPartitionEvidenceValid(s MigrationOwnershipPartitionState, p MigrationOwnershipPartition) bool {
	if s.Generation != p.Generation || !validPartition(p) || !partitionIsActive(s, p) {
		return false
	}
	for _, r := range s.Receipts {
		if !bytes.Equal(r.ReceiptID, p.ReceiptID) || !validPartitionReceipt(r) {
			continue
		}
		for _, candidate := range r.Transition.After {
			if samePartition(p, candidate) {
				return true
			}
		}
	}
	return false
}

func partitionIsActive(s MigrationOwnershipPartitionState, p MigrationOwnershipPartition) bool {
	for _, active := range s.Partitions {
		if samePartition(active, p) {
			return true
		}
	}
	return false
}

func validPartitionReceipt(r MigrationOwnershipPartitionReceipt) bool {
	t := r.Transition
	if !validPartitionReceiptHeader(r) {
		return false
	}
	id, err := MigrationOwnershipPartitionReceiptIDFor(t)
	if err != nil || !bytes.Equal(id, r.ReceiptID) {
		return false
	}
	for _, p := range t.After {
		if !validPartition(p) || !partitionReceiptStampMatches(r, p) {
			return false
		}
	}
	return true
}

func validPartitionReceiptHeader(r MigrationOwnershipPartitionReceipt) bool {
	t := r.Transition
	return r.AppliedIndex != 0 && t.CatalogVersion != 0 && t.ConfigurationIndex != 0 && len(t.After) > 0 && validPartitionReceiptOperation(t)
}

func validPartitionReceiptOperation(t MigrationOwnershipPartitionTransition) bool {
	if t.Operation == MigrationOwnershipPartitionJobOperation {
		return len(t.RootManifestHash) == 0
	}
	return (t.Operation == MigrationOwnershipPartitionSplit || t.Operation == MigrationOwnershipPartitionRefresh) && len(t.RootManifestHash) == sha256.Size
}

func partitionReceiptStampMatches(r MigrationOwnershipPartitionReceipt, p MigrationOwnershipPartition) bool {
	t := r.Transition
	return p.Generation == t.Generation && p.LocalGroupID == t.LocalGroupID && p.CatalogVersion == t.CatalogVersion &&
		p.ConfigurationIndex == t.ConfigurationIndex && p.AppliedIndex == r.AppliedIndex && bytes.Equal(p.ReceiptID, r.ReceiptID)
}

func encodeMigrationOwnershipPartitions(s MigrationOwnershipPartitionState) ([]byte, error) {
	data, err := json.Marshal(clonePartitionState(s))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return append([]byte{migrationMetadataVersion}, data...), nil
}

func decodeMigrationOwnershipPartitions(data []byte) (MigrationOwnershipPartitionState, error) {
	var s MigrationOwnershipPartitionState
	if len(data) == 0 || data[0] != migrationMetadataVersion {
		return s, errors.New("invalid ownership partition encoding")
	}
	if err := json.Unmarshal(data[1:], &s); err != nil {
		return s, errors.Wrap(err, "decode ownership partition")
	}
	if err := validatePartitionReceipts(s); err != nil {
		return s, err
	}
	s = clonePartitionState(s)
	return s, validatePartitionFrontier(s)
}

func validatePartitionReceipts(s MigrationOwnershipPartitionState) error {
	seen := make(map[string]bool)
	for _, r := range s.Receipts {
		if r.Transition.Generation != s.Generation || seen[r.Transition.OperationID] || !validPartitionReceipt(r) {
			return errors.New("invalid ownership partition receipt")
		}
		seen[r.Transition.OperationID] = true
	}
	return nil
}

func partitionFrontierProofs(s MigrationOwnershipPartitionState) (map[string]struct{}, error) {
	proofs := make(map[string]struct{})
	for _, r := range s.Receipts {
		for _, p := range r.Transition.After {
			key, err := partitionDigest("elastickv/ownership-partition/frontier/v1", clonePartition(p))
			if err != nil {
				return nil, err
			}
			proofs[string(key)] = struct{}{}
		}
	}
	return proofs, nil
}

func validatePartitionFrontier(s MigrationOwnershipPartitionState) error {
	// decode validates every receipt once before indexing its outputs, never
	// rehashing historical transitions once per active partition.
	proofs, err := partitionFrontierProofs(s)
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for i, p := range s.Partitions {
		key, err := partitionDigest("elastickv/ownership-partition/frontier/v1", clonePartition(p))
		if err != nil {
			return err
		}
		_, supported := proofs[string(key)]
		if p.Generation != s.Generation || !validPartition(p) || !supported || seen[string(p.PartitionID)] ||
			(i > 0 && rangesOverlap(s.Partitions[i-1], p)) {
			return errors.New("invalid ownership partition frontier")
		}
		seen[string(p.PartitionID)] = true
	}
	return nil
}

func rangesOverlap(a, b MigrationOwnershipPartition) bool {
	return (len(a.RouteEnd) == 0 || bytes.Compare(b.RouteStart, a.RouteEnd) < 0) &&
		(len(b.RouteEnd) == 0 || bytes.Compare(a.RouteStart, b.RouteEnd) < 0)
}

func partitionFromLegacy(s MigrationOwnershipState) MigrationOwnershipPartition {
	p := MigrationOwnershipPartition{Generation: s.Generation, LocalGroupID: s.LocalGroupID, OwnerGroupID: s.OwnerGroupID,
		CatalogVersion: s.CatalogVersion, ConfigurationIndex: s.ConfigurationIndex, AppliedIndex: s.AppliedIndex,
		RouteID: s.RouteID, ParentRouteID: s.ParentRouteID, RouteStart: bytes.Clone(s.RouteStart), RouteEnd: bytes.Clone(s.RouteEnd),
		DescriptorHash: bytes.Clone(s.DescriptorHash), Role: s.Role, MinWriteTSExclusive: s.MinWriteTSExclusive,
		WriteFloorKnown: ownershipWriteFloorKnown(s), Staged: s.Staged, MigrationJobID: s.JobID,
		SourceTrackerVersion: s.SourceTrackerVersion, SourceAppliedIndex: s.SourceAppliedIndex}
	for _, r := range s.Receipts {
		p.Origins = append(p.Origins, MigrationOwnershipOperationReference{JobID: s.JobID, Operation: r.Operation,
			CatalogVersion: r.CatalogVersion, ConfigurationIndex: r.ConfigurationIndex, AppliedIndex: r.AppliedIndex,
			DescriptorHash: bytes.Clone(r.DescriptorHash), Staged: r.Staged})
	}
	return clonePartition(p)
}

type partitionCandidate struct {
	partition MigrationOwnershipPartition
	reference MigrationOwnershipPartitionReference
}

func partitionCandidates(s MigrationOwnershipPartitionState, legacy []MigrationOwnershipState) ([]partitionCandidate, error) {
	var out []partitionCandidate
	consumed := partitionAdoptedJobs(s)
	for _, p := range s.Partitions {
		ref, err := MigrationOwnershipPartitionReferenceFor(p)
		if err != nil {
			return nil, err
		}
		out = append(out, partitionCandidate{p, ref})
	}
	for _, p := range legacy {
		if consumed[p.JobID] && MigrationOwnershipEvidenceValid(p) {
			continue
		}
		// Unknown legacy evidence is kept as a negative candidate. It cannot be
		// omitted or used to seed a replacement merely because validation fails.
		ref, _ := MigrationOwnershipLegacyReferenceFor(p)
		ref.LegacyJobID = p.JobID
		out = append(out, partitionCandidate{partitionFromLegacy(p), ref})
	}
	return out, nil
}

func equalPartitionReference(a, b MigrationOwnershipPartitionReference) bool {
	return a.LegacyJobID == b.LegacyJobID && bytes.Equal(a.PartitionID, b.PartitionID) &&
		len(a.AttestationHash) == sha256.Size && bytes.Equal(a.AttestationHash, b.AttestationHash)
}

func inheritedPartition(a, b MigrationOwnershipPartition) bool {
	a = clonePartition(a)
	b = clonePartition(b)
	a.PartitionID, b.PartitionID = nil, nil
	a.ReceiptID, b.ReceiptID = nil, nil
	a.CatalogVersion, b.CatalogVersion = 0, 0
	a.ConfigurationIndex, b.ConfigurationIndex = 0, 0
	a.AppliedIndex, b.AppliedIndex = 0, 0
	a.RouteID, b.RouteID = 0, 0
	a.ParentRouteID, b.ParentRouteID = 0, 0
	a.RouteStart, b.RouteStart = nil, nil
	a.RouteEnd, b.RouteEnd = nil, nil
	a.DescriptorHash, b.DescriptorHash = nil, nil
	return samePartition(a, b)
}

// Check each elementary interval, including the two unbounded intervals. This
// compares complete unions even when old job records overlay one another.
func validatePartitionUnion(before, after []MigrationOwnershipPartition) error {
	boundaries := partitionUnionBoundaries(before, after)
	for i, start := range boundaries {
		var end []byte
		if i+1 < len(boundaries) {
			end = boundaries[i+1]
		}
		if !partitionIntervalMatches(before, after, start, end) {
			return ErrMigrationOwnership
		}
	}
	return nil
}

func partitionUnionBoundaries(before, after []MigrationOwnershipPartition) [][]byte {
	boundaries := [][]byte{nil}
	for _, list := range [][]MigrationOwnershipPartition{before, after} {
		for _, p := range list {
			if len(p.RouteStart) != 0 {
				boundaries = append(boundaries, p.RouteStart)
			}
			if len(p.RouteEnd) != 0 {
				boundaries = append(boundaries, p.RouteEnd)
			}
		}
	}
	sort.Slice(boundaries, func(i, j int) bool { return bytes.Compare(boundaries[i], boundaries[j]) < 0 })
	return slices.CompactFunc(boundaries, bytes.Equal)
}

func partitionIntervalMatches(before, after []MigrationOwnershipPartition, start, end []byte) bool {
	old, oldCount, ambiguous := partitionCovering(before, start, end)
	child, count, _ := partitionCovering(after, start, end)
	if ambiguous || count > 1 || (oldCount == 0) != (count == 0) {
		return false
	}
	return count == 0 || (inheritedPartition(old, child) && child.CatalogVersion >= old.CatalogVersion &&
		child.ConfigurationIndex >= old.ConfigurationIndex && child.AppliedIndex >= old.AppliedIndex)
}

func partitionCovering(list []MigrationOwnershipPartition, start, end []byte) (MigrationOwnershipPartition, int, bool) {
	var chosen MigrationOwnershipPartition
	count := 0
	ambiguous := false
	for _, p := range list {
		if bytes.Compare(p.RouteStart, start) > 0 || (len(p.RouteEnd) != 0 && (len(end) == 0 || bytes.Compare(p.RouteEnd, end) < 0)) {
			continue
		}
		count++
		if chosen.CatalogVersion == 0 || p.CatalogVersion > chosen.CatalogVersion {
			chosen, ambiguous = p, false
		} else if p.CatalogVersion == chosen.CatalogVersion && !samePartition(p, chosen) {
			ambiguous = true
		}
	}
	return chosen, count, ambiguous
}

func applyPartitionTransition(s MigrationOwnershipPartitionState, legacy []MigrationOwnershipState, t MigrationOwnershipPartitionTransition, index, configuration uint64) (MigrationOwnershipPartitionState, error) {
	t = canonicalPartitionTransition(t)
	id, err := MigrationOwnershipPartitionReceiptIDFor(t)
	if err != nil || !validPartitionTransitionEnvelope(s, t, index) {
		return s, ErrMigrationOwnership
	}
	if replay, replayErr := partitionTransitionReplay(s, t.OperationID, id); replay || replayErr != nil {
		return clonePartitionState(s), replayErr
	}
	if configuration == 0 || configuration != t.ConfigurationIndex {
		return s, ErrMigrationOwnership
	}
	before, retained, err := resolvePartitionBefore(s, legacy, t)
	if err != nil {
		return s, err
	}
	if err := validatePartitionChildren(&t, before, index, id); err != nil {
		return s, err
	}
	if err := validatePartitionUnion(before, t.After); err != nil {
		return s, err
	}
	return appendPartitionReceipt(s, retained, t, index, id)
}

func validPartitionTransitionEnvelope(s MigrationOwnershipPartitionState, t MigrationOwnershipPartitionTransition, index uint64) bool {
	return (t.Operation == MigrationOwnershipPartitionSplit || t.Operation == MigrationOwnershipPartitionRefresh) &&
		len(t.RootManifestHash) == sha256.Size && len(t.Before) > 0 && len(t.After) > 0 && index != 0 &&
		(!partitionHasEvidence(s) || s.Generation == t.Generation)
}

func partitionTransitionReplay(s MigrationOwnershipPartitionState, operation string, id []byte) (bool, error) {
	for _, receipt := range s.Receipts {
		if receipt.Transition.OperationID != operation {
			continue
		}
		if !bytes.Equal(id, receipt.ReceiptID) {
			return false, ErrMigrationOwnership
		}
		return true, nil
	}
	return false, nil
}

func resolvePartitionBefore(s MigrationOwnershipPartitionState, legacy []MigrationOwnershipState, t MigrationOwnershipPartitionTransition) ([]MigrationOwnershipPartition, []MigrationOwnershipPartition, error) {
	candidates, err := partitionCandidates(s, legacy)
	if err != nil {
		return nil, nil, err
	}
	var before, retained []MigrationOwnershipPartition
	used := make([]bool, len(t.Before))
	for _, c := range candidates {
		matched, matchErr := matchPartitionReference(t.Before, c.reference, used)
		if matchErr != nil {
			return nil, nil, matchErr
		}
		if intersectsPartitionList(c.partition, t.After) != matched {
			return nil, nil, ErrMigrationOwnership
		}
		if matched {
			if c.partition.Generation != t.Generation || c.partition.LocalGroupID != t.LocalGroupID {
				return nil, nil, ErrMigrationOwnership
			}
			before = append(before, c.partition)
		} else if c.reference.LegacyJobID == 0 {
			retained = append(retained, c.partition)
		}
	}
	if slices.Contains(used, false) {
		return nil, nil, ErrMigrationOwnership
	}
	return before, retained, nil
}

func matchPartitionReference(refs []MigrationOwnershipPartitionReference, candidate MigrationOwnershipPartitionReference, used []bool) (bool, error) {
	matched := false
	for i, ref := range refs {
		if !equalPartitionReference(ref, candidate) {
			continue
		}
		if used[i] || matched {
			return false, ErrMigrationOwnership
		}
		used[i], matched = true, true
	}
	return matched, nil
}

func intersectsPartitionList(p MigrationOwnershipPartition, list []MigrationOwnershipPartition) bool {
	for _, other := range list {
		if rangesOverlap(p, other) {
			return true
		}
	}
	return false
}

func validatePartitionChildren(t *MigrationOwnershipPartitionTransition, before []MigrationOwnershipPartition, index uint64, id []byte) error {
	for i := range t.After {
		p := &t.After[i]
		if p.Generation != t.Generation || p.LocalGroupID != t.LocalGroupID || p.CatalogVersion != t.CatalogVersion || p.ConfigurationIndex != t.ConfigurationIndex {
			return ErrMigrationOwnership
		}
		p.AppliedIndex, p.ReceiptID = index, bytes.Clone(id)
		if !validPartition(*p) {
			return ErrMigrationOwnership
		}
		if !partitionChildIdentityMatches(*t, i, before) {
			return ErrMigrationOwnership
		}
	}
	return nil
}

func partitionChildIdentityMatches(t MigrationOwnershipPartitionTransition, i int, before []MigrationOwnershipPartition) bool {
	p := t.After[i]
	if t.Operation != MigrationOwnershipPartitionRefresh {
		expected, err := MigrationOwnershipPartitionIDFor(t, i)
		return err == nil && bytes.Equal(p.PartitionID, expected)
	}
	for _, old := range before {
		a := clonePartition(old)
		a.CatalogVersion, a.ConfigurationIndex, a.AppliedIndex, a.ReceiptID = p.CatalogVersion, p.ConfigurationIndex, p.AppliedIndex, p.ReceiptID
		if samePartition(a, p) {
			return true
		}
	}
	return false
}

func appendPartitionReceipt(s MigrationOwnershipPartitionState, retained []MigrationOwnershipPartition, t MigrationOwnershipPartitionTransition, index uint64, id []byte) (MigrationOwnershipPartitionState, error) {
	s = clonePartitionState(s)
	s.Generation = t.Generation
	s.Partitions = retained
	s.Partitions = append(s.Partitions, t.After...)
	s.Receipts = append(s.Receipts, MigrationOwnershipPartitionReceipt{ReceiptID: id, Transition: t, AppliedIndex: index})
	data, err := encodeMigrationOwnershipPartitions(s)
	if err != nil {
		return s, err
	}
	return decodeMigrationOwnershipPartitions(data)
}

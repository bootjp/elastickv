package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"sort"

	"github.com/cockroachdb/errors"
)

var migrationOwnershipPartitionMetaKey = []byte("!migstage|ownership_partition")

func MigrationOwnershipPartitionMetadataKey() []byte {
	return bytes.Clone(migrationOwnershipPartitionMetaKey)
}

func IsMigrationOwnershipPartitionMetadataKey(key []byte) bool {
	return bytes.Equal(key, migrationOwnershipPartitionMetaKey)
}

func partitionDigest(domain string, value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sum := sha256.Sum256(append([]byte(domain+"\x00"), data...))
	return bytes.Clone(sum[:]), nil
}

func clonePartition(p MigrationOwnershipPartition) MigrationOwnershipPartition {
	p.PartitionID = bytes.Clone(p.PartitionID)
	p.ReceiptID = bytes.Clone(p.ReceiptID)
	p.RouteStart = bytes.Clone(p.RouteStart)
	p.RouteEnd = bytes.Clone(p.RouteEnd)
	p.DescriptorHash = bytes.Clone(p.DescriptorHash)
	p.Origins = append([]MigrationOwnershipOperationReference(nil), p.Origins...)
	for i := range p.Origins {
		p.Origins[i].DescriptorHash = bytes.Clone(p.Origins[i].DescriptorHash)
	}
	sort.Slice(p.Origins, func(i, j int) bool {
		a, _ := json.Marshal(p.Origins[i])
		b, _ := json.Marshal(p.Origins[j])
		return bytes.Compare(a, b) < 0
	})
	return p
}

func canonicalPartitionTransition(t MigrationOwnershipPartitionTransition) MigrationOwnershipPartitionTransition {
	t.RootManifestHash = bytes.Clone(t.RootManifestHash)
	t.Before = append([]MigrationOwnershipPartitionReference(nil), t.Before...)
	for i := range t.Before {
		t.Before[i].PartitionID = bytes.Clone(t.Before[i].PartitionID)
		t.Before[i].AttestationHash = bytes.Clone(t.Before[i].AttestationHash)
	}
	sort.Slice(t.Before, func(i, j int) bool {
		if t.Before[i].LegacyJobID != t.Before[j].LegacyJobID {
			return t.Before[i].LegacyJobID < t.Before[j].LegacyJobID
		}
		return bytes.Compare(t.Before[i].PartitionID, t.Before[j].PartitionID) < 0
	})
	t.After = append([]MigrationOwnershipPartition(nil), t.After...)
	for i := range t.After {
		t.After[i] = clonePartition(t.After[i])
	}
	sort.Slice(t.After, func(i, j int) bool {
		if c := bytes.Compare(t.After[i].RouteStart, t.After[j].RouteStart); c != 0 {
			return c < 0
		}
		return bytes.Compare(t.After[i].PartitionID, t.After[j].PartitionID) < 0
	})
	return t
}

func clonePartitionState(s MigrationOwnershipPartitionState) MigrationOwnershipPartitionState {
	s.Partitions = append([]MigrationOwnershipPartition(nil), s.Partitions...)
	for i := range s.Partitions {
		s.Partitions[i] = clonePartition(s.Partitions[i])
	}
	sort.Slice(s.Partitions, func(i, j int) bool {
		return bytes.Compare(s.Partitions[i].RouteStart, s.Partitions[j].RouteStart) < 0
	})
	s.Receipts = append([]MigrationOwnershipPartitionReceipt(nil), s.Receipts...)
	for i := range s.Receipts {
		s.Receipts[i].ReceiptID = bytes.Clone(s.Receipts[i].ReceiptID)
		s.Receipts[i].Transition = canonicalPartitionTransition(s.Receipts[i].Transition)
	}
	sort.Slice(s.Receipts, func(i, j int) bool {
		return bytes.Compare(s.Receipts[i].ReceiptID, s.Receipts[j].ReceiptID) < 0
	})
	return s
}

func MigrationOwnershipPartitionReferenceFor(p MigrationOwnershipPartition) (MigrationOwnershipPartitionReference, error) {
	if !validPartition(p) {
		return MigrationOwnershipPartitionReference{}, ErrMigrationOwnership
	}
	hash, err := partitionDigest("elastickv/ownership-partition/attestation/v1", clonePartition(p))
	return MigrationOwnershipPartitionReference{PartitionID: bytes.Clone(p.PartitionID), AttestationHash: hash}, err
}

func MigrationOwnershipLegacyReferenceFor(p MigrationOwnershipState) (MigrationOwnershipPartitionReference, error) {
	if !MigrationOwnershipEvidenceValid(p) {
		return MigrationOwnershipPartitionReference{}, ErrMigrationOwnership
	}
	hash, err := partitionDigest("elastickv/ownership-partition/legacy/v1", cloneMigrationOwnership([]MigrationOwnershipState{p})[0])
	return MigrationOwnershipPartitionReference{LegacyJobID: p.JobID, AttestationHash: hash}, err
}

func MigrationOwnershipPartitionIDFor(t MigrationOwnershipPartitionTransition, childIndex int) ([]byte, error) {
	if childIndex < 0 || childIndex >= len(t.After) || t.OperationID == "" || t.Generation == "" || t.LocalGroupID == 0 {
		return nil, ErrMigrationOwnership
	}
	child := t.After[childIndex]
	if t.Operation == MigrationOwnershipPartitionRefresh {
		if len(child.PartitionID) != sha256.Size {
			return nil, ErrMigrationOwnership
		}
		return bytes.Clone(child.PartitionID), nil
	}
	parents := canonicalPartitionTransition(t).Before
	for i := range parents {
		if parents[i].LegacyJobID == 0 {
			parents[i].AttestationHash = nil
		}
	}
	return partitionDigest("elastickv/ownership-partition/id/v1", struct {
		Generation string
		Group      uint64
		Operation  string
		Parents    []MigrationOwnershipPartitionReference
		Route      uint64
		Start, End []byte
	}{t.Generation, t.LocalGroupID, t.OperationID, parents, child.RouteID, child.RouteStart, child.RouteEnd})
}

func MigrationOwnershipPartitionReceiptIDFor(t MigrationOwnershipPartitionTransition) ([]byte, error) {
	t = canonicalPartitionTransition(t)
	if t.OperationID == "" || t.Generation == "" || t.LocalGroupID == 0 {
		return nil, ErrMigrationOwnership
	}
	for i := range t.After {
		t.After[i].AppliedIndex = 0
		t.After[i].ReceiptID = nil
	}
	return partitionDigest("elastickv/ownership-partition/receipt/v1", t)
}

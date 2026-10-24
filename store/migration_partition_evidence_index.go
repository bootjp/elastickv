package store

import (
	"bytes"
	"crypto/sha256"
)

type partitionEvidenceEntry struct {
	digest   [sha256.Size]byte
	position int
}

// This index attests immutable durable support, not mutable caller receipt
// copies. Both the supplied proof and its current active member must still
// match the sealed canonical attestation; knowing an ID cannot grant authority.
type partitionEvidenceIndex struct {
	generation string
	active     map[string]partitionEvidenceEntry
}

type partitionEvidenceKey struct {
	receipt, partition string
}

func partitionEvidenceDigest(p MigrationOwnershipPartition) [sha256.Size]byte {
	// MigrationOwnershipPartition has no custom marshalers or unsupported fields.
	digest, _ := partitionDigest("elastickv/ownership-partition/attestation/v1", clonePartition(p))
	return [sha256.Size]byte(digest)
}

func partitionEvidenceSupport(receipts []MigrationOwnershipPartitionReceipt) map[partitionEvidenceKey][sha256.Size]byte {
	support := make(map[partitionEvidenceKey][sha256.Size]byte)
	for _, receipt := range receipts {
		if !validPartitionReceipt(receipt) {
			continue
		}
		for _, p := range receipt.Transition.After {
			key := partitionEvidenceKey{string(receipt.ReceiptID), string(p.PartitionID)}
			support[key] = partitionEvidenceDigest(p)
		}
	}
	return support
}

func buildPartitionEvidenceIndex(state MigrationOwnershipPartitionState) *partitionEvidenceIndex {
	if len(state.Partitions) == 0 {
		return nil
	}
	support := partitionEvidenceSupport(state.Receipts)
	index := &partitionEvidenceIndex{generation: state.Generation, active: make(map[string]partitionEvidenceEntry, len(state.Partitions))}
	for position, p := range state.Partitions {
		if p.Generation != state.Generation || !validPartition(p) {
			continue
		}
		digest := partitionEvidenceDigest(p)
		if supported, ok := support[partitionEvidenceKey{string(p.ReceiptID), string(p.PartitionID)}]; !ok || supported != digest {
			continue
		}
		id := string(p.PartitionID)
		if _, duplicate := index.active[id]; duplicate {
			position = -1
		}
		index.active[id] = partitionEvidenceEntry{digest: digest, position: position}
	}
	return index
}

func (i *partitionEvidenceIndex) valid(state MigrationOwnershipPartitionState, p MigrationOwnershipPartition) bool {
	if state.Generation != i.generation || p.Generation != i.generation {
		return false
	}
	entry, ok := i.active[string(p.PartitionID)]
	if !ok || entry.position < 0 || partitionEvidenceDigest(p) != entry.digest {
		return false
	}
	return partitionEvidenceMemberValid(state.Partitions, p.PartitionID, entry)
}

func partitionEvidenceMemberValid(partitions []MigrationOwnershipPartition, id []byte, entry partitionEvidenceEntry) bool {
	if entry.position < len(partitions) && bytes.Equal(partitions[entry.position].PartitionID, id) {
		return partitionEvidenceDigest(partitions[entry.position]) == entry.digest
	}
	// Caller reordering/truncation cannot introduce new authority. Only this
	// exceptional, modified-view path needs to search for an original member.
	for _, active := range partitions {
		if bytes.Equal(active.PartitionID, id) && partitionEvidenceDigest(active) == entry.digest {
			return true
		}
	}
	return false
}

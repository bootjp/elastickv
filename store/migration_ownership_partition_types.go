package store

import "context"

// MigrationOwnershipOperationReference binds a partition to an immutable job
// operation. Partitioning never rewrites the referenced receipt's descriptor.
type MigrationOwnershipOperationReference struct {
	JobID              uint64
	Operation          MigrationOwnershipOperation
	CatalogVersion     uint64
	ConfigurationIndex uint64
	AppliedIndex       uint64
	DescriptorHash     []byte
	Staged             bool
}

// MigrationOwnershipPartition is active serving evidence, not a job record.
// PartitionID remains stable across unchanged-descriptor attestations; several
// child partitions may share a job receipt or staged-data lineage.
type MigrationOwnershipPartition struct {
	PartitionID          []byte
	ReceiptID            []byte
	Generation           string
	LocalGroupID         uint64
	OwnerGroupID         uint64
	CatalogVersion       uint64
	ConfigurationIndex   uint64
	AppliedIndex         uint64
	RouteID              uint64
	ParentRouteID        uint64
	RouteStart           []byte
	RouteEnd             []byte
	DescriptorHash       []byte
	Role                 MigrationOwnershipRole
	MinWriteTSExclusive  uint64
	WriteFloorKnown      bool
	Staged               bool
	MigrationJobID       uint64
	SourceTrackerVersion uint64
	SourceAppliedIndex   uint64
	Origins              []MigrationOwnershipOperationReference
}

// MigrationOwnershipPartitionReference is an exact prior-attestation CAS.
// Legacy references set LegacyJobID and leave PartitionID empty; they bind the
// complete old job record and do not turn JobID into a child partition key.
type MigrationOwnershipPartitionReference struct {
	PartitionID     []byte
	AttestationHash []byte
	LegacyJobID     uint64
}

type MigrationOwnershipPartitionOperation uint8

const (
	MigrationOwnershipPartitionSplit MigrationOwnershipPartitionOperation = iota + 1
	MigrationOwnershipPartitionRefresh
	// MigrationOwnershipPartitionJobOperation is an internal receipt kind for
	// validated Acquire/Clear operations. External partition controls reject it.
	MigrationOwnershipPartitionJobOperation
)

// MigrationOwnershipPartitionTransition replaces exactly the complete Before
// union. Split preserves ownership and lineage; Refresh preserves geometry too.
// RootManifestHash identifies committed root evidence, not a proposed version.
type MigrationOwnershipPartitionTransition struct {
	OperationID        string
	Operation          MigrationOwnershipPartitionOperation
	RootManifestHash   []byte
	Generation         string
	LocalGroupID       uint64
	CatalogVersion     uint64
	ConfigurationIndex uint64
	Before             []MigrationOwnershipPartitionReference
	After              []MigrationOwnershipPartition
}

// MigrationOwnershipPartitionReceipt is immutable operation history. It remains
// after its child frontier is replaced and after split-job history is collected.
type MigrationOwnershipPartitionReceipt struct {
	ReceiptID    []byte
	Transition   MigrationOwnershipPartitionTransition
	AppliedIndex uint64
}

// MigrationOwnershipPartitionState is the independently persisted frontier and
// receipt container. It never replaces the original per-job ownership ledger.
type MigrationOwnershipPartitionState struct {
	Generation string
	Partitions []MigrationOwnershipPartition
	Receipts   []MigrationOwnershipPartitionReceipt
}

// MigrationOwnershipPartitionStore atomically replaces serving evidence,
// records its immutable receipt, and advances the supplied Raft applied index.
// Semantic, generation and configuration mismatches refuse before side effects.
type MigrationOwnershipPartitionStore interface {
	MigrationOwnershipPartitions(context.Context) (MigrationOwnershipPartitionState, error)
	ApplyMigrationOwnershipPartition(context.Context, MigrationOwnershipPartitionTransition, uint64) error
}

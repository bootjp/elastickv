package adapter

import (
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
)

const (
	MigrationPartitionCapability                      = "cap_migration_partition_v1"
	MigrationOwnershipPartitionProtocolVersion uint32 = 1
)

// WithInternalMigrationPartitionProtocolReady attests installed partition
// support, independently of an active Raft configuration. A candidate learner
// must be able to answer this before being added. The callback must check the
// real control, public admission and committed-root consumer integrations.
// It never enables migration activation or operational ownership readiness.
func WithInternalMigrationPartitionProtocolReady(ready func() bool) InternalOption {
	return func(i *Internal) { i.migrationPartitionProtocolReady = ready }
}

func (i *Internal) implementedPartitionProtocol() uint32 {
	if i.migrationPartitionProtocolReady == nil || !i.migrationPartitionProtocolReady() {
		return 0
	}
	if i.migrationProposer == nil || !kv.MigrationPartitionRequestAdmissionReady(i.transactionManager) {
		return 0
	}
	if _, ok := i.store.(store.MigrationOwnershipPartitionStore); !ok {
		return 0
	}
	return MigrationOwnershipPartitionProtocolVersion
}

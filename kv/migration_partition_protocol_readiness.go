package kv

import (
	"reflect"

	"github.com/bootjp/elastickv/store"
)

// MigrationPartitionRequestAdmissionReady attests the installed final-request
// proposal hook and its coherent partition store. It neither enables migration
// nor attests an active configuration, root consumer, or member agreement.
func MigrationPartitionRequestAdmissionReady(tx Transactional) bool {
	switch t := tx.(type) {
	case *TransactionManager:
		return migrationPartitionManagerReady(t)
	case *LeaderProxy:
		return t != nil && MigrationPartitionRequestAdmissionReady(t.tm)
	case *leaseRefreshingTxn:
		return t != nil && MigrationPartitionRequestAdmissionReady(t.inner)
	case *timestampAdmissionTransactional:
		return t != nil && MigrationPartitionRequestAdmissionReady(t.inner)
	default:
		return false
	}
}

func migrationPartitionManagerReady(t *TransactionManager) bool {
	if t == nil || migrationPartitionComponentNil(t.proposer) {
		return false
	}
	admission := t.migrationAdmission.Load()
	return admission != nil && admission.groupID != 0 && migrationPartitionStoreReady(admission.store)
}

// MigrationPartitionReadAdmissionReady attests the actual public selected-route
// admission path and coherent partition store. Committed-root consumer and
// all-member protocol/configuration agreement remain separate runtime gates.
func (s *ShardStore) MigrationPartitionReadAdmissionReady(groupID uint64) bool {
	return s.MigrationReadAdmissionReady(groupID) && migrationPartitionStoreReady(s.groups[groupID].Store)
}

func migrationPartitionStoreReady(st store.MVCCStore) bool {
	if migrationPartitionComponentNil(st) {
		return false
	}
	_, partitions := st.(store.MigrationOwnershipPartitionStore)
	_, legacy := st.(store.MigrationOwnershipStore)
	_, coherent := st.(store.MigrationServingAdmissionReader)
	return partitions && legacy && coherent
}

func migrationPartitionComponentNil(component any) bool {
	if component == nil {
		return true
	}
	v := reflect.ValueOf(component)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	case reflect.Invalid, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.Array,
		reflect.String, reflect.Struct, reflect.UnsafePointer:
		return false
	}
	return false
}

package store

import "bytes"

// These existing MVCC namespaces do not advertise data progress. They still
// participate in OCC, so LastCommitTS cannot justify skipping their checks.
// Configuration is an exact key; adjacent ordinary keys remain data.
func isNonAdvertisedMVCCKey(key []byte) bool {
	return bytes.Equal(key, MigrationConfigurationIndexKey()) || isLocalAuxiliaryMVCCKey(key)
}

func hasNonAdvertisedMVCCDependencies(mutations []*KVPairMutation, readKeys [][]byte) bool {
	for _, mutation := range mutations {
		if isNonAdvertisedMVCCKey(mutation.Key) {
			return true
		}
	}
	for _, key := range readKeys {
		if isNonAdvertisedMVCCKey(key) {
			return true
		}
	}
	return false
}

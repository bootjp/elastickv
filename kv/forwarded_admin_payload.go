package kv

// ValidateForwardedAdminPayload limits the forwarding RPC to the typed backup
// and PREPARE-evidence entries used by leaderAdminProposer. In particular, the
// RPC is not an alternative authorization path for user or migration proposals.
func ValidateForwardedAdminPayload(payload []byte) error {
	_, err := decodeBackupEntry(payload)
	return err
}

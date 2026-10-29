package kv

// ValidateForwardedAdminPayload limits the forwarding RPC to the typed backup,
// PREPARE-evidence and read-watermark-advance entries used by
// leaderAdminProposer. In particular, the RPC is not an alternative
// authorization path for user or migration proposals.
func ValidateForwardedAdminPayload(payload []byte) error {
	if _, ok, err := DecodeWatermarkAdvance(payload); ok {
		return err
	}
	_, err := decodeBackupEntry(payload)
	return err
}

package kv

const (
	defaultTxnLockTTLms uint64 = 30_000
	// Keep lock TTL bounded to avoid effectively-permanent locks from malformed
	// or extreme client-provided TTL values.
	maxTxnLockTTLms uint64 = 86_400_000 // 24h

	txnPrepareStoreMutationFactor = 2

	txnCommitStoreMutationFactor = 3
	txnCommitStoreMutationSlack  = 2

	txnAbortStoreMutationFactor = 2
	txnAbortStoreMutationSlack  = 1
)

package kv

const (
	defaultTxnLockTTLms uint64 = 30_000

	txnPrepareStoreMutationFactor = 2

	txnCommitStoreMutationFactor = 3
	txnCommitStoreMutationSlack  = 2

	txnAbortStoreMutationFactor = 2
	txnAbortStoreMutationSlack  = 1
)

package kv

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Implementations must atomically check user/read-key OCC at originalStartTS,
// compare every expected txn-internal latest version byte-for-byte, and only
// exempt those exact prepared versions from cleanup conflicts. Arbitrary user
// keys, missing/version-changed records and extra exemptions must be rejected.
type PreparedClaimMutationApplier interface {
	ApplyPreparedMutationsRaftAt(context.Context, []*store.KVPairMutation, [][]byte, uint64, uint64, []store.MVCCVersion, uint64) error
}

func (f *kvFSM) expectedPreparedClaimVersions(ctx context.Context, mutations []*store.KVPairMutation, startTS, commitTS uint64) ([]store.MVCCVersion, error) {
	expected := make([]store.MVCCVersion, 0)
	for _, mutation := range mutations {
		if mutation.Op != store.OpTypeDelete {
			continue
		}
		user, ok := preparedClaimCleanupUserKey(mutation.Key)
		if !ok {
			continue
		}
		lock, err := f.preparedClaimCleanupLock(ctx, user, startTS, commitTS)
		if errors.Is(err, store.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if lock.ClaimEvidenceHash == ([32]byte{}) {
			continue
		}
		version, err := f.expectedPreparedInternalVersion(ctx, mutation.Key, commitTS)
		if err != nil {
			return nil, err
		}
		expected = append(expected, version)
	}
	return expected, nil
}

func (f *kvFSM) preparedClaimCleanupLock(ctx context.Context, user []byte, startTS, commitTS uint64) (txnLock, error) {
	lockBytes, err := f.store.GetAt(ctx, txnLockKey(user), ^uint64(0))
	if err != nil {
		return txnLock{}, errors.WithStack(err)
	}
	lock, err := decodeTxnLock(lockBytes)
	if err != nil || lock.ClaimEvidenceHash == ([32]byte{}) {
		return lock, err
	}
	if lock.StartTS != startTS || lock.CommitTS != commitTS {
		return txnLock{}, errors.WithStack(ErrTSOClaimInvalid)
	}
	_, evidence, err := readTxnClaimEvidence(ctx, f.store, lock.PrimaryKey, startTS)
	if err != nil {
		return txnLock{}, err
	}
	if sha256.Sum256(evidence) != lock.ClaimEvidenceHash {
		return txnLock{}, errors.WithStack(ErrTSOClaimInvalid)
	}
	return lock, nil
}

func preparedClaimCleanupUserKey(key []byte) ([]byte, bool) {
	if bytes.HasPrefix(key, txnLockPrefixBytes) {
		return key[len(txnLockPrefixBytes):], true
	}
	if bytes.HasPrefix(key, txnIntentPrefixBytes) {
		return key[len(txnIntentPrefixBytes):], true
	}
	return nil, false
}

func (f *kvFSM) expectedPreparedInternalVersion(ctx context.Context, key []byte, commitTS uint64) (store.MVCCVersion, error) {
	value, err := f.store.GetAt(ctx, key, ^uint64(0))
	if err != nil {
		return store.MVCCVersion{}, errors.WithStack(err)
	}
	latest, exists, err := f.store.LatestCommitTS(ctx, key)
	if err != nil {
		return store.MVCCVersion{}, errors.WithStack(err)
	}
	if !exists || latest != commitTS {
		return store.MVCCVersion{}, errors.WithStack(ErrTSOClaimInvalid)
	}
	return store.MVCCVersion{Key: bytes.Clone(key), Value: bytes.Clone(value), CommitTS: latest}, nil
}

func (f *kvFSM) applyPreparedClaimVersions(ctx context.Context, mutations []*store.KVPairMutation, startTS, commitTS uint64, expected []store.MVCCVersion) error {
	applier, ok := f.store.(PreparedClaimMutationApplier)
	if !ok {
		return errors.Wrap(ErrTSOProtocolUnsupported, "store lacks atomic prepared-claim mutation apply")
	}
	// Write-participant reads are checked at PREPARE. Preserve the existing
	// resolution contract, not new read-set revalidation after primary decision.
	return errors.Wrap(applier.ApplyPreparedMutationsRaftAt(ctx, mutations, nil, startTS, commitTS, expected, f.pendingApplyIdx), "apply prepared concrete claim mutations")
}

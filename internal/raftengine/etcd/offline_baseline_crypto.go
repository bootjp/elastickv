package etcd

import (
	"context"
	"io"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
)

// OfflineBaselineCrypto is an immutable, read-only view of the independently
// verified encryption checkpoint and ALL retained historical decryption keys.
// It never allocates nonces, changes keys, or rewrites a source/backup record.
// ValidateSnapshot must consume and authenticate the complete original FSM
// stream with the actual storage codec, including its checkpoint/header.
type OfflineBaselineCrypto struct {
	RaftCipher               *encryption.Cipher
	RaftEnvelopeCutoverIndex uint64
	ValidateSnapshot         func(context.Context, uint64, io.Reader) error
}

func validateOfflineBaselineCrypto(crypto *OfflineBaselineCrypto) error {
	if crypto == nil {
		return nil
	}
	if crypto.ValidateSnapshot == nil || (crypto.RaftEnvelopeCutoverIndex != 0 && crypto.RaftCipher == nil) {
		return errors.Wrap(ErrOfflineBaseline, "incomplete explicit offline encryption context")
	}
	return nil
}

func readOfflineBaselineWAL(ctx context.Context, dir string, crypto *OfflineBaselineCrypto, keep bool) (*baselineWALImage, error) {
	if err := validateOfflineBaselineCrypto(crypto); err != nil {
		return nil, err
	}
	return readBaselineWALInspected(dir, keep, func(entry *raftpb.Entry) error {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		if crypto == nil {
			return nil
		}
		payload, err := offlineBaselineReplayPayload(entry, crypto)
		clear(payload)
		return err
	})
}

func offlineBaselineReplayPayload(entry *raftpb.Entry, crypto *OfflineBaselineCrypto) ([]byte, error) {
	payload, err := baselineReplayPayload(entry)
	if err != nil {
		return nil, err
	}
	if crypto == nil || entry.GetType() != raftpb.EntryNormal || len(entry.Data) == 0 ||
		crypto.RaftEnvelopeCutoverIndex == 0 || entry.GetIndex() <= crypto.RaftEnvelopeCutoverIndex {
		return payload, nil
	}
	defer clear(payload)
	plain, err := encryption.UnwrapRaftPayload(crypto.RaftCipher, payload)
	if err != nil {
		return nil, errors.Wrap(err, "authenticate retained offline raft payload")
	}
	return plain, nil
}

func validateOfflineBaselineSnapshot(ctx context.Context, dir string, snapshot *raftpb.Snapshot, crypto *OfflineBaselineCrypto) error {
	if crypto == nil || snapshot.GetMetadata().GetIndex() == 0 {
		return nil
	}
	reader, err := openBaselineSnapshotPayload(dir, snapshot)
	if err != nil {
		return err
	}
	verifyErr := crypto.ValidateSnapshot(ctx, snapshot.GetMetadata().GetIndex(), reader)
	if verifyErr == nil {
		var tail [1]byte
		if n, err := reader.Read(tail[:]); n != 0 || !errors.Is(err, io.EOF) {
			verifyErr = errors.Wrap(ErrOfflineBaseline, "snapshot validator did not consume the complete stream")
		}
	}
	return errors.WithStack(errors.CombineErrors(verifyErr, reader.Close()))
}

func offlineBaselineSourceProof(options OfflineBaselineSourceOptions, commit uint64) (BaselineStoreProof, error) {
	var proof BaselineStoreProof
	if options.StoreProof != nil {
		proof = *options.StoreProof
		if proof.AppliedIndex != commit || proof.ConfigurationIndex == 0 || proof.PreparedTransactions != 0 {
			return proof, errors.Wrap(ErrOfflineBaseline, "offline source store proof is not at the complete committed cut")
		}
		return proof, validateOfflineBaselineCryptoCut(options.Crypto, proof, commit)
	}
	if options.Crypto != nil && options.Crypto.RaftEnvelopeCutoverIndex > commit {
		return proof, errors.Wrap(ErrOfflineBaseline, "offline source envelope cutover is beyond committed state")
	}
	return proof, nil
}

func validateOfflineBaselineCryptoCut(crypto *OfflineBaselineCrypto, proof BaselineStoreProof, commit uint64) error {
	if proof.EncryptionContextRequired != (crypto != nil) {
		return errors.Wrap(ErrOfflineBaseline, "offline encryption mode differs from independently verified store proof")
	}
	if crypto == nil {
		if proof.RaftEnvelopeCutoverIndex != 0 {
			return errors.Wrap(ErrOfflineBaseline, "encrypted store proof requires explicit offline encryption context")
		}
		return nil
	}
	if crypto.RaftEnvelopeCutoverIndex != proof.RaftEnvelopeCutoverIndex || crypto.RaftEnvelopeCutoverIndex > commit {
		return errors.Wrap(ErrOfflineBaseline, "offline envelope cutover differs from independently verified store checkpoint")
	}
	return nil
}

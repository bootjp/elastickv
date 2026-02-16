package kv

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type kvFSM struct {
	store store.MVCCStore
	log   *slog.Logger
}

type FSM interface {
	raft.FSM
}

func NewKvFSM(store store.MVCCStore) FSM {
	return &kvFSM{
		store: store,
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}
}

var _ FSM = (*kvFSM)(nil)
var _ raft.FSM = (*kvFSM)(nil)

var ErrUnknownRequestType = errors.New("unknown request type")

func (f *kvFSM) Apply(l *raft.Log) interface{} {
	ctx := context.TODO()

	r := &pb.Request{}
	err := proto.Unmarshal(l.Data, r)
	if err != nil {
		return errors.WithStack(err)
	}

	commitTS := r.Ts
	if r.IsTxn && (r.Phase == pb.Phase_COMMIT || r.Phase == pb.Phase_ABORT) {
		meta, _, err := extractTxnMeta(r.Mutations)
		if err != nil {
			return errors.WithStack(err)
		}
		if meta.CommitTS == 0 {
			return errors.WithStack(ErrTxnCommitTSRequired)
		}
		commitTS = meta.CommitTS
	}

	err = f.handleRequest(ctx, r, commitTS)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (f *kvFSM) handleRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	switch {
	case r.IsTxn:
		return f.handleTxnRequest(ctx, r, commitTS)
	default:
		return f.handleRawRequest(ctx, r, commitTS)
	}
}

func (f *kvFSM) handleRawRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	for _, mut := range r.Mutations {
		if mut == nil || len(mut.Key) == 0 {
			return errors.WithStack(ErrInvalidRequest)
		}
		// Raw requests should not mutate txn-internal keys.
		if isTxnInternalKey(mut.Key) {
			return errors.WithStack(ErrInvalidRequest)
		}
		if err := f.assertNoConflictingTxnLock(ctx, mut.Key, 0); err != nil {
			return err
		}
	}

	muts, err := toStoreMutations(r.Mutations)
	if err != nil {
		return errors.WithStack(err)
	}
	// Raw requests always commit against the latest state; use commitTS as both
	// the validation snapshot and the commit timestamp.
	return errors.WithStack(f.store.ApplyMutations(ctx, muts, commitTS, commitTS))
}

var ErrNotImplemented = errors.New("not implemented")

func (f *kvFSM) Snapshot() (raft.FSMSnapshot, error) {
	buf, err := f.store.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &kvFSMSnapshot{
		buf,
	}, nil
}

func (f *kvFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	return errors.WithStack(f.store.Restore(r))
}

func (f *kvFSM) handleTxnRequest(ctx context.Context, r *pb.Request, commitTS uint64) error {
	switch r.Phase {
	case pb.Phase_PREPARE:
		return f.handlePrepareRequest(ctx, r)
	case pb.Phase_COMMIT:
		return f.handleCommitRequest(ctx, r)
	case pb.Phase_ABORT:
		return f.handleAbortRequest(ctx, r, commitTS)
	case pb.Phase_NONE:
		// not reached
		return errors.WithStack(ErrUnknownRequestType)
	default:
		return errors.WithStack(ErrUnknownRequestType)
	}
}

func (f *kvFSM) validateConflicts(ctx context.Context, muts []*pb.Mutation, startTS uint64) error {
	seen := make(map[string]struct{}, len(muts))
	for _, mut := range muts {
		keyStr := string(mut.Key)
		if _, ok := seen[keyStr]; ok {
			continue
		}
		seen[keyStr] = struct{}{}

		latest, exists, err := f.store.LatestCommitTS(ctx, mut.Key)
		if err != nil {
			return errors.WithStack(err)
		}
		if exists && latest > startTS {
			return errors.Wrapf(store.ErrWriteConflict, "key: %s", string(mut.Key))
		}
	}
	return nil
}

func uniqueMutations(muts []*pb.Mutation) ([]*pb.Mutation, error) {
	if len(muts) == 0 {
		return []*pb.Mutation{}, nil
	}
	seen := make(map[string]struct{}, len(muts))
	reversed := make([]*pb.Mutation, 0, len(muts))
	// Keep the last mutation per key to avoid dropping final operations like
	// PUT followed by DEL in the same transactional batch.
	for i := len(muts) - 1; i >= 0; i-- {
		mut := muts[i]
		if mut == nil || len(mut.Key) == 0 {
			return nil, errors.WithStack(ErrInvalidRequest)
		}
		keyStr := string(mut.Key)
		if _, ok := seen[keyStr]; ok {
			continue
		}
		seen[keyStr] = struct{}{}
		reversed = append(reversed, mut)
	}

	out := make([]*pb.Mutation, 0, len(reversed))
	for i := len(reversed) - 1; i >= 0; i-- {
		out = append(out, reversed[i])
	}
	return out, nil
}

func (f *kvFSM) handlePrepareRequest(ctx context.Context, r *pb.Request) error {
	meta, muts, err := extractTxnMeta(r.Mutations)
	if err != nil {
		return err
	}
	if len(meta.PrimaryKey) == 0 {
		return errors.WithStack(ErrTxnPrimaryKeyRequired)
	}
	if len(muts) == 0 {
		return errors.WithStack(ErrInvalidRequest)
	}

	startTS := r.Ts
	uniq, err := uniqueMutations(muts)
	if err != nil {
		return err
	}
	if err := f.validateConflicts(ctx, uniq, startTS); err != nil {
		return errors.WithStack(err)
	}

	ttlMs := meta.LockTTLms
	if ttlMs == 0 {
		// Default when callers don't specify TTL (for example, Redis MULTI/EXEC).
		ttlMs = defaultTxnLockTTLms
	}
	expireAt := hlcWallFromNowMs(ttlMs)

	storeMuts, err := f.buildPrepareStoreMutations(ctx, uniq, meta.PrimaryKey, startTS, expireAt)
	if err != nil {
		return err
	}

	if err := f.store.ApplyMutations(ctx, storeMuts, startTS, startTS); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (f *kvFSM) handleCommitRequest(ctx context.Context, r *pb.Request) error {
	meta, muts, err := extractTxnMeta(r.Mutations)
	if err != nil {
		return err
	}
	if len(muts) == 0 {
		return errors.WithStack(ErrInvalidRequest)
	}
	commitTS := meta.CommitTS
	startTS := r.Ts
	if commitTS <= startTS {
		return errors.WithStack(ErrTxnCommitTSRequired)
	}

	uniq, err := uniqueMutations(muts)
	if err != nil {
		return err
	}
	storeMuts, err := f.buildCommitStoreMutations(ctx, uniq, meta, startTS, commitTS)
	if err != nil {
		return err
	}

	if len(storeMuts) == 0 {
		return nil
	}
	return errors.WithStack(f.store.ApplyMutations(ctx, storeMuts, startTS, commitTS))
}

func (f *kvFSM) handleAbortRequest(ctx context.Context, r *pb.Request, abortTS uint64) error {
	meta, muts, err := extractTxnMeta(r.Mutations)
	if err != nil {
		return err
	}
	if len(meta.PrimaryKey) == 0 {
		return errors.WithStack(ErrTxnPrimaryKeyRequired)
	}
	if len(muts) == 0 {
		return errors.WithStack(ErrInvalidRequest)
	}
	startTS := r.Ts
	if abortTS <= startTS {
		return errors.WithStack(ErrTxnCommitTSRequired)
	}

	uniq, err := uniqueMutations(muts)
	if err != nil {
		return err
	}
	storeMuts, abortingPrimary, err := f.buildAbortCleanupStoreMutations(ctx, uniq, meta.PrimaryKey, startTS)
	if err != nil {
		return err
	}
	if abortingPrimary {
		if err := f.appendRollbackRecord(ctx, meta.PrimaryKey, startTS, &storeMuts); err != nil {
			return err
		}
	}

	if len(storeMuts) == 0 {
		return nil
	}
	return errors.WithStack(f.store.ApplyMutations(ctx, storeMuts, startTS, abortTS))
}

func (f *kvFSM) buildPrepareStoreMutations(ctx context.Context, muts []*pb.Mutation, primaryKey []byte, startTS, expireAt uint64) ([]*store.KVPairMutation, error) {
	storeMuts := make([]*store.KVPairMutation, 0, len(muts)*txnPrepareStoreMutationFactor)
	for _, mut := range muts {
		preparedMuts, err := f.prepareTxnMutation(ctx, mut, primaryKey, startTS, expireAt)
		if err != nil {
			return nil, err
		}
		storeMuts = append(storeMuts, preparedMuts...)
	}
	return storeMuts, nil
}

func (f *kvFSM) buildCommitStoreMutations(ctx context.Context, muts []*pb.Mutation, meta TxnMeta, startTS, commitTS uint64) ([]*store.KVPairMutation, error) {
	storeMuts := make([]*store.KVPairMutation, 0, len(muts)*txnCommitStoreMutationFactor+txnCommitStoreMutationSlack)

	committingPrimary := false
	for _, mut := range muts {
		key := mut.Key
		if bytes.Equal(key, meta.PrimaryKey) {
			committingPrimary = true
		}

		keyMuts, err := f.commitTxnKeyMutations(ctx, key, meta.PrimaryKey, startTS)
		if err != nil {
			return nil, err
		}
		storeMuts = append(storeMuts, keyMuts...)
	}

	if committingPrimary {
		storeMuts = append(storeMuts, &store.KVPairMutation{
			Op:    store.OpTypePut,
			Key:   txnCommitKey(meta.PrimaryKey, startTS),
			Value: encodeTxnCommitRecord(commitTS),
		})
	}

	return storeMuts, nil
}

func (f *kvFSM) buildAbortCleanupStoreMutations(ctx context.Context, muts []*pb.Mutation, primaryKey []byte, startTS uint64) ([]*store.KVPairMutation, bool, error) {
	storeMuts := make([]*store.KVPairMutation, 0, len(muts)*txnAbortStoreMutationFactor)
	abortingPrimary := false
	for _, mut := range muts {
		key := mut.Key
		if bytes.Equal(key, primaryKey) {
			abortingPrimary = true
		}

		shouldClear, err := f.shouldClearAbortKey(ctx, key, primaryKey, startTS)
		if err != nil {
			return nil, false, err
		}
		if shouldClear {
			storeMuts = append(storeMuts, txnCleanupMutations(key)...)
		}
	}
	return storeMuts, abortingPrimary, nil
}

func (f *kvFSM) appendRollbackRecord(ctx context.Context, primaryKey []byte, startTS uint64, storeMuts *[]*store.KVPairMutation) error {
	// Don't allow rollback to win after commit record exists.
	if _, err := f.store.GetAt(ctx, txnCommitKey(primaryKey, startTS), ^uint64(0)); err == nil {
		return errors.WithStack(ErrTxnAlreadyCommitted)
	} else if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return errors.WithStack(err)
	}

	*storeMuts = append(*storeMuts, &store.KVPairMutation{
		Op:    store.OpTypePut,
		Key:   txnRollbackKey(primaryKey, startTS),
		Value: encodeTxnRollbackRecord(),
	})
	return nil
}

func (f *kvFSM) prepareTxnMutation(ctx context.Context, mut *pb.Mutation, primaryKey []byte, startTS, expireAt uint64) ([]*store.KVPairMutation, error) {
	if err := f.assertNoConflictingTxnLock(ctx, mut.Key, startTS); err != nil {
		return nil, err
	}

	lockVal := encodeTxnLock(txnLock{
		StartTS:      startTS,
		TTLExpireAt:  expireAt,
		PrimaryKey:   primaryKey,
		IsPrimaryKey: bytes.Equal(mut.Key, primaryKey),
	})
	intent, err := txnIntentFromPBMutation(mut, startTS)
	if err != nil {
		return nil, err
	}

	storeMuts := make([]*store.KVPairMutation, 0, txnPrepareStoreMutationFactor)
	storeMuts = append(storeMuts,
		&store.KVPairMutation{Op: store.OpTypePut, Key: txnLockKey(mut.Key), Value: lockVal},
		&store.KVPairMutation{Op: store.OpTypePut, Key: txnIntentKey(mut.Key), Value: encodeTxnIntent(intent)},
	)
	return storeMuts, nil
}

func txnIntentFromPBMutation(mut *pb.Mutation, startTS uint64) (txnIntent, error) {
	switch mut.Op {
	case pb.Op_PUT:
		return txnIntent{StartTS: startTS, Op: txnIntentOpPut, Value: mut.Value}, nil
	case pb.Op_DEL:
		return txnIntent{StartTS: startTS, Op: txnIntentOpDel, Value: nil}, nil
	default:
		return txnIntent{}, errors.WithStack(ErrUnknownRequestType)
	}
}

func txnCleanupMutations(key []byte) []*store.KVPairMutation {
	return []*store.KVPairMutation{
		{Op: store.OpTypeDelete, Key: txnLockKey(key)},
		{Op: store.OpTypeDelete, Key: txnIntentKey(key)},
	}
}

func (f *kvFSM) txnLockForCommit(ctx context.Context, key []byte) (txnLock, bool, error) {
	lockBytes, err := f.store.GetAt(ctx, txnLockKey(key), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return txnLock{}, false, nil
		}
		return txnLock{}, false, errors.WithStack(err)
	}
	lock, derr := decodeTxnLock(lockBytes)
	if derr != nil {
		return txnLock{}, false, errors.WithStack(derr)
	}
	return lock, true, nil
}

func (f *kvFSM) txnIntentForCommit(ctx context.Context, key []byte) (txnIntent, bool, error) {
	intentBytes, err := f.store.GetAt(ctx, txnIntentKey(key), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return txnIntent{}, false, nil
		}
		return txnIntent{}, false, errors.WithStack(err)
	}
	intent, derr := decodeTxnIntent(intentBytes)
	if derr != nil {
		return txnIntent{}, false, errors.WithStack(derr)
	}
	return intent, true, nil
}

func storeMutationForIntent(key []byte, intent txnIntent) (*store.KVPairMutation, error) {
	switch intent.Op {
	case txnIntentOpPut:
		return &store.KVPairMutation{Op: store.OpTypePut, Key: key, Value: intent.Value}, nil
	case txnIntentOpDel:
		return &store.KVPairMutation{Op: store.OpTypeDelete, Key: key}, nil
	default:
		return nil, errors.WithStack(ErrUnknownRequestType)
	}
}

func (f *kvFSM) commitTxnKeyMutations(ctx context.Context, key, primaryKey []byte, startTS uint64) ([]*store.KVPairMutation, error) {
	lock, ok, err := f.txnLockForCommit(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Already resolved (committed/rolled back).
		return nil, nil
	}
	if lock.StartTS != startTS {
		return nil, errors.Wrapf(ErrTxnLocked, "key: %s", string(key))
	}
	if !bytes.Equal(lock.PrimaryKey, primaryKey) {
		return nil, errors.Wrapf(ErrTxnInvalidMeta, "lock primary_key mismatch for key %s", string(key))
	}

	intent, ok, err := f.txnIntentForCommit(ctx, key)
	if err != nil {
		return nil, err
	}

	out := make([]*store.KVPairMutation, 0, txnCommitStoreMutationFactor)
	if ok {
		if intent.StartTS != startTS {
			return nil, errors.Wrapf(ErrTxnInvalidMeta, "intent start_ts mismatch for key %s", string(key))
		}
		mut, err := storeMutationForIntent(key, intent)
		if err != nil {
			return nil, err
		}
		out = append(out, mut)
	}
	out = append(out, txnCleanupMutations(key)...)
	return out, nil
}

func (f *kvFSM) shouldClearAbortKey(ctx context.Context, key, primaryKey []byte, startTS uint64) (bool, error) {
	lockBytes, err := f.store.GetAt(ctx, txnLockKey(key), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return true, nil
		}
		return false, errors.WithStack(err)
	}
	lock, derr := decodeTxnLock(lockBytes)
	if derr != nil {
		return false, errors.WithStack(derr)
	}
	if lock.StartTS != startTS {
		return false, nil
	}
	if !bytes.Equal(lock.PrimaryKey, primaryKey) {
		return false, errors.Wrapf(ErrTxnInvalidMeta, "abort primary_key mismatch for key %s", string(key))
	}
	return true, nil
}

func (f *kvFSM) assertNoConflictingTxnLock(ctx context.Context, key []byte, startTS uint64) error {
	lockBytes, err := f.store.GetAt(ctx, txnLockKey(key), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil
		}
		return errors.WithStack(err)
	}
	lock, err := decodeTxnLock(lockBytes)
	if err != nil {
		return errors.WithStack(err)
	}
	if startTS != 0 && lock.StartTS == startTS {
		return nil
	}
	return errors.Wrapf(ErrTxnLocked, "key: %s", string(key))
}

func toStoreMutations(muts []*pb.Mutation) ([]*store.KVPairMutation, error) {
	out := make([]*store.KVPairMutation, 0, len(muts))
	for _, mut := range muts {
		switch mut.Op {
		case pb.Op_PUT:
			out = append(out, &store.KVPairMutation{
				Op:    store.OpTypePut,
				Key:   mut.Key,
				Value: mut.Value,
			})
		case pb.Op_DEL:
			out = append(out, &store.KVPairMutation{
				Op:  store.OpTypeDelete,
				Key: mut.Key,
			})
		default:
			return nil, ErrUnknownRequestType
		}
	}
	return out, nil
}

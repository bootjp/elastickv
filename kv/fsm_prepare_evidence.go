package kv

import (
	"bytes"
	"context"
	"encoding/binary"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var prepareEvidenceEnabledKey = []byte(txnBackupPrefix + "prepare_evidence_enabled")
var prepareEvidenceEnabledValue = []byte{1}
var prepareEvidenceGateKey = []byte(txnBackupPrefix + "prepare_evidence_gate")
var prepareEvidenceTimestampFloorKey = []byte(txnBackupPrefix + "prepare_evidence_timestamp_floor")

func (f *kvFSM) validatePrepareEvidenceMeta(meta TxnMeta, startTS uint64) error {
	if meta.PrepareCommitTS == 0 {
		return nil
	}
	if !f.prepareEvidenceEnabled.Load() {
		return errors.Wrap(ErrTSOProtocolUnsupported, "durable prepare evidence is not enabled")
	}
	if meta.PrepareCommitTS <= startTS {
		return errors.Wrap(ErrTxnInvalidMeta, "prepare commit timestamp must follow start timestamp")
	}
	return nil
}

func (f *kvFSM) validatePrepareLock(ctx context.Context, key, primaryKey []byte, startTS, commitTS uint64) error {
	lock, exists, err := f.txnLockAtKey(ctx, txnLockKey(key))
	if err != nil {
		return err
	}
	if exists && (lock.StartTS != startTS || !bytes.Equal(lock.PrimaryKey, primaryKey)) {
		return NewTxnLockedError(key)
	}
	if exists && lock.CommitTS != commitTS {
		return errors.Wrap(ErrTxnInvalidMeta, "prepare cannot change durable commit timestamp evidence")
	}
	if f.prepareEvidenceEnabled.Load() && commitTS == 0 && !exists {
		return errors.Wrap(ErrTSOProtocolUnsupported, "new prepare requires durable commit timestamp evidence")
	}
	return nil
}

// PrepareRequestCommitTimestamp extracts only the durable proof timestamp of
// a PREPARE, leaving resolution and ordinary request metadata untouched.
func PrepareRequestCommitTimestamp(request *pb.Request) (uint64, error) {
	if request == nil || !request.IsTxn || request.Phase != pb.Phase_PREPARE || len(request.Mutations) == 0 {
		return 0, nil
	}
	meta, _, err := extractTxnMeta(request.Mutations)
	return meta.PrepareCommitTS, err
}

// PrepareEvidenceEnabled is monotonic for an upgraded data group's store.
func PrepareEvidenceEnabled(ctx context.Context, st store.MVCCStore) (bool, error) {
	value, err := st.GetAt(ctx, prepareEvidenceEnabledKey, ^uint64(0))
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	if !bytes.Equal(value, prepareEvidenceEnabledValue) {
		return false, errors.Wrap(ErrTxnInvalidMeta, "invalid prepare evidence marker")
	}
	return true, nil
}

func (f *kvFSM) reloadPrepareEvidence(ctx context.Context) error {
	enabled, err := PrepareEvidenceEnabled(ctx, f.store)
	if err != nil {
		return err
	}
	f.prepareEvidenceEnabled.Store(enabled)
	sealed, released, err := readPrepareEvidenceCreationGate(ctx, f.store)
	if err != nil {
		return err
	}
	f.prepareCreationSealed.Store(sealed)
	f.prepareEvidenceReleaseFloor.Store(released)
	floor, err := readPrepareEvidenceFenceFloor(ctx, f.store)
	if err != nil {
		return err
	}
	f.prepareEvidenceTimestampFloor.Store(floor)
	return nil
}

func (f *kvFSM) applyPrepareEvidenceFence(floor uint64) error {
	if released := f.prepareEvidenceReleaseFloor.Load(); released != 0 && floor != 0 && floor != released {
		return errors.Wrap(ErrTxnInvalidMeta, "released prepare timestamp floor cannot change")
	}
	currentFloor := f.prepareEvidenceTimestampFloor.Load()
	if f.prepareEvidenceEnabled.Load() && floor <= currentFloor {
		return nil
	}
	stamp := max(uint64(1), f.store.LastCommitTS(), floor)
	muts := []*store.KVPairMutation{{Op: store.OpTypePut, Key: prepareEvidenceEnabledKey, Value: prepareEvidenceEnabledValue}}
	if floor > currentFloor {
		muts = append(muts, &store.KVPairMutation{Op: store.OpTypePut, Key: prepareEvidenceTimestampFloorKey, Value: binary.BigEndian.AppendUint64(nil, floor)})
	}
	if err := f.store.ApplyMutationsRaftAt(context.Background(), muts, nil, stamp, stamp, f.pendingApplyIdx); err != nil {
		return errors.Wrap(err, "persist prepare evidence fence")
	}
	f.prepareEvidenceEnabled.Store(true)
	if floor > currentFloor {
		f.prepareEvidenceTimestampFloor.Store(floor)
	}
	return nil
}

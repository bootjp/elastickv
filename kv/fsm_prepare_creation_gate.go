package kv

import (
	"context"
	"encoding/binary"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrPrepareCreationSealed = errors.New("phase-D cutover temporarily seals new write creation")
var ErrPrepareAdmissionFenced = errors.Wrap(ErrInvalidRequest, "write does not carry current phase-D timestamp admission")

const (
	prepareCreationSealed        byte = 1
	prepareCreationReleased      byte = 2
	prepareCreationGateValueSize      = 9
)

func isPrepareEvidenceControlSubtype(subtype byte) bool {
	return subtype >= backupSubtypePrepareEvidence && subtype <= backupSubtypePrepareRelease
}

func (f *kvFSM) applyPrepareEvidenceControl(entry backupEntry) error {
	switch entry.subtype {
	case backupSubtypePrepareEvidence:
		return f.applyPrepareEvidenceFence(entry.prepareFloor)
	case backupSubtypePrepareSeal:
		return f.applyPrepareCreationSeal()
	case backupSubtypePrepareRelease:
		return f.applyPrepareCreationRelease(entry.prepareFloor)
	default:
		return ErrBackupWireSubtype
	}
}

func (f *kvFSM) verifyPrepareCreationGate(request *pb.Request) error {
	if request != nil && !timestampAdmissionCreation(request) {
		return nil
	}
	if f.prepareCreationSealed.Load() {
		return errors.WithStack(ErrPrepareCreationSealed)
	}
	floor := f.prepareEvidenceReleaseFloor.Load()
	if floor == 0 {
		return nil
	}
	if request == nil || request.GetTimestampAdmissionFloor() != floor {
		return errors.WithStack(ErrPrepareAdmissionFenced)
	}
	timestamp, err := prepareCreationPersistenceTimestamp(request)
	if err != nil {
		return err
	}
	if timestamp <= floor {
		return errors.WithStack(ErrPrepareAdmissionFenced)
	}
	return nil
}

func prepareCreationPersistenceTimestamp(request *pb.Request) (uint64, error) {
	if request.IsTxn && request.Phase == pb.Phase_PREPARE {
		meta, _, err := extractTxnMeta(request.Mutations)
		return meta.PrepareCommitTS, err
	}
	return requestCommitTS(request)
}

func readPrepareEvidenceCreationGate(ctx context.Context, st store.MVCCStore) (bool, uint64, error) {
	value, err := st.GetAt(ctx, prepareEvidenceGateKey, ^uint64(0))
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, errors.WithStack(err)
	}
	if len(value) != prepareCreationGateValueSize {
		return false, 0, errors.Wrap(ErrTxnInvalidMeta, "invalid prepare creation gate")
	}
	floor := binary.BigEndian.Uint64(value[1:])
	switch {
	case value[0] == prepareCreationSealed && floor == 0:
		return true, 0, nil
	case value[0] == prepareCreationReleased && floor != 0:
		return false, floor, nil
	default:
		return false, 0, errors.Wrap(ErrTxnInvalidMeta, "invalid prepare creation phase or floor")
	}
}

func (f *kvFSM) persistPrepareCreationGate(phase byte, floor uint64) error {
	stamp := max(uint64(1), f.store.LastCommitTS(), floor)
	value := binary.BigEndian.AppendUint64([]byte{phase}, floor)
	return errors.WithStack(f.store.ApplyMutationsRaftAt(context.Background(), []*store.KVPairMutation{{
		Op: store.OpTypePut, Key: prepareEvidenceGateKey, Value: value,
	}}, nil, stamp, stamp, f.pendingApplyIdx))
}

func (f *kvFSM) applyPrepareCreationSeal() error {
	if !f.prepareEvidenceEnabled.Load() {
		return errors.Wrap(ErrTSOProtocolUnsupported, "prepare protocol must be enabled before sealing")
	}
	if f.prepareCreationSealed.Load() || f.prepareEvidenceReleaseFloor.Load() != 0 {
		return nil
	}
	if err := f.persistPrepareCreationGate(prepareCreationSealed, 0); err != nil {
		return err
	}
	f.prepareCreationSealed.Store(true)
	return nil
}

func (f *kvFSM) applyPrepareCreationRelease(floor uint64) error {
	if floor == 0 || f.prepareEvidenceTimestampFloor.Load() < floor || !f.prepareEvidenceEnabled.Load() {
		return errors.Wrap(ErrTxnInvalidMeta, "prepare creation release requires its durable timestamp fence")
	}
	if old := f.prepareEvidenceReleaseFloor.Load(); old != 0 {
		if old == floor {
			return nil
		}
		return errors.Wrap(ErrTxnInvalidMeta, "prepare creation release floor cannot change")
	}
	if !f.prepareCreationSealed.Load() {
		return errors.Wrap(ErrTxnInvalidMeta, "prepare creation must be sealed before release")
	}
	if err := f.persistPrepareCreationGate(prepareCreationReleased, floor); err != nil {
		return err
	}
	f.prepareEvidenceReleaseFloor.Store(floor)
	f.prepareCreationSealed.Store(false)
	return nil
}

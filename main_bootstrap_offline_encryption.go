package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"strconv"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

type bootstrapOfflineEncryptionConfig struct {
	SidecarPath string
	Wrapper     kek.Wrapper
	MemberID    uint64
}

// Private verified context. It holds no nonce factory and cannot bump epochs or
// install runtime writers. Keep the sidecar/process fence owned across ALL use.
type bootstrapOfflineEncryption struct {
	path        string
	fingerprint [32]byte
	checkpoint  encryption.SnapshotCheckpoint
	cutover     uint64
	cipher      *encryption.Cipher
	decoder     *store.OfflineBaselineDecoder
	memberID    uint64
	active      [2]bootstrapOfflineEncryptionWriter
	secretFile  string
}

type bootstrapOfflineEncryptionWriter struct {
	id    uint32
	epoch uint16
}

func newBootstrapOfflineEncryption(cfg bootstrapOfflineEncryptionConfig) (*bootstrapOfflineEncryption, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	sc, err := bootstrapOfflineReadSidecar(cfg.SidecarPath)
	if err != nil {
		return nil, errors.Wrap(errBootstrapOfflineStore, "cannot read encryption sidecar")
	}
	if err := bootstrapOfflineValidateSidecar(sc); err != nil {
		return nil, err
	}
	cipher, err := bootstrapOfflineCipher(sc, cfg.Wrapper)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	decoder, err := store.NewOfflineBaselineDecoder(cipher)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cp, err := encryption.SidecarCheckpoint(sc)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	fingerprint, err := bootstrapOfflineSidecarFingerprint(sc)
	if err != nil {
		return nil, err
	}
	e := &bootstrapOfflineEncryption{path: cfg.SidecarPath, fingerprint: fingerprint, checkpoint: cp, cutover: sc.RaftEnvelopeCutoverIndex, cipher: cipher, decoder: decoder, memberID: cfg.MemberID}
	for i, id := range []uint32{sc.Active.Storage, sc.Active.Raft} {
		e.active[i] = bootstrapOfflineEncryptionWriter{id: id, epoch: sc.Keys[strconv.FormatUint(uint64(id), 10)].LocalEpoch}
	}
	return e, nil
}

func (cfg bootstrapOfflineEncryptionConfig) validate() error {
	if cfg.SidecarPath == "" || cfg.Wrapper == nil || cfg.MemberID == 0 {
		return errors.Wrap(errBootstrapOfflineStore, "explicit sidecar, member and verified KEK required")
	}
	return nil
}

func bootstrapOfflineReadSidecar(path string) (*encryption.Sidecar, error) {
	info, err := bootstrapOfflineLstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > raftbootstrap.MaxFileSize {
		return nil, errors.Wrap(errBootstrapOfflineStore, "sidecar must be a bounded existing regular file")
	}
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		return nil, errors.Wrap(errBootstrapOfflineStore, "invalid encryption sidecar")
	}
	return sc, nil
}

func bootstrapOfflineValidateSidecar(sc *encryption.Sidecar) error {
	if len(sc.Keys) == 0 || (sc.Active.Storage == 0 && sc.Active.Raft == 0) {
		return errors.Wrap(errBootstrapOfflineStore, "encryption context has no active keys")
	}
	if err := validateBootstrapEncryptionKeyPurposes(sc); err != nil {
		return err
	}
	if sc.StorageEnvelopeActive != (sc.StorageEnvelopeCutoverIndex != 0) || (sc.RaftEnvelopeCutoverIndex != 0 && sc.Active.Raft == 0) {
		return errors.Wrap(errBootstrapOfflineStore, "sidecar envelope state is inconsistent")
	}
	return bootstrapOfflineValidateCutovers(sc)
}

func bootstrapOfflineValidateCutovers(sc *encryption.Sidecar) error {
	if sc.RaftAppliedIndex == 0 || sc.StorageEnvelopeCutoverIndex > sc.RaftAppliedIndex || sc.RaftEnvelopeCutoverIndex > sc.RaftAppliedIndex {
		return errors.Wrap(errBootstrapOfflineStore, "encryption checkpoint/cutover invalid")
	}
	return nil
}

func bootstrapOfflineCipher(sc *encryption.Sidecar, wrapper kek.Wrapper) (*encryption.Cipher, error) {
	keys := encryption.NewKeystore()
	for id, key := range sc.Keys {
		if err := bootstrapOfflineLoadKey(keys, wrapper, id, key); err != nil {
			return nil, err
		}
	}
	cipher, err := encryption.NewCipher(keys)
	return cipher, errors.WithStack(err)
}

func bootstrapOfflineLoadKey(keys *encryption.Keystore, wrapper kek.Wrapper, id string, key encryption.SidecarKey) error {
	number, err := strconv.ParseUint(id, 10, 32)
	if err != nil || number == 0 {
		return errors.Wrap(errBootstrapOfflineStore, "invalid encryption key ID")
	}
	wrapped := append([]byte(nil), key.Wrapped...)
	dek, err := wrapper.Unwrap(wrapped)
	defer clear(dek)
	if err != nil || len(dek) != encryption.KeySize {
		return errors.Wrap(errBootstrapOfflineStore, "historical DEK authentication failed")
	}
	if err := keys.Set(uint32(number), dek); err != nil {
		return errors.Wrap(errBootstrapOfflineStore, "historical DEK installation failed")
	}
	return nil
}

func bootstrapOfflineSidecarFingerprint(sc *encryption.Sidecar) ([32]byte, error) {
	data, err := json.Marshal(sc)
	if err != nil {
		return [32]byte{}, errors.Wrap(errBootstrapOfflineStore, "invalid sidecar image")
	}
	return sha256.Sum256(data), nil
}

func (e *bootstrapOfflineEncryption) unchanged() error {
	sc, err := bootstrapOfflineReadSidecar(e.path)
	if err != nil {
		return errors.Wrap(errBootstrapOfflineStore, "encryption sidecar became unavailable")
	}
	digest, err := bootstrapOfflineSidecarFingerprint(sc)
	if err != nil {
		return err
	}
	if digest != e.fingerprint {
		return errors.Wrap(errBootstrapOfflineStore, "encryption sidecar changed during offline operation")
	}
	return nil
}

func (e *bootstrapOfflineEncryption) validatePreparationContext(c etcd.BaselinePreparationContext) error {
	if e == nil {
		return nil
	}
	if err := e.unchanged(); err != nil {
		return err
	}
	if e.secretFile == "" {
		return nil
	}
	for _, dir := range []string{c.BackupDir, c.DataDir} {
		if err := bootstrapOfflineSecretOutsideSource(dir, e.secretFile); err != nil {
			return err
		}
	}
	return nil
}

func bootstrapOfflineEncryptionOption(optional []*bootstrapOfflineEncryption) (*bootstrapOfflineEncryption, error) {
	if len(optional) > 1 {
		return nil, errors.Wrap(errBootstrapOfflineStore, "multiple offline encryption contexts")
	}
	if len(optional) == 0 {
		return nil, nil
	}
	if optional[0] == nil {
		return nil, errors.Wrap(errBootstrapOfflineStore, "explicit encryption context is missing")
	}
	return optional[0], optional[0].unchanged()
}

func (e *bootstrapOfflineEncryption) probe(ctx context.Context, snapshot *pebble.Snapshot) (store.BootstrapBaselineProbe, error) {
	if err := e.unchanged(); err != nil {
		return store.BootstrapBaselineProbe{}, err
	}
	if err := store.VerifyEncryptionCheckpointSnapshot(snapshot, e.checkpoint); err != nil {
		return store.BootstrapBaselineProbe{}, errors.WithStack(err)
	}
	for _, writer := range e.active {
		if writer.id != 0 {
			if err := store.VerifyOfflineWriterRegistrationSnapshot(snapshot, writer.id, e.memberID, writer.epoch); err != nil {
				return store.BootstrapBaselineProbe{}, errors.WithStack(err)
			}
		}
	}
	proof, err := store.ProbeBootstrapBaselineSnapshotWithDecoder(ctx, snapshot, e.decoder)
	if err != nil {
		return store.BootstrapBaselineProbe{}, errors.WithStack(err)
	}
	if err := e.unchanged(); err != nil {
		return store.BootstrapBaselineProbe{}, err
	}
	return proof, nil
}

// NativeCrypto supplies concrete AEAD, not a callback that could fake decrypt
// success. Native independently binds cutover to the verified StoreProof.
func (e *bootstrapOfflineEncryption) NativeCrypto() *etcd.OfflineBaselineCrypto {
	if e == nil {
		return nil
	}
	return &etcd.OfflineBaselineCrypto{RaftCipher: e.cipher, RaftEnvelopeCutoverIndex: e.cutover, ValidateSnapshot: e.validateSnapshot}
}

func (e *bootstrapOfflineEncryption) validateSnapshot(ctx context.Context, index uint64, input io.Reader) error {
	if err := e.unchanged(); err != nil {
		return err
	}
	reader := bufio.NewReader(input)
	_, cutover, cp, err := kv.ReadSnapshotHeaderWithCheckpoint(reader)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := e.validateSnapshotHeader(index, cutover, cp); err != nil {
		return err
	}
	proof, err := store.ProbeBootstrapBaselineStreamWithDecoder(ctx, reader, e.decoder)
	if err != nil {
		return errors.WithStack(err)
	}
	if proof.AppliedIndex > index || !proof.EncryptionCheckpointPresent || proof.EncryptionCheckpoint != cp {
		return errors.Wrap(errBootstrapOfflineStore, "snapshot applied/checkpoint projections disagree")
	}
	return e.unchanged()
}

func (e *bootstrapOfflineEncryption) validateSnapshotHeader(index, cutover uint64, cp encryption.SnapshotCheckpoint) error {
	if index == 0 || cp.Index == 0 || cp.Index > index || cp.Index > e.checkpoint.Index {
		return errors.Wrap(errBootstrapOfflineStore, "snapshot lacks bounded encryption checkpoint")
	}
	if cutover != 0 && cutover != e.cutover {
		return errors.Wrap(errBootstrapOfflineStore, "snapshot raft cutover differs from verified context")
	}
	if e.cutover != 0 && index > e.cutover && cutover == 0 {
		return errors.Wrap(errBootstrapOfflineStore, "snapshot raft cutover missing")
	}
	return nil
}

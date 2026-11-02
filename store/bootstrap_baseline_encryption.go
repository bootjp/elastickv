package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// OfflineBaselineDecoder exposes only authenticated observation. It has no DB,
// nonce factory, write gates or epoch-bump operation. Caller pins the cipher's
// complete historical keyset under exclusive offline ownership for its lifetime.
type OfflineBaselineDecoder struct{ cipher *encryption.Cipher }

func NewOfflineBaselineDecoder(cipher *encryption.Cipher) (*OfflineBaselineDecoder, error) {
	if cipher == nil || len(cipher.LoadedKeyIDs()) == 0 {
		return nil, errors.WithStack(ErrBootstrapBaselineUnsupported)
	}
	return &OfflineBaselineDecoder{cipher: cipher}, nil
}

func (d *OfflineBaselineDecoder) canonical(key, value []byte) ([]byte, error) {
	if d == nil || d.cipher == nil {
		return nil, errors.WithStack(ErrBootstrapBaselineUnsupported)
	}
	if isPebbleWriterRegistryKey(key) {
		if _, err := encryption.DecodeRegistryValue(value); err != nil {
			return nil, errors.Wrap(err, "offline registry codec")
		}
		return bytes.Clone(value), nil // This namespace's actual codec is raw, not enveloped.
	}
	if isPebbleMetaKey(key) && !IsRaftControlMetadataKey(key) {
		return bytes.Clone(value), nil
	}
	return d.canonicalStoredValue(key, value)
}

func (d *OfflineBaselineDecoder) canonicalStoredValue(key, value []byte) ([]byte, error) {
	sv, err := decodeValueView(value)
	if err != nil {
		return nil, errors.Wrap(err, "offline stored-value header")
	}
	if IsRaftControlMetadataKey(key) && (sv.Tombstone || sv.ExpireAt != 0) {
		return nil, errors.WithStack(ErrBootstrapBaselineInvalid)
	}
	// Use the exact runtime AAD/header, rebadging defense and bounded V2
	// decompression. Authentication happens even for expired/tombstone rows.
	reader := pebbleStore{cipher: d.cipher}
	plain, flag, err := reader.decryptAuthenticatedForKey(key, sv, sv.Value)
	if err != nil {
		return nil, errors.Wrap(err, "offline stored-value authentication")
	}
	plain, err = decompressAuthenticatedValue(plain, flag)
	if err != nil {
		return nil, errors.Wrap(err, "offline stored-value decompression")
	}
	return encodeValue(plain, sv.Tombstone, sv.ExpireAt, encStateCleartext), nil
}

// ProbeBootstrapBaselineSnapshotWithDecoder hashes every authenticated logical
// row with the SAME framing as the plaintext full-history probe. Only physical
// encryption/compression encoding is normalized. Nothing is excluded: TTL,
// tombstones, all MVCC history, raw metadata, control records and registry remain.
func ProbeBootstrapBaselineSnapshotWithDecoder(ctx context.Context, snapshot *pebble.Snapshot, decoder *OfflineBaselineDecoder) (BootstrapBaselineProbe, error) {
	if decoder == nil {
		return BootstrapBaselineProbe{}, errors.WithStack(ErrBootstrapBaselineUnsupported)
	}
	return probeBootstrapBaselineSnapshot(ctx, snapshot, decoder)
}

// VerifyBootstrapBaselinePreparedTransformWithDecoder authenticates BOTH full
// logical images, then compares the ORIGINAL physical rows exactly. Encryption
// does not permit replacements, reencryptions, nonce changes or extra exclusions.
func VerifyBootstrapBaselinePreparedTransformWithDecoder(ctx context.Context, source, target *pebble.Snapshot, allowed map[string][]byte, decoder *OfflineBaselineDecoder) error {
	key, value, err := baselinePreparedAddition(allowed)
	if err != nil {
		return err
	}
	if _, err := ProbeBootstrapBaselineSnapshotWithDecoder(ctx, source, decoder); err != nil {
		return err
	}
	if _, err := ProbeBootstrapBaselineSnapshotWithDecoder(ctx, target, decoder); err != nil {
		return err
	}
	return verifyBaselinePreparedRows(ctx, source, target, key, value)
}

// ProbeBootstrapBaselineStreamWithDecoder accepts only the actual native row
// stream EKVPBBL1, not SST bundles/streaming-MVCC encodings lacking this proof.
// It never restores a DB or writes a plaintext snapshot/artifact to disk.
func ProbeBootstrapBaselineStreamWithDecoder(ctx context.Context, input io.Reader, decoder *OfflineBaselineDecoder) (BootstrapBaselineProbe, error) {
	var zero BootstrapBaselineProbe
	if ctx == nil || input == nil || decoder == nil {
		return zero, errors.WithStack(ErrBootstrapBaselineInvalid)
	}
	magic, watermark, err := readPebbleNativeHeader(input)
	if err != nil {
		return zero, errors.WithStack(err)
	}
	if magic != pebbleSnapshotMagic {
		return zero, errors.Wrap(ErrBootstrapBaselineUnsupported, "encrypted snapshot must use native row stream")
	}
	r := baselineProbeReader{result: BootstrapBaselineProbe{Version: BootstrapBaselineProbeVersion}, hash: sha256.New(), txnHeads: make(map[string]baselineTxnHead), decoder: decoder}
	_, _ = r.hash.Write([]byte("elastickv/offline-full-mvcc-baseline/v1\x00"))
	if err := scanOfflineBaselineStream(ctx, input, &r); err != nil {
		return zero, err
	}
	if err := r.finish(ctx); err != nil {
		return zero, err
	}
	if !r.result.WatermarkPresent || r.result.Watermark != watermark {
		return zero, errors.Wrap(ErrBootstrapBaselineInvalid, "snapshot watermark disagrees with raw projection")
	}
	copy(r.result.DataDigest[:], r.hash.Sum(nil))
	return r.result, nil
}

func scanOfflineBaselineStream(ctx context.Context, input io.Reader, r *baselineProbeReader) error {
	var key, previous []byte
	for {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		klen, vlen, eof, err := readRestoreEntry(input, &key)
		if err != nil {
			return err
		}
		if eof {
			return nil
		}
		if previous != nil && bytes.Compare(previous, key[:klen]) >= 0 {
			return errors.Wrap(ErrBootstrapBaselineInvalid, "snapshot rows not strictly ordered")
		}
		value := make([]byte, vlen)
		if _, err := io.ReadFull(input, value); err != nil {
			return errors.WithStack(err)
		}
		if err := r.row(key[:klen], value); err != nil {
			return err
		}
		previous = append(previous[:0], key[:klen]...)
	}
}

// VerifyEncryptionCheckpointSnapshot binds an explicit sidecar to the actual
// durable raw checkpoint; this is read-only and does not synthesize a checkpoint.
func VerifyEncryptionCheckpointSnapshot(snapshot *pebble.Snapshot, want encryption.SnapshotCheckpoint) error {
	if snapshot == nil || want.Index == 0 {
		return errors.WithStack(ErrBootstrapBaselineInvalid)
	}
	raw, close, err := snapshot.Get(encryptionCheckpointKey)
	if err != nil {
		return errors.Wrap(err, "offline encryption checkpoint missing")
	}
	defer close.Close()
	actual, err := decodeOfflineEncryptionCheckpoint(raw)
	if err != nil {
		return err
	}
	if actual != want {
		return errors.Wrap(ErrBootstrapBaselineInvalid, "sidecar differs from durable encryption checkpoint")
	}
	return nil
}

func decodeOfflineEncryptionCheckpoint(raw []byte) (encryption.SnapshotCheckpoint, error) {
	var cp encryption.SnapshotCheckpoint
	if len(raw) != encryptionCheckpointSize {
		return cp, errors.Wrap(ErrBootstrapBaselineInvalid, "malformed encryption checkpoint")
	}
	cp.Index = binary.BigEndian.Uint64(raw[:8])
	copy(cp.State[:], raw[8:])
	if cp.Index == 0 {
		return encryption.SnapshotCheckpoint{}, errors.Wrap(ErrBootstrapBaselineInvalid, "zero encryption checkpoint")
	}
	return cp, nil
}

// VerifyOfflineWriterRegistrationSnapshot reads the real raw registry codec.
// A context cannot assert that active ciphertext belongs to another member or
// silently authorize stale/unregistered local epochs. No registry write occurs.
func VerifyOfflineWriterRegistrationSnapshot(snapshot *pebble.Snapshot, id uint32, member uint64, epoch uint16) error {
	if snapshot == nil || id == 0 || member == 0 {
		return errors.WithStack(ErrBootstrapBaselineInvalid)
	}
	raw, close, err := snapshot.Get(encryption.RegistryKey(id, encryption.NodeID16(member)))
	if err != nil {
		return errors.Wrap(ErrBootstrapBaselineInvalid, "active local writer registration missing")
	}
	defer close.Close()
	row, err := encryption.DecodeRegistryValue(raw)
	if err != nil {
		return errors.Wrap(err, "offline writer registration")
	}
	if row.FullNodeID != member || row.LastSeenLocalEpoch != epoch || row.FirstSeenLocalEpoch > epoch {
		return errors.Wrap(ErrBootstrapBaselineInvalid, "active local writer identity/epoch mismatch")
	}
	return nil
}

// VerifyBootstrapBaselinePhysicalEquality requires exact retained physical rows,
// independently of the logical digest. It never permits re-encryption on resume.
func VerifyBootstrapBaselinePhysicalEquality(ctx context.Context, source, target *pebble.Snapshot) (err error) {
	if ctx == nil || source == nil || target == nil {
		return errors.WithStack(ErrBootstrapBaselineTransform)
	}
	a, err := source.NewIter(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	b, err := target.NewIter(nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.Join(err, b.Close(), a.Error(), b.Error()) }()
	valid := a.First()
	for other := b.First(); other; other = b.Next() {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		if err := compareBaselinePreparedRow(a, valid, b); err != nil {
			return err
		}
		valid = a.Next()
	}
	if valid {
		return errors.Wrap(ErrBootstrapBaselineTransform, "physical source rows deleted")
	}
	return nil
}

package encryption

import (
	"strconv"

	"github.com/cockroachdb/errors"
)

// HydrateKeystoreFromSidecar unwraps every DEK recorded in the
// sidecar under the supplied KEK and installs it into ks. It is the
// startup counterpart to the FSM-apply keystore install path
// (ApplyBootstrap / ApplyRotation): on a fresh process load the
// keystore comes up empty, and FSM replay only re-installs DEKs whose
// OpBootstrap / OpRotation entries are still in the Raft log. After a
// log-compaction window those entries are gone, so a node restarting
// against a compacted log would have no DEK bytes and the cipher
// could not decrypt existing §4.1 envelopes. The sidecar is the
// durable record of every unretired wrapped DEK, so this function
// rebuilds the in-memory keystore from it.
//
// Every key in sc.Keys is hydrated (not just sc.Active.Storage):
// reads of versions written before a rotation need the historical
// DEK, so the cipher must hold every unretired DEK to satisfy
// Cipher.LoadedKeyIDs.
//
// Idempotent: keystore.Set is a no-op for byte-identical DEKs, so
// calling this after some DEKs were already installed by FSM replay
// is safe. A Set that returns ErrKeyConflict means the KEK-unwrap
// produced different bytes for an id the keystore already holds —
// a halt condition surfaced to the caller.
//
// nil ks or nil kek is a configuration error (the caller gates this
// on encryption being active); an empty sidecar (no Keys) is a no-op.
func HydrateKeystoreFromSidecar(ks *Keystore, kek KEKUnwrapper, sc *Sidecar) error {
	if ks == nil {
		return errors.New("encryption: HydrateKeystoreFromSidecar: keystore is nil")
	}
	if kek == nil {
		return errors.New("encryption: HydrateKeystoreFromSidecar: kek is nil")
	}
	if sc == nil {
		return errors.New("encryption: HydrateKeystoreFromSidecar: sidecar is nil")
	}
	for idStr, k := range sc.Keys {
		id, err := strconv.ParseUint(idStr, 10, 32)
		if err != nil {
			return errors.Wrapf(err, "encryption: HydrateKeystoreFromSidecar: parse key_id %q", idStr)
		}
		dek, err := kek.Unwrap(k.Wrapped)
		if err != nil {
			return errors.Wrapf(err, "encryption: HydrateKeystoreFromSidecar: kek-unwrap key_id %s", idStr)
		}
		if err := ks.Set(uint32(id), dek); err != nil {
			return errors.Wrapf(err, "encryption: HydrateKeystoreFromSidecar: keystore set key_id %s", idStr)
		}
	}
	return nil
}

// BumpLocalEpoch increments the §4.1 local_epoch for the DEK at dekID,
// fsyncs the sidecar, and returns the new value. It MUST be called on
// every process load that will issue storage-envelope nonces, BEFORE
// the first such nonce — the §4.1 nonce construction resets the
// in-process write_count to 0 each load, and a bumped-and-fsync'd
// local_epoch is what keeps `node_id ‖ local_epoch ‖ write_count`
// unique across restarts. Without the bump a restart would re-issue
// `node_id ‖ epoch ‖ {0,1,2,…}` and recycle previously-used GCM
// nonces under the same DEK — catastrophic.
//
// The bump refuses at the uint16 ceiling: if the current epoch is
// already 0xFFFF the function returns ErrLocalEpochExhausted (the
// §9.1 CheckStartupGuards guard catches this earlier on the happy
// path; the check here is defense-in-depth for callers that bump
// without having run the guard). DEK rotation resets the epoch to 0
// under a fresh DEK and is the only recovery.
//
// Durability: the increment is persisted via WriteSidecar (the same
// write-temp + fsync + rename + dir-sync sequence the apply paths
// use), so a crash after the return guarantees the new epoch is on
// disk. A crash before the return leaves the old epoch and the next
// start bumps again — no nonce was issued in between because no store
// has opened yet.
func BumpLocalEpoch(sidecarPath string, dekID uint32) (uint16, error) {
	if sidecarPath == "" {
		return 0, errors.New("encryption: BumpLocalEpoch: sidecar path is empty")
	}
	if dekID == ReservedKeyID {
		return 0, errors.New("encryption: BumpLocalEpoch: dek_id 0 is reserved (cluster not bootstrapped)")
	}
	sc, err := ReadSidecar(sidecarPath)
	if err != nil {
		return 0, errors.Wrap(err, "encryption: BumpLocalEpoch: read sidecar")
	}
	idStr := strconv.FormatUint(uint64(dekID), 10)
	k, ok := sc.Keys[idStr]
	if !ok {
		return 0, errors.Errorf("encryption: BumpLocalEpoch: no sidecar key for dek_id %d", dekID)
	}
	if k.LocalEpoch == localEpochMax {
		return 0, errors.Wrapf(ErrLocalEpochExhausted,
			"encryption: BumpLocalEpoch: dek_id %d already at local_epoch=0x%04X", dekID, localEpochMax)
	}
	k.LocalEpoch++
	sc.Keys[idStr] = k
	if err := WriteSidecar(sidecarPath, sc); err != nil {
		return 0, errors.Wrap(err, "encryption: BumpLocalEpoch: write sidecar")
	}
	return k.LocalEpoch, nil
}

// localEpochMax is the §4.1 16-bit local_epoch saturation value. A
// DEK at this epoch cannot bump further without nonce reuse, so
// BumpLocalEpoch refuses (matching ErrLocalEpochExhausted at startup).
const localEpochMax = uint16(0xFFFF)

// NodeID16 narrows a 64-bit full node id to the 16-bit node_id field
// of the §4.1 nonce (and of the writer-registry key). The truncation
// is the documented `uint16(full_node_id & 0xFFFF)` rule; cluster-wide
// uniqueness of the narrowed value is enforced by the writer registry
// + ErrNodeIDCollision guard, not by this function. Centralised here
// so call sites (main.go nonce-factory wiring, applier registry keys)
// share one masked-narrowing site instead of repeating the
// gosec-suppressed conversion.
func NodeID16(fullNodeID uint64) uint16 {
	return uint16(fullNodeID & nodeIDMask) //nolint:gosec // masked to 16 bits; G115 cannot trace the bitwise narrowing
}

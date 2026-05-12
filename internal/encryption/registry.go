package encryption

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// WriterRegistryPrefix reserves the Pebble key prefix used by the
// §4.1 writer registry. Format: `!encryption|writers|<be4 dek_id>|<be2 uint16(node_id)>`.
//
// The leading `!` and the pipe-separated layout match the existing
// `!admin|` reservation in adapter/distribution_server.go and the
// `txnInternalKeyPrefix` reservation in store/. The pebbleStore's
// applyMutationsBatch refuses user mutations whose key starts with
// this prefix so a malformed user Put cannot clobber registry rows.
//
// Registry rows are written through pebbleStore.EncryptionRegistryBatch
// (Stage 4 admin path), NOT through the MVCC encoded-key flow —
// these are operational state, not versioned user values.
var WriterRegistryPrefix = []byte("!encryption|writers|")

// registryKeySuffixSize is the per-row suffix that follows
// WriterRegistryPrefix: 4 bytes dek_id (BE) + 1 byte separator '|'
// + 2 bytes uint16(node_id) (BE).
const registryKeySuffixSize = 4 + 1 + 2

// registryValueSize is the on-disk value layout per §4.1:
//
//	full_node_id            uint64 BE  (8 bytes)
//	first_seen_local_epoch  uint16 BE  (2 bytes)
//	last_seen_local_epoch   uint16 BE  (2 bytes)
const registryValueSize = 8 + 2 + 2

// registry-key separator byte. Single-character pipe matches the
// outer prefix.
const registryKeySep = '|'

// Errors surfaced by registry codec helpers.
var (
	// ErrRegistryKeyMalformed indicates a Pebble key returned to
	// DecodeRegistryKey did not match the WriterRegistryPrefix +
	// fixed-suffix shape produced by RegistryKey.
	ErrRegistryKeyMalformed = errors.New("encryption: writer registry key is malformed")

	// ErrRegistryValueMalformed indicates a Pebble value returned to
	// DecodeRegistryValue did not match the fixed registryValueSize
	// layout. Stage 4 callers fail closed on this — a corrupted
	// registry row is fail-fast rather than silently treated as
	// "no entry".
	ErrRegistryValueMalformed = errors.New("encryption: writer registry value is malformed")
)

// RegistryKey constructs the Pebble key for the (dek_id,
// uint16(node_id)) writer-registry row. The 16-bit truncation of
// the full FNV-64a node id is per §4.1 — the registry distinguishes
// truncation collisions via the value's full_node_id field, not the
// key.
func RegistryKey(dekID uint32, nodeID16 uint16) []byte {
	out := make([]byte, len(WriterRegistryPrefix)+registryKeySuffixSize)
	n := copy(out, WriterRegistryPrefix)
	binary.BigEndian.PutUint32(out[n:n+4], dekID)
	out[n+4] = registryKeySep
	binary.BigEndian.PutUint16(out[n+5:n+7], nodeID16)
	return out
}

// RegistryDEKPrefix returns the prefix that covers every writer
// registry row for dekID. Used by §5.4 retirement to drop the
// entire `!encryption|writers|<dek_id>|*` slice in the same Raft
// entry that retires the DEK.
func RegistryDEKPrefix(dekID uint32) []byte {
	out := make([]byte, len(WriterRegistryPrefix)+4+1)
	n := copy(out, WriterRegistryPrefix)
	binary.BigEndian.PutUint32(out[n:n+4], dekID)
	out[n+4] = registryKeySep
	return out
}

// DecodeRegistryKey parses a registry-row key back into its
// (dek_id, uint16 node_id) tuple. Returns ErrRegistryKeyMalformed
// when the key does not start with WriterRegistryPrefix or has the
// wrong length. The decoder does NOT range-check the parsed
// dek_id against ReservedKeyID — that is the caller's policy.
func DecodeRegistryKey(key []byte) (dekID uint32, nodeID16 uint16, err error) {
	if len(key) != len(WriterRegistryPrefix)+registryKeySuffixSize {
		return 0, 0, errors.Wrapf(ErrRegistryKeyMalformed, "got %d bytes", len(key))
	}
	for i, b := range WriterRegistryPrefix {
		if key[i] != b {
			return 0, 0, errors.WithStack(ErrRegistryKeyMalformed)
		}
	}
	body := key[len(WriterRegistryPrefix):]
	if body[4] != registryKeySep {
		return 0, 0, errors.WithStack(ErrRegistryKeyMalformed)
	}
	dekID = binary.BigEndian.Uint32(body[0:4])
	nodeID16 = binary.BigEndian.Uint16(body[5:7])
	return dekID, nodeID16, nil
}

// RegistryValue is the in-memory form of a writer-registry row's
// stored value: the full untruncated FNV-64a node id and the
// per-DEK first/last-seen local_epoch counters used by the §4.1
// case 1/2/3 dispatch on RegisterEncryptionWriter apply.
type RegistryValue struct {
	FullNodeID          uint64
	FirstSeenLocalEpoch uint16
	LastSeenLocalEpoch  uint16
}

// EncodeRegistryValue serialises rv to its on-disk byte form.
// Always returns exactly registryValueSize bytes (12).
func EncodeRegistryValue(rv RegistryValue) []byte {
	out := make([]byte, registryValueSize)
	binary.BigEndian.PutUint64(out[0:8], rv.FullNodeID)
	binary.BigEndian.PutUint16(out[8:10], rv.FirstSeenLocalEpoch)
	binary.BigEndian.PutUint16(out[10:12], rv.LastSeenLocalEpoch)
	return out
}

// DecodeRegistryValue parses an on-disk row value. Fails closed on
// length mismatch — a truncated or padded row is operational
// corruption, not a "missing" entry, so the caller surfaces
// ErrRegistryValueMalformed rather than silently treating the
// node as un-registered.
func DecodeRegistryValue(raw []byte) (RegistryValue, error) {
	if len(raw) != registryValueSize {
		return RegistryValue{}, errors.Wrapf(ErrRegistryValueMalformed,
			"got %d bytes, want %d", len(raw), registryValueSize)
	}
	return RegistryValue{
		FullNodeID:          binary.BigEndian.Uint64(raw[0:8]),
		FirstSeenLocalEpoch: binary.BigEndian.Uint16(raw[8:10]),
		LastSeenLocalEpoch:  binary.BigEndian.Uint16(raw[10:12]),
	}, nil
}

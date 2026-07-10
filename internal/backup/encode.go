package backup

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/cockroachdb/errors"
)

// encode.go is the Phase 0b encoder core (design:
// docs/design/2026_05_25_implemented_snapshot_logical_encoder.md). It is
// the inverse of decode.go: per-adapter reverse encoders (added in the
// Redis/DynamoDB/S3/SQS milestones) hand reconstructed internal
// (userKey, userValue, expireAt) records to a snapshotBuilder, which
// MVCC-frames them, sorts by encoded key, and streams the native
// EKVPBBL1 `.fsm` the live store loads.
//
// This file owns only the format-level machinery that every adapter
// shares:
//
//   - commit-timestamp resolution (MANIFEST.last_commit_ts plus the
//     fail-closed `--last-commit-ts` override),
//   - MVCC re-encoding (the inverse of snapshot_reader.go's
//     decodeSnapshotEntry: invTS key suffix + value header),
//   - duplicate-key rejection and per-entry size caps,
//   - the sorted EKVPBBL1 writer (matching store/snapshot_pebble.go
//     WriteTo — magic + lastCommitTS + sorted KV stream, no checksum
//     footer),
//   - a round-trip self-test harness that decodes the just-written
//     bytes back through ReadSnapshot so a caller never emits an
//     unloadable `.fsm`.
//
// The MVCC constants (snapshotTSSize, snapshotValueHeaderSize,
// Max*EncodedKeySize, PebbleSnapshotMagic, the flag masks) are the
// same ones snapshot_reader.go defines for the decode direction; encode
// reuses them so the two directions cannot drift.

// ErrEncodeDuplicateKey is returned when two reconstructed records
// MVCC-encode to the same key. The live store's keyspace is a set;
// a duplicate means an adapter reverse-encoder produced colliding
// internal keys, which would make the loaded snapshot
// order-dependent. The encoder fails closed rather than emit a
// `.fsm` whose Pebble image depends on insertion order.
var ErrEncodeDuplicateKey = errors.New("backup: duplicate encoded key in snapshot build")

// ErrEncodeKeyTooLarge / ErrEncodeValueTooLarge mirror the decode-side
// MaxSnapshotEncodedKeySize / MaxSnapshotEncodedValueSize caps. A
// reconstructed entry exceeding them would produce a `.fsm` the live
// restore path rejects, so the encoder fails closed before writing a
// single byte rather than emit an unloadable file.
var ErrEncodeKeyTooLarge = errors.New("backup: encoded key length exceeds limit")
var ErrEncodeValueTooLarge = errors.New("backup: encoded value length exceeds limit")

// ErrSnapshotBuilderReused is returned by WriteTo when it is called
// more than once on the same builder. A builder is single-use (one
// per encode run); a second WriteTo would re-emit the already-written
// entries, producing a valid-but-unintended stream. Enforced so the
// per-adapter feed loops in later milestones cannot silently double-
// emit (claude review on PR #825).
var ErrSnapshotBuilderReused = errors.New("backup: snapshotBuilder.WriteTo called more than once")

// ErrLastCommitTSRegression is returned by ResolveCommitTS when a
// `--last-commit-ts` override is older than MANIFEST.last_commit_ts.
// Seeding the restored node's HLC ceiling below a timestamp already
// durable in the dump would let a post-restart leader re-issue a
// timestamp at-or-below a restored row's commit ts — the exact
// HLC-ceiling regression the design's §"MVCC re-encoding" forbids.
// Raising the ceiling (T >= manifest) is always safe; lowering it is
// not, so the override is accepted in one direction only.
var ErrLastCommitTSRegression = errors.New("backup: --last-commit-ts override is older than manifest last_commit_ts")

// ResolveCommitTS returns the commit timestamp the encoder stamps on
// every reconstructed key (design §"MVCC re-encoding": uniform
// stamping). manifestTS is MANIFEST.last_commit_ts; override is the
// optional `--last-commit-ts` value (nil = no override).
//
// Fail-closed contract: an override is accepted only when it is
// >= manifestTS (raising the restored HLC ceiling is safe; lowering
// it risks a post-restart timestamp colliding with a restored row).
// The returned value is used verbatim for BOTH the EKVPBBL1 header
// and every key's invTS, so the two never disagree.
func ResolveCommitTS(manifestTS uint64, override *uint64) (uint64, error) {
	if override == nil {
		return manifestTS, nil
	}
	if *override < manifestTS {
		return 0, errors.Wrapf(ErrLastCommitTSRegression,
			"override %d < manifest %d", *override, manifestTS)
	}
	return *override, nil
}

// encodeMVCCKey is the inverse of the key half of
// snapshot_reader.go::decodeSnapshotEntry: it appends the 8-byte
// big-endian inverted-timestamp suffix the live store's
// fillEncodedKey writes. invTS = ^commitTS so that, under Pebble's
// ascending byte order, newer (higher-ts) versions of a user key sort
// before older ones — the MVCC ordering the live store relies on.
func encodeMVCCKey(userKey []byte, commitTS uint64) []byte {
	out := make([]byte, len(userKey)+snapshotTSSize)
	copy(out, userKey)
	binary.BigEndian.PutUint64(out[len(userKey):], ^commitTS)
	return out
}

// encodeMVCCValue is the inverse of the value half of
// decodeSnapshotEntry: it prepends the 9-byte value header
// (flags byte + 8-byte little-endian expireAt). Phase 0b emits live,
// cleartext, non-tombstone records only, so flags is always zero.
func encodeMVCCValue(userValue []byte, expireAt uint64) []byte {
	// Build by append from a const-capacity, zero-length slice: the
	// header (flags byte + 8-byte LE expireAt) then the body. Using a
	// constant cap (not snapshotValueHeaderSize+len(userValue)) keeps
	// the `const + len(userValue)` arithmetic out of make(), which
	// CodeQL flags as a potential allocation-size overflow; the
	// zero-length start keeps makezero happy. The builder already caps
	// userValue at MaxSnapshotEncodedValueSize before this is reached.
	out := make([]byte, 0, snapshotValueHeaderSize)
	out = append(out, 0) // flags: tombstone=0, encryption_state=cleartext, reserved=0
	out = binary.LittleEndian.AppendUint64(out, expireAt)
	return append(out, userValue...)
}

// encodedKV is one fully MVCC-framed entry held by snapshotBuilder
// until the sorted write.
type encodedKV struct {
	key []byte
	val []byte
}

// snapshotBuilder accumulates MVCC-framed entries and writes the
// sorted EKVPBBL1 stream. One per encode run. Not safe for concurrent
// use — adapters feed it sequentially from the directory-tree walk.
type snapshotBuilder struct {
	commitTS uint64
	entries  []encodedKV
	seen     map[string]struct{}
	written  bool
}

// newSnapshotBuilder constructs a builder that stamps every key with
// commitTS (the value ResolveCommitTS returned).
func newSnapshotBuilder(commitTS uint64) *snapshotBuilder {
	return &snapshotBuilder{
		commitTS: commitTS,
		seen:     make(map[string]struct{}),
	}
}

// Add MVCC-frames one reconstructed live record and stages it for the
// sorted write. Applies the per-entry size caps and duplicate-key
// rejection before retaining the bytes, so a violating record fails
// the whole encode rather than producing an unloadable or
// order-dependent `.fsm`. userKey/userValue are copied — callers may
// reuse their buffers after Add returns.
func (b *snapshotBuilder) Add(userKey, userValue []byte, expireAt uint64) error {
	// A builder is single-use. Once WriteTo has flushed, any further
	// Add would stage entries that can never be written (the second
	// WriteTo fails closed before re-sorting), silently dropping
	// records. Reject with the same sentinel WriteTo uses so a caller
	// reusing an exhausted builder gets one consistent signal
	// regardless of which method it calls (claude review on PR #825 —
	// a silent-data-loss footgun for the M2-M5 adapter feed loops).
	if b.written {
		return errors.WithStack(ErrSnapshotBuilderReused)
	}
	// Size-check from the user-buffer lengths before allocating the
	// framed buffers — encKey = userKey + snapshotTSSize, encVal =
	// snapshotValueHeaderSize + userValue — so an oversize record
	// fails closed without a wasted allocation (claude review nit).
	if uint64(len(userKey))+snapshotTSSize > MaxSnapshotEncodedKeySize {
		return errors.Wrapf(ErrEncodeKeyTooLarge,
			"length %d > %d", len(userKey)+snapshotTSSize, MaxSnapshotEncodedKeySize)
	}
	if uint64(len(userValue))+snapshotValueHeaderSize > MaxSnapshotEncodedValueSize {
		return errors.Wrapf(ErrEncodeValueTooLarge,
			"length %d > %d", len(userValue)+snapshotValueHeaderSize, MaxSnapshotEncodedValueSize)
	}
	key := encodeMVCCKey(userKey, b.commitTS)
	if _, dup := b.seen[string(key)]; dup {
		return errors.Wrapf(ErrEncodeDuplicateKey, "userKey %q", userKey)
	}
	b.seen[string(key)] = struct{}{}
	b.entries = append(b.entries, encodedKV{key: key, val: encodeMVCCValue(userValue, expireAt)})
	return nil
}

// Len reports how many entries have been staged.
func (b *snapshotBuilder) Len() int { return len(b.entries) }

// WriteTo sorts the staged entries by encoded key (ascending byte
// order, matching the live Pebble-snapshot iteration order) and writes
// the native EKVPBBL1 stream: magic + lastCommitTS (LE) + length-
// prefixed (key, value) pairs. There is no checksum footer — the
// format terminates on a clean EOF (store/snapshot_pebble.go WriteTo,
// internal/backup/snapshot_reader.go ReadSnapshot).
func (b *snapshotBuilder) WriteTo(w io.Writer) (int64, error) {
	if b.written {
		return 0, errors.WithStack(ErrSnapshotBuilderReused)
	}
	b.written = true
	sort.Slice(b.entries, func(i, j int) bool {
		return bytes.Compare(b.entries[i].key, b.entries[j].key) < 0
	})
	cw := &countingWriter{w: w}
	if _, err := cw.Write([]byte(PebbleSnapshotMagic)); err != nil {
		return cw.n, errors.WithStack(err)
	}
	if err := binary.Write(cw, binary.LittleEndian, b.commitTS); err != nil {
		return cw.n, errors.WithStack(err)
	}
	for i := range b.entries {
		if err := writeLengthPrefixed(cw, b.entries[i].key); err != nil {
			return cw.n, err
		}
		if err := writeLengthPrefixed(cw, b.entries[i].val); err != nil {
			return cw.n, err
		}
	}
	return cw.n, nil
}

// writeLengthPrefixed writes an 8-byte little-endian length followed
// by the bytes — the per-field framing ReadSnapshot's readEntryLen
// consumes.
func writeLengthPrefixed(w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(len(b))); err != nil {
		return errors.WithStack(err)
	}
	if _, err := w.Write(b); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// countingWriter mirrors store/snapshot_pebble.go's helper so WriteTo
// can report the byte count without importing the store package
// (the offline-tool boundary the design requires).
type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

// RoundTripEntry is one live record recovered by DecodeLiveEntries.
// The byte slices are owned by the caller (cloned out of the reader's
// scratch buffer).
type RoundTripEntry struct {
	UserKey   []byte
	UserValue []byte
	ExpireAt  uint64
}

// DecodeLiveEntries decodes an EKVPBBL1 stream through ReadSnapshot
// and returns its live (non-tombstone) entries plus the header. It is
// the round-trip self-test primitive from the design's §"Round-trip
// self-test": the encoder decodes its own just-written bytes and
// compares the recovered records against what it fed the builder
// before committing the final `.fsm` to disk, so a node never
// receives an unloadable snapshot.
//
// Tombstones are skipped (the encoder never writes them, so seeing one
// would indicate a corrupted build; surfacing only live records keeps
// the comparison symmetric with what Add accepts). Byte slices are
// cloned because ReadSnapshot reuses its scratch buffers across the
// callback.
func DecodeLiveEntries(r io.Reader) ([]RoundTripEntry, SnapshotHeader, error) {
	var out []RoundTripEntry
	hdr, err := ReadSnapshotWithHeader(r, func(_ SnapshotHeader, e SnapshotEntry) error {
		if e.Tombstone {
			return nil
		}
		out = append(out, RoundTripEntry{
			UserKey:   bytes.Clone(e.UserKey),
			UserValue: bytes.Clone(e.UserValue),
			ExpireAt:  e.ExpireAt,
		})
		return nil
	})
	if err != nil {
		return nil, SnapshotHeader{}, err
	}
	return out, hdr, nil
}

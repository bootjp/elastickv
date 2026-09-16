package s3keys

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"time"
)

// Reference counting and GC eligibility for content-addressed
// chunkblobs (design §3.5).
//
// Two keyspaces, both Raft-replicated:
//
//	!s3|chunkref-rc|<sha-hex>                    -> ChunkRefRC
//	!s3|chunkblob-gc-queue|<commitTS>|<sha-hex>  -> empty
//
// Every timestamp here is an elastickv HLC commit timestamp —
// (UnixMilli << HLCLogicalBits) | logical — NOT Unix nanoseconds. The
// queue key is built from the commitTS of the txn that drove the
// reference count to zero, so the sweeper's grace boundary has to be
// expressed in the same domain. Mixing the two silently produces a
// boundary off by roughly six orders of magnitude, which would either
// sweep everything immediately or never sweep at all.
//
// The queue key carries the commit timestamp in its NAME rather than
// its value, and that is the whole point: a counter sitting at zero
// records *that* a blob became reclaimable but not *when*, so the
// grace window would be unimplementable. Big-endian fixed-width
// encoding makes the queue sort by eligibility time, so a sweeper
// finds everything past the grace boundary with one range scan ending
// at ChunkBlobGCQueueScanEnd(now - grace).
const (
	ChunkRefRCPrefix       = "!s3|chunkref-rc|"
	ChunkBlobGCQueuePrefix = "!s3|chunkblob-gc-queue|"

	// hlcLogicalBits mirrors kv.HLCLogicalBits. It is duplicated
	// rather than imported because internal/s3keys cannot import kv
	// without a cycle (kv -> distribution -> s3keys). The external
	// test asserts the two stay equal, so the duplication cannot
	// drift silently.
	hlcLogicalBits = 16

	// chunkRefRCValueBytes is the fixed width of an encoded
	// ChunkRefRC: the count followed by the queue timestamp.
	chunkRefRCValueBytes = 2 * u64Bytes

	// chunkBlobGCQueueSeparator delimits the timestamp from the SHA.
	// It must sort below every hex digit so the scan-end key built
	// from a bare timestamp excludes that timestamp's own entries
	// only when intended; '|' (0x7C) is above hex, so the separator
	// is chosen to match the surrounding key grammar and the end key
	// is built explicitly rather than by string concatenation.
	chunkBlobGCQueueSeparator = '|'
)

var (
	chunkRefRCPrefixBytes       = []byte(ChunkRefRCPrefix)
	chunkBlobGCQueuePrefixBytes = []byte(ChunkBlobGCQueuePrefix)
)

// ChunkRefRCKey builds the reference-count key for a content hash.
func ChunkRefRCKey(contentSHA256 [chunkBlobSHA256Bytes]byte) []byte {
	out := make([]byte, 0, len(ChunkRefRCPrefix)+chunkBlobSHA256HexBytes)
	out = append(out, chunkRefRCPrefixBytes...)
	return hex.AppendEncode(out, contentSHA256[:])
}

// ParseChunkRefRCKey decodes a reference-count key.
func ParseChunkRefRCKey(key []byte) ([chunkBlobSHA256Bytes]byte, bool) {
	var sha [chunkBlobSHA256Bytes]byte
	if !bytes.HasPrefix(key, chunkRefRCPrefixBytes) {
		return sha, false
	}
	return decodeSHAHex(key[len(chunkRefRCPrefixBytes):], sha)
}

// ChunkRefRC is the reference-count record for one content hash.
//
// QueuedAtTS carries the timestamp of this SHA's GC-queue entry, or
// zero when it has none. It is part of the VALUE because §3.5 requires
// a txn that re-references a SHA to delete the queue entry atomically
// with incrementing the count — and the queue key embeds the
// eligibility timestamp, which that txn has no other way to learn.
// Without it the re-referencing txn cannot name the key it must
// delete, leaving a stale entry that points the sweeper at a blob
// which is once again live.
type ChunkRefRC struct {
	Count      uint64
	QueuedAtTS uint64
}

// Queued reports whether this SHA currently has a GC-queue entry.
func (r ChunkRefRC) Queued() bool { return r.QueuedAtTS != 0 }

// EncodeChunkRefRC encodes a reference-count record.
func EncodeChunkRefRC(rc ChunkRefRC) []byte {
	out := make([]byte, 0, chunkRefRCValueBytes)
	out = binary.BigEndian.AppendUint64(out, rc.Count)
	return binary.BigEndian.AppendUint64(out, rc.QueuedAtTS)
}

// DecodeChunkRefRC decodes a reference-count record. A missing key and
// an explicit zero count are equivalent to the caller — both mean "no
// live reference" — but a malformed value is not, so it fails closed
// rather than defaulting to zero and making a live blob look
// collectable.
func DecodeChunkRefRC(value []byte) (ChunkRefRC, bool) {
	if len(value) != chunkRefRCValueBytes {
		return ChunkRefRC{}, false
	}
	return ChunkRefRC{
		Count:      binary.BigEndian.Uint64(value[:u64Bytes]),
		QueuedAtTS: binary.BigEndian.Uint64(value[u64Bytes:]),
	}, true
}

// ChunkBlobGCQueueKey builds the eligibility-queue key for a content
// hash that became unreferenced at commitTS.
//
// The timestamp is fixed-width big-endian so the queue sorts by
// eligibility time; a decimal or variable-width encoding would order
// 9 after 10 and silently break the grace-boundary scan.
// A zero commitTS is rejected by the caller contract: ChunkRefRC uses
// zero as its "no queue entry" sentinel, so a record genuinely queued
// at timestamp zero would report Queued() == false and its entry would
// become unreachable. A real HLC commit timestamp is never zero — the
// physical half is Unix milliseconds — so this costs nothing.
func ChunkBlobGCQueueKey(commitTS uint64, contentSHA256 [chunkBlobSHA256Bytes]byte) []byte {
	out := make([]byte, 0,
		len(ChunkBlobGCQueuePrefix)+u64Bytes+1+chunkBlobSHA256HexBytes)
	out = append(out, chunkBlobGCQueuePrefixBytes...)
	out = binary.BigEndian.AppendUint64(out, commitTS)
	out = append(out, chunkBlobGCQueueSeparator)
	return hex.AppendEncode(out, contentSHA256[:])
}

// ParseChunkBlobGCQueueKey decodes an eligibility-queue key into the
// timestamp at which the blob became unreferenced and its content hash.
func ParseChunkBlobGCQueueKey(key []byte) (uint64, [chunkBlobSHA256Bytes]byte, bool) {
	var sha [chunkBlobSHA256Bytes]byte
	if !bytes.HasPrefix(key, chunkBlobGCQueuePrefixBytes) {
		return 0, sha, false
	}
	rest := key[len(chunkBlobGCQueuePrefixBytes):]
	if len(rest) != u64Bytes+1+chunkBlobSHA256HexBytes {
		return 0, sha, false
	}
	if rest[u64Bytes] != chunkBlobGCQueueSeparator {
		return 0, sha, false
	}
	commitTS := binary.BigEndian.Uint64(rest[:u64Bytes])
	sha, ok := decodeSHAHex(rest[u64Bytes+1:], sha)
	if !ok {
		return 0, sha, false
	}
	return commitTS, sha, true
}

// ChunkBlobGCQueueScanStart is the inclusive lower bound for a sweeper
// scan: the start of the whole queue.
func ChunkBlobGCQueueScanStart() []byte {
	return append([]byte(nil), chunkBlobGCQueuePrefixBytes...)
}

// ChunkBlobGCQueueScanEnd is the EXCLUSIVE upper bound for a sweeper
// scan covering everything that became eligible strictly before
// boundaryTS.
//
// boundaryTS is an HLC commit timestamp, not a Unix nanosecond count.
// Build it with ChunkBlobGCGraceBoundary rather than from
// time.Now().UnixNano(), which is a different domain entirely.
//
// Exclusivity matters: passing the current timestamp would sweep a
// blob that became eligible this instant, skipping the grace window
// entirely. An entry stamped exactly at the boundary is excluded — it
// has not yet served the full grace.
func ChunkBlobGCQueueScanEnd(boundaryTS uint64) []byte {
	out := make([]byte, 0, len(ChunkBlobGCQueuePrefix)+u64Bytes)
	out = append(out, chunkBlobGCQueuePrefixBytes...)
	return binary.BigEndian.AppendUint64(out, boundaryTS)
}

// decodeSHAHex decodes a lowercase hex SHA-256 of the exact expected
// width. Length is checked before decoding so a short or padded key
// cannot decode into a partially-populated digest.
func decodeSHAHex(encoded []byte, sha [chunkBlobSHA256Bytes]byte) ([chunkBlobSHA256Bytes]byte, bool) {
	if len(encoded) != chunkBlobSHA256HexBytes {
		return sha, false
	}
	if _, err := hex.Decode(sha[:], encoded); err != nil {
		return sha, false
	}
	return sha, true
}

// ChunkBlobGCGraceBoundary converts a wall-clock grace period into the
// HLC boundary timestamp a sweeper passes to ChunkBlobGCQueueScanEnd.
//
// It exists so callers never have to open-code the HLC layout, which
// is where the domain confusion would creep in: the queue keys carry
// HLC commit timestamps, so subtracting a duration means subtracting
// milliseconds from the PHYSICAL half, not nanoseconds from the whole
// value.
//
// A grace period that reaches back past the epoch clamps to zero
// rather than wrapping, so an absurd configuration sweeps nothing
// instead of sweeping everything.
func ChunkBlobGCGraceBoundary(nowTS uint64, grace time.Duration) uint64 {
	physicalMs := nowTS >> hlcLogicalBits
	graceMs := uint64(0)
	if ms := grace.Milliseconds(); ms > 0 {
		// Guarded above zero, so the conversion cannot go negative.
		graceMs = uint64(ms)
	}
	if graceMs >= physicalMs {
		return 0
	}
	return (physicalMs - graceMs) << hlcLogicalBits
}

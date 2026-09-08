package s3keys

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
)

// Reference counting and GC eligibility for content-addressed
// chunkblobs (design §3.5).
//
// Two keyspaces, both Raft-replicated:
//
//	!s3|chunkref-rc|<sha-hex>                    -> uint64 reference count
//	!s3|chunkblob-gc-queue|<commitTS>|<sha-hex>  -> empty
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

// EncodeChunkRefRC encodes a reference count.
func EncodeChunkRefRC(count uint64) []byte {
	out := make([]byte, u64Bytes)
	binary.BigEndian.PutUint64(out, count)
	return out
}

// DecodeChunkRefRC decodes a reference count. A missing key and an
// explicit zero are equivalent to the caller — both mean "no live
// reference" — but a malformed value is not, so it fails closed
// rather than defaulting to zero and making a live blob look
// collectable.
func DecodeChunkRefRC(value []byte) (uint64, bool) {
	if len(value) != u64Bytes {
		return 0, false
	}
	return binary.BigEndian.Uint64(value), true
}

// ChunkBlobGCQueueKey builds the eligibility-queue key for a content
// hash that became unreferenced at commitTSNanos.
//
// The timestamp is fixed-width big-endian so the queue sorts by
// eligibility time; a decimal or variable-width encoding would order
// 9 after 10 and silently break the grace-boundary scan.
func ChunkBlobGCQueueKey(commitTSNanos uint64, contentSHA256 [chunkBlobSHA256Bytes]byte) []byte {
	out := make([]byte, 0,
		len(ChunkBlobGCQueuePrefix)+u64Bytes+1+chunkBlobSHA256HexBytes)
	out = append(out, chunkBlobGCQueuePrefixBytes...)
	out = binary.BigEndian.AppendUint64(out, commitTSNanos)
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
	commitTSNanos := binary.BigEndian.Uint64(rest[:u64Bytes])
	sha, ok := decodeSHAHex(rest[u64Bytes+1:], sha)
	if !ok {
		return 0, sha, false
	}
	return commitTSNanos, sha, true
}

// ChunkBlobGCQueueScanStart is the inclusive lower bound for a sweeper
// scan: the start of the whole queue.
func ChunkBlobGCQueueScanStart() []byte {
	return append([]byte(nil), chunkBlobGCQueuePrefixBytes...)
}

// ChunkBlobGCQueueScanEnd is the EXCLUSIVE upper bound for a sweeper
// scan covering everything that became eligible strictly before
// boundaryNanos.
//
// Exclusivity matters: passing `now` would sweep a blob that became
// eligible this instant, skipping the grace window entirely. Callers
// pass `now - gracePeriod`, and an entry stamped exactly at the
// boundary is excluded — it has not yet served the full grace.
func ChunkBlobGCQueueScanEnd(boundaryNanos uint64) []byte {
	out := make([]byte, 0, len(ChunkBlobGCQueuePrefix)+u64Bytes)
	out = append(out, chunkBlobGCQueuePrefixBytes...)
	return binary.BigEndian.AppendUint64(out, boundaryNanos)
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

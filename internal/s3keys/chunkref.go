package s3keys

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
)

const (
	chunkBlobSHA256Bytes      = 32
	chunkBlobSHA256HexBytes   = chunkBlobSHA256Bytes * 2
	chunkRefValueVersionV1    = byte(1)
	chunkRefValueVersionV2    = byte(2)
	chunkRefValueFixedBytes   = 1 + chunkBlobSHA256Bytes + u64Bytes + 2
	chunkRefSourcePeerLenSize = 2
	chunkRefReplicaCountSize  = 2
	chunkRefPeerFieldLenSize  = 2
	maxChunkRefSourcePeerLen  = int(^uint16(0))
	maxChunkRefReplicaCount   = int(^uint16(0))
)

// ErrChunkRefSourcePeerTooLong is returned when a ChunkRefValue source peer
// cannot be encoded into the fixed-width value header.
var ErrChunkRefSourcePeerTooLong = errors.New("s3 chunkref source peer is too long")

// ErrChunkRefReplicaSetTooLarge is returned when a write-time replica set
// cannot be represented by the V2 chunkref format.
var ErrChunkRefReplicaSetTooLarge = errors.New("s3 chunkref replica set is too large")

// ErrChunkRefPeerFieldTooLong is returned when a replica node ID or address
// cannot be represented by the V2 chunkref format.
var ErrChunkRefPeerFieldTooLong = errors.New("s3 chunkref replica field is too long")

// ChunkRefPeer records a peer that durably acknowledged the chunkblob before
// its reference became reachable through Raft.
type ChunkRefPeer struct {
	NodeID  string
	Address string
}

// ChunkRefValue is the stored value for a !s3|chunkref| entry. It points from
// an object part chunk to the content-addressed !s3|chunkblob| payload.
type ChunkRefValue struct {
	ContentSHA256 [chunkBlobSHA256Bytes]byte
	Size          uint64
	SourcePeer    string
	ReplicaPeers  []ChunkRefPeer
}

// ParseChunkRefKey decodes a !s3|chunkref| key into its object-part location.
func ParseChunkRefKey(key []byte) (bucket string, generation uint64, object string, uploadID string, partNo uint64, chunkNo uint64, ok bool) {
	bucket, generation, object, uploadID, partNo, chunkNo, _, ok = ParseVersionedChunkRefKey(key)
	return bucket, generation, object, uploadID, partNo, chunkNo, ok
}

// ParseVersionedChunkRefKey decodes both the original key shape and the
// attempt-versioned shape. partVersion is zero for original keys.
func ParseVersionedChunkRefKey(key []byte) (bucket string, generation uint64, object string, uploadID string, partNo uint64, chunkNo uint64, partVersion uint64, ok bool) {
	if !bytes.HasPrefix(key, chunkRefPrefixBytes) {
		return "", 0, "", "", 0, 0, 0, false
	}
	parts, ok := parseObjectChunkKeyHead(key, len(chunkRefPrefixBytes))
	if !ok {
		return "", 0, "", "", 0, 0, 0, false
	}
	partVersion, ok = parseOptionalPartVersion(key, parts.next)
	if !ok {
		return "", 0, "", "", 0, 0, 0, false
	}
	return parts.bucket, parts.generation, parts.object, parts.uploadID, parts.partNo, parts.chunkNo, partVersion, true
}

// ChunkBlobKey builds a content-addressed blob key from a SHA-256 digest.
func ChunkBlobKey(contentSHA256 [chunkBlobSHA256Bytes]byte) []byte {
	out := make([]byte, 0, len(ChunkBlobPrefix)+chunkBlobSHA256HexBytes)
	out = append(out, chunkBlobPrefixBytes...)
	out = hex.AppendEncode(out, contentSHA256[:])
	return out
}

// ParseChunkBlobKey decodes a !s3|chunkblob| key into its SHA-256 digest.
func ParseChunkBlobKey(key []byte) ([chunkBlobSHA256Bytes]byte, bool) {
	var sha [chunkBlobSHA256Bytes]byte
	if !bytes.HasPrefix(key, chunkBlobPrefixBytes) {
		return sha, false
	}
	encoded := key[len(chunkBlobPrefixBytes):]
	if len(encoded) != chunkBlobSHA256HexBytes {
		return sha, false
	}
	if _, err := hex.Decode(sha[:], encoded); err != nil {
		return sha, false
	}
	return sha, true
}

// EncodeChunkRefValue encodes a chunk reference value using a versioned binary
// shape so future offload metadata can fail closed on unknown versions.
func EncodeChunkRefValue(ref ChunkRefValue) ([]byte, error) {
	sourcePeerLen, err := chunkRefSourcePeerLen(len(ref.SourcePeer))
	if err != nil {
		return nil, err
	}
	version := chunkRefValueVersionV1
	if len(ref.ReplicaPeers) > 0 {
		version = chunkRefValueVersionV2
	}
	out := make([]byte, 0, chunkRefValueFixedBytes+len(ref.SourcePeer))
	out = append(out, version)
	out = append(out, ref.ContentSHA256[:]...)
	out = appendU64(out, ref.Size)
	var sourceLen [chunkRefSourcePeerLenSize]byte
	binary.BigEndian.PutUint16(sourceLen[:], sourcePeerLen)
	out = append(out, sourceLen[:]...)
	out = append(out, ref.SourcePeer...)
	if version == chunkRefValueVersionV1 {
		return out, nil
	}
	if len(ref.ReplicaPeers) > maxChunkRefReplicaCount {
		return nil, ErrChunkRefReplicaSetTooLarge
	}
	var replicaCount [chunkRefReplicaCountSize]byte
	binary.BigEndian.PutUint16(replicaCount[:], uint16(len(ref.ReplicaPeers))) //nolint:gosec // Bounded above.
	out = append(out, replicaCount[:]...)
	for _, peer := range ref.ReplicaPeers {
		out, err = appendChunkRefPeerField(out, peer.NodeID)
		if err != nil {
			return nil, err
		}
		out, err = appendChunkRefPeerField(out, peer.Address)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func appendChunkRefPeerField(out []byte, value string) ([]byte, error) {
	if len(value) > int(^uint16(0)) {
		return nil, ErrChunkRefPeerFieldTooLong
	}
	var encodedLen [chunkRefPeerFieldLenSize]byte
	binary.BigEndian.PutUint16(encodedLen[:], uint16(len(value))) //nolint:gosec // Bounded above.
	out = append(out, encodedLen[:]...)
	out = append(out, value...)
	return out, nil
}

func chunkRefSourcePeerLen(n int) (uint16, error) {
	if n < 0 || n > maxChunkRefSourcePeerLen {
		return 0, ErrChunkRefSourcePeerTooLong
	}
	return uint16(n), nil
}

// DecodeChunkRefValue decodes a chunk reference value. It returns ok=false for
// unknown versions, truncated values, or trailing-length mismatches.
func DecodeChunkRefValue(value []byte) (ChunkRefValue, bool) {
	ref, version, next, ok := decodeChunkRefHeader(value)
	if !ok {
		return ChunkRefValue{}, false
	}
	if version == chunkRefValueVersionV1 {
		return ref, next == len(value)
	}
	return decodeChunkRefV2(value, ref, next)
}

func decodeChunkRefHeader(value []byte) (ChunkRefValue, byte, int, bool) {
	var ref ChunkRefValue
	if len(value) < chunkRefValueFixedBytes || (value[0] != chunkRefValueVersionV1 && value[0] != chunkRefValueVersionV2) {
		return ref, 0, 0, false
	}
	copy(ref.ContentSHA256[:], value[1:1+chunkBlobSHA256Bytes])
	sizeOffset := 1 + chunkBlobSHA256Bytes
	var ok bool
	ref.Size, sizeOffset, ok = readU64(value, sizeOffset)
	if !ok || len(value)-sizeOffset < chunkRefSourcePeerLenSize {
		return ChunkRefValue{}, 0, 0, false
	}
	sourceLen := int(binary.BigEndian.Uint16(value[sizeOffset : sizeOffset+chunkRefSourcePeerLenSize]))
	sourceOffset := sizeOffset + chunkRefSourcePeerLenSize
	if len(value)-sourceOffset < sourceLen {
		return ChunkRefValue{}, 0, 0, false
	}
	ref.SourcePeer = string(value[sourceOffset : sourceOffset+sourceLen])
	return ref, value[0], sourceOffset + sourceLen, true
}

func decodeChunkRefV2(value []byte, ref ChunkRefValue, next int) (ChunkRefValue, bool) {
	if len(value)-next < chunkRefReplicaCountSize {
		return ChunkRefValue{}, false
	}
	replicaCount := int(binary.BigEndian.Uint16(value[next : next+chunkRefReplicaCountSize]))
	next += chunkRefReplicaCountSize
	const minimumEncodedPeerBytes = 2 * chunkRefPeerFieldLenSize
	if replicaCount > (len(value)-next)/minimumEncodedPeerBytes {
		return ChunkRefValue{}, false
	}
	ref.ReplicaPeers = make([]ChunkRefPeer, 0, replicaCount)
	for range replicaCount {
		nodeID, afterNode, valid := readChunkRefPeerField(value, next)
		if !valid {
			return ChunkRefValue{}, false
		}
		address, afterAddress, valid := readChunkRefPeerField(value, afterNode)
		if !valid {
			return ChunkRefValue{}, false
		}
		ref.ReplicaPeers = append(ref.ReplicaPeers, ChunkRefPeer{NodeID: nodeID, Address: address})
		next = afterAddress
	}
	if next != len(value) {
		return ChunkRefValue{}, false
	}
	return ref, true
}

func readChunkRefPeerField(value []byte, offset int) (string, int, bool) {
	if len(value)-offset < chunkRefPeerFieldLenSize {
		return "", offset, false
	}
	fieldLen := int(binary.BigEndian.Uint16(value[offset : offset+chunkRefPeerFieldLenSize]))
	start := offset + chunkRefPeerFieldLenSize
	if len(value)-start < fieldLen {
		return "", offset, false
	}
	return string(value[start : start+fieldLen]), start + fieldLen, true
}

type parsedObjectChunkKey struct {
	bucket     string
	generation uint64
	object     string
	uploadID   string
	partNo     uint64
	chunkNo    uint64
	next       int
}

func parseObjectChunkKeyHead(key []byte, offset int) (parsedObjectChunkKey, bool) {
	var p parsedObjectChunkKey
	bucketRaw, next, ok := decodeSegment(key, offset)
	if !ok {
		return p, false
	}
	if p.generation, next, ok = readU64(key, next); !ok {
		return p, false
	}
	objectRaw, next, ok := decodeSegment(key, next)
	if !ok {
		return p, false
	}
	uploadIDRaw, next, ok := decodeSegment(key, next)
	if !ok {
		return p, false
	}
	if p.partNo, next, ok = readU64(key, next); !ok {
		return p, false
	}
	if p.chunkNo, next, ok = readU64(key, next); !ok {
		return p, false
	}
	p.bucket = string(bucketRaw)
	p.object = string(objectRaw)
	p.uploadID = string(uploadIDRaw)
	p.next = next
	return p, true
}

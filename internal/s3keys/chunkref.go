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
	chunkRefValueFixedBytes   = 1 + chunkBlobSHA256Bytes + u64Bytes + 2
	chunkRefSourcePeerLenSize = 2
	maxChunkRefSourcePeerLen  = int(^uint16(0))
)

// ErrChunkRefSourcePeerTooLong is returned when a ChunkRefValue source peer
// cannot be encoded into the fixed-width value header.
var ErrChunkRefSourcePeerTooLong = errors.New("s3 chunkref source peer is too long")

// ChunkRefValue is the stored value for a !s3|chunkref| entry. It points from
// an object part chunk to the content-addressed !s3|chunkblob| payload.
type ChunkRefValue struct {
	ContentSHA256 [chunkBlobSHA256Bytes]byte
	Size          uint64
	SourcePeer    string
}

// ParseChunkRefKey decodes a !s3|chunkref| key into its object-part location.
func ParseChunkRefKey(key []byte) (bucket string, generation uint64, object string, uploadID string, partNo uint64, chunkNo uint64, ok bool) {
	if !bytes.HasPrefix(key, chunkRefPrefixBytes) {
		return "", 0, "", "", 0, 0, false
	}
	parts, ok := parseObjectChunkKey(key, len(chunkRefPrefixBytes))
	if !ok {
		return "", 0, "", "", 0, 0, false
	}
	return parts.bucket, parts.generation, parts.object, parts.uploadID, parts.partNo, parts.chunkNo, true
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
	out := make([]byte, 0, chunkRefValueFixedBytes+len(ref.SourcePeer))
	out = append(out, chunkRefValueVersionV1)
	out = append(out, ref.ContentSHA256[:]...)
	out = appendU64(out, ref.Size)
	var sourceLen [chunkRefSourcePeerLenSize]byte
	binary.BigEndian.PutUint16(sourceLen[:], sourcePeerLen)
	out = append(out, sourceLen[:]...)
	out = append(out, ref.SourcePeer...)
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
	var ref ChunkRefValue
	if len(value) < chunkRefValueFixedBytes || value[0] != chunkRefValueVersionV1 {
		return ref, false
	}
	copy(ref.ContentSHA256[:], value[1:1+chunkBlobSHA256Bytes])
	sizeOffset := 1 + chunkBlobSHA256Bytes
	var ok bool
	ref.Size, sizeOffset, ok = readU64(value, sizeOffset)
	if !ok || len(value)-sizeOffset < chunkRefSourcePeerLenSize {
		return ChunkRefValue{}, false
	}
	sourceLen := int(binary.BigEndian.Uint16(value[sizeOffset : sizeOffset+chunkRefSourcePeerLenSize]))
	sourceOffset := sizeOffset + chunkRefSourcePeerLenSize
	if len(value)-sourceOffset != sourceLen {
		return ChunkRefValue{}, false
	}
	ref.SourcePeer = string(value[sourceOffset:])
	return ref, true
}

type parsedObjectChunkKey struct {
	bucket     string
	generation uint64
	object     string
	uploadID   string
	partNo     uint64
	chunkNo    uint64
}

func parseObjectChunkKey(key []byte, offset int) (parsedObjectChunkKey, bool) {
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
	if p.chunkNo, next, ok = readU64(key, next); !ok || next != len(key) {
		return p, false
	}
	p.bucket = string(bucketRaw)
	p.object = string(objectRaw)
	p.uploadID = string(uploadIDRaw)
	return p, true
}

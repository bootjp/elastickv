package s3keys

import (
	"bytes"
	"encoding/binary"
)

const (
	BucketMetaPrefix       = "!s3|bucket|meta|"
	BucketGenerationPrefix = "!s3|bucket|gen|"
	ObjectManifestPrefix   = "!s3|obj|head|"
	UploadMetaPrefix       = "!s3|upload|meta|"
	UploadPartPrefix       = "!s3|upload|part|"
	BlobPrefix             = "!s3|blob|"
	GCUploadPrefix         = "!s3|gc|upload|"
	RoutePrefix            = "!s3route|"

	escapeByte      = byte(0x00)
	terminatorByte  = byte(0x01)
	escapedZeroByte = byte(0xFF)
	u64Bytes        = 8

	segmentEscapeOverhead  = 2
	routeKeyExtraBytes     = 6
	buildObjectExtraBytes  = 8
	segmentTerminatorBytes = 2
)

var (
	bucketMetaPrefixBytes       = []byte(BucketMetaPrefix)
	bucketGenerationPrefixBytes = []byte(BucketGenerationPrefix)
	objectManifestPrefixBytes   = []byte(ObjectManifestPrefix)
	uploadMetaPrefixBytes       = []byte(UploadMetaPrefix)
	uploadPartPrefixBytes       = []byte(UploadPartPrefix)
	blobPrefixBytes             = []byte(BlobPrefix)
	gcUploadPrefixBytes         = []byte(GCUploadPrefix)
	routePrefixBytes            = []byte(RoutePrefix)
)

func EncodeSegment(raw []byte) []byte {
	out := EncodeSegmentPrefix(raw)
	out = append(out, escapeByte, terminatorByte)
	return out
}

func EncodeSegmentPrefix(raw []byte) []byte {
	out := make([]byte, 0, len(raw)+segmentEscapeOverhead)
	for _, b := range raw {
		if b == escapeByte {
			out = append(out, escapeByte, escapedZeroByte)
			continue
		}
		out = append(out, b)
	}
	return out
}

func BucketMetaKey(bucket string) []byte {
	out := make([]byte, 0, len(BucketMetaPrefix)+len(bucket)+segmentEscapeOverhead)
	out = append(out, bucketMetaPrefixBytes...)
	out = append(out, EncodeSegment([]byte(bucket))...)
	return out
}

func BucketGenerationKey(bucket string) []byte {
	out := make([]byte, 0, len(BucketGenerationPrefix)+len(bucket)+segmentEscapeOverhead)
	out = append(out, bucketGenerationPrefixBytes...)
	out = append(out, EncodeSegment([]byte(bucket))...)
	return out
}

func ParseBucketMetaKey(key []byte) (string, bool) {
	if !bytes.HasPrefix(key, bucketMetaPrefixBytes) {
		return "", false
	}
	segment, next, ok := decodeSegment(key, len(bucketMetaPrefixBytes))
	if !ok || next != len(key) {
		return "", false
	}
	return string(segment), true
}

func ObjectManifestKey(bucket string, generation uint64, object string) []byte {
	return buildObjectKey(objectManifestPrefixBytes, bucket, generation, object, "", 0, 0)
}

func UploadMetaKey(bucket string, generation uint64, object string, uploadID string) []byte {
	return buildObjectKey(uploadMetaPrefixBytes, bucket, generation, object, uploadID, 0, 0)
}

func UploadPartKey(bucket string, generation uint64, object string, uploadID string, partNo uint64) []byte {
	return buildObjectKey(uploadPartPrefixBytes, bucket, generation, object, uploadID, partNo, 0)
}

func BlobKey(bucket string, generation uint64, object string, uploadID string, partNo uint64, chunkNo uint64) []byte {
	return buildObjectKey(blobPrefixBytes, bucket, generation, object, uploadID, partNo, chunkNo)
}

// VersionedBlobKey returns the blob key for a specific part attempt identified by
// partVersion (typically the part's commit timestamp). When partVersion is 0 the
// result is identical to BlobKey, preserving backward compatibility with data
// written before versioned keys were introduced. When partVersion is non-zero the
// key includes an extra u64 suffix so concurrent re-uploads of the same part
// (e.g. retry or parallel UploadPart for the same partNo) write to a distinct key
// space, making blob data immutable per attempt.
func VersionedBlobKey(bucket string, generation uint64, object string, uploadID string, partNo uint64, chunkNo uint64, partVersion uint64) []byte {
	if partVersion == 0 {
		return BlobKey(bucket, generation, object, uploadID, partNo, chunkNo)
	}
	// Capacity breakdown:
	//   - len(BlobPrefix)               prefix bytes
	//   - len(bucket)+len(object)+len(uploadID)  segment content
	//   - buildObjectExtraBytes         escape/terminator overhead for 3 segments (bucket, object, uploadID)
	//   - 4*u64Bytes                    four fixed-width u64 fields: generation, partNo, chunkNo, partVersion
	out := make([]byte, 0, len(BlobPrefix)+len(bucket)+len(object)+len(uploadID)+buildObjectExtraBytes+4*u64Bytes)
	out = append(out, blobPrefixBytes...)
	out = append(out, EncodeSegment([]byte(bucket))...)
	out = appendU64(out, generation)
	out = append(out, EncodeSegment([]byte(object))...)
	out = append(out, EncodeSegment([]byte(uploadID))...)
	out = appendU64(out, partNo)
	out = appendU64(out, chunkNo)
	out = appendU64(out, partVersion)
	return out
}

func GCUploadKey(bucket string, generation uint64, object string, uploadID string) []byte {
	return buildObjectKey(gcUploadPrefixBytes, bucket, generation, object, uploadID, 0, 0)
}

// UploadPartPrefixForUpload returns the key prefix that covers all part descriptors
// for a specific multipart upload. Used to scan/list parts for ListParts and cleanup.
func UploadPartPrefixForUpload(bucket string, generation uint64, object string, uploadID string) []byte {
	out := make([]byte, 0, len(UploadPartPrefix)+len(bucket)+len(object)+len(uploadID)+u64Bytes+buildObjectExtraBytes)
	out = append(out, uploadPartPrefixBytes...)
	out = append(out, EncodeSegment([]byte(bucket))...)
	out = appendU64(out, generation)
	out = append(out, EncodeSegment([]byte(object))...)
	out = append(out, EncodeSegment([]byte(uploadID))...)
	return out
}

// BlobPrefixForUpload returns the key prefix that covers all blob chunks
// for a specific multipart upload. Used for cleanup of all chunks in an upload.
func BlobPrefixForUpload(bucket string, generation uint64, object string, uploadID string) []byte {
	out := make([]byte, 0, len(BlobPrefix)+len(bucket)+len(object)+len(uploadID)+u64Bytes+buildObjectExtraBytes)
	out = append(out, blobPrefixBytes...)
	out = append(out, EncodeSegment([]byte(bucket))...)
	out = appendU64(out, generation)
	out = append(out, EncodeSegment([]byte(object))...)
	out = append(out, EncodeSegment([]byte(uploadID))...)
	return out
}

// ParseUploadPartKey extracts bucket, generation, object, uploadID, and partNo from a part descriptor key.
func ParseUploadPartKey(key []byte) (bucket string, generation uint64, object string, uploadID string, partNo uint64, ok bool) {
	if !bytes.HasPrefix(key, uploadPartPrefixBytes) {
		return "", 0, "", "", 0, false
	}
	bucketRaw, next, valid := decodeSegment(key, len(uploadPartPrefixBytes))
	if !valid {
		return "", 0, "", "", 0, false
	}
	generation, next, valid = readU64(key, next)
	if !valid {
		return "", 0, "", "", 0, false
	}
	objectRaw, next, valid := decodeSegment(key, next)
	if !valid {
		return "", 0, "", "", 0, false
	}
	uploadIDRaw, next, valid := decodeSegment(key, next)
	if !valid {
		return "", 0, "", "", 0, false
	}
	partNo, next, valid = readU64(key, next)
	if !valid || next != len(key) {
		return "", 0, "", "", 0, false
	}
	return string(bucketRaw), generation, string(objectRaw), string(uploadIDRaw), partNo, true
}

func RouteKey(bucket string, generation uint64, object string) []byte {
	out := make([]byte, 0, len(RoutePrefix)+len(bucket)+len(object)+2*u64Bytes+routeKeyExtraBytes)
	out = append(out, routePrefixBytes...)
	out = append(out, EncodeSegment([]byte(bucket))...)
	out = appendU64(out, generation)
	out = append(out, EncodeSegment([]byte(object))...)
	return out
}

func ObjectManifestPrefixForBucket(bucket string, generation uint64) []byte {
	out := make([]byte, 0, len(ObjectManifestPrefix)+len(bucket)+u64Bytes+segmentEscapeOverhead)
	out = append(out, objectManifestPrefixBytes...)
	out = append(out, EncodeSegment([]byte(bucket))...)
	out = appendU64(out, generation)
	return out
}

func ObjectManifestScanStart(bucket string, generation uint64, objectPrefix string) []byte {
	out := ObjectManifestPrefixForBucket(bucket, generation)
	out = append(out, EncodeSegmentPrefix([]byte(objectPrefix))...)
	return out
}

func ParseObjectManifestKey(key []byte) (bucket string, generation uint64, object string, ok bool) {
	if !bytes.HasPrefix(key, objectManifestPrefixBytes) {
		return "", 0, "", false
	}
	bucketRaw, next, ok := decodeSegment(key, len(objectManifestPrefixBytes))
	if !ok {
		return "", 0, "", false
	}
	generation, next, ok = readU64(key, next)
	if !ok {
		return "", 0, "", false
	}
	objectRaw, next, ok := decodeSegment(key, next)
	if !ok || next != len(key) {
		return "", 0, "", false
	}
	return string(bucketRaw), generation, string(objectRaw), true
}

func ExtractRouteKey(key []byte) []byte {
	prefix := objectScopedPrefix(key)
	if prefix == nil {
		return nil
	}

	offset := len(prefix)
	bucketEnd, ok := segmentEnd(key, offset)
	if !ok {
		return nil
	}
	offset = bucketEnd
	var next int
	if _, next, ok = readU64(key, offset); !ok {
		return nil
	}
	offset = next
	objectEnd, ok := segmentEnd(key, offset)
	if !ok {
		return nil
	}

	out := make([]byte, 0, len(RoutePrefix)+objectEnd-len(prefix))
	out = append(out, routePrefixBytes...)
	out = append(out, key[len(prefix):objectEnd]...)
	return out
}

func ManifestScanRouteBounds(start []byte, end []byte) ([]byte, []byte, bool) {
	routeStart, ok := manifestScanBound(start)
	if !ok {
		return nil, nil, false
	}
	if end == nil {
		return routeStart, nil, true
	}
	routeEnd, ok := manifestScanBound(end)
	if !ok {
		return nil, nil, false
	}
	return routeStart, routeEnd, true
}

func manifestScanBound(key []byte) ([]byte, bool) {
	if !bytes.HasPrefix(key, objectManifestPrefixBytes) {
		return nil, false
	}

	offset := len(objectManifestPrefixBytes)
	bucketEnd, ok := segmentEnd(key, offset)
	if !ok {
		return nil, false
	}
	offset = bucketEnd
	if _, _, ok := readU64(key, offset); !ok {
		return nil, false
	}

	out := make([]byte, 0, len(RoutePrefix)+len(key)-len(objectManifestPrefixBytes))
	out = append(out, routePrefixBytes...)
	out = append(out, key[len(objectManifestPrefixBytes):]...)
	return out, true
}

func buildObjectKey(prefix []byte, bucket string, generation uint64, object string, uploadID string, partNo uint64, chunkNo uint64) []byte {
	out := make([]byte, 0, len(prefix)+len(bucket)+len(object)+len(uploadID)+4*u64Bytes+buildObjectExtraBytes)
	out = append(out, prefix...)
	out = append(out, EncodeSegment([]byte(bucket))...)
	out = appendU64(out, generation)
	out = append(out, EncodeSegment([]byte(object))...)
	if uploadID == "" && bytes.Equal(prefix, objectManifestPrefixBytes) {
		return out
	}
	out = append(out, EncodeSegment([]byte(uploadID))...)
	if bytes.Equal(prefix, uploadMetaPrefixBytes) || bytes.Equal(prefix, gcUploadPrefixBytes) {
		return out
	}
	out = appendU64(out, partNo)
	if bytes.Equal(prefix, uploadPartPrefixBytes) {
		return out
	}
	out = appendU64(out, chunkNo)
	return out
}

func objectScopedPrefix(key []byte) []byte {
	switch {
	case bytes.HasPrefix(key, objectManifestPrefixBytes):
		return objectManifestPrefixBytes
	case bytes.HasPrefix(key, uploadMetaPrefixBytes):
		return uploadMetaPrefixBytes
	case bytes.HasPrefix(key, uploadPartPrefixBytes):
		return uploadPartPrefixBytes
	case bytes.HasPrefix(key, blobPrefixBytes):
		return blobPrefixBytes
	case bytes.HasPrefix(key, gcUploadPrefixBytes):
		return gcUploadPrefixBytes
	default:
		return nil
	}
}

func appendU64(dst []byte, v uint64) []byte {
	var raw [u64Bytes]byte
	binary.BigEndian.PutUint64(raw[:], v)
	return append(dst, raw[:]...)
}

func readU64(src []byte, offset int) (uint64, int, bool) {
	if offset < 0 || len(src)-offset < u64Bytes {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(src[offset : offset+u64Bytes]), offset + u64Bytes, true
}

func decodeSegment(src []byte, offset int) ([]byte, int, bool) {
	if offset < 0 || offset > len(src) {
		return nil, 0, false
	}
	out := make([]byte, 0, len(src)-offset)
	for i := offset; i < len(src); i++ {
		if src[i] != escapeByte {
			out = append(out, src[i])
			continue
		}
		if i+1 >= len(src) {
			return nil, 0, false
		}
		switch src[i+1] {
		case escapedZeroByte:
			out = append(out, escapeByte)
			i++
		case terminatorByte:
			return out, i + segmentTerminatorBytes, true
		default:
			return nil, 0, false
		}
	}
	return nil, 0, false
}

func segmentEnd(src []byte, offset int) (int, bool) {
	if offset < 0 || offset > len(src) {
		return 0, false
	}
	for i := offset; i < len(src); i++ {
		if src[i] != escapeByte {
			continue
		}
		if i+1 >= len(src) {
			return 0, false
		}
		switch src[i+1] {
		case escapedZeroByte:
			i++
		case terminatorByte:
			return i + segmentTerminatorBytes, true
		default:
			return 0, false
		}
	}
	return 0, false
}

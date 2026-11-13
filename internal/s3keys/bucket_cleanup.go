package s3keys

import (
	"bytes"
	"encoding/binary"
)

// BucketCleanupPrefix marks bucket generations whose post-delete safety net
// has not finished. The marker is committed with bucket metadata deletion.
const BucketCleanupPrefix = "!s3|bucket|cleanup|"

var bucketCleanupPrefixBytes = []byte(BucketCleanupPrefix)

func BucketCleanupKey(bucket string, generation uint64) []byte {
	out := make([]byte, 0, len(BucketCleanupPrefix)+len(bucket)+segmentEscapeOverhead+u64Bytes)
	out = append(out, bucketCleanupPrefixBytes...)
	out = append(out, EncodeSegment([]byte(bucket))...)
	return binary.BigEndian.AppendUint64(out, generation)
}

func ParseBucketCleanupKey(key []byte) (string, uint64, bool) {
	if !bytes.HasPrefix(key, bucketCleanupPrefixBytes) {
		return "", 0, false
	}
	bucket, next, ok := decodeSegment(key, len(bucketCleanupPrefixBytes))
	if !ok || len(key)-next != u64Bytes {
		return "", 0, false
	}
	return string(bucket), binary.BigEndian.Uint64(key[next:]), true
}

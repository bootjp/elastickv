package proto

import (
	"encoding/binary"
	"fmt"
	"io"
)

// EncodeRawLatestCommitTSKeyBatch packs exact-key version-presence probes into
// a single protobuf bytes field. The service-side decoder can reject an
// oversized count before allocating per-key slices.
func EncodeRawLatestCommitTSKeyBatch(keys [][]byte) []byte {
	size := binary.MaxVarintLen64
	for _, key := range keys {
		size += binary.MaxVarintLen64 + len(key)
	}
	out := make([]byte, 0, size)
	out = binary.AppendUvarint(out, uint64(len(keys)))
	for _, key := range keys {
		out = binary.AppendUvarint(out, uint64(len(key)))
		out = append(out, key...)
	}
	return out
}

func DecodeRawLatestCommitTSKeyBatch(data []byte, maxKeys int) ([][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if maxKeys < 0 {
		return nil, fmt.Errorf("raw latest commit timestamp key batch max_keys must be non-negative")
	}
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("raw latest commit timestamp key batch has invalid count")
	}
	if count > uint64(maxKeys) {
		return nil, fmt.Errorf("raw latest commit timestamp key batch has %d keys, max %d", count, maxKeys)
	}
	data = data[n:]
	keys := make([][]byte, 0, count)
	for range count {
		keyLen, n := binary.Uvarint(data)
		if n <= 0 {
			return nil, fmt.Errorf("raw latest commit timestamp key batch has invalid key length")
		}
		data = data[n:]
		if keyLen > uint64(len(data)) {
			return nil, io.ErrUnexpectedEOF
		}
		keys = append(keys, data[:keyLen])
		data = data[keyLen:]
	}
	if len(data) != 0 {
		return nil, fmt.Errorf("raw latest commit timestamp key batch has trailing bytes")
	}
	return keys, nil
}

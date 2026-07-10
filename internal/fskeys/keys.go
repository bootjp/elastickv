package fskeys

import (
	"bytes"
	"encoding/binary"
)

const (
	u64Bytes               = 8
	orderedTerminatorBytes = 2
	orderedZeroEscape      = 0xff

	inodePrefix    = "!fs|ino|"
	dirPrefix      = "!fs|dir|"
	dirVerPrefix   = "!fs|dirv|"
	homePrefix     = "!fs|home|"
	chunkPrefix    = "!fs|chk|"
	refPrefix      = "!fs|ref|"
	refFencePrefix = "!fs|reffence|"
	intentPrefix   = "!fs|intent|"
	moveJobPrefix  = "!fs|job|move|"
	usageKey       = "!fs|usage"

	chunkRoutePrefix = "!fs|route|chk|"

	chunkRouteKeyBytes = len(chunkRoutePrefix) + 2*u64Bytes
)

var (
	chunkPrefixBytes      = []byte(chunkPrefix)
	chunkRoutePrefixBytes = []byte(chunkRoutePrefix)
)

// InodeKey returns the metadata key for an inode.
func InodeKey(inode uint64) []byte {
	return appendU64Key([]byte(inodePrefix), inode)
}

// InodeAllPrefix returns the scan prefix for every inode metadata row.
func InodeAllPrefix() []byte {
	return []byte(inodePrefix)
}

// HomeKey returns the home-placement key for an inode.
func HomeKey(inode uint64) []byte {
	return appendU64Key([]byte(homePrefix), inode)
}

// DirVersionKey returns the mutation-version key for a directory inode.
func DirVersionKey(parent uint64) []byte {
	return appendU64Key([]byte(dirVerPrefix), parent)
}

// DirEntryPrefix returns the scan prefix for a directory's entries.
func DirEntryPrefix(parent uint64) []byte {
	return appendU64Key([]byte(dirPrefix), parent)
}

// DirEntryKey returns the key for one directory entry. Names are encoded as an
// order-preserving binary segment so readdir scans stay lexicographic by raw
// name bytes.
func DirEntryKey(parent uint64, name []byte) []byte {
	out := DirEntryPrefix(parent)
	return appendOrderedBytes(out, name)
}

// DirEntryNameFromKey decodes a name from a key produced by DirEntryKey.
func DirEntryNameFromKey(parent uint64, key []byte) ([]byte, bool) {
	prefix := DirEntryPrefix(parent)
	if !bytes.HasPrefix(key, prefix) {
		return nil, false
	}
	name, rest, ok := decodeOrderedBytes(key[len(prefix):])
	if !ok || len(rest) != 0 {
		return nil, false
	}
	return name, true
}

// ChunkKey returns the payload key for one file chunk.
func ChunkKey(homeSlot, inode, chunkIndex uint64) []byte {
	out := make([]byte, 0, len(chunkPrefix)+3*u64Bytes)
	out = append(out, []byte(chunkPrefix)...)
	out = appendU64(out, homeSlot)
	out = appendU64(out, inode)
	out = appendU64(out, chunkIndex)
	return out
}

// ChunkPrefix returns the scan prefix for every chunk of an inode on one home.
func ChunkPrefix(homeSlot, inode uint64) []byte {
	out := make([]byte, 0, len(chunkPrefix)+2*u64Bytes)
	out = append(out, []byte(chunkPrefix)...)
	out = appendU64(out, homeSlot)
	out = appendU64(out, inode)
	return out
}

// ChunkAllPrefix returns the scan prefix for every file chunk payload.
func ChunkAllPrefix() []byte {
	return []byte(chunkPrefix)
}

// ChunkRouteKey returns the logical route domain for every chunk of the same
// file home. It intentionally excludes chunkIndex so range splitting does not
// scatter one file's chunks across routes under normal placement.
func ChunkRouteKey(homeSlot, inode uint64) []byte {
	out := make([]byte, 0, len(chunkRoutePrefix)+2*u64Bytes)
	out = append(out, chunkRoutePrefixBytes...)
	out = appendU64(out, homeSlot)
	out = appendU64(out, inode)
	return out
}

// ExtractRouteKey normalizes filesystem chunk keys to the file-home route
// domain. Non-chunk keys return nil and keep the caller's default routing.
func ExtractRouteKey(key []byte) []byte {
	if !bytes.HasPrefix(key, chunkPrefixBytes) {
		return nil
	}
	rest := key[len(chunkPrefixBytes):]
	if len(rest) < 3*u64Bytes {
		return nil
	}
	out := make([]byte, 0, len(chunkRoutePrefixBytes)+2*u64Bytes)
	out = append(out, chunkRoutePrefixBytes...)
	out = append(out, rest[:2*u64Bytes]...)
	return out
}

// ChunkScanRouteBounds maps raw chunk scan bounds to the virtual chunk route
// domain. The caller still scans raw chunk keys, but route selection uses these
// bounds so a ShardStore scan reaches the groups selected for ChunkKey writes.
func ChunkScanRouteBounds(start []byte, end []byte) ([]byte, []byte, bool) {
	routeStart, ok := chunkScanBound(start)
	if !ok {
		return nil, nil, false
	}
	if end == nil {
		return routeStart, nil, true
	}
	if bytes.Equal(end, prefixEnd(chunkPrefixBytes)) {
		return routeStart, prefixEnd(chunkRoutePrefixBytes), true
	}
	routeEnd, ok := chunkScanBound(end)
	if !ok {
		return nil, nil, false
	}
	if bytes.Equal(routeStart, routeEnd) && bytes.Compare(start, end) < 0 {
		return routeStart, prefixEnd(routeStart), true
	}
	return routeStart, routeEnd, true
}

func chunkScanBound(key []byte) ([]byte, bool) {
	if !bytes.HasPrefix(key, chunkPrefixBytes) {
		return nil, false
	}
	rest := key[len(chunkPrefixBytes):]
	if len(rest) == 0 {
		return append([]byte(nil), chunkRoutePrefixBytes...), true
	}
	if len(rest) < 2*u64Bytes {
		return nil, false
	}
	out := make([]byte, 0, len(chunkRoutePrefixBytes)+2*u64Bytes)
	out = append(out, chunkRoutePrefixBytes...)
	out = append(out, rest[:2*u64Bytes]...)
	return out, true
}

// NormalizeSplitBoundary snaps filesystem chunk-domain split candidates to the
// file boundary used by routeKey. This prevents a split key from bisecting one
// file's chunk-domain route.
func NormalizeSplitBoundary(key []byte) []byte {
	if routeKey := ExtractRouteKey(key); routeKey != nil {
		return routeKey
	}
	if !bytes.HasPrefix(key, chunkRoutePrefixBytes) || len(key) <= chunkRouteKeyBytes {
		return key
	}
	out := make([]byte, chunkRouteKeyBytes)
	copy(out, key[:chunkRouteKeyBytes])
	return out
}

// RefKey returns an open-handle lease key.
func RefKey(inode uint64, clientID []byte, fhID uint64) []byte {
	out := make([]byte, 0, len(refPrefix)+2*u64Bytes+len(clientID)+orderedTerminatorBytes)
	out = append(out, []byte(refPrefix)...)
	out = appendU64(out, inode)
	out = appendOrderedBytes(out, clientID)
	out = appendU64(out, fhID)
	return out
}

// RefPrefix returns the scan prefix for open-handle leases on an inode.
func RefPrefix(inode uint64) []byte {
	return appendU64Key([]byte(refPrefix), inode)
}

// RefAllPrefix returns the scan prefix for every open-handle lease.
func RefAllPrefix() []byte {
	return []byte(refPrefix)
}

// RefFenceKey serializes inode GC against newly-created open-handle refs.
func RefFenceKey(inode uint64) []byte {
	return appendU64Key([]byte(refFencePrefix), inode)
}

// UsageKey returns the filesystem-wide StatFS counter key.
func UsageKey() []byte {
	return []byte(usageKey)
}

// IntentKey returns a crash-recovery intent key.
func IntentKey(intentID []byte) []byte {
	out := make([]byte, 0, len(intentPrefix)+len(intentID)+orderedTerminatorBytes)
	out = append(out, []byte(intentPrefix)...)
	return appendOrderedBytes(out, intentID)
}

// MoveJobKey returns a whole-file migration job key.
func MoveJobKey(jobID []byte) []byte {
	out := make([]byte, 0, len(moveJobPrefix)+len(jobID)+orderedTerminatorBytes)
	out = append(out, []byte(moveJobPrefix)...)
	return appendOrderedBytes(out, jobID)
}

func appendU64Key(prefix []byte, v uint64) []byte {
	out := make([]byte, 0, len(prefix)+u64Bytes)
	out = append(out, prefix...)
	return appendU64(out, v)
}

func appendU64(dst []byte, v uint64) []byte {
	var buf [u64Bytes]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

func prefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] == ^byte(0) {
			continue
		}
		out[i]++
		return out[:i+1]
	}
	return nil
}

func appendOrderedBytes(dst []byte, src []byte) []byte {
	for _, b := range src {
		if b == 0 {
			dst = append(dst, 0, orderedZeroEscape)
			continue
		}
		dst = append(dst, b)
	}
	return append(dst, 0, 0)
}

func decodeOrderedBytes(src []byte) ([]byte, []byte, bool) {
	out := make([]byte, 0, len(src))
	for i := 0; i < len(src); i++ {
		b := src[i]
		if b != 0 {
			out = append(out, b)
			continue
		}
		if i+1 >= len(src) {
			return nil, nil, false
		}
		switch src[i+1] {
		case 0:
			return out, src[i+2:], true
		case orderedZeroEscape:
			out = append(out, 0)
			i++
		default:
			return nil, nil, false
		}
	}
	return nil, nil, false
}

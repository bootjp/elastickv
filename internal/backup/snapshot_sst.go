package backup

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"

	cockroachdberr "github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// snapshot_sst.go reads the SST ingest bundle a live store emits when
// ELASTICKV_PEBBLE_SST_INGEST_SNAPSHOT is on
// (store/snapshot_pebble_sst.go pebbleSSTIngestBundle.WriteTo):
//
//	[8 bytes]   magic "EKVSSTI1"
//	[8 bytes]   manifest length (BE int64)
//	[32 bytes]  SHA-256 of the manifest
//	[n bytes]   manifest JSON: version, last_commit_ts, files[]
//	repeated per manifest file, in order:
//	  [size bytes] one SST file written with Pebble's default comparer
//
// The SSTs hold the same physical keys and values as the EKVPBBL1 stream
// (the store's reserved metadata keys included), in key order across
// files, so each entry goes through emitRawEntry. The bounds mirror the
// store's; each file is buffered whole (the store's target file size is
// 64 MiB) and checked against its SHA-256 before it is parsed.

const (
	sstIngestVersion          = 1
	sstIngestDigestSize       = sha256.Size
	sstIngestManifestMaxBytes = 64 << 20
	sstIngestMaxFiles         = 1 << 20
	// sstIngestMaxFileBytes bounds one buffered file: the store's 64 MiB
	// target plus room for the entry that crosses it, up to the largest
	// stored key and value.
	sstIngestMaxFileBytes = 1 << 30
)

// ErrSnapshotSSTInvalid is returned for an SST ingest bundle whose
// manifest or files fail validation (digest, bounds, ordering, kinds).
var ErrSnapshotSSTInvalid = cockroachdberr.New("backup: invalid SST ingest snapshot")

type sstIngestManifest struct {
	Version      int             `json:"version"`
	LastCommitTS uint64          `json:"last_commit_ts"`
	Files        []sstIngestFile `json:"files"`
}

type sstIngestFile struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
	Smallest []byte `json:"smallest"`
	Largest  []byte `json:"largest"`
}

// readSSTIngestBundle reads the bundle from br and yields every entry of
// every SST, in order.
func readSSTIngestBundle(br *bufio.Reader, fn func(SnapshotHeader, SnapshotEntry) error) (SnapshotHeader, error) {
	manifest, err := readSSTIngestManifest(br)
	if err != nil {
		return SnapshotHeader{}, err
	}
	header := SnapshotHeader{LastCommitTS: manifest.LastCommitTS}
	var previousLargest []byte
	for i, file := range manifest.Files {
		if err := validateSSTIngestFile(file, i, previousLargest); err != nil {
			return header, err
		}
		data, err := readSSTIngestFile(br, file)
		if err != nil {
			return header, err
		}
		if err := emitSSTEntries(data, file, header, fn); err != nil {
			return header, err
		}
		previousLargest = file.Largest
	}
	return header, nil
}

func readSSTIngestManifest(r io.Reader) (sstIngestManifest, error) {
	manifestJSON, err := readSSTIngestManifestJSON(r)
	if err != nil {
		return sstIngestManifest{}, err
	}
	var manifest sstIngestManifest
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		return sstIngestManifest{}, cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "manifest JSON: %v", err)
	}
	if manifest.Version != sstIngestVersion {
		return sstIngestManifest{}, cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "manifest version %d, want %d", manifest.Version, sstIngestVersion)
	}
	if len(manifest.Files) > sstIngestMaxFiles {
		return sstIngestManifest{}, cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "%d files exceed the limit %d", len(manifest.Files), sstIngestMaxFiles)
	}
	return manifest, nil
}

// readSSTIngestManifestJSON reads the magic, the manifest length, and the
// manifest bytes, and checks them against the manifest's SHA-256.
func readSSTIngestManifestJSON(r io.Reader) ([]byte, error) {
	var magic [snapshotMagicLen]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, truncatedIfEOF(err)
	}
	var manifestLen int64
	if err := binary.Read(r, binary.BigEndian, &manifestLen); err != nil {
		return nil, truncatedIfEOF(err)
	}
	if manifestLen <= 0 || manifestLen > sstIngestManifestMaxBytes {
		return nil, cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "manifest length %d", manifestLen)
	}
	var digest [sstIngestDigestSize]byte
	if _, err := io.ReadFull(r, digest[:]); err != nil {
		return nil, truncatedIfEOF(err)
	}
	manifestJSON := make([]byte, manifestLen)
	if _, err := io.ReadFull(r, manifestJSON); err != nil {
		return nil, truncatedIfEOF(err)
	}
	if sha256.Sum256(manifestJSON) != digest {
		return nil, cockroachdberr.Wrap(ErrSnapshotSSTInvalid, "manifest SHA-256 mismatch")
	}
	return manifestJSON, nil
}

// validateSSTIngestFile mirrors store's validateSSTIngestFileMeta: a
// positive size within the decoder's buffer bound, a SHA-256, ordered key
// bounds, and no overlap with the previous file, which is what makes the
// concatenated files one key-ordered stream.
func validateSSTIngestFile(file sstIngestFile, index int, previousLargest []byte) error {
	if file.Size <= 0 || file.Size > sstIngestMaxFileBytes {
		return cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "file %d (%s) size %d", index, file.Name, file.Size)
	}
	if len(file.Smallest) == 0 || len(file.Largest) == 0 || bytes.Compare(file.Smallest, file.Largest) > 0 {
		return cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "file %d (%s) key bounds", index, file.Name)
	}
	if index > 0 && bytes.Compare(previousLargest, file.Smallest) >= 0 {
		return cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "file %d (%s) overlaps the previous file", index, file.Name)
	}
	if digest, err := hex.DecodeString(file.SHA256); err != nil || len(digest) != sstIngestDigestSize {
		return cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "file %d (%s) SHA-256 %q", index, file.Name, file.SHA256)
	}
	return nil
}

func readSSTIngestFile(r io.Reader, file sstIngestFile) ([]byte, error) {
	data := make([]byte, file.Size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, cockroachdberr.Wrapf(truncatedIfEOF(err), "SST file %s", file.Name)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != file.SHA256 {
		return nil, cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "file %s SHA-256 mismatch", file.Name)
	}
	return data, nil
}

// emitSSTEntries yields every point entry of one SST.
func emitSSTEntries(data []byte, file sstIngestFile, header SnapshotHeader, fn func(SnapshotHeader, SnapshotEntry) error) error {
	return withSSTIterator(data, file, func(iter sstable.Iterator) error {
		var valBuf []byte
		for kv := iter.First(); kv != nil; kv = iter.Next() {
			value, err := sstEntryValue(kv.K, kv.Value, &valBuf, file)
			if err != nil {
				return err
			}
			if err := emitRawEntry(kv.K.UserKey, value, header, fn); err != nil {
				return err
			}
		}
		return cockroachdberr.WithStack(iter.Error())
	})
}

// sstEntryValue checks one SST entry and returns its value. The store
// writes only Set entries (pebbleSSTExporter.Add), so any other kind means
// the file is not one of the store's; the size bounds match the EKVPBBL1
// reader's. valBuf is reused for values the reader copies out; a value
// that is not caller-owned points into the reader's block and must not be
// written to.
func sstEntryValue(key sstable.InternalKey, value func([]byte) ([]byte, bool, error), valBuf *[]byte, file sstIngestFile) ([]byte, error) {
	if kind := key.Kind(); kind != sstable.InternalKeyKindSet {
		return nil, cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "file %s holds a %s entry", file.Name, kind)
	}
	if len(key.UserKey) > MaxSnapshotEncodedKeySize {
		return nil, cockroachdberr.Wrapf(ErrSnapshotKeyTooLarge, "length %d > %d", len(key.UserKey), MaxSnapshotEncodedKeySize)
	}
	v, callerOwned, err := value((*valBuf)[:0])
	if err != nil {
		return nil, cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "value in %s: %v", file.Name, err)
	}
	if len(v) > MaxSnapshotEncodedValueSize {
		return nil, cockroachdberr.Wrapf(ErrSnapshotValueTooLarge, "length %d > %d", len(v), MaxSnapshotEncodedValueSize)
	}
	if callerOwned {
		*valBuf = v
	}
	return v, nil
}

// withSSTIterator opens an in-memory reader and a point iterator over one
// SST, runs body, and closes both.
func withSSTIterator(data []byte, file sstIngestFile, body func(sstable.Iterator) error) (retErr error) {
	reader, err := sstable.NewMemReader(data, sstable.ReaderOptions{})
	if err != nil {
		return cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "open %s: %v", file.Name, err)
	}
	defer func() {
		if err := reader.Close(); err != nil && retErr == nil {
			retErr = cockroachdberr.WithStack(err)
		}
	}()
	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.TableBlobContext{})
	if err != nil {
		return cockroachdberr.Wrapf(ErrSnapshotSSTInvalid, "iterate %s: %v", file.Name, err)
	}
	defer func() {
		if err := iter.Close(); err != nil && retErr == nil {
			retErr = cockroachdberr.WithStack(err)
		}
	}()
	return body(iter)
}

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	sstIngestSnapshotEnv           = "ELASTICKV_PEBBLE_SST_INGEST_SNAPSHOT"
	sstIngestSnapshotVersion       = 1
	sstIngestManifestMaxBytes      = 64 << 20
	sstIngestSnapshotDigestBytes   = sha256.Size
	sstIngestSnapshotFileNameWidth = 6
	sstIngestSnapshotMaxFiles      = 1 << 20
	defaultSSTIngestTargetFileSize = 64 << 20
	sstIngestFilePerm              = 0600
)

var pebbleSSTIngestSnapshotMagic = [8]byte{'E', 'K', 'V', 'S', 'S', 'T', 'I', '1'}

type pebbleSnapshotMetadata struct {
	LastCommitTS         uint64
	MinRetainedTS        uint64
	PendingMinRetainedTS uint64
	AppliedIndex         uint64
	AppliedIndexPresent  bool
}

type pebbleSSTIngestManifest struct {
	Version              int                       `json:"version"`
	LastCommitTS         uint64                    `json:"last_commit_ts"`
	MinRetainedTS        uint64                    `json:"min_retained_ts"`
	PendingMinRetainedTS uint64                    `json:"pending_min_retained_ts"`
	AppliedIndex         *uint64                   `json:"applied_index,omitempty"`
	EntryCount           uint64                    `json:"entry_count"`
	UncompressedBytes    uint64                    `json:"uncompressed_bytes"`
	TotalFileBytes       int64                     `json:"total_file_bytes"`
	Files                []pebbleSSTIngestFileMeta `json:"files"`
}

type pebbleSSTIngestFileMeta struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
	Smallest []byte `json:"smallest"`
	Largest  []byte `json:"largest"`
}

type pebbleSSTIngestSnapshot struct {
	checkpointDir      string
	fallback           *pebbleSnapshot
	metadata           pebbleSnapshotMetadata
	targetFileBytes    uint64
	disableCompression bool
	log                *slog.Logger
	once               sync.Once
	err                error
}

type pebbleSSTIngestBundle struct {
	dir          string
	manifest     pebbleSSTIngestManifest
	manifestJSON []byte
}

type pebbleSSTExporter struct {
	db              *pebble.DB
	dir             string
	manifest        pebbleSSTIngestManifest
	writer          *sstable.Writer
	currentPath     string
	currentName     string
	currentBytes    uint64
	smallest        []byte
	largest         []byte
	fileIndex       int
	targetFileBytes uint64
	compression     *sstable.CompressionProfile
}

func resolveSSTIngestSnapshots(raw string) bool {
	enabled, err := strconv.ParseBool(strings.TrimSpace(raw))
	return err == nil && enabled
}

func cleanupPebbleSnapshotArtifacts(dir string) error {
	clean := filepath.Clean(dir)
	parent := filepath.Dir(clean)
	base := filepath.Base(clean)
	if err := recoverPebbleRestoreBackup(clean); err != nil {
		return err
	}
	prefixes := []string{
		base + "-sst-checkpoint-",
		base + "-sst-ingest-",
		base + "-sst-receive-",
	}
	for _, prefix := range prefixes {
		matches, err := siblingPathsWithPrefix(parent, prefix)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, match := range matches {
			if err := os.RemoveAll(match); err != nil {
				return errors.Wrapf(err, "remove stale snapshot artifact %s", match)
			}
		}
	}
	return nil
}

func siblingPathsWithPrefix(parent, prefix string) ([]string, error) {
	entries, err := os.ReadDir(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errors.WithStack(err)
	}
	paths := make([]string, 0)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			paths = append(paths, filepath.Join(parent, entry.Name()))
		}
	}
	return paths, nil
}

func recoverPebbleRestoreBackup(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	clean := filepath.Clean(dir)
	backups, err := siblingPathsWithPrefix(filepath.Dir(clean), filepath.Base(clean)+"-restore-backup-")
	if err != nil {
		return errors.WithStack(err)
	}
	if len(backups) == 0 {
		return nil
	}
	if len(backups) != 1 {
		return errors.WithStack(errors.Newf("cannot recover Pebble store with %d restore backups", len(backups)))
	}
	if err := os.Rename(backups[0], dir); err != nil {
		return errors.Wrapf(err, "recover Pebble restore backup %s", backups[0])
	}
	return nil
}

func cleanupPebbleRestoreBackups(dir string) error {
	clean := filepath.Clean(dir)
	backups, err := siblingPathsWithPrefix(filepath.Dir(clean), filepath.Base(clean)+"-restore-backup-")
	if err != nil {
		return errors.WithStack(err)
	}
	for _, backup := range backups {
		if err := os.RemoveAll(backup); err != nil {
			return errors.Wrapf(err, "remove stale Pebble restore backup %s", backup)
		}
	}
	return nil
}

func (s *pebbleStore) newSSTIngestSnapshot() (Snapshot, error) {
	// Checkpoint must represent the exact state visible when the Raft run loop
	// calls Snapshot. Block compaction/restore and all commit paths while the
	// memtable is flushed and Pebble hard-links the stable checkpoint files.
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	fallback := func(reason error) (Snapshot, error) {
		snap, err := s.newLegacyPebbleSnapshotLocked()
		if err != nil {
			return nil, err
		}
		if s.log != nil {
			s.log.Warn("falling back to legacy pebble snapshot stream", "error", reason)
		}
		return snap, nil
	}

	if err := s.db.Flush(); err != nil {
		return fallback(errors.Wrap(err, "flush pebble before SST checkpoint"))
	}
	checkpointDir, err := makeSiblingTempDir(s.dir, "sst-checkpoint")
	if err != nil {
		return fallback(err)
	}
	if err := s.db.Checkpoint(checkpointDir, pebble.WithFlushedWAL()); err != nil {
		_ = os.RemoveAll(checkpointDir)
		return fallback(errors.Wrap(err, "create pebble SST checkpoint"))
	}

	pebbleSnap := s.db.NewSnapshot()
	metadata, err := readPebbleSnapshotMetadata(pebbleSnap)
	if err != nil {
		_ = pebbleSnap.Close()
		_ = os.RemoveAll(checkpointDir)
		return nil, err
	}
	targetFileBytes := s.sstIngestTargetFileBytes
	if targetFileBytes == 0 {
		targetFileBytes = defaultSSTIngestTargetFileSize
	}
	return &pebbleSSTIngestSnapshot{
		checkpointDir: checkpointDir,
		fallback: &pebbleSnapshot{
			snapshot:     pebbleSnap,
			lastCommitTS: metadata.LastCommitTS,
		},
		metadata:           metadata,
		targetFileBytes:    targetFileBytes,
		disableCompression: s.cipher != nil,
		log:                s.log,
	}, nil
}

func (s *pebbleStore) newLegacyPebbleSnapshotLocked() (*pebbleSnapshot, error) {
	snap := s.db.NewSnapshot()
	lastCommitTS, err := readPebbleSnapshotLastCommitTS(snap)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	return &pebbleSnapshot{snapshot: snap, lastCommitTS: lastCommitTS}, nil
}

func readPebbleSnapshotMetadata(snapshot *pebble.Snapshot) (pebbleSnapshotMetadata, error) {
	return readPebbleMetadata(snapshot)
}

func readPebbleMetadata(src pebbleUint64Getter) (pebbleSnapshotMetadata, error) {
	lastCommitTS, err := readPebbleUint64(src, metaLastCommitTSBytes)
	if err != nil {
		return pebbleSnapshotMetadata{}, err
	}
	minRetainedTS, err := readPebbleUint64(src, metaMinRetainedTSBytes)
	if err != nil {
		return pebbleSnapshotMetadata{}, err
	}
	pendingMinRetainedTS, err := readPebbleUint64(src, metaPendingMinRetainedTSBytes)
	if err != nil {
		return pebbleSnapshotMetadata{}, err
	}
	appliedIndex, present, err := readPebbleUint64Present(src, metaAppliedIndexBytes)
	if err != nil {
		return pebbleSnapshotMetadata{}, err
	}
	return pebbleSnapshotMetadata{
		LastCommitTS:         lastCommitTS,
		MinRetainedTS:        minRetainedTS,
		PendingMinRetainedTS: pendingMinRetainedTS,
		AppliedIndex:         appliedIndex,
		AppliedIndexPresent:  present,
	}, nil
}

func readPebbleUint64Present(src pebbleUint64Getter, key []byte) (uint64, bool, error) {
	val, closer, err := src.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, false, nil
		}
		return 0, false, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) < timestampSize {
		return 0, false, nil
	}
	return binary.LittleEndian.Uint64(val), true, nil
}

func (s *pebbleSSTIngestSnapshot) WriteTo(w io.Writer) (int64, error) {
	if s == nil || s.fallback == nil {
		return 0, errors.New("snapshot is not available")
	}
	bundle, err := buildPebbleSSTIngestBundle(s.checkpointDir, s.metadata, s.targetFileBytes, s.disableCompression)
	if err != nil {
		if s.log != nil {
			s.log.Warn("failed to build SST ingest snapshot; using legacy stream", "error", err)
		}
		return s.fallback.WriteTo(w)
	}
	defer bundle.Close()
	return bundle.WriteTo(w)
}

func (s *pebbleSSTIngestSnapshot) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		var errs []error
		if s.fallback != nil {
			errs = append(errs, s.fallback.Close())
			s.fallback = nil
		}
		if s.checkpointDir != "" {
			errs = append(errs, os.RemoveAll(s.checkpointDir))
			s.checkpointDir = ""
		}
		s.err = errors.Join(errs...)
	})
	return errors.WithStack(s.err)
}

func buildPebbleSSTIngestBundle(checkpointDir string, metadata pebbleSnapshotMetadata, targetFileBytes uint64, disableCompression bool) (*pebbleSSTIngestBundle, error) {
	opts, cache := defaultPebbleOptionsWithCache(disableCompression)
	opts.ReadOnly = true
	db, err := pebble.Open(checkpointDir, opts)
	if err != nil {
		cache.Unref()
		return nil, errors.WithStack(err)
	}

	exportDir, err := os.MkdirTemp(checkpointDir, "sst-export-")
	if err != nil {
		_ = db.Close()
		cache.Unref()
		return nil, errors.WithStack(err)
	}
	bundle := &pebbleSSTIngestBundle{dir: exportDir}
	cleanup := func(err error) (*pebbleSSTIngestBundle, error) {
		_ = db.Close()
		cache.Unref()
		_ = bundle.Close()
		return nil, err
	}

	manifest, err := exportPebbleSnapshotSSTs(db, exportDir, metadata, targetFileBytes, disableCompression)
	if err != nil {
		return cleanup(err)
	}
	if err := db.Close(); err != nil {
		cache.Unref()
		_ = bundle.Close()
		return nil, errors.WithStack(err)
	}
	cache.Unref()

	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		_ = bundle.Close()
		return nil, errors.WithStack(err)
	}
	if len(manifestJSON) > sstIngestManifestMaxBytes {
		_ = bundle.Close()
		return nil, errors.WithStack(errors.Newf("SST ingest manifest is too large: %d bytes", len(manifestJSON)))
	}
	bundle.manifest = manifest
	bundle.manifestJSON = manifestJSON
	return bundle, nil
}

func exportPebbleSnapshotSSTs(db *pebble.DB, exportDir string, metadata pebbleSnapshotMetadata, targetFileBytes uint64, disableCompression bool) (pebbleSSTIngestManifest, error) {
	if targetFileBytes == 0 {
		targetFileBytes = defaultSSTIngestTargetFileSize
	}
	exporter := pebbleSSTExporter{
		db:              db,
		dir:             exportDir,
		targetFileBytes: targetFileBytes,
		compression:     sstExportCompression(disableCompression),
		manifest: pebbleSSTIngestManifest{
			Version:              sstIngestSnapshotVersion,
			LastCommitTS:         metadata.LastCommitTS,
			MinRetainedTS:        metadata.MinRetainedTS,
			PendingMinRetainedTS: metadata.PendingMinRetainedTS,
		},
	}
	if metadata.AppliedIndexPresent {
		applied := metadata.AppliedIndex
		exporter.manifest.AppliedIndex = &applied
	}

	iter, err := db.NewIter(nil)
	if err != nil {
		return exporter.manifest, errors.WithStack(err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := exporter.Add(iter.Key(), iter.Value()); err != nil {
			exporter.Abort()
			return exporter.manifest, err
		}
	}
	if err := iter.Error(); err != nil {
		exporter.Abort()
		return exporter.manifest, errors.WithStack(err)
	}
	if err := exporter.Finish(); err != nil {
		exporter.Abort()
		return exporter.manifest, err
	}
	return exporter.manifest, nil
}

func sstExportCompression(disable bool) *sstable.CompressionProfile {
	if disable {
		return sstable.NoCompression
	}
	return nil
}

func (e *pebbleSSTExporter) Add(key, value []byte) error {
	if e.writer == nil {
		if err := e.startFile(key); err != nil {
			return err
		}
	}
	if err := e.writer.Set(key, value); err != nil {
		return errors.WithStack(err)
	}
	e.largest = bytes.Clone(key)
	entryBytes := uint64(len(key)) + uint64(len(value))
	if e.manifest.EntryCount == math.MaxUint64 || math.MaxUint64-e.manifest.UncompressedBytes < entryBytes {
		return errors.New("SST ingest entry counters overflow uint64")
	}
	e.manifest.EntryCount++
	e.manifest.UncompressedBytes += entryBytes
	e.currentBytes += entryBytes
	if !isPebbleOperationalKey(key) {
		_, commitTS := decodeKeyView(key)
		if commitTS > e.manifest.LastCommitTS {
			e.manifest.LastCommitTS = commitTS
		}
	}
	if e.currentBytes >= e.targetFileBytes {
		return e.finishFile()
	}
	return nil
}

func (e *pebbleSSTExporter) startFile(firstKey []byte) error {
	if e.fileIndex >= sstIngestSnapshotMaxFiles {
		return errors.New("SST ingest snapshot exceeds file-count limit")
	}
	e.currentName = fmt.Sprintf("%0*d.sst", sstIngestSnapshotFileNameWidth, e.fileIndex)
	e.currentPath = filepath.Join(e.dir, e.currentName)
	file, err := vfs.Default.Create(e.currentPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return errors.WithStack(err)
	}
	e.writer = sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{
		Comparer:    pebble.DefaultComparer,
		TableFormat: e.db.TableFormat(),
		Compression: e.compression,
	})
	e.currentBytes = 0
	e.smallest = bytes.Clone(firstKey)
	e.fileIndex++
	return nil
}

func (e *pebbleSSTExporter) finishFile() error {
	if e.writer == nil {
		return nil
	}
	if err := e.writer.Close(); err != nil {
		return errors.WithStack(err)
	}
	e.writer = nil
	fileMeta, err := inspectSSTIngestFile(e.currentPath, e.currentName, e.smallest, e.largest)
	if err != nil {
		return err
	}
	if math.MaxInt64-e.manifest.TotalFileBytes < fileMeta.Size {
		return errors.New("SST ingest total file size overflows int64")
	}
	e.manifest.Files = append(e.manifest.Files, fileMeta)
	e.manifest.TotalFileBytes += fileMeta.Size
	return nil
}

func (e *pebbleSSTExporter) Finish() error {
	return e.finishFile()
}

func (e *pebbleSSTExporter) Abort() {
	if e.writer != nil {
		_ = e.writer.Close()
		e.writer = nil
	}
}

func inspectSSTIngestFile(path, name string, smallest, largest []byte) (_ pebbleSSTIngestFileMeta, retErr error) {
	f, err := os.Open(path)
	if err != nil {
		return pebbleSSTIngestFileMeta{}, errors.WithStack(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			retErr = errors.Join(retErr, errors.WithStack(err))
		}
	}()
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return pebbleSSTIngestFileMeta{}, errors.WithStack(err)
	}
	return pebbleSSTIngestFileMeta{
		Name:     name,
		Size:     size,
		SHA256:   hex.EncodeToString(h.Sum(nil)),
		Smallest: bytes.Clone(smallest),
		Largest:  bytes.Clone(largest),
	}, nil
}

func (b *pebbleSSTIngestBundle) WriteTo(w io.Writer) (int64, error) {
	cw := &countingWriter{w: w}
	if _, err := cw.Write(pebbleSSTIngestSnapshotMagic[:]); err != nil {
		return cw.n, err
	}
	if err := binary.Write(cw, binary.BigEndian, int64(len(b.manifestJSON))); err != nil {
		return cw.n, errors.WithStack(err)
	}
	digest := sha256.Sum256(b.manifestJSON)
	if _, err := cw.Write(digest[:]); err != nil {
		return cw.n, err
	}
	if _, err := cw.Write(b.manifestJSON); err != nil {
		return cw.n, err
	}
	for _, file := range b.manifest.Files {
		if err := b.writeFileTo(cw, file); err != nil {
			return cw.n, err
		}
	}
	return cw.n, nil
}

func (b *pebbleSSTIngestBundle) writeFileTo(w io.Writer, fileMeta pebbleSSTIngestFileMeta) (retErr error) {
	f, err := os.Open(filepath.Join(b.dir, fileMeta.Name))
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			retErr = errors.Join(retErr, errors.WithStack(err))
		}
	}()
	written, err := io.Copy(w, f)
	if err != nil {
		return errors.WithStack(err)
	}
	if written != fileMeta.Size {
		return errors.WithStack(errors.Newf("SST ingest file %s changed while streaming", fileMeta.Name))
	}
	return nil
}

func (b *pebbleSSTIngestBundle) Close() error {
	if b == nil || b.dir == "" {
		return nil
	}
	err := os.RemoveAll(b.dir)
	b.dir = ""
	return errors.WithStack(err)
}

func (s *pebbleStore) restorePebbleSSTIngestAtomic(r io.Reader) error {
	manifest, err := readPebbleSSTIngestManifest(r)
	if err != nil {
		return err
	}
	stageDir, err := os.MkdirTemp(filepath.Dir(filepath.Clean(s.dir)), filepath.Base(s.dir)+"-sst-receive-*")
	if err != nil {
		return errors.WithStack(err)
	}
	defer os.RemoveAll(stageDir)

	paths, err := receiveSSTIngestFiles(r, stageDir, manifest)
	if err != nil {
		return err
	}
	tmpDir, err := makeSiblingTempDir(s.dir, "sst-ingest")
	if err != nil {
		return err
	}
	metadata := metadataFromSSTIngestManifest(manifest)
	if err := ingestSSTSnapshotIntoTempDB(tmpDir, paths, metadata, s.cipher != nil); err != nil {
		_ = os.RemoveAll(tmpDir)
		return err
	}
	return s.swapInTempDBWithMetadata(tmpDir, &metadata)
}

func readPebbleSSTIngestManifest(r io.Reader) (pebbleSSTIngestManifest, error) {
	manifestJSON, err := readPebbleSSTIngestManifestBytes(r)
	if err != nil {
		return pebbleSSTIngestManifest{}, err
	}
	manifest, err := decodePebbleSSTIngestManifest(manifestJSON)
	if err != nil {
		return pebbleSSTIngestManifest{}, err
	}
	if err := validateSSTIngestManifest(manifest); err != nil {
		return pebbleSSTIngestManifest{}, err
	}
	return manifest, nil
}

func readPebbleSSTIngestManifestBytes(r io.Reader) ([]byte, error) {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	if !bytes.Equal(magic[:], pebbleSSTIngestSnapshotMagic[:]) {
		return nil, errors.New("invalid SST ingest snapshot magic header")
	}
	var manifestLen int64
	if err := binary.Read(r, binary.BigEndian, &manifestLen); err != nil {
		return nil, errors.WithStack(err)
	}
	if manifestLen <= 0 || manifestLen > sstIngestManifestMaxBytes {
		return nil, errors.WithStack(errors.Newf("invalid SST ingest manifest length: %d", manifestLen))
	}
	var expectedDigest [sstIngestSnapshotDigestBytes]byte
	if _, err := io.ReadFull(r, expectedDigest[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	manifestJSON := make([]byte, manifestLen)
	if _, err := io.ReadFull(r, manifestJSON); err != nil {
		return nil, errors.WithStack(err)
	}
	actualDigest := sha256.Sum256(manifestJSON)
	if actualDigest != expectedDigest {
		return nil, errors.New("SST ingest manifest SHA-256 mismatch")
	}
	return manifestJSON, nil
}

func decodePebbleSSTIngestManifest(manifestJSON []byte) (pebbleSSTIngestManifest, error) {
	var manifest pebbleSSTIngestManifest
	decoder := json.NewDecoder(bytes.NewReader(manifestJSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return pebbleSSTIngestManifest{}, errors.WithStack(err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return pebbleSSTIngestManifest{}, errors.New("SST ingest manifest contains trailing JSON")
	}
	return manifest, nil
}

func validateSSTIngestManifest(manifest pebbleSSTIngestManifest) error {
	if manifest.Version != sstIngestSnapshotVersion {
		return errors.WithStack(errors.Newf("unsupported SST ingest snapshot version: %d", manifest.Version))
	}
	if len(manifest.Files) > sstIngestSnapshotMaxFiles {
		return errors.WithStack(errors.Newf("SST ingest snapshot has too many files: %d", len(manifest.Files)))
	}
	var total int64
	var previousLargest []byte
	for i, file := range manifest.Files {
		if err := validateSSTIngestFileMeta(file, i, previousLargest); err != nil {
			return err
		}
		if math.MaxInt64-total < file.Size {
			return errors.New("SST ingest total size overflows int64")
		}
		total += file.Size
		previousLargest = file.Largest
	}
	if total != manifest.TotalFileBytes {
		return errors.WithStack(errors.Newf("SST ingest total size mismatch: manifest=%d files=%d", manifest.TotalFileBytes, total))
	}
	return nil
}

func validateSSTIngestFileMeta(file pebbleSSTIngestFileMeta, index int, previousLargest []byte) error {
	expectedName := fmt.Sprintf("%0*d.sst", sstIngestSnapshotFileNameWidth, index)
	if file.Name != expectedName {
		return errors.WithStack(errors.Newf("invalid SST ingest file name %q", file.Name))
	}
	if file.Size <= 0 {
		return errors.WithStack(errors.Newf("invalid SST ingest file size for %s: %d", file.Name, file.Size))
	}
	if len(file.Smallest) == 0 || len(file.Largest) == 0 || bytes.Compare(file.Smallest, file.Largest) > 0 {
		return errors.WithStack(errors.Newf("invalid SST ingest key bounds for %s", file.Name))
	}
	if index > 0 && bytes.Compare(previousLargest, file.Smallest) >= 0 {
		return errors.WithStack(errors.Newf("overlapping SST ingest key bounds at %s", file.Name))
	}
	digest, err := hex.DecodeString(file.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return errors.WithStack(errors.Newf("invalid SST ingest SHA-256 for %s", file.Name))
	}
	return nil
}

func receiveSSTIngestFiles(r io.Reader, stageDir string, manifest pebbleSSTIngestManifest) ([]string, error) {
	paths := make([]string, 0, len(manifest.Files))
	for _, fileMeta := range manifest.Files {
		path, err := receiveSSTIngestFile(r, stageDir, fileMeta)
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	var trailing [1]byte
	if _, err := io.ReadFull(r, trailing[:]); err == nil {
		return nil, errors.New("SST ingest snapshot contains trailing bytes")
	} else if !errors.Is(err, io.EOF) {
		return nil, errors.WithStack(err)
	}
	return paths, nil
}

func receiveSSTIngestFile(r io.Reader, stageDir string, fileMeta pebbleSSTIngestFileMeta) (_ string, retErr error) {
	path := filepath.Join(stageDir, fileMeta.Name)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, sstIngestFilePerm)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			retErr = errors.Join(retErr, errors.WithStack(err))
		}
	}()
	h := sha256.New()
	written, err := io.CopyN(io.MultiWriter(file, h), r, fileMeta.Size)
	if err != nil {
		return "", errors.Wrapf(err, "receive SST ingest file %s", fileMeta.Name)
	}
	if err := file.Sync(); err != nil {
		return "", errors.WithStack(err)
	}
	if written != fileMeta.Size || hex.EncodeToString(h.Sum(nil)) != fileMeta.SHA256 {
		return "", errors.WithStack(errors.Newf("SST ingest file integrity mismatch: %s", fileMeta.Name))
	}
	return path, nil
}

func metadataFromSSTIngestManifest(manifest pebbleSSTIngestManifest) pebbleSnapshotMetadata {
	metadata := pebbleSnapshotMetadata{
		LastCommitTS:         manifest.LastCommitTS,
		MinRetainedTS:        manifest.MinRetainedTS,
		PendingMinRetainedTS: manifest.PendingMinRetainedTS,
	}
	if manifest.AppliedIndex != nil {
		metadata.AppliedIndex = *manifest.AppliedIndex
		metadata.AppliedIndexPresent = true
	}
	return metadata
}

func ingestSSTSnapshotIntoTempDB(tmpDir string, paths []string, metadata pebbleSnapshotMetadata, disableCompression bool) error {
	opts, cache := defaultPebbleOptionsWithCache(disableCompression)
	db, err := pebble.Open(tmpDir, opts)
	if err != nil {
		cache.Unref()
		return errors.WithStack(err)
	}
	defer cache.Unref()
	cleanup := func(err error) error {
		_ = db.Close()
		return err
	}
	if len(paths) > 0 {
		if err := db.Ingest(context.Background(), paths); err != nil {
			return cleanup(errors.Wrap(err, "ingest snapshot SST files"))
		}
	}
	if err := writePebbleSnapshotMetadata(db, metadata); err != nil {
		return cleanup(err)
	}
	if err := verifyPebbleSnapshotMetadata(db, metadata); err != nil {
		return cleanup(err)
	}
	return errors.WithStack(db.Close())
}

func writePebbleSnapshotMetadata(db *pebble.DB, metadata pebbleSnapshotMetadata) error {
	batch := db.NewBatch()
	defer batch.Close()
	if err := setPebbleUint64InBatch(batch, metaLastCommitTSBytes, metadata.LastCommitTS); err != nil {
		return err
	}
	if err := setPebbleUint64InBatch(batch, metaMinRetainedTSBytes, metadata.MinRetainedTS); err != nil {
		return err
	}
	if err := setPebbleUint64InBatch(batch, metaPendingMinRetainedTSBytes, metadata.PendingMinRetainedTS); err != nil {
		return err
	}
	if metadata.AppliedIndexPresent {
		if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, metadata.AppliedIndex); err != nil {
			return err
		}
	}
	return errors.WithStack(batch.Commit(pebble.Sync))
}

func verifyPebbleSnapshotMetadata(db *pebble.DB, expected pebbleSnapshotMetadata) error {
	actual, err := readPebbleMetadata(db)
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.New("SST ingest snapshot metadata verification failed")
	}
	return nil
}

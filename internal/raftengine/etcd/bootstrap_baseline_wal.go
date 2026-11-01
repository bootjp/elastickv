package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const baselineCopyBufferBytes = 64 << 10
const (
	baselineNormalKind byte = iota
	baselineConfKind
	baselineConfV2Kind
)

type baselineWALImage struct {
	metadataSeen   bool
	metadata       []byte
	hardState      *raftpb.HardState
	entries        map[uint64]*raftpb.Entry
	entryDigests   map[uint64]raftbootstrap.Digest
	entrySequence  hash.Hash
	segmentPaths   []string
	keepPayloads   bool
	recordSink     func(*walpb.Record) error
	genesisAllowed bool
	genesis        *raftpb.ConfState
	entryCount     uint64
	snapshots      []*walpb.Snapshot
	fileDigest     raftbootstrap.Digest
}

func (w *baselineWALImage) lastIndex() uint64 {
	var index uint64
	for i := range w.entries {
		index = max(index, i)
	}
	for _, snapshot := range w.snapshots {
		index = max(index, snapshot.GetIndex())
	}
	return index
}

func readBaselineWAL(dir string) (*baselineWALImage, error) {
	return readBaselineWALPayloads(dir, false)
}

func newBaselineWALImage() *baselineWALImage {
	return &baselineWALImage{hardState: &raftpb.HardState{}, entries: make(map[uint64]*raftpb.Entry), entryDigests: make(map[uint64]raftbootstrap.Digest), entrySequence: sha256.New()}
}

func readBaselineWALPayloads(dir string, keep bool) (*baselineWALImage, error) {
	names, err := baselineWALNames(dir)
	if err != nil {
		return nil, err
	}
	image := newBaselineWALImage()
	image.keepPayloads = keep
	image.genesisAllowed = names[0] == "0000000000000000-0000000000000000.wal"
	var crc uint32
	for _, name := range names {
		path := filepath.Join(dir, name)
		image.segmentPaths = append(image.segmentPaths, path)
		crc, err = readBaselineSegment(path, image, crc)
		if err != nil {
			return nil, errors.Wrapf(err, "read offline WAL %s", name)
		}
	}
	if !image.metadataSeen {
		return nil, errors.Wrap(ErrOfflineBaseline, "missing WAL metadata record")
	}
	image.fileDigest, err = baselineTreeDigest(context.Background(), dir, "")
	if err != nil {
		return nil, err
	}
	return image, nil
}

func baselineWALNames(dir string) ([]string, error) {
	if err := baselineRegularDirectory(dir); err != nil {
		return nil, err
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var names []string
	var previous uint64
	for _, file := range files {
		var seq, index uint64
		name := file.Name()
		if _, err := fmt.Sscanf(name, "%016x-%016x.wal", &seq, &index); err != nil || name != fmt.Sprintf("%016x-%016x.wal", seq, index) {
			return nil, errors.Wrap(ErrOfflineBaseline, "unknown WAL directory entry")
		}
		if err := baselineRegularFile(filepath.Join(dir, name)); err != nil {
			return nil, err
		}
		if len(names) != 0 && seq != previous+1 {
			return nil, errors.Wrap(ErrOfflineBaseline, "WAL segment gap")
		}
		previous = seq
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil, errors.Wrap(ErrOfflineBaseline, "existing WAL required; no bootstrap")
	}
	return names, nil
}

func readBaselineSegment(path string, image *baselineWALImage, priorCRC uint32) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer f.Close()
	d := wal.NewDecoder(fileutil.NewFileReader(f))
	first := true
	for {
		record := &walpb.Record{}
		err := d.Decode(record)
		if errors.Is(err, io.EOF) {
			if first {
				return 0, errors.Wrap(ErrOfflineBaseline, "empty WAL segment")
			}
			if err := baselineZeroTail(f, d.LastOffset()); err != nil {
				return 0, err
			}
			return d.LastCRC(), nil
		}
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if err := consumeBaselineSegmentRecord(image, d, record, first, priorCRC); err != nil {
			return 0, err
		}
		first = false
	}
}

func consumeBaselineSegmentRecord(image *baselineWALImage, d wal.Decoder, record *walpb.Record, first bool, priorCRC uint32) error {
	if first && record.GetType() != wal.CrcType {
		return errors.Wrap(ErrOfflineBaseline, "missing segment CRC")
	}
	if record.GetType() != wal.CrcType {
		return image.consume(record)
	}
	crc := d.LastCRC()
	if crc == 0 {
		crc = priorCRC
	}
	if crc != 0 && record.GetCrc() != crc {
		return errors.Wrap(ErrOfflineBaseline, "WAL chain CRC mismatch")
	}
	d.UpdateCRC(record.GetCrc())
	return nil
}

func baselineZeroTail(f *os.File, offset int64) error {
	info, err := f.Stat()
	if err != nil {
		return errors.WithStack(err)
	}
	r := io.NewSectionReader(f, offset, info.Size()-offset)
	buffer := make([]byte, baselineCopyBufferBytes)
	zero := make([]byte, len(buffer))
	for {
		n, err := r.Read(buffer)
		if !bytes.Equal(buffer[:n], zero[:n]) {
			return errors.Wrap(ErrOfflineBaseline, "nonzero data after WAL end")
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
}

func (w *baselineWALImage) consume(record *walpb.Record) error {
	if err := w.consumeValue(record); err != nil {
		return err
	}
	if w.recordSink != nil {
		return w.recordSink(record)
	}
	return nil
}

func (w *baselineWALImage) consumeValue(record *walpb.Record) error {
	switch record.GetType() {
	case wal.MetadataType:
		if w.metadataSeen && !bytes.Equal(w.metadata, record.Data) {
			return errors.Wrap(ErrOfflineBaseline, "conflicting WAL metadata")
		}
		w.metadata = bytes.Clone(record.Data)
		w.metadataSeen = true
	case wal.StateType:
		return w.consumeState(record.Data)
	case wal.EntryType:
		return w.consumeEntry(record.Data)
	case wal.SnapshotType:
		snapshot := &walpb.Snapshot{}
		if err := proto.Unmarshal(record.Data, snapshot); err != nil {
			return errors.WithStack(err)
		}
		w.observeGenesis(snapshot)
		w.snapshots = append(w.snapshots, snapshot)
	default:
		return errors.Wrap(ErrOfflineBaseline, "unknown WAL record")
	}
	return nil
}

func (w *baselineWALImage) observeGenesis(snapshot *walpb.Snapshot) {
	if !w.genesisBeforeState() {
		return
	}
	if snapshot.GetIndex() != 1 || snapshot.GetTerm() != 1 || snapshot.ConfState == nil {
		return
	}
	if len(w.snapshots) != 0 && w.snapshots[len(w.snapshots)-1].GetIndex() != 0 {
		return
	}
	w.genesis, _ = proto.Clone(snapshot.ConfState).(*raftpb.ConfState)
}

func (w *baselineWALImage) genesisBeforeState() bool {
	return w.genesisAllowed && w.entryCount == 0 && w.hardState.GetTerm() == 0 && w.hardState.GetCommit() == 0 && w.hardState.GetVote() == 0
}

func (w *baselineWALImage) consumeState(data []byte) error {
	state := &raftpb.HardState{}
	if err := proto.Unmarshal(data, state); err != nil {
		return errors.WithStack(err)
	}
	if state.GetTerm() < w.hardState.GetTerm() || state.GetCommit() < w.hardState.GetCommit() {
		return errors.Wrap(ErrOfflineBaseline, "regressing HardState")
	}
	if state.GetTerm() == w.hardState.GetTerm() && w.hardState.GetVote() != 0 && state.GetVote() != w.hardState.GetVote() {
		return errors.Wrap(ErrOfflineBaseline, "conflicting vote in one term")
	}
	w.hardState = state
	return nil
}

func (w *baselineWALImage) consumeEntry(data []byte) error {
	w.entryCount++
	entry := &raftpb.Entry{}
	if err := proto.Unmarshal(data, entry); err != nil {
		return errors.WithStack(err)
	}
	if entry.GetIndex() == 0 || entry.GetTerm() == 0 {
		return errors.Wrap(ErrOfflineBaseline, "invalid retained entry")
	}
	digest := sha256.Sum256(entry.Data)
	frame := baselineAppendUint64(nil, entry.GetIndex())
	frame = baselineAppendUint64(frame, entry.GetTerm())
	kind, err := baselineEntryKind(entry)
	if err != nil {
		return err
	}
	frame = append(frame, kind)
	frame = append(frame, digest[:]...)
	unknown := entry.ProtoReflect().GetUnknown()
	frame = baselineAppendUint64(frame, uint64(len(unknown)))
	frame = append(frame, unknown...)
	_, _ = w.entrySequence.Write(frame)
	for index := range w.entries {
		if index >= entry.GetIndex() {
			delete(w.entries, index)
			delete(w.entryDigests, index)
		}
	}
	w.entryDigests[entry.GetIndex()] = digest
	if !w.keepPayloads && entry.GetType() == raftpb.EntryNormal {
		entry.Data = nil
	}
	w.entries[entry.GetIndex()] = entry
	return nil
}

func baselineEntryKind(entry *raftpb.Entry) (byte, error) {
	switch entry.GetType() {
	case raftpb.EntryNormal:
		return baselineNormalKind, nil
	case raftpb.EntryConfChange:
		return baselineConfKind, nil
	case raftpb.EntryConfChangeV2:
		return baselineConfV2Kind, nil
	default:
		return 0, errors.Wrap(ErrOfflineBaseline, "unknown entry type")
	}
}

func baselineRegularFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	if !info.Mode().IsRegular() {
		return errors.Wrap(ErrOfflineBaseline, "non-regular source file")
	}
	return nil
}

func baselineRegularDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Wrap(ErrOfflineBaseline, "non-directory or symlink source")
	}
	return nil
}

// Length-framed relative names and contents bind the complete local source,
// including metadata and retained history, without trusting a selected row.
func baselineTreeDigest(ctx context.Context, root, skip string) (raftbootstrap.Digest, error) {
	var zero raftbootstrap.Digest
	if err := baselineRegularDirectory(root); err != nil {
		return zero, err
	}
	h := sha256.New()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		return baselineHashTreeEntry(ctx, h, root, skip, path, entry, walkErr)
	})
	if err != nil {
		return zero, errors.WithStack(err)
	}
	copy(zero[:], h.Sum(nil))
	return zero, nil
}

func baselineHashTreeEntry(ctx context.Context, h io.Writer, root, skip, path string, entry os.DirEntry, walkErr error) error {
	if walkErr != nil {
		return errors.WithStack(walkErr)
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return errors.WithStack(err)
	}
	if rel == "." {
		return nil
	}
	if rel == skip && entry.IsDir() {
		return filepath.SkipDir
	}
	if entry.Type()&os.ModeSymlink != 0 {
		return errors.Wrap(ErrOfflineBaseline, "symlink source")
	}
	if entry.IsDir() {
		return nil
	}
	return baselineHashFile(h, path, filepath.ToSlash(rel))
}

func baselineHashFile(w io.Writer, path, name string) error {
	if err := baselineRegularFile(path); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return errors.WithStack(err)
	}
	size := info.Size()
	if size < 0 {
		return errors.Wrap(ErrOfflineBaseline, "invalid file size")
	}
	frame := baselineAppendUint64(nil, uint64(len(name)))
	frame = append(frame, name...)
	frame = baselineAppendUint64(frame, uint64(size))
	if _, err := w.Write(frame); err != nil {
		return errors.WithStack(err)
	}
	if _, err := io.Copy(w, f); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (w *baselineWALImage) equivalent(other *baselineWALImage) bool {
	if !proto.Equal(w.hardState, other.hardState) || len(w.entries) != len(other.entries) || !bytes.Equal(w.entrySequence.Sum(nil), other.entrySequence.Sum(nil)) {
		return false
	}
	for index, entry := range w.entries {
		if !proto.Equal(entry, other.entries[index]) || w.entryDigests[index] != other.entryDigests[index] {
			return false
		}
	}
	return slices.EqualFunc(baselineSnapshotRecords(w.snapshots), baselineSnapshotRecords(other.snapshots), func(a, b *walpb.Snapshot) bool { return proto.Equal(a, b) })
}

func baselineSnapshotRecords(records []*walpb.Snapshot) []*walpb.Snapshot {
	var result []*walpb.Snapshot
	for _, record := range records {
		if record.GetIndex() != 0 {
			result = append(result, record)
		}
	}
	return result
}

package etcd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	stateFileName          = "etcd-raft-state.bin"
	stateFileVersion       = uint32(1)
	metadataFileName       = "etcd-raft-meta.bin"
	metadataFileVersion    = uint32(1)
	entriesFileName        = "etcd-raft-entries.bin"
	entriesFileVersion     = uint32(1)
	snapshotDataFileName   = "etcd-fsm-snapshot.bin"
	defaultDirPerm         = 0o755
	defaultFilePerm        = 0o600
	maxPersistedEntries    = uint32(1 << 20)
	entryCapacityCap       = uint32(1024)
	persistedEntryHeadroom = uint32(1 << 20)
	// Leave room for raftpb.Entry metadata above the current 64 MiB transport
	// and command payload budget so persistence does not reject commands that the
	// write path already accepted.
	maxPersistedEntryMessage = uint32(internalutil.GRPCMaxMessageBytes) + persistedEntryHeadroom
	maxPersistedSnapshotMeta = uint32(1 << 20)
	maxPersistedHardState    = uint32(1 << 20)
)

var (
	stateFileMagic    = [4]byte{'E', 'K', 'V', 'R'}
	metadataFileMagic = [4]byte{'E', 'K', 'V', 'M'}
	entriesFileMagic  = [4]byte{'E', 'K', 'V', 'W'}
)

type persistedState struct {
	HardState raftpb.HardState
	Snapshot  raftpb.Snapshot
	Entries   []raftpb.Entry
}

type fileFormat struct {
	magic   [4]byte
	version uint32
}

func bootstrapState(nodeID uint64) persistedState {
	return persistedState{
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				ConfState: raftpb.ConfState{Voters: []uint64{nodeID}},
				Index:     1,
				Term:      1,
			},
		},
	}
}

func stateFilePath(dataDir string) string {
	return filepath.Join(dataDir, stateFileName)
}

func metadataFilePath(dataDir string) string {
	return filepath.Join(dataDir, metadataFileName)
}

func entriesFilePath(dataDir string) string {
	return filepath.Join(dataDir, entriesFileName)
}

func snapshotDataFilePath(dataDir string) string {
	return filepath.Join(dataDir, snapshotDataFileName)
}

func loadOrCreateState(dataDir string, nodeID uint64) (persistedState, error) {
	if err := os.MkdirAll(dataDir, defaultDirPerm); err != nil {
		return persistedState{}, errors.WithStack(err)
	}
	if err := cleanupReplaceTempFiles(dataDir); err != nil {
		return persistedState{}, err
	}

	state, err := loadStateFiles(dataDir)
	if err == nil {
		return state, nil
	}
	if !os.IsNotExist(errors.UnwrapAll(err)) {
		return persistedState{}, err
	}

	legacyPath := stateFilePath(dataDir)
	legacy, legacyErr := loadStateFile(legacyPath)
	if legacyErr == nil {
		if err := saveSplitState(dataDir, legacy); err != nil {
			return persistedState{}, err
		}
		return loadStateFiles(dataDir)
	}
	if !os.IsNotExist(errors.UnwrapAll(legacyErr)) {
		return persistedState{}, legacyErr
	}

	state = bootstrapState(nodeID)
	if err := saveMetadataFile(metadataFilePath(dataDir), state.HardState, state.Snapshot); err != nil {
		return persistedState{}, err
	}
	return state, nil
}

func loadStateFiles(dataDir string) (persistedState, error) {
	state, err := loadMetadataFile(metadataFilePath(dataDir))
	if err != nil {
		return persistedState{}, err
	}
	entries, err := loadEntriesFile(entriesFilePath(dataDir))
	if err != nil && !os.IsNotExist(errors.UnwrapAll(err)) {
		return persistedState{}, err
	}
	state.Entries = entries
	return state, nil
}

func saveSplitState(dataDir string, state persistedState) error {
	if err := saveMetadataFile(metadataFilePath(dataDir), state.HardState, state.Snapshot); err != nil {
		return err
	}
	if err := rewriteEntriesFile(entriesFilePath(dataDir), state.Entries); err != nil {
		return err
	}
	if len(state.Snapshot.Data) == 0 {
		return removeFileIfExists(snapshotDataFilePath(dataDir))
	}
	return writeAndSyncFile(snapshotDataFilePath(dataDir), state.Snapshot.Data)
}

func cleanupReplaceTempFiles(dataDir string) error {
	var err error
	for _, name := range [...]string{stateFileName, metadataFileName, entriesFileName, snapshotDataFileName} {
		matches, globErr := filepath.Glob(filepath.Join(dataDir, name+".tmp-*"))
		if globErr != nil {
			err = errors.CombineErrors(err, errors.WithStack(globErr))
			continue
		}
		for _, match := range matches {
			removeErr := os.Remove(match)
			if removeErr == nil || os.IsNotExist(removeErr) {
				continue
			}
			err = errors.CombineErrors(err, errors.WithStack(removeErr))
		}
	}
	return errors.WithStack(err)
}

func loadMetadataFile(path string) (persistedState, error) {
	file, err := os.Open(path)
	if err != nil {
		return persistedState{}, errors.WithStack(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	if err := readVersionedHeader(reader, fileFormat{magic: metadataFileMagic, version: metadataFileVersion}, "etcd raft metadata"); err != nil {
		return persistedState{}, err
	}

	var state persistedState
	if err := readMessage(reader, &state.HardState, maxPersistedHardState, "hard state"); err != nil {
		return persistedState{}, err
	}
	if err := readMessage(reader, &state.Snapshot, maxPersistedSnapshotMeta, "snapshot metadata"); err != nil {
		return persistedState{}, err
	}
	state.Snapshot.Data = nil
	return state, nil
}

func saveMetadataFile(path string, hardState raftpb.HardState, snapshot raftpb.Snapshot) error {
	snapshot.Data = nil
	return replaceFile(path, func(w io.Writer) error {
		writer := bufio.NewWriter(w)
		if err := writeVersionedHeader(writer, fileFormat{magic: metadataFileMagic, version: metadataFileVersion}); err != nil {
			return err
		}
		if err := writeMessage(writer, hardState.Marshal); err != nil {
			return err
		}
		if err := writeMessage(writer, snapshot.Marshal); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
}

func loadEntriesFile(path string) ([]raftpb.Entry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	if err := readVersionedHeader(reader, fileFormat{magic: entriesFileMagic, version: entriesFileVersion}, "etcd raft entries"); err != nil {
		return nil, err
	}

	entries := make([]raftpb.Entry, 0)
	for {
		var entry raftpb.Entry
		ok, err := readOptionalMessage(reader, &entry, maxPersistedEntryMessage, "entry")
		if err != nil {
			return nil, err
		}
		if !ok {
			return entries, nil
		}
		entries = append(entries, entry)
		if len(entries) > int(maxPersistedEntries) {
			return nil, errors.WithStack(errors.Newf("persisted entry count %d exceeds limit %d", len(entries), maxPersistedEntries))
		}
	}
}

func rewriteEntriesFile(path string, entries []raftpb.Entry) error {
	return replaceFile(path, func(w io.Writer) error {
		writer := bufio.NewWriter(w)
		if err := writeVersionedHeader(writer, fileFormat{magic: entriesFileMagic, version: entriesFileVersion}); err != nil {
			return err
		}
		for _, entry := range entries {
			if err := writeMessage(writer, entry.Marshal); err != nil {
				return err
			}
		}
		if err := writer.Flush(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
}

func removeFileIfExists(path string) error {
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(errors.UnwrapAll(err)) {
			return nil
		}
		return errors.WithStack(err)
	}
	return syncDir(filepath.Dir(path))
}

func loadStateFile(path string) (persistedState, error) {
	file, err := os.Open(path)
	if err != nil {
		return persistedState{}, errors.WithStack(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	if err := readVersionedHeader(reader, fileFormat{magic: stateFileMagic, version: stateFileVersion}, "etcd raft state"); err != nil {
		return persistedState{}, err
	}

	var state persistedState
	if err := readMessage(reader, &state.HardState, maxPersistedHardState, "hard state"); err != nil {
		return persistedState{}, err
	}
	if err := readMessage(reader, &state.Snapshot, maxPersistedSnapshotMeta+maxPersistedEntryMessage, "snapshot"); err != nil {
		return persistedState{}, err
	}

	entryCount, err := readU32(reader)
	if err != nil {
		return persistedState{}, err
	}
	if entryCount > maxPersistedEntries {
		return persistedState{}, errors.WithStack(errors.Newf("persisted entry count %d exceeds limit %d", entryCount, maxPersistedEntries))
	}
	state.Entries = make([]raftpb.Entry, 0, minEntryCapacity(entryCount))
	for range entryCount {
		var entry raftpb.Entry
		if err := readMessage(reader, &entry, maxPersistedEntryMessage, "entry"); err != nil {
			return persistedState{}, err
		}
		state.Entries = append(state.Entries, entry)
	}

	return state, nil
}

func saveStateFile(path string, state persistedState) error {
	encoded, err := encodeStateFile(state)
	if err != nil {
		return err
	}
	return writeAndSyncFile(path, encoded)
}

func encodeStateFile(state persistedState) ([]byte, error) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	if err := writeVersionedHeader(writer, fileFormat{magic: stateFileMagic, version: stateFileVersion}); err != nil {
		return nil, err
	}
	if err := writeMessage(writer, state.HardState.Marshal); err != nil {
		return nil, err
	}
	if err := writeMessage(writer, state.Snapshot.Marshal); err != nil {
		return nil, err
	}
	entryCount, err := uint32Len(len(state.Entries))
	if err != nil {
		return nil, err
	}
	if err := writeU32(writer, entryCount); err != nil {
		return nil, err
	}
	for _, entry := range state.Entries {
		if err := writeMessage(writer, entry.Marshal); err != nil {
			return nil, err
		}
	}
	if err := writer.Flush(); err != nil {
		return nil, errors.WithStack(err)
	}
	return buf.Bytes(), nil
}

func newMemoryStorage(state persistedState) (*etcdraft.MemoryStorage, error) {
	storage := etcdraft.NewMemoryStorage()
	snapshot := state.Snapshot
	snapshot.Data = nil
	if !etcdraft.IsEmptySnap(snapshot) {
		if err := storage.ApplySnapshot(snapshot); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if !etcdraft.IsEmptyHardState(state.HardState) {
		if err := storage.SetHardState(state.HardState); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if len(state.Entries) > 0 {
		if err := storage.Append(append([]raftpb.Entry(nil), state.Entries...)); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return storage, nil
}

func writeU32(w io.Writer, v uint32) error {
	return errors.WithStack(binary.Write(w, binary.BigEndian, v))
}

func readU32(r io.Reader) (uint32, error) {
	var v uint32
	if err := binary.Read(r, binary.BigEndian, &v); err != nil {
		return 0, errors.WithStack(err)
	}
	return v, nil
}

func writeMessage(w io.Writer, marshal func() ([]byte, error)) error {
	raw, err := marshal()
	if err != nil {
		return errors.WithStack(err)
	}
	size, err := uint32Len(len(raw))
	if err != nil {
		return err
	}
	if err := writeU32(w, size); err != nil {
		return err
	}
	if _, err := w.Write(raw); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func readVersionedHeader(r io.Reader, format fileFormat, kind string) error {
	var actual [4]byte
	if _, err := io.ReadFull(r, actual[:]); err != nil {
		return errors.WithStack(err)
	}
	if actual != format.magic {
		return errors.WithStack(errors.Newf("invalid %s magic", kind))
	}

	got, err := readU32(r)
	if err != nil {
		return err
	}
	if got != format.version {
		return errors.WithStack(errors.Newf("unsupported %s version %d", kind, got))
	}
	return nil
}

func writeVersionedHeader(w io.Writer, format fileFormat) error {
	if _, err := w.Write(format.magic[:]); err != nil {
		return errors.WithStack(err)
	}
	return writeU32(w, format.version)
}

func uint32Len(n int) (uint32, error) {
	if n < 0 || n > math.MaxUint32 {
		return 0, errors.New("length exceeds uint32")
	}
	return uint32(n), nil
}

func minEntryCapacity(entryCount uint32) int {
	// Keep the initial slice modest even after count validation so a truncated or
	// adversarial file does not force a large allocation before the entry bodies
	// themselves are validated and read.
	if entryCount > entryCapacityCap {
		return int(entryCapacityCap)
	}
	return int(entryCount)
}

func replaceFile(path string, write func(io.Writer) error) (err error) {
	dir := filepath.Dir(path)
	// Create the replacement file in the destination directory so the final
	// rename stays on the same filesystem and remains atomic.
	file, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return errors.WithStack(err)
	}
	if err := file.Chmod(defaultFilePerm); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return errors.WithStack(err)
	}
	tmpPath := file.Name()
	closed := false
	renamed := false
	closeFile := func() error {
		if closed {
			return nil
		}
		closed = true
		return errors.WithStack(file.Close())
	}
	defer func() {
		err = errors.CombineErrors(err, closeFile())
		if err != nil && !renamed {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := write(file); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return errors.WithStack(err)
	}
	if err := closeFile(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.WithStack(err)
	}
	renamed = true
	return syncDir(dir)
}

func writeAndSyncFile(path string, data []byte) error {
	return replaceFile(path, func(w io.Writer) error {
		if _, err := w.Write(data); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer dir.Close()
	return errors.WithStack(dir.Sync())
}

type protoMessage interface {
	Unmarshal([]byte) error
}

func readMessage(r io.Reader, msg protoMessage, maxSize uint32, kind string) error {
	ok, err := readOptionalMessage(r, msg, maxSize, kind)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return nil
}

func readOptionalMessage(r io.Reader, msg protoMessage, maxSize uint32, kind string) (bool, error) {
	size, err := readU32(r)
	if err != nil {
		if errors.Is(errors.UnwrapAll(err), io.EOF) {
			return false, nil
		}
		return false, err
	}
	if size == 0 {
		return true, nil
	}
	if maxSize > 0 && size > maxSize {
		return false, errors.WithStack(errors.Newf("persisted %s size %d exceeds limit %d", kind, size, maxSize))
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return false, errors.WithStack(err)
	}
	if err := msg.Unmarshal(raw); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

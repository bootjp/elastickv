package etcd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	stateFileName    = "etcd-raft-state.bin"
	stateFileVersion = uint32(1)
	defaultDirPerm   = 0o755
	defaultFilePerm  = 0o600
)

var stateFileMagic = [4]byte{'E', 'K', 'V', 'R'}

type persistedState struct {
	HardState raftpb.HardState
	Snapshot  raftpb.Snapshot
	Entries   []raftpb.Entry
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

func loadOrCreateState(dataDir string, nodeID uint64) (persistedState, error) {
	if err := os.MkdirAll(dataDir, defaultDirPerm); err != nil {
		return persistedState{}, errors.WithStack(err)
	}

	path := stateFilePath(dataDir)
	state, err := loadStateFile(path)
	if err == nil {
		return state, nil
	}
	if !os.IsNotExist(errors.UnwrapAll(err)) {
		return persistedState{}, err
	}

	state = bootstrapState(nodeID)
	if err := saveStateFile(path, state); err != nil {
		return persistedState{}, err
	}
	return state, nil
}

func loadStateFile(path string) (persistedState, error) {
	file, err := os.Open(path)
	if err != nil {
		return persistedState{}, errors.WithStack(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	if err := readFileHeader(reader); err != nil {
		return persistedState{}, err
	}

	var state persistedState
	if err := readMessage(reader, &state.HardState); err != nil {
		return persistedState{}, err
	}
	if err := readMessage(reader, &state.Snapshot); err != nil {
		return persistedState{}, err
	}

	entryCount, err := readU32(reader)
	if err != nil {
		return persistedState{}, err
	}
	state.Entries = make([]raftpb.Entry, 0, entryCount)
	for range entryCount {
		var entry raftpb.Entry
		if err := readMessage(reader, &entry); err != nil {
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
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, encoded, defaultFilePerm); err != nil {
		return errors.WithStack(err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return errors.WithStack(err)
	}
	return nil
}

func encodeStateFile(state persistedState) ([]byte, error) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	if err := writeFileHeader(writer); err != nil {
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

func persistedStateFromStorage(storage *etcdraft.MemoryStorage) (persistedState, error) {
	if storage == nil {
		return persistedState{}, errors.New("memory storage is not configured")
	}

	hardState, _, err := storage.InitialState()
	if err != nil {
		return persistedState{}, errors.WithStack(err)
	}
	snapshot, err := storage.Snapshot()
	if err != nil {
		return persistedState{}, errors.WithStack(err)
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		return persistedState{}, errors.WithStack(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return persistedState{}, errors.WithStack(err)
	}

	var entries []raftpb.Entry
	if lastIndex >= firstIndex {
		entries, err = storage.Entries(firstIndex, lastIndex+1, ^uint64(0))
		if err != nil {
			return persistedState{}, errors.WithStack(err)
		}
		entries = append([]raftpb.Entry(nil), entries...)
	}

	return persistedState{
		HardState: hardState,
		Snapshot:  snapshot,
		Entries:   entries,
	}, nil
}

func newMemoryStorage(state persistedState) (*etcdraft.MemoryStorage, error) {
	storage := etcdraft.NewMemoryStorage()
	if !etcdraft.IsEmptySnap(state.Snapshot) {
		if err := storage.ApplySnapshot(state.Snapshot); err != nil {
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

func readFileHeader(r io.Reader) error {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return errors.WithStack(err)
	}
	if magic != stateFileMagic {
		return errors.New("invalid etcd raft state magic")
	}

	version, err := readU32(r)
	if err != nil {
		return err
	}
	if version != stateFileVersion {
		return errors.WithStack(errors.Newf("unsupported etcd raft state version %d", version))
	}
	return nil
}

func writeFileHeader(w io.Writer) error {
	if _, err := w.Write(stateFileMagic[:]); err != nil {
		return errors.WithStack(err)
	}
	return writeU32(w, stateFileVersion)
}

func uint32Len(n int) (uint32, error) {
	if n < 0 || n > math.MaxUint32 {
		return 0, errors.New("length exceeds uint32")
	}
	return uint32(n), nil
}

type protoMessage interface {
	Unmarshal([]byte) error
}

func readMessage(r io.Reader, msg protoMessage) error {
	size, err := readU32(r)
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return errors.WithStack(err)
	}
	if err := msg.Unmarshal(raw); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

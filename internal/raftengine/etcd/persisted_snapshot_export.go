package etcd

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	etcdsnap "go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	ErrPersistedSnapshotExportInvalid = errors.New("etcd persisted snapshot export: invalid source")
	ErrPersistedSnapshotExportUsed    = errors.New("etcd persisted snapshot export: stream already consumed")
)

// PersistedSnapshotExportMetadata identifies the Raft metadata paired with an
// opaque FSM payload. The payload format is deliberately not interpreted here;
// store-level receivers own compatibility and validation.
type PersistedSnapshotExportMetadata struct {
	Index        uint64
	Term         uint64
	ConfState    *raftpb.ConfState
	PayloadBytes int64
	CRC32C       uint32
}

// PersistedSnapshotExport is a single-use, read-only handle for the newest
// locally persisted FSM snapshot. WriteTo keeps an open descriptor pinned while
// streaming, so runtime snapshot retention may unlink the path without
// changing the bytes being exported.
type PersistedSnapshotExport struct {
	mu          sync.Mutex
	metadata    PersistedSnapshotExportMetadata
	reader      io.Reader
	closer      io.Closer
	expectedCRC uint32
	checkCRC    bool
	consumed    bool
	closed      bool
}

// OpenPersistedSnapshotExport opens the FSM payload referenced by the newest
// WAL-valid Raft snapshot under dataDir. It performs no WAL repair and does not
// mutate the source directory. A false boolean means no persisted snapshot is
// available yet.
func OpenPersistedSnapshotExport(dataDir string) (*PersistedSnapshotExport, bool, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, false, errors.Wrap(ErrPersistedSnapshotExportInvalid, "data dir is required")
	}
	walDir := filepath.Join(dataDir, walDirName)
	if !wal.Exist(walDir) {
		return nil, false, nil
	}
	snapshotter := etcdsnap.New(zap.NewNop(), filepath.Join(dataDir, snapDirName))
	snapshot, err := loadPersistedSnapshot(zap.NewNop(), walDir, snapshotter)
	if err != nil {
		return nil, false, err
	}
	if len(snapshot.Data) == 0 {
		return nil, false, nil
	}
	metadata, err := persistedSnapshotExportMetadata(snapshot)
	if err != nil {
		return nil, false, err
	}
	if !isSnapshotToken(snapshot.Data) {
		payload := bytes.Clone(snapshot.Data)
		metadata.PayloadBytes = int64(len(payload))
		return &PersistedSnapshotExport{
			metadata: metadata,
			reader:   bytes.NewReader(payload),
		}, true, nil
	}

	tok, err := decodeSnapshotToken(snapshot.Data)
	if err != nil {
		return nil, false, err
	}
	if tok.Index != metadata.Index {
		return nil, false, errors.Wrapf(ErrFSMSnapshotTokenInvalid,
			"token index %d does not match snapshot metadata index %d", tok.Index, metadata.Index)
	}
	path := fsmSnapPath(filepath.Join(dataDir, fsmSnapDirName), tok.Index)
	file, payloadBytes, err := openPersistedFSMSnapshotForExport(path, tok.CRC32C)
	if err != nil {
		return nil, false, err
	}
	metadata.PayloadBytes = payloadBytes
	metadata.CRC32C = tok.CRC32C
	return &PersistedSnapshotExport{
		metadata:    metadata,
		reader:      io.LimitReader(file, payloadBytes),
		closer:      file,
		expectedCRC: tok.CRC32C,
		checkCRC:    true,
	}, true, nil
}

// Metadata returns an owned copy of the metadata paired with this export.
func (e *PersistedSnapshotExport) Metadata() PersistedSnapshotExportMetadata {
	if e == nil {
		return PersistedSnapshotExportMetadata{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.metadata
	if e.metadata.ConfState != nil {
		if cloned, ok := proto.Clone(e.metadata.ConfState).(*raftpb.ConfState); ok {
			out.ConfState = cloned
		} else {
			out.ConfState = nil
		}
	}
	return out
}

func persistedSnapshotExportMetadata(snapshot raftpb.Snapshot) (PersistedSnapshotExportMetadata, error) {
	index := snapshot.GetMetadata().GetIndex()
	term := snapshot.GetMetadata().GetTerm()
	confState := snapshot.GetMetadata().GetConfState()
	if index == 0 || term == 0 || confState == nil {
		return PersistedSnapshotExportMetadata{}, errors.Wrapf(ErrPersistedSnapshotExportInvalid,
			"snapshot metadata requires non-zero index and term plus ConfState: index=%d term=%d", index, term)
	}
	cloned, ok := proto.Clone(confState).(*raftpb.ConfState)
	if !ok || cloned == nil {
		return PersistedSnapshotExportMetadata{}, errors.Wrap(ErrPersistedSnapshotExportInvalid, "clone ConfState")
	}
	return PersistedSnapshotExportMetadata{
		Index:     index,
		Term:      term,
		ConfState: cloned,
	}, nil
}

func openPersistedFSMSnapshotForExport(path string, tokenCRC uint32) (*os.File, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, statFSMFileError(err)
	}
	closeOnError := func(err error) (*os.File, int64, error) {
		_ = file.Close()
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return closeOnError(errors.WithStack(err))
	}
	if info.Size() < fsmMinFileSize {
		return closeOnError(errors.Wrapf(ErrFSMSnapshotTooSmall,
			"file too small: %d bytes (minimum %d)", info.Size(), fsmMinFileSize))
	}
	payloadBytes := info.Size() - fsmFooterSize
	var footer [fsmFooterSize]byte
	if _, err := file.ReadAt(footer[:], payloadBytes); err != nil {
		return closeOnError(errors.WithStack(err))
	}
	footerCRC := binary.BigEndian.Uint32(footer[:])
	if footerCRC != tokenCRC {
		return closeOnError(errors.Wrapf(ErrFSMSnapshotTokenCRC,
			"path=%s footer=%08x token=%08x", path, footerCRC, tokenCRC))
	}
	return file, payloadBytes, nil
}

// WriteTo streams the complete FSM payload, excluding the local .fsm CRC
// footer. Token-backed snapshots are CRC-checked during the same pass.
func (e *PersistedSnapshotExport) WriteTo(w io.Writer) (int64, error) {
	if e == nil || w == nil {
		return 0, errors.Wrap(ErrPersistedSnapshotExportInvalid, "export and writer are required")
	}
	e.mu.Lock()
	if e.closed || e.consumed || e.reader == nil {
		e.mu.Unlock()
		return 0, errors.WithStack(ErrPersistedSnapshotExportUsed)
	}
	e.consumed = true
	reader := e.reader
	metadata := e.metadata
	checkCRC := e.checkCRC
	expectedCRC := e.expectedCRC
	e.mu.Unlock()

	if !checkCRC {
		n, err := io.Copy(w, reader)
		return n, errors.WithStack(err)
	}
	h := crc32.New(crc32cTable)
	n, err := io.Copy(io.MultiWriter(w, h), reader)
	if err != nil {
		return n, errors.WithStack(err)
	}
	if n != metadata.PayloadBytes {
		return n, errors.Wrapf(io.ErrUnexpectedEOF, "persisted FSM payload: got %d bytes, expected %d", n, metadata.PayloadBytes)
	}
	if h.Sum32() != expectedCRC {
		return n, errors.Wrapf(ErrFSMSnapshotFileCRC,
			"exported payload computed=%08x token=%08x", h.Sum32(), expectedCRC)
	}
	return n, nil
}

// Close releases the pinned source descriptor. It is safe to call repeatedly.
func (e *PersistedSnapshotExport) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	if e.closer == nil {
		return nil
	}
	return errors.WithStack(e.closer.Close())
}

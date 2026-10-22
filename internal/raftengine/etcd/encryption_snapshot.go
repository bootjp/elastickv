package etcd

import (
	"bytes"
	"io"
	"os"

	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
)

// EncryptionSnapshot returns only the snapshot currently installed in this
// engine, never an unreferenced/newer file found by directory enumeration.
// The caller may use its metadata only after the token and full payload agree.
func (e *Engine) EncryptionSnapshot() (io.ReadCloser, uint64, error) {
	if e == nil || e.storage == nil {
		return nil, 0, errors.New("encryption snapshot: engine storage unavailable")
	}
	source, err := e.openEncryptionSnapshotSource()
	if err != nil {
		return nil, 0, err
	}
	if source.file == nil {
		if source.index == 0 {
			return nil, 0, nil
		}
		return io.NopCloser(bytes.NewReader(source.legacy)), source.index, nil
	}
	file := source.file
	if err := verifyEncryptionSnapshotDescriptor(file, file.Name(), source.crc); err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, errors.WithStack(err)
	}
	return &limitedReadCloser{Reader: io.LimitReader(file, info.Size()-fsmFooterSize), Closer: file}, source.index, nil
}

type encryptionSnapshotSource struct {
	file   *os.File
	legacy []byte
	index  uint64
	crc    uint32
}

func (e *Engine) openEncryptionSnapshotSource() (encryptionSnapshotSource, error) {
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()
	var source encryptionSnapshotSource
	snapshot, err := e.storage.Snapshot()
	if err != nil {
		return source, errors.Wrap(err, "encryption snapshot: read installed snapshot")
	}
	if etcdraft.IsEmptySnap(snapshot) || len(snapshot.Data) == 0 {
		return source, nil
	}
	source.index = snapshot.GetMetadata().GetIndex()
	if !isSnapshotToken(snapshot.Data) {
		source.legacy = snapshot.Data
		return source, nil
	}
	tok, err := decodeSnapshotToken(snapshot.Data)
	if err != nil {
		return source, err
	}
	if tok.Index != source.index {
		return source, errors.Wrap(ErrFSMSnapshotTokenInvalid, "encryption snapshot: token/metadata index mismatch")
	}
	source.crc = tok.CRC32C
	source.file, err = os.Open(fsmSnapPath(e.fsmSnapDir, source.index))
	if err != nil {
		return source, statFSMFileError(err)
	}
	return source, nil
}

func verifyEncryptionSnapshotDescriptor(file *os.File, path string, tokenCRC uint32) error {
	info, err := file.Stat()
	if err != nil {
		return errors.WithStack(err)
	}
	footer, err := verifyFSMSnapshotPrefix(file, info.Size(), path, tokenCRC)
	if err != nil {
		return err
	}
	computed, err := computeFSMSnapshotPayloadCRC(file, info.Size())
	if err != nil {
		return err
	}
	if computed != footer {
		return errors.Wrap(ErrFSMSnapshotFileCRC, "encryption snapshot payload checksum mismatch")
	}
	_, err = file.Seek(0, io.SeekStart)
	return errors.WithStack(err)
}

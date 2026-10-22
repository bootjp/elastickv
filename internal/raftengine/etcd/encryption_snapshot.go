package etcd

import (
	"bytes"
	"io"

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
	snapshot, err := e.storage.Snapshot()
	if err != nil {
		return nil, 0, errors.Wrap(err, "encryption snapshot: read installed snapshot")
	}
	if etcdraft.IsEmptySnap(snapshot) || len(snapshot.Data) == 0 {
		return nil, 0, nil
	}
	index := snapshot.GetMetadata().GetIndex()
	if !isSnapshotToken(snapshot.Data) {
		return io.NopCloser(bytes.NewReader(snapshot.Data)), index, nil
	}
	tok, err := decodeSnapshotToken(snapshot.Data)
	if err != nil {
		return nil, 0, err
	}
	if tok.Index != index {
		return nil, 0, errors.Wrap(ErrFSMSnapshotTokenInvalid, "encryption snapshot: token/metadata index mismatch")
	}
	path := fsmSnapPath(e.fsmSnapDir, index)
	if err := verifyFSMSnapshotFileWithToken(path, tok.CRC32C, true); err != nil {
		return nil, 0, err
	}
	payload, err := openFSMSnapshotPayloadReader(path)
	return payload, index, err
}

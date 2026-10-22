package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const externalRestoreOriginFile = "external-restore-origin.json"
const externalRestoreOriginVersion = 1
const externalRestoreOriginMaxBytes = 4096
const externalRestoreOriginPerm = 0o600

var ErrExternalSnapshotRestoreOrigin = errors.New("external snapshot restore: invalid origin binding")
var ErrExternalSnapshotRestoreUnsupported = errors.New("external snapshot restore: FSM lacks origin-aware validation")

type externalRestoreOrigin struct {
	Version int    `json:"version"`
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	CRC32C  uint32 `json:"crc32c"`
	// The entire FSM payload, including its header, excluding only the footer.
	PayloadSHA256 string `json:"fsm_payload_sha256"`
	Completed     bool   `json:"completed,omitempty"`
}

func writeExternalRestoreOrigin(dir string, opts ExternalSnapshotRestoreOptions, crc uint32) error {
	f, err := os.Open(fsmSnapPath(filepath.Join(dir, fsmSnapDirName), opts.Index))
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()
	digest, _, err := verifiedExternalRestorePayload(f, crc)
	if err != nil {
		return err
	}
	data, err := json.Marshal(externalRestoreOrigin{Version: externalRestoreOriginVersion,
		Index: opts.Index, Term: opts.Term, CRC32C: crc, PayloadSHA256: digest})
	if err != nil {
		return errors.WithStack(err)
	}
	marker, err := os.OpenFile(filepath.Join(dir, externalRestoreOriginFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL, externalRestoreOriginPerm)
	if err != nil {
		return errors.WithStack(err)
	}
	_, writeErr := marker.Write(data)
	syncErr := marker.Sync()
	closeErr := marker.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return errors.WithStack(err)
	}
	return syncDir(dir)
}

func readExternalRestoreOrigin(dir string) (*externalRestoreOrigin, error) {
	f, err := openRegularExternalFSMInput(filepath.Join(dir, externalRestoreOriginFile))
	if os.IsNotExist(errors.UnwrapAll(err)) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, externalRestoreOriginMaxBytes+1))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return decodeExternalRestoreOrigin(data)
}

func decodeExternalRestoreOrigin(data []byte) (*externalRestoreOrigin, error) {
	var marker externalRestoreOrigin
	if len(data) > externalRestoreOriginMaxBytes {
		return nil, errors.WithStack(ErrExternalSnapshotRestoreOrigin)
	}
	if err := json.Unmarshal(data, &marker); err != nil {
		return nil, errors.Wrap(ErrExternalSnapshotRestoreOrigin, err.Error())
	}
	digest, err := hex.DecodeString(marker.PayloadSHA256)
	if err != nil || len(digest) != sha256.Size || marker.Version != externalRestoreOriginVersion || marker.Index == 0 || marker.Term == 0 {
		return nil, errors.WithStack(ErrExternalSnapshotRestoreOrigin)
	}
	return &marker, nil
}

func verifiedExternalRestorePayload(f *os.File, tokenCRC uint32) (string, int64, error) {
	info, err := f.Stat()
	if err != nil {
		return "", 0, errors.WithStack(err)
	}
	footer, err := verifyFSMSnapshotPrefix(f, info.Size(), f.Name(), tokenCRC)
	if err != nil {
		return "", 0, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", 0, errors.WithStack(err)
	}
	payloadSize := info.Size() - fsmFooterSize
	digest, crc := sha256.New(), crc32.New(crc32cTable)
	n, err := io.Copy(io.MultiWriter(digest, crc), io.LimitReader(f, payloadSize))
	if err != nil {
		return "", 0, errors.WithStack(err)
	}
	if n != payloadSize || crc.Sum32() != footer {
		return "", 0, errors.WithStack(ErrFSMSnapshotFileCRC)
	}
	return hex.EncodeToString(digest.Sum(nil)), payloadSize, nil
}

func restoreWithExternalOrigin(fsm StateMachine, snapshot raftpb.Snapshot, dir string, marker *externalRestoreOrigin) (uint64, error) {
	token, err := decodeSnapshotToken(snapshot.Data)
	if err != nil {
		return 0, err
	}
	if !marker.matchesSnapshot(snapshot, token) {
		return 0, errors.WithStack(ErrExternalSnapshotRestoreOrigin)
	}
	f, err := os.Open(fsmSnapPath(dir, token.Index))
	if err != nil {
		return 0, statFSMFileError(err)
	}
	defer f.Close()
	digest, size, err := verifiedExternalRestorePayload(f, token.CRC32C)
	if err != nil {
		return 0, err
	}
	if digest != marker.PayloadSHA256 {
		return 0, errors.WithStack(ErrExternalSnapshotRestoreOrigin)
	}
	restorer, ok := fsm.(raftengine.ContextSnapshotRestorer)
	if !ok {
		return 0, errors.WithStack(ErrExternalSnapshotRestoreUnsupported)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, errors.WithStack(err)
	}
	if err := restorer.RestoreWithContext(context.Background(), io.LimitReader(f, size), raftengine.SnapshotRestoreOptions{
		Origin: raftengine.SnapshotRestoreExternalRebase,
	}); err != nil {
		return 0, errors.Wrap(err, "validate and restore external FSM snapshot")
	}
	return marker.Index, nil
}

func (m externalRestoreOrigin) matchesSnapshot(snapshot raftpb.Snapshot, token snapshotToken) bool {
	return m.Index == snapshot.GetMetadata().GetIndex() && m.Term == snapshot.GetMetadata().GetTerm() &&
		token.Index == m.Index && token.CRC32C == m.CRC32C
}

// Completion permits later WAL-published native snapshots, never ordinary
// restore of the original bytes. Keep the immutable binding for every fallback.
// Open persists this before starting transport or the snapshot/apply workers.
func completeExternalRestoreOrigin(dir string, validated bool) error {
	if !validated {
		return nil
	}
	marker, err := readExternalRestoreOrigin(dir)
	if err != nil {
		return err
	}
	if marker == nil {
		return errors.WithStack(ErrExternalSnapshotRestoreOrigin)
	}
	if marker.Completed {
		return nil
	}
	marker.Completed = true
	data, err := json.Marshal(marker)
	if err != nil {
		return errors.WithStack(err)
	}
	return writeAndSyncFile(filepath.Join(dir, externalRestoreOriginFile), data)
}

// This decision requires the real WAL selection and successful ReadAll, not an
// arbitrary snapshot whose index merely exceeds the imported snapshot's index.
// Preparation seeds a fresh directory with only the original snapshot; Open
// durably completes validation before local/config capture or received Ready
// can publish any later snapshot. The selected WAL boundary is their provenance.
func validateNativeAfterExternalRestore(selected *walRestoreState, marker *externalRestoreOrigin, dir string) error {
	snapshot := selected.snapshot
	token, err := decodeSnapshotToken(snapshot.Data)
	if err != nil {
		return errors.Wrap(ErrExternalSnapshotRestoreOrigin, "later native snapshot requires a supported FSM token")
	}
	meta := snapshot.GetMetadata()
	if !marker.Completed || meta.GetIndex() <= marker.Index || meta.GetTerm() < marker.Term ||
		token.Index != meta.GetIndex() || selected.hardState.GetCommit() < meta.GetIndex() ||
		selected.hardState.GetTerm() < meta.GetTerm() {
		return errors.WithStack(ErrExternalSnapshotRestoreOrigin)
	}
	// Validate the entire file even when the ordinary cold-start gate would
	// use only its header. Neither copied epoch rows nor an applied index are
	// evidence that an unvalidated snapshot belongs to this Raft lineage.
	return verifyFSMSnapshotFile(fsmSnapPath(dir, token.Index), token.CRC32C)
}

package etcd

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	ErrExternalSnapshotRestoreInvalid = errors.New("etcd external snapshot restore: invalid options")
	ErrExternalSnapshotRestoreExists  = errors.New("etcd external snapshot restore: destination already exists")
	ErrExternalSnapshotRestoreSHA256  = errors.New("etcd external snapshot restore: payload SHA-256 mismatch")
)

type ExternalSnapshotRestoreOptions struct {
	InputFSMPath          string
	DataDir               string
	Index                 uint64
	Term                  uint64
	Peers                 []Peer
	SnapshotCeilingMs     uint64
	ExpectedPayloadSHA256 string
}

type ExternalSnapshotRestoreResult struct {
	DataDir       string
	FSMPath       string
	SnapPath      string
	CRC32C        uint32
	PayloadBytes  int64
	PayloadSHA256 string
	Peers         int
}

// PrepareExternalSnapshotRestore seeds a fresh etcd-raft data directory from
// an externally produced EKVPBBL1 FSM payload. The input file is the raw payload
// emitted by elastickv-snapshot-encode; this helper writes the runtime
// fsm-snap/<index>.fsm form by appending the CRC32C footer and persists the
// matching EKVT token snapshot under snap/.
func PrepareExternalSnapshotRestore(opts ExternalSnapshotRestoreOptions) (*ExternalSnapshotRestoreResult, error) {
	if err := validateExternalSnapshotRestoreOptions(opts); err != nil {
		return nil, err
	}
	destDataDir, tempDir, err := prepareExternalSnapshotRestoreDest(opts.DataDir)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(tempDir)
		}
	}()

	fsmSnapDir := filepath.Join(tempDir, fsmSnapDirName)
	crc, payloadBytes, payloadSHA, err := writeExternalFSMSnapshotFile(opts.InputFSMPath, fsmSnapDir, opts.Index, opts.SnapshotCeilingMs)
	if err != nil {
		return nil, err
	}
	if opts.ExpectedPayloadSHA256 != "" && !strings.EqualFold(payloadSHA, opts.ExpectedPayloadSHA256) {
		return nil, errors.Wrapf(ErrExternalSnapshotRestoreSHA256,
			"copied payload has %s, expected %s", payloadSHA, opts.ExpectedPayloadSHA256)
	}
	token := encodeSnapshotToken(opts.Index, crc)
	if err := seedExternalSnapshotRestoreDir(tempDir, opts, token); err != nil {
		return nil, err
	}
	if err := finalizeMigrationDir(tempDir, destDataDir); err != nil {
		if errors.Is(err, errMigrationDestinationExists) {
			return nil, errors.Wrapf(ErrExternalSnapshotRestoreExists, "destination exists: %s", destDataDir)
		}
		return nil, err
	}
	committed = true

	return &ExternalSnapshotRestoreResult{
		DataDir:       destDataDir,
		FSMPath:       fsmSnapPath(filepath.Join(destDataDir, fsmSnapDirName), opts.Index),
		SnapPath:      snapPath(filepath.Join(destDataDir, snapDirName), opts.Term, opts.Index),
		CRC32C:        crc,
		PayloadBytes:  payloadBytes,
		PayloadSHA256: payloadSHA,
		Peers:         len(opts.Peers),
	}, nil
}

func validateExternalSnapshotRestoreOptions(opts ExternalSnapshotRestoreOptions) error {
	switch {
	case opts.InputFSMPath == "":
		return errors.Wrap(ErrExternalSnapshotRestoreInvalid, "input FSM path is required")
	case opts.DataDir == "":
		return errors.Wrap(ErrExternalSnapshotRestoreInvalid, "data dir is required")
	case opts.Index == 0:
		return errors.Wrap(ErrExternalSnapshotRestoreInvalid, "snapshot index must be > 0")
	case opts.Term == 0:
		return errors.Wrap(ErrExternalSnapshotRestoreInvalid, "snapshot term must be > 0")
	case len(opts.Peers) == 0:
		return errors.Wrap(ErrExternalSnapshotRestoreInvalid, "at least one peer is required")
	}
	for i, peer := range opts.Peers {
		if peer.NodeID == 0 {
			return errors.Wrapf(ErrExternalSnapshotRestoreInvalid, "peer[%d] has zero node id", i)
		}
	}
	seenNodeIDs := make(map[uint64]struct{}, len(opts.Peers))
	for i, peer := range opts.Peers {
		if _, ok := seenNodeIDs[peer.NodeID]; ok {
			return errors.Wrapf(ErrExternalSnapshotRestoreInvalid, "peer[%d] has duplicate node id %d", i, peer.NodeID)
		}
		seenNodeIDs[peer.NodeID] = struct{}{}
	}
	return nil
}

func prepareExternalSnapshotRestoreDest(destDataDir string) (string, string, error) {
	destDataDir = filepath.Clean(destDataDir)
	if err := ensureExternalRestorePathAbsent(destDataDir, "destination"); err != nil {
		return "", "", err
	}
	tempDir := destDataDir + ".restore-prep"
	if err := createExternalRestoreTempDir(tempDir); err != nil {
		return "", "", err
	}
	return destDataDir, tempDir, nil
}

func createExternalRestoreTempDir(tempDir string) error {
	if err := os.Mkdir(tempDir, defaultDirPerm); err != nil {
		if os.IsExist(err) {
			return errors.Wrapf(ErrExternalSnapshotRestoreExists, "temporary destination exists: %s", tempDir)
		}
		return errors.WithStack(err)
	}
	return nil
}

func ensureExternalRestorePathAbsent(path string, kind string) error {
	if _, err := os.Stat(path); err == nil {
		return errors.Wrapf(ErrExternalSnapshotRestoreExists, "%s exists: %s", kind, path)
	} else if !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	return nil
}

func writeExternalFSMSnapshotFile(inputPath, fsmSnapDir string, index uint64, ceilingMs uint64) (uint32, int64, string, error) {
	if err := os.MkdirAll(fsmSnapDir, defaultDirPerm); err != nil {
		return 0, 0, "", errors.WithStack(err)
	}
	in, err := openRegularExternalFSMInput(inputPath)
	if err != nil {
		return 0, 0, "", err
	}
	defer func() { _ = in.Close() }()

	tmpFile, err := os.CreateTemp(fsmSnapDir, "*.fsm.tmp")
	if err != nil {
		return 0, 0, "", errors.WithStack(err)
	}
	tmpPath := tmpFile.Name()
	finalPath := fsmSnapPath(fsmSnapDir, index)
	closed := false
	defer func() {
		if !closed {
			_ = tmpFile.Close()
		}
		_ = os.Remove(tmpPath)
	}()

	crc, bytesWritten, payloadSHA, err := copyExternalPayloadWithHeaderAndFooter(in, tmpFile, ceilingMs)
	if err != nil {
		return 0, 0, "", err
	}
	closed = true
	if err := tmpFile.Close(); err != nil {
		return 0, 0, "", errors.WithStack(err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return 0, 0, "", errors.WithStack(err)
	}
	if err := syncDir(fsmSnapDir); err != nil {
		return 0, 0, "", errors.WithStack(err)
	}
	return crc, bytesWritten, payloadSHA, nil
}

func openRegularExternalFSMInput(inputPath string) (*os.File, error) {
	in, err := openExternalSnapshotInput(inputPath)
	if err != nil {
		if isExternalSnapshotInputSymlink(err) {
			return nil, errors.Wrapf(ErrExternalSnapshotRestoreInvalid, "%s is a symlink", inputPath)
		}
		return nil, errors.WithStack(err)
	}
	if err := requireRegularFile(in, inputPath); err != nil {
		_ = in.Close()
		return nil, err
	}
	return in, nil
}

func requireRegularFile(f *os.File, path string) error {
	info, err := f.Stat()
	if err != nil {
		return errors.WithStack(err)
	}
	if !info.Mode().IsRegular() {
		return errors.Wrapf(ErrExternalSnapshotRestoreInvalid, "%s is not a regular file", path)
	}
	return nil
}

func copyExternalPayloadWithHeaderAndFooter(in io.Reader, out *os.File, ceilingMs uint64) (uint32, int64, string, error) {
	bw := bufio.NewWriterSize(out, fsmWriteBufSize)
	crcHash := crc32.New(crc32cTable)
	payloadHash := sha256.New()
	header := encodeExternalRestoreSnapshotHeader(ceilingMs)
	if _, err := bw.Write(header[:]); err != nil {
		return 0, 0, "", errors.WithStack(err)
	}
	if _, err := crcHash.Write(header[:]); err != nil {
		return 0, 0, "", errors.WithStack(err)
	}
	n, err := io.Copy(io.MultiWriter(bw, crcHash, payloadHash), in)
	if err != nil {
		return 0, n, "", errors.WithStack(err)
	}
	crc := crcHash.Sum32()
	if err := binary.Write(bw, binary.BigEndian, crc); err != nil {
		return 0, n, "", errors.WithStack(err)
	}
	if err := bw.Flush(); err != nil {
		return 0, n, "", errors.WithStack(err)
	}
	if err := out.Sync(); err != nil {
		return 0, n, "", errors.WithStack(err)
	}
	return crc, n, hex.EncodeToString(payloadHash.Sum(nil)), nil
}

func encodeExternalRestoreSnapshotHeader(ceilingMs uint64) [externalRestoreSnapshotHeaderLen]byte {
	var header [externalRestoreSnapshotHeaderLen]byte
	copy(header[:externalRestoreSnapshotMagicLen], externalRestoreSnapshotMagic[:])
	binary.BigEndian.PutUint64(header[externalRestoreSnapshotMagicLen:], ceilingMs)
	return header
}

var externalRestoreSnapshotMagic = [externalRestoreSnapshotMagicLen]byte{'E', 'K', 'V', 'T', 'H', 'L', 'C', '1'}

const (
	externalRestoreSnapshotMagicLen  = 8
	externalRestoreSnapshotHeaderLen = 16
)

func seedExternalSnapshotRestoreDir(tempDir string, opts ExternalSnapshotRestoreOptions, token []byte) error {
	confState := confStateForPeers(opts.Peers)
	state := persistedState{
		HardState: raftpb.HardState{
			Term:   proto.Uint64(opts.Term),
			Commit: proto.Uint64(opts.Index),
		},
		Snapshot: raftpb.Snapshot{
			Data: token,
			Metadata: &raftpb.SnapshotMetadata{
				ConfState: &confState,
				Index:     proto.Uint64(opts.Index),
				Term:      proto.Uint64(opts.Term),
			},
		},
	}
	if etcdraft.IsEmptySnap(&state.Snapshot) {
		return errors.Wrap(ErrExternalSnapshotRestoreInvalid, "empty snapshot metadata")
	}
	disk, err := persistBootState(zap.NewNop(),
		filepath.Join(tempDir, walDirName),
		filepath.Join(tempDir, snapDirName),
		filepath.Join(tempDir, fsmSnapDirName),
		nil,
		state)
	if err != nil {
		return err
	}
	if err := closePersist(disk.Persist); err != nil {
		return err
	}
	return savePersistedPeers(tempDir, opts.Index, opts.Peers)
}

func snapPath(snapDir string, term, index uint64) string {
	return filepath.Join(snapDir, formatSnapName(term, index))
}

func formatSnapName(term, index uint64) string {
	return fmt.Sprintf("%016x-%016x%s", term, index, snapFileExt)
}

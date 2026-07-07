// Command elastickv-snapshot-prepare-restore turns an encoded EKVPBBL1
// snapshot payload into a fresh etcd-raft data directory that can be used by a
// stopped node.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"syscall"

	"github.com/bootjp/elastickv/internal/backup"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
)

const (
	exitSuccess             = 0
	exitUserErr             = 1
	exitDataErr             = 2
	maxEncodeInfoBytes      = 1 << 20
	restoreInputKindPayload = "payload"
	restoreInputKindSidecar = "encode-info"
)

var (
	errRestoreClusterIDMismatch        = errors.New("snapshot restore: cluster_id mismatch")
	errRestoreClusterIDMissing         = errors.New("snapshot restore: cluster_id missing")
	errRestoreEncodeInfoMalformed      = errors.New("snapshot restore: ENCODE_INFO sidecar malformed")
	errRestoreEncodeInfoTooLarge       = errors.New("snapshot restore: ENCODE_INFO sidecar too large")
	errRestoreHLCCeilingOverflow       = errors.New("snapshot restore: HLC ceiling overflow")
	errRestoreKeyFormatMismatch        = errors.New("snapshot restore: encoder key-format version mismatch")
	errRestoreSHA256Mismatch           = errors.New("snapshot restore: encoded FSM SHA-256 mismatch")
	errRestoreSHA256Missing            = errors.New("snapshot restore: encoded FSM SHA-256 missing")
	errRestoreSelfTestMissing          = errors.New("snapshot restore: encoder self-test did not pass")
	errRestoreSnapshotTimestampCeiling = errors.New("snapshot restore: entry commit_ts exceeds snapshot header last_commit_ts")
)

const (
	hlcLogicalBits   = 16
	hlcPhysicalBits  = 64 - hlcLogicalBits
	maxHLCPhysicalMs = (uint64(1) << hlcPhysicalBits) - 1
)

type config struct {
	inputPath               string
	encodeInfoPath          string
	dataDir                 string
	index                   uint64
	term                    uint64
	peerCSV                 string
	targetClusterID         string
	freshCluster            bool
	allowMissingClusterID   bool
	allowUnverifiedSelfTest bool
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	code, err := run(os.Args[1:], logger)
	if err != nil {
		logger.Error("elastickv-snapshot-prepare-restore", "err", err)
	}
	os.Exit(code)
}

func run(argv []string, logger *slog.Logger) (int, error) {
	cfg, err := parseFlags(argv)
	if err != nil {
		return exitUserErr, err
	}
	if err := prepare(cfg, logger); err != nil {
		return classifyError(err), err
	}
	return exitSuccess, nil
}

func classifyError(err error) int {
	switch {
	case errors.Is(err, backup.ErrSnapshotBadMagic),
		errors.Is(err, backup.ErrSnapshotTruncated),
		errors.Is(err, backup.ErrSnapshotEncryptedEntry),
		errors.Is(err, backup.ErrSnapshotEncryptedReserved),
		errors.Is(err, backup.ErrSnapshotKeyTooLarge),
		errors.Is(err, backup.ErrSnapshotShortKey),
		errors.Is(err, backup.ErrSnapshotShortValue),
		errors.Is(err, backup.ErrSnapshotValueTooLarge),
		errors.Is(err, backup.ErrUnsupportedEncodeInfoFormatVersion),
		errors.Is(err, errRestoreClusterIDMismatch),
		errors.Is(err, errRestoreClusterIDMissing),
		errors.Is(err, errRestoreEncodeInfoMalformed),
		errors.Is(err, errRestoreEncodeInfoTooLarge),
		errors.Is(err, errRestoreHLCCeilingOverflow),
		errors.Is(err, errRestoreKeyFormatMismatch),
		errors.Is(err, errRestoreSHA256Mismatch),
		errors.Is(err, errRestoreSHA256Missing),
		errors.Is(err, etcd.ErrExternalSnapshotRestoreSHA256),
		errors.Is(err, errRestoreSelfTestMissing),
		errors.Is(err, errRestoreSnapshotTimestampCeiling):
		return exitDataErr
	default:
		return exitUserErr
	}
}

func parseFlags(argv []string) (*config, error) {
	fs := flag.NewFlagSet("elastickv-snapshot-prepare-restore", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg := &config{term: 1}
	fs.StringVar(&cfg.inputPath, "input", "", "Encoded EKVPBBL1 .fsm payload from elastickv-snapshot-encode (required)")
	fs.StringVar(&cfg.encodeInfoPath, "encode-info", "", "Path to ENCODE_INFO sidecar (default <input>.encode_info.json)")
	fs.StringVar(&cfg.dataDir, "data-dir", "", "Fresh target raft data directory to create (required; must not already exist)")
	fs.Uint64Var(&cfg.index, "index", 0, "Snapshot raft index to stamp into snap/fsm-snap files (required, > 0)")
	fs.Uint64Var(&cfg.term, "term", 1, "Snapshot raft term to stamp into the .snap file")
	fs.StringVar(&cfg.peerCSV, "peers", "", "Comma-separated raft peers id=addr,id=addr (required)")
	fs.StringVar(&cfg.targetClusterID, "target-cluster-id", "", "Expected target cluster_id for non-fresh restores")
	fs.BoolVar(&cfg.freshCluster, "fresh-cluster", false, "Allow restoring into a brand-new cluster with no target cluster_id yet")
	fs.BoolVar(&cfg.allowMissingClusterID, "allow-missing-cluster-id", false, "Allow ENCODE_INFO without manifest_cluster_id")
	fs.BoolVar(&cfg.allowUnverifiedSelfTest, "allow-unverified-self-test", false, "Allow ENCODE_INFO where encoder self-test did not run")
	if err := fs.Parse(argv); err != nil {
		return nil, errors.WithStack(err)
	}
	if cfg.inputPath == "" {
		return nil, errors.New("--input is required")
	}
	if cfg.encodeInfoPath == "" {
		cfg.encodeInfoPath = backup.EncodeInfoSidecarPath(cfg.inputPath)
	}
	if cfg.dataDir == "" {
		return nil, errors.New("--data-dir is required")
	}
	if cfg.index == 0 {
		return nil, errors.New("--index must be > 0")
	}
	if cfg.term == 0 {
		return nil, errors.New("--term must be > 0")
	}
	if strings.TrimSpace(cfg.peerCSV) == "" {
		return nil, errors.New("--peers is required")
	}
	if cfg.freshCluster && cfg.targetClusterID != "" {
		return nil, errors.New("--fresh-cluster and --target-cluster-id are mutually exclusive")
	}
	return cfg, nil
}

func prepare(cfg *config, logger *slog.Logger) error {
	info, err := readEncodeInfo(cfg.encodeInfoPath)
	if err != nil {
		return err
	}
	header, shaHex, err := validatePayload(cfg.inputPath)
	if err != nil {
		return err
	}
	if err := validateEncodeInfo(info, shaHex, cfg); err != nil {
		return err
	}
	peers, err := parsePeers(cfg.peerCSV)
	if err != nil {
		return err
	}
	snapshotCeilingMs, err := hlcCeilingMsAfterLastCommitTS(header.LastCommitTS)
	if err != nil {
		return err
	}
	result, err := etcd.PrepareExternalSnapshotRestore(etcd.ExternalSnapshotRestoreOptions{
		InputFSMPath:          cfg.inputPath,
		DataDir:               cfg.dataDir,
		Index:                 cfg.index,
		Term:                  cfg.term,
		Peers:                 peers,
		SnapshotCeilingMs:     snapshotCeilingMs,
		ExpectedPayloadSHA256: info.OutputFSMSHA256,
	})
	if err != nil {
		return errors.Wrap(err, "prepare external snapshot restore")
	}
	logger.Info("snapshot restore data dir prepared",
		"data_dir", result.DataDir,
		"fsm", result.FSMPath,
		"snap", result.SnapPath,
		"crc32c", fmt.Sprintf("%08x", result.CRC32C),
		"payload_sha256", result.PayloadSHA256,
		"payload_bytes", result.PayloadBytes,
		"last_commit_ts", header.LastCommitTS,
		"peers", result.Peers,
	)
	return nil
}

func readEncodeInfo(path string) (backup.EncodeInfo, error) {
	f, err := openRegularRestoreFile(path, restoreInputKindSidecar)
	if err != nil {
		return backup.EncodeInfo{}, err
	}
	defer func() { _ = f.Close() }()
	body, err := io.ReadAll(io.LimitReader(f, maxEncodeInfoBytes+1))
	if err != nil {
		return backup.EncodeInfo{}, errors.WithStack(err)
	}
	if int64(len(body)) > maxEncodeInfoBytes {
		return backup.EncodeInfo{}, errors.Wrapf(errRestoreEncodeInfoTooLarge, "%s exceeds %d bytes", path, maxEncodeInfoBytes)
	}
	info, err := backup.ReadEncodeInfo(bytes.NewReader(body))
	if err != nil {
		if errors.Is(err, backup.ErrUnsupportedEncodeInfoFormatVersion) {
			return backup.EncodeInfo{}, errors.Wrap(err, "read encode info")
		}
		return backup.EncodeInfo{}, errors.Wrapf(errRestoreEncodeInfoMalformed, "%v", err)
	}
	return info, nil
}

func validatePayload(path string) (backup.SnapshotHeader, string, error) {
	f, err := openRegularRestoreFile(path, restoreInputKindPayload)
	if err != nil {
		return backup.SnapshotHeader{}, "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	var maxEntryCommitTS uint64
	header, err := backup.ReadSnapshotWithHeader(io.TeeReader(f, h), func(_ backup.SnapshotHeader, entry backup.SnapshotEntry) error {
		if entry.CommitTS > maxEntryCommitTS {
			maxEntryCommitTS = entry.CommitTS
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return backup.SnapshotHeader{}, "", errors.Wrap(backup.ErrSnapshotTruncated, err.Error())
		}
		return backup.SnapshotHeader{}, "", errors.Wrap(err, "read encoded snapshot payload")
	}
	if maxEntryCommitTS > header.LastCommitTS {
		return backup.SnapshotHeader{}, "", errors.Wrapf(errRestoreSnapshotTimestampCeiling,
			"max entry commit_ts %d > header last_commit_ts %d", maxEntryCommitTS, header.LastCommitTS)
	}
	return header, hex.EncodeToString(h.Sum(nil)), nil
}

func openRegularRestoreFile(path string, kind string) (*os.File, error) {
	f, err := openNoFollowNonblocking(path)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, errors.Wrapf(etcd.ErrExternalSnapshotRestoreInvalid, "%s %s is a symlink", kind, path)
		}
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, errors.WithStack(err)
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, errors.Wrapf(etcd.ErrExternalSnapshotRestoreInvalid, "%s %s is not a regular file", kind, path)
	}
	return f, nil
}

func openNoFollowNonblocking(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0) //nolint:gosec // operator-supplied restore artifact path
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return os.NewFile(uintptr(fd), path), nil
}

func validateEncodeInfo(info backup.EncodeInfo, gotSHA string, cfg *config) error {
	if info.OutputFSMSHA256 == "" {
		return errors.Wrap(errRestoreSHA256Missing, "ENCODE_INFO output_fsm_sha256 is empty")
	}
	if !strings.EqualFold(info.OutputFSMSHA256, gotSHA) {
		return errors.Wrapf(errRestoreSHA256Mismatch, "input %s has %s, sidecar records %s",
			cfg.inputPath, gotSHA, info.OutputFSMSHA256)
	}
	if info.EncoderKeyFormatVersion != backup.CurrentFormatVersion {
		return errors.Wrapf(errRestoreKeyFormatMismatch, "got %d, current %d",
			info.EncoderKeyFormatVersion, backup.CurrentFormatVersion)
	}
	if !cfg.allowUnverifiedSelfTest && (!info.SelfTest.Ran || !info.SelfTest.Matched) {
		return errors.Wrap(errRestoreSelfTestMissing, "rerun elastickv-snapshot-encode with --self-test or pass --allow-unverified-self-test")
	}
	return validateClusterID(info.ManifestClusterID, cfg)
}

func validateClusterID(sourceClusterID string, cfg *config) error {
	switch {
	case sourceClusterID == "" && cfg.allowMissingClusterID:
		return nil
	case sourceClusterID == "":
		return errors.Wrap(errRestoreClusterIDMissing, "ENCODE_INFO manifest_cluster_id is empty")
	case cfg.freshCluster:
		return nil
	case cfg.targetClusterID == "":
		return errors.Wrap(errRestoreClusterIDMissing, "pass --target-cluster-id or --fresh-cluster")
	case sourceClusterID != cfg.targetClusterID:
		return errors.Wrapf(errRestoreClusterIDMismatch, "source %q target %q", sourceClusterID, cfg.targetClusterID)
	default:
		return nil
	}
}

func parsePeers(raw string) ([]etcd.Peer, error) {
	peers, err := etcd.ParsePeers(raw)
	if err != nil {
		return nil, errors.Wrap(err, "parse peers")
	}
	if len(peers) == 0 {
		return nil, errors.New("--peers selected zero peers")
	}
	seen := make(map[uint64]struct{}, len(peers))
	for i := range peers {
		if peers[i].NodeID == 0 {
			peers[i].NodeID = etcd.DeriveNodeID(peers[i].ID)
		}
		if peers[i].NodeID == 0 {
			return nil, errors.Errorf("peer %q derived node id 0", peers[i].ID)
		}
		if _, ok := seen[peers[i].NodeID]; ok {
			return nil, errors.Errorf("duplicate peer node id %d", peers[i].NodeID)
		}
		seen[peers[i].NodeID] = struct{}{}
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].NodeID < peers[j].NodeID
	})
	return peers, nil
}

func hlcCeilingMsAfterLastCommitTS(ts uint64) (uint64, error) {
	if ts == 0 {
		return 0, nil
	}
	physicalMs := ts >> hlcLogicalBits
	if physicalMs == maxHLCPhysicalMs {
		return 0, errors.Wrapf(errRestoreHLCCeilingOverflow, "last_commit_ts %d has physical ms %d", ts, physicalMs)
	}
	return physicalMs + 1, nil
}

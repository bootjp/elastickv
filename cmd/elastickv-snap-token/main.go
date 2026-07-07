// Command elastickv-snap-token prepares an offline-encoded .fsm for
// stop-replace-restart restore by writing the disk-offload .fsm file
// and its matching etcd .snap metadata token.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
	etcdsnap "go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/wal"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	snapshotTokenVersion = byte(0x01)
	snapshotTokenSize    = 17
	snapshotTokenMagic   = "EKVT"
	pebbleSnapshotMagic  = "EKVPBBL1"
	fsmFooterSize        = 4
	copyBufferSize       = 1 << 20
	dirPerm              = 0o755
	filePerm             = 0o600
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type config struct {
	inputPath  string
	dataDir    string
	snapDir    string
	fsmSnapDir string
	walDir     string
	index      uint64
	term       uint64
	voters     []uint64
	learners   []uint64
	force      bool
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "elastickv-snap-token: %v\n", err)
		os.Exit(1)
	}
}

func run(argv []string, out io.Writer) error {
	cfg, err := parseFlags(argv)
	if err != nil {
		return err
	}
	result, err := writeRestorePair(cfg)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "wrote %s, %s, and %s (crc32c=%08x)\n",
		result.fsmPath,
		result.snapPath,
		result.walDir,
		result.crc32c,
	)
	return errors.WithStack(err)
}

type writeResult struct {
	fsmPath  string
	snapPath string
	walDir   string
	crc32c   uint32
}

func parseFlags(argv []string) (config, error) {
	fs := flag.NewFlagSet("elastickv-snap-token", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var (
		cfg         config
		indexRaw    string
		termRaw     string
		votersRaw   string
		learnersRaw string
	)
	fs.StringVar(&cfg.inputPath, "input", "", "Input EKVPBBL1 .fsm payload produced by elastickv-snapshot-encode")
	fs.StringVar(&cfg.dataDir, "data-dir", "", "Raft data directory containing snap/ and fsm-snap/")
	fs.StringVar(&cfg.snapDir, "snap-dir", "", "Directory for etcd .snap metadata; overrides --data-dir/snap")
	fs.StringVar(&cfg.fsmSnapDir, "fsm-snap-dir", "", "Directory for footer-sealed .fsm files; overrides --data-dir/fsm-snap")
	fs.StringVar(&cfg.walDir, "wal-dir", "", "Directory for etcd WAL metadata; overrides --data-dir/wal")
	fs.StringVar(&indexRaw, "index", "", "Snapshot applied index to write into .snap metadata and fsm-snap filename")
	fs.StringVar(&termRaw, "term", "", "Raft term to write into .snap metadata filename")
	fs.StringVar(&votersRaw, "voters", "", "Comma-separated raft IDs for ConfState voters; use node:<uint64> for an explicit node ID")
	fs.StringVar(&learnersRaw, "learners", "", "Comma-separated raft IDs for ConfState learners; use node:<uint64> for an explicit node ID")
	fs.BoolVar(&cfg.force, "force", false, "Overwrite an existing target .fsm/.snap pair")
	if err := fs.Parse(argv); err != nil {
		return config{}, errors.WithStack(err)
	}
	if cfg.inputPath == "" {
		return config{}, errors.New("--input is required")
	}
	index, err := parseRequiredUint64("--index", indexRaw)
	if err != nil {
		return config{}, err
	}
	term, err := parseRequiredUint64("--term", termRaw)
	if err != nil {
		return config{}, err
	}
	cfg.index = index
	cfg.term = term
	if err := resolveOutputDirs(&cfg); err != nil {
		return config{}, err
	}
	cfg.voters, err = parseNodeIDs("--voters", votersRaw, true)
	if err != nil {
		return config{}, err
	}
	cfg.learners, err = parseNodeIDs("--learners", learnersRaw, false)
	if err != nil {
		return config{}, err
	}
	if err := validateConfStateDisjoint(cfg.voters, cfg.learners); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func parseRequiredUint64(name string, raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.Errorf("%s is required", name)
	}
	v, err := strconv.ParseUint(raw, 0, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parse %s", name)
	}
	if v == 0 {
		return 0, errors.Errorf("%s must be > 0", name)
	}
	return v, nil
}

func resolveOutputDirs(cfg *config) error {
	if cfg.dataDir != "" {
		if cfg.snapDir == "" {
			cfg.snapDir = filepath.Join(cfg.dataDir, "snap")
		}
		if cfg.fsmSnapDir == "" {
			cfg.fsmSnapDir = filepath.Join(cfg.dataDir, "fsm-snap")
		}
		if cfg.walDir == "" {
			cfg.walDir = filepath.Join(cfg.dataDir, "wal")
		}
	}
	if cfg.snapDir == "" || cfg.fsmSnapDir == "" || cfg.walDir == "" {
		return errors.New("pass --data-dir or --snap-dir, --fsm-snap-dir, and --wal-dir")
	}
	return nil
}

func parseNodeIDs(name string, raw string, required bool) ([]uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if required {
			return nil, errors.Errorf("%s is required", name)
		}
		return nil, nil
	}
	seen := make(map[uint64]struct{})
	var out []uint64
	for _, part := range strings.Split(raw, ",") {
		id, err := parseOneNodeID(strings.TrimSpace(part))
		if err != nil {
			return nil, errors.Wrapf(err, "parse %s", name)
		}
		if _, ok := seen[id]; ok {
			return nil, errors.Errorf("%s contains duplicate node id %d", name, id)
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

func validateConfStateDisjoint(voters []uint64, learners []uint64) error {
	if len(voters) == 0 || len(learners) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(voters))
	for _, id := range voters {
		seen[id] = struct{}{}
	}
	for _, id := range learners {
		if _, ok := seen[id]; ok {
			return errors.Errorf("node id %d cannot be listed as both voter and learner", id)
		}
	}
	return nil
}

func parseOneNodeID(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("empty node id")
	}
	if explicit, ok := strings.CutPrefix(raw, "node:"); ok {
		explicit = strings.TrimSpace(explicit)
		id, err := strconv.ParseUint(explicit, 0, 64)
		if err != nil {
			return 0, errors.Wrap(err, "parse explicit node id")
		}
		if id == 0 {
			return 0, errors.New("node id must be > 0")
		}
		return id, nil
	}
	id := etcdraftengine.DeriveNodeID(raw)
	if id == 0 {
		return 0, errors.New("derived node id is zero")
	}
	return id, nil
}

type restorePaths struct {
	fsm  string
	snap string
	wal  string
}

type stagedRestoreArtifacts struct {
	fsm     string
	snapDir string
	snap    string
	walDir  string
	crc     uint32
}

func writeRestorePair(cfg config) (writeResult, error) {
	if err := validateUnsealedEKVPBBL1(cfg.inputPath); err != nil {
		return writeResult{}, err
	}
	if err := prepareOutputParents(cfg); err != nil {
		return writeResult{}, err
	}

	paths := restorePathsForConfig(cfg)
	staged, cleanup, err := stageRestoreArtifacts(cfg, paths)
	if err != nil {
		cleanup()
		return writeResult{}, err
	}
	defer cleanup()

	if err := publishRestoreArtifacts(cfg, paths, staged); err != nil {
		return writeResult{}, err
	}
	return writeResult{fsmPath: paths.fsm, snapPath: paths.snap, walDir: paths.wal, crc32c: staged.crc}, nil
}

func prepareOutputParents(cfg config) error {
	if err := os.MkdirAll(cfg.fsmSnapDir, dirPerm); err != nil {
		return errors.Wrap(err, "create fsm-snap dir")
	}
	if err := os.MkdirAll(cfg.snapDir, dirPerm); err != nil {
		return errors.Wrap(err, "create snap dir")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.walDir), dirPerm); err != nil {
		return errors.Wrap(err, "create WAL parent dir")
	}
	return nil
}

func restorePathsForConfig(cfg config) restorePaths {
	return restorePaths{
		fsm:  filepath.Join(cfg.fsmSnapDir, fmt.Sprintf("%016x.fsm", cfg.index)),
		snap: filepath.Join(cfg.snapDir, fmt.Sprintf("%016x-%016x.snap", cfg.term, cfg.index)),
		wal:  cfg.walDir,
	}
}

func stageRestoreArtifacts(cfg config, paths restorePaths) (stagedRestoreArtifacts, func(), error) {
	var staged stagedRestoreArtifacts
	cleanup := func() {
		_ = os.Remove(staged.fsm)
		_ = os.RemoveAll(staged.snapDir)
		_ = os.RemoveAll(staged.walDir)
	}
	var err error
	stagedFSM, crc, err := writeFooterSealedFSMToTemp(cfg.inputPath, cfg.fsmSnapDir)
	if err != nil {
		return staged, cleanup, err
	}
	staged.fsm = stagedFSM
	staged.crc = crc

	stagedSnapDir, err := os.MkdirTemp(cfg.snapDir, ".snap-token-*")
	if err != nil {
		return staged, cleanup, errors.Wrap(err, "create staged snap dir")
	}
	staged.snapDir = stagedSnapDir

	stagedWALDir, err := reserveTempPath(filepath.Dir(cfg.walDir), ".wal-token-*")
	if err != nil {
		return staged, cleanup, errors.Wrap(err, "reserve staged WAL dir")
	}
	staged.walDir = stagedWALDir

	if err := writeSnapshotState(cfg, crc, stagedSnapDir, stagedWALDir); err != nil {
		return staged, cleanup, err
	}

	staged.snap = filepath.Join(stagedSnapDir, filepath.Base(paths.snap))
	if _, err := os.Lstat(staged.snap); err != nil {
		return staged, cleanup, errors.Wrap(err, "stat staged .snap token")
	}
	return staged, cleanup, nil
}

func publishRestoreArtifacts(cfg config, paths restorePaths, staged stagedRestoreArtifacts) error {
	publisher := newPublishTxn()
	if err := publisher.publishFile(staged.fsm, paths.fsm, cfg.force); err != nil {
		publisher.rollback()
		return err
	}
	if err := publisher.publishFile(staged.snap, paths.snap, cfg.force); err != nil {
		publisher.rollback()
		return err
	}
	if err := publisher.publishDir(staged.walDir, paths.wal, cfg.force); err != nil {
		publisher.rollback()
		return err
	}
	if err := publisher.commit(); err != nil {
		return err
	}
	return nil
}

func validateUnsealedEKVPBBL1(path string) error {
	in, err := os.Open(path) //nolint:gosec // operator-supplied path.
	if err != nil {
		return errors.Wrap(err, "open input .fsm")
	}
	defer func() { _ = in.Close() }()

	size, err := validateInputMagic(in)
	if err != nil {
		return err
	}
	return rejectFooterSealedInput(in, size)
}

func validateInputMagic(in *os.File) (int64, error) {
	info, err := in.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "stat input .fsm")
	}
	if info.Size() < int64(len(pebbleSnapshotMagic)) {
		return 0, errors.Errorf("input is not an %s payload: file too small", pebbleSnapshotMagic)
	}
	magic := make([]byte, len(pebbleSnapshotMagic))
	if _, err := io.ReadFull(in, magic); err != nil {
		return 0, errors.Wrap(err, "read input magic")
	}
	if string(magic) != pebbleSnapshotMagic {
		return 0, errors.Errorf("input is not an %s payload", pebbleSnapshotMagic)
	}
	return info.Size(), nil
}

func rejectFooterSealedInput(in *os.File, size int64) error {
	if size <= int64(len(pebbleSnapshotMagic)+fsmFooterSize) {
		return nil
	}
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "rewind input .fsm")
	}
	payloadSize := size - fsmFooterSize
	h := crc32.New(crc32cTable)
	if _, err := io.CopyBuffer(h, io.LimitReader(in, payloadSize), make([]byte, copyBufferSize)); err != nil {
		return errors.Wrap(err, "checksum input .fsm")
	}
	var footer [fsmFooterSize]byte
	if _, err := io.ReadFull(in, footer[:]); err != nil {
		return errors.Wrap(err, "read input .fsm footer candidate")
	}
	if binary.BigEndian.Uint32(footer[:]) == h.Sum32() {
		return errors.New("input appears to be an already footer-sealed .fsm")
	}
	return nil
}

func reserveTempPath(dir string, pattern string) (string, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return "", errors.WithStack(err)
	}
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", errors.WithStack(err)
	}
	path := f.Name()
	closeErr := f.Close()
	removeErr := os.Remove(path)
	if err := errors.CombineErrors(closeErr, removeErr); err != nil {
		return "", errors.Wrap(err, "reserve temp path")
	}
	return path, nil
}

type publishTxn struct {
	paths []publishedPath
}

type publishedPath struct {
	final  string
	backup string
	isDir  bool
}

func newPublishTxn() *publishTxn {
	return &publishTxn{}
}

func (p *publishTxn) publishFile(src string, dst string, force bool) error {
	return p.publishPath(src, dst, force, false)
}

func (p *publishTxn) publishDir(src string, dst string, force bool) error {
	return p.publishPath(src, dst, force, true)
}

func (p *publishTxn) publishPath(src string, dst string, force bool, isDir bool) error {
	if err := os.MkdirAll(filepath.Dir(dst), dirPerm); err != nil {
		return errors.WithStack(err)
	}
	backup, err := moveExistingAside(dst, force)
	if err != nil {
		return err
	}
	if err := os.Rename(src, dst); err != nil {
		if backup != "" {
			_ = os.Rename(backup, dst)
		}
		return errors.Wrapf(err, "publish %s", dst)
	}
	p.paths = append(p.paths, publishedPath{final: dst, backup: backup, isDir: isDir})
	if err := syncDir(filepath.Dir(dst)); err != nil {
		return err
	}
	return nil
}

func moveExistingAside(path string, force bool) (string, error) {
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		if !force {
			return "", errors.Errorf("%s already exists; pass --force to overwrite", path)
		}
	case errors.Is(err, os.ErrNotExist):
		return "", nil
	default:
		return "", errors.Wrapf(err, "stat %s", path)
	}

	backup, err := reserveBackupPath(filepath.Dir(path), filepath.Base(path), info.IsDir())
	if err != nil {
		return "", err
	}
	if err := os.Rename(path, backup); err != nil {
		return "", errors.Wrapf(err, "move existing %s aside", path)
	}
	return backup, nil
}

func reserveBackupPath(dir string, base string, isDir bool) (string, error) {
	pattern := "." + base + ".bak-*"
	if isDir {
		path, err := os.MkdirTemp(dir, pattern)
		if err != nil {
			return "", errors.WithStack(err)
		}
		if err := os.Remove(path); err != nil {
			return "", errors.WithStack(err)
		}
		return path, nil
	}
	return reserveTempPath(dir, pattern)
}

func (p *publishTxn) rollback() {
	for i := len(p.paths) - 1; i >= 0; i-- {
		published := p.paths[i]
		_ = removePublishedPath(published.final, published.isDir)
		if published.backup != "" {
			_ = os.Rename(published.backup, published.final)
		}
		_ = syncDir(filepath.Dir(published.final))
	}
	p.paths = nil
}

func (p *publishTxn) commit() error {
	var err error
	for _, published := range p.paths {
		if published.backup == "" {
			continue
		}
		err = errors.CombineErrors(err, errors.WithStack(os.RemoveAll(published.backup)))
	}
	p.paths = nil
	if err != nil {
		return errors.Wrap(err, "remove replaced restore artifacts")
	}
	return nil
}

func removePublishedPath(path string, isDir bool) error {
	if isDir {
		return errors.WithStack(os.RemoveAll(path))
	}
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return errors.WithStack(err)
}

func writeFooterSealedFSMToTemp(inputPath string, outputDir string) (string, uint32, error) {
	in, err := os.Open(inputPath) //nolint:gosec // operator-supplied path.
	if err != nil {
		return "", 0, errors.Wrap(err, "open input .fsm")
	}
	defer func() { _ = in.Close() }()

	tmp, err := os.CreateTemp(outputDir, "*.fsm.tmp")
	if err != nil {
		return "", 0, errors.Wrap(err, "create temp .fsm")
	}
	tmpPath := tmp.Name()
	closed := false
	keep := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(filePerm); err != nil {
		return "", 0, errors.Wrap(err, "chmod temp .fsm")
	}
	crc, err := copyPayloadWithFooter(in, tmp)
	if err != nil {
		return "", 0, err
	}
	closed = true
	if err := tmp.Close(); err != nil {
		return "", 0, errors.Wrap(err, "close temp .fsm")
	}
	if err := syncDir(outputDir); err != nil {
		return "", 0, err
	}
	keep = true
	return tmpPath, crc, nil
}

func copyPayloadWithFooter(in io.Reader, out *os.File) (uint32, error) {
	buf := bufio.NewWriterSize(out, copyBufferSize)
	crcWriter := newCRC32CWriter(buf)
	if _, err := io.CopyBuffer(crcWriter, in, make([]byte, copyBufferSize)); err != nil {
		return 0, errors.Wrap(err, "copy .fsm payload")
	}
	crc := crcWriter.Sum32()
	if err := binary.Write(buf, binary.BigEndian, crc); err != nil {
		return 0, errors.Wrap(err, "write .fsm footer")
	}
	if err := buf.Flush(); err != nil {
		return 0, errors.Wrap(err, "flush .fsm")
	}
	if err := out.Sync(); err != nil {
		return 0, errors.Wrap(err, "sync .fsm")
	}
	return crc, nil
}

type crc32CWriter struct {
	w io.Writer
	h hash.Hash32
}

func newCRC32CWriter(w io.Writer) *crc32CWriter {
	return &crc32CWriter{w: w, h: crc32.New(crc32cTable)}
}

func (c *crc32CWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.h.Write(p[:n])
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (c *crc32CWriter) Sum32() uint32 {
	return c.h.Sum32()
}

func writeSnapshotState(cfg config, crc uint32, snapDir string, walDir string) error {
	logger := zap.NewNop()
	w, err := wal.Create(logger, walDir, nil)
	if err != nil {
		return errors.Wrap(err, "create WAL")
	}
	persist := etcdstorage.NewStorage(logger, w, etcdsnap.New(logger, snapDir))
	closed := false
	defer func() {
		if !closed {
			_ = persist.Close()
		}
	}()
	if err := persist.SaveSnap(snapshotForConfig(cfg, crc)); err != nil {
		return errors.Wrap(err, "write .snap token and WAL snapshot")
	}
	hardState := raftpb.HardState{Term: cfg.term, Commit: cfg.index}
	if err := persist.Save(hardState, nil); err != nil {
		return errors.Wrap(err, "write WAL hard state")
	}
	closed = true
	if err := persist.Close(); err != nil {
		return errors.Wrap(err, "close WAL")
	}
	if err := syncDir(snapDir); err != nil {
		return err
	}
	if err := syncDir(walDir); err != nil {
		return err
	}
	return nil
}

func snapshotForConfig(cfg config, crc uint32) raftpb.Snapshot {
	return raftpb.Snapshot{
		Data: encodeSnapshotToken(cfg.index, crc),
		Metadata: raftpb.SnapshotMetadata{
			Index: cfg.index,
			Term:  cfg.term,
			ConfState: raftpb.ConfState{
				Voters:   cfg.voters,
				Learners: cfg.learners,
			},
		},
	}
}

func encodeSnapshotToken(index uint64, crc uint32) []byte {
	token := make([]byte, snapshotTokenSize)
	copy(token[:4], snapshotTokenMagic)
	token[4] = snapshotTokenVersion
	binary.BigEndian.PutUint64(token[5:13], index)
	binary.BigEndian.PutUint32(token[13:17], crc)
	return token
}

func syncDir(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // operator-supplied path.
	if err != nil {
		return errors.Wrap(err, "open directory for fsync")
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		return errors.Wrap(err, "fsync directory")
	}
	return nil
}

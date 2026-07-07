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
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	snapshotTokenVersion = byte(0x01)
	snapshotTokenSize    = 17
	snapshotTokenMagic   = "EKVT"
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
	_, err = fmt.Fprintf(out, "wrote %s and %s (crc32c=%08x)\n", result.fsmPath, result.snapPath, result.crc32c)
	return errors.WithStack(err)
}

type writeResult struct {
	fsmPath  string
	snapPath string
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
	return cfg, nil
}

func parseRequiredUint64(name string, raw string) (uint64, error) {
	if strings.TrimSpace(raw) == "" {
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
	}
	if cfg.snapDir == "" || cfg.fsmSnapDir == "" {
		return errors.New("pass --data-dir or both --snap-dir and --fsm-snap-dir")
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

func parseOneNodeID(raw string) (uint64, error) {
	if raw == "" {
		return 0, errors.New("empty node id")
	}
	if explicit, ok := strings.CutPrefix(raw, "node:"); ok {
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

func writeRestorePair(cfg config) (writeResult, error) {
	if err := os.MkdirAll(cfg.fsmSnapDir, dirPerm); err != nil {
		return writeResult{}, errors.Wrap(err, "create fsm-snap dir")
	}
	if err := os.MkdirAll(cfg.snapDir, dirPerm); err != nil {
		return writeResult{}, errors.Wrap(err, "create snap dir")
	}
	fsmPath := filepath.Join(cfg.fsmSnapDir, fmt.Sprintf("%016x.fsm", cfg.index))
	snapPath := filepath.Join(cfg.snapDir, fmt.Sprintf("%016x-%016x.snap", cfg.term, cfg.index))
	if err := ensureCanWrite(fsmPath, cfg.force); err != nil {
		return writeResult{}, err
	}
	if err := ensureCanWrite(snapPath, cfg.force); err != nil {
		return writeResult{}, err
	}
	crc, err := writeFooterSealedFSM(cfg.inputPath, fsmPath)
	if err != nil {
		return writeResult{}, err
	}
	if err := saveSnapshotToken(cfg, crc); err != nil {
		return writeResult{}, err
	}
	return writeResult{fsmPath: fsmPath, snapPath: snapPath, crc32c: crc}, nil
}

func ensureCanWrite(path string, force bool) error {
	if force {
		return nil
	}
	if _, err := os.Lstat(path); err == nil {
		return errors.Errorf("%s already exists; pass --force to overwrite", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.Wrapf(err, "stat %s", path)
	}
	return nil
}

func writeFooterSealedFSM(inputPath string, outputPath string) (uint32, error) {
	in, err := os.Open(inputPath) //nolint:gosec // operator-supplied path.
	if err != nil {
		return 0, errors.Wrap(err, "open input .fsm")
	}
	defer func() { _ = in.Close() }()

	tmp, err := os.CreateTemp(filepath.Dir(outputPath), "*.fsm.tmp")
	if err != nil {
		return 0, errors.Wrap(err, "create temp .fsm")
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(filePerm); err != nil {
		return 0, errors.Wrap(err, "chmod temp .fsm")
	}
	crc, err := copyPayloadWithFooter(in, tmp)
	if err != nil {
		return 0, err
	}
	closed = true
	if err := tmp.Close(); err != nil {
		return 0, errors.Wrap(err, "close temp .fsm")
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		return 0, errors.Wrap(err, "publish .fsm")
	}
	if err := syncDir(filepath.Dir(outputPath)); err != nil {
		return 0, err
	}
	return crc, nil
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

func saveSnapshotToken(cfg config, crc uint32) error {
	snapshot := raftpb.Snapshot{
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
	if err := etcdsnap.New(zap.NewNop(), cfg.snapDir).SaveSnap(snapshot); err != nil {
		return errors.Wrap(err, "write .snap token")
	}
	if err := syncDir(cfg.snapDir); err != nil {
		return err
	}
	return nil
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

package backup

import (
	"bytes"
	"crypto/sha256"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// ErrSelfTestLowerLastCommitTS is returned by EncodeSnapshot when the
// effective LastCommitTS in EncodeOptions is below the manifest's value.
// The HLC ceiling invariant (CLAUDE.md "Timestamp Oracle") forbids
// lowering the ceiling on restore: a lower T would let a post-restart
// leader issue a read ts ≤ a restored row's commit ts.
//
// Surfaced at the EncodeSnapshot layer so the CLI's main exits with code
// 2 (data-correctness failure, per parent §"Exit codes"). Caller must
// check errors.Is on this sentinel to map to the right exit code.
var ErrSelfTestLowerLastCommitTS = errors.New("backup: --last-commit-ts T < manifest.last_commit_ts (HLC ceiling regression)")

// The encoder dispatch order (redis → dynamodb → s3 → sqs) is encoded
// inside adapterRunners() and is intentionally distinct from decode.go's
// finalize order (dynamodb → s3 → redis → sqs). The final .fsm byte
// sequence is determined by encoded-key sort (snapshotBuilder.WriteTo),
// not by adapter fan-out order, so either ordering is correct as long
// as it is fixed. The encoder follows the parent design doc's
// enumeration order so ENCODE_INFO.json adapters_enabled is bytewise
// reproducible across runs that pass --adapter in different sequences
// (claude review v7 #896).

// EncodeOptions configures EncodeSnapshot. Mirrors the decoder's
// DecodeOptions in shape: required InputRoot, AdapterSet, then per-adapter
// option flags read back from the input MANIFEST.json by the CLI.
type EncodeOptions struct {
	// InputRoot is the directory tree root produced by the decoder.
	// Must contain MANIFEST.json; per-adapter encoders read their
	// subtrees (redis/, dynamodb/, s3/, sqs/) directly off this root.
	InputRoot string
	// Adapters selects which adapter encoders to invoke; disabled
	// adapters are skipped without error. Mirrors DecodeOptions.Adapters.
	Adapters AdapterSet
	// LastCommitTS is the EFFECTIVE T used for both the EKVPBBL1
	// header and every key's invTS = ^T. Callers pass manifest.last_commit_ts
	// by default and the --last-commit-ts override otherwise.
	LastCommitTS uint64
	// SelfTest enables the round-trip self-test. When true,
	// EncodeSnapshot writes the FSM to an on-disk temp file under
	// SelfTestDecodeOptions.OutRoot (encode-self-test-fsm-*), streams
	// it through DecodeSnapshot, and copies to the caller's io.Writer
	// ONLY if the decode survives — i.e. the bytes the encoder
	// produced are loadable. When false, the FSM streams straight to
	// the writer with no extra buffering. Memory cost in self-test
	// mode is O(1) on top of the sort working set (the temp file
	// holds the snapshot; only a small streaming buffer is in RAM).
	SelfTest bool
	// SelfTestDecodeOptions are threaded into the scratch DecodeSnapshot
	// call. The CLI reads MANIFEST.json's Exclusions + DynamoDBLayout
	// and populates this so the self-test's scratch tree matches what
	// the original decoder would have produced.
	SelfTestDecodeOptions DecodeOptions

	// corruptBufferForTest is an unexported test-only hook that fires
	// against the on-disk self-test buffer AFTER snapshotBuilder.WriteTo
	// returns but BEFORE the self-test DecodeSnapshot call (when
	// SelfTest=true). Same-package tests use it to inject corruption
	// reachable by the self-test but never reaching the io.Writer
	// passed to EncodeSnapshot (the write-then-rename invariant: a
	// self-test failure must not publish corrupt bytes — codex P2 v6
	// #896). External callers cannot set it (lowercase identifier).
	//
	// The hook receives the *os.File handle (positioned at offset 0)
	// of the disk-backed self-test buffer; tests typically WriteAt
	// a byte flip and rely on Seek-back-to-0 before returning so
	// the encoder's subsequent Read sees the corrupted bytes.
	corruptBufferForTest func(*os.File)
}

// EncodeResult is the public return value from EncodeSnapshot. Mirrors
// the decoder's DecodeResult shape.
type EncodeResult struct {
	// Header is what ReadSnapshotWithHeader returned when the encoder
	// decoded its own output for the self-test. Header.LastCommitTS
	// equals the effective T (uniform-stamping rule per parent doc
	// §"MVCC re-encoding").
	Header SnapshotHeader
	// BytesWritten is the number of bytes written to the caller's
	// io.Writer (the SHA256-anchored payload).
	BytesWritten int64
	// SHA256 of the produced .fsm bytes (raw 32-byte digest; the CLI
	// hex-encodes it via encoding/hex when writing ENCODE_INFO.json).
	SHA256 [32]byte
	// SelfTestRan is true iff opts.SelfTest was true AND the encoder
	// ran (i.e. no earlier per-adapter error short-circuited).
	SelfTestRan bool
	// SelfTestMatched is meaningful only when SelfTestRan; reports
	// whether the re-decode produced no diff against InputRoot.
	SelfTestMatched bool
	// SelfTestMismatchTxt is non-nil when SelfTestRan && !SelfTestMatched.
	// The CLI writes it as <output>.mismatch.txt at exit 2.
	SelfTestMismatchTxt []byte
	// AdaptersEnabled is the canonical fan-out order of adapters that
	// were actually invoked; ENCODE_INFO.json embeds this verbatim.
	AdaptersEnabled []string
}

// EncodeSnapshot reads the directory tree at opts.InputRoot, invokes the
// enabled per-adapter encoders in canonical fan-out order, optionally
// runs the round-trip self-test, and writes the .fsm bytes to out.
// The .fsm bytes are NOT returned; they go to out.
//
// When opts.SelfTest=false the FSM streams straight to out with a
// sha256 tee and no extra buffering. When opts.SelfTest=true the FSM
// is written to an on-disk temp file (encode-self-test-fsm-*) under
// opts.SelfTestDecodeOptions.OutRoot, the file is streamed through
// DecodeSnapshot, and bytes are copied to out ONLY if the decode
// survives. Memory cost in self-test mode is O(1) on top of the
// sort working set (gemini high #904 — the earlier *bytes.Buffer
// version would OOM on multi-GB snapshots).
//
// Self-test failure returns (result, nil) with result.SelfTestMatched
// == false and result.SelfTestMismatchTxt populated. Callers MUST
// check result.SelfTestMatched before treating a nil error as success.
// The CLI relies on this contract to write mismatch.txt + exit 2;
// library callers should follow the same pattern.
//
// Returns ErrSelfTestLowerLastCommitTS when opts.LastCommitTS is below
// the manifest's value — caller is responsible for reading the manifest
// and computing the effective T (this layer just validates the floor).
// The CLI maps that error to exit code 2.
func EncodeSnapshot(opts EncodeOptions, out io.Writer) (EncodeResult, error) {
	if opts.InputRoot == "" {
		return EncodeResult{}, errors.New("backup: EncodeOptions.InputRoot is required")
	}
	if out == nil {
		return EncodeResult{}, errors.New("backup: EncodeSnapshot out writer is nil")
	}

	b := newSnapshotBuilder(opts.LastCommitTS)
	enabled, err := runAdapterEncoders(b, opts)
	if err != nil {
		return EncodeResult{}, err
	}

	if !opts.SelfTest {
		return encodeStream(b, opts, enabled, out)
	}
	return encodeBuffered(b, opts, enabled, out)
}

// encodeStream is the no-self-test path: SHA256 + writer tee with no
// extra buffering. FSM bytes go straight to out.
func encodeStream(b *snapshotBuilder, opts EncodeOptions, enabled []string, out io.Writer) (EncodeResult, error) {
	hashWriter := newSHA256Writer(out)
	bytesWritten, err := b.WriteTo(hashWriter)
	if err != nil {
		return EncodeResult{}, errors.WithStack(err)
	}
	return EncodeResult{
		Header:          SnapshotHeader{LastCommitTS: opts.LastCommitTS},
		BytesWritten:    bytesWritten,
		SHA256:          hashWriter.Sum(),
		SelfTestRan:     false,
		AdaptersEnabled: enabled,
	}, nil
}

// encodeBuffered is the SelfTest=true path: write the FSM to a temp
// file on disk (NOT in memory — gemini high #904, OOM risk on large
// snapshots), self-test by streaming the temp file through DecodeSnapshot,
// copy to out only on match. The temp file is os.Remove'd via defer on
// every exit path.
//
// Memory cost: O(1) — only the sha256 running state + read buffer for
// the final io.Copy. Replaces the prior in-memory bytes.Buffer.
//
// Corruption hook (if set) fires against the temp file between WriteTo
// and self-test so the self-test sees the corruption but out never does
// (codex P2 v6 #896, codex P2 v7 #896).
func encodeBuffered(b *snapshotBuilder, opts EncodeOptions, enabled []string, out io.Writer) (EncodeResult, error) {
	tempFile, err := os.CreateTemp(opts.SelfTestDecodeOptions.OutRoot, "encode-self-test-fsm-")
	if err != nil {
		return EncodeResult{}, errors.Wrap(err, "create self-test temp file")
	}
	tempPath := tempFile.Name()
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
	}()

	hasher := sha256.New()
	tee := &teeWriter{w: tempFile, h: hasher}
	bytesWritten, err := b.WriteTo(tee)
	if err != nil {
		return EncodeResult{}, errors.WithStack(err)
	}
	if err := tempFile.Sync(); err != nil {
		return EncodeResult{}, errors.Wrap(err, "fsync self-test temp file")
	}
	if opts.corruptBufferForTest != nil {
		opts.corruptBufferForTest(tempFile)
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return EncodeResult{}, errors.Wrap(err, "seek self-test temp file")
	}

	header, mismatchTxt, matched, stErr := runSelfTest(tempFile, opts)
	var sha [32]byte
	copy(sha[:], hasher.Sum(nil))
	result := EncodeResult{
		Header:              header,
		BytesWritten:        bytesWritten,
		SHA256:              sha,
		SelfTestRan:         true,
		SelfTestMatched:     matched,
		SelfTestMismatchTxt: mismatchTxt,
		AdaptersEnabled:     enabled,
	}
	if stErr != nil {
		return result, stErr
	}
	if !matched {
		return result, nil
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return result, errors.Wrap(err, "rewind self-test temp file for copy")
	}
	if _, err := io.Copy(out, tempFile); err != nil {
		return result, errors.Wrap(err, "copy buffered fsm to out")
	}
	return result, nil
}

// teeWriter tees writes into a hash.Hash + an underlying writer in a
// single pass, avoiding a second read for the SHA-256 anchor that
// ENCODE_INFO.json records.
type teeWriter struct {
	w io.Writer
	h hash.Hash
}

func (t *teeWriter) Write(p []byte) (int, error) {
	if _, err := t.h.Write(p); err != nil {
		return 0, errors.WithStack(err)
	}
	n, err := t.w.Write(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

// adapterRunner pairs an enabled-check with an Encode call, keeping
// runAdapterEncoders's per-iteration body to two branches (cyclop).
type adapterRunner struct {
	name    string
	enabled func(AdapterSet) bool
	encode  func(*snapshotBuilder, string) error
}

func adapterRunners() []adapterRunner {
	return []adapterRunner{
		{"redis", func(s AdapterSet) bool { return s.Redis }, func(b *snapshotBuilder, root string) error {
			return errors.Wrap(NewRedisEncoder(root, 0).Encode(b), "redis encoder")
		}},
		{"dynamodb", func(s AdapterSet) bool { return s.DynamoDB }, func(b *snapshotBuilder, root string) error {
			return errors.Wrap(NewDynamoDBEncoder(root).Encode(b), "dynamodb encoder")
		}},
		{"s3", func(s AdapterSet) bool { return s.S3 }, func(b *snapshotBuilder, root string) error {
			return errors.Wrap(NewS3RecordEncoder(root).Encode(b), "s3 encoder")
		}},
		{"sqs", func(s AdapterSet) bool { return s.SQS }, func(b *snapshotBuilder, root string) error {
			return errors.Wrap(NewSQSRecordEncoder(root).Encode(b), "sqs encoder")
		}},
	}
}

// runAdapterEncoders invokes each enabled adapter encoder in
// canonicalAdapterFanOutOrder, returning the list of adapter names
// actually invoked (for ENCODE_INFO.json adapters_enabled).
func runAdapterEncoders(b *snapshotBuilder, opts EncodeOptions) ([]string, error) {
	var enabled []string
	for _, r := range adapterRunners() {
		if !r.enabled(opts.Adapters) {
			continue
		}
		if err := r.encode(b, opts.InputRoot); err != nil {
			return nil, err
		}
		enabled = append(enabled, r.name)
	}
	return enabled, nil
}

// runSelfTest streams fsmFile through DecodeSnapshot into a unique
// scratch subdir, structurally diffs against opts.InputRoot, and returns
// (header, mismatchTxt, matched, err). matched=false with err=nil
// indicates a structural diff; matched=true with err=nil indicates
// success. err is non-nil only on infrastructure failure (mkdir, decoder
// error, walk error).
//
// fsmFile is read from its current position (caller must Seek(0) before
// calling). The scratch subdir is removed via defer regardless of
// outcome. The caller cleans up <output>.mismatch.txt at the start of
// each run.
func runSelfTest(fsmFile io.Reader, opts EncodeOptions) (SnapshotHeader, []byte, bool, error) {
	scratchBase := opts.SelfTestDecodeOptions.OutRoot
	scratchDir, err := os.MkdirTemp(scratchBase, "encode-self-test-")
	if err != nil {
		return SnapshotHeader{}, nil, false, errors.Wrap(err, "mkdir scratch")
	}
	defer func() {
		_ = os.RemoveAll(scratchDir)
	}()

	decOpts := opts.SelfTestDecodeOptions
	decOpts.OutRoot = scratchDir

	result, derr := DecodeSnapshot(fsmFile, decOpts)
	if derr != nil {
		// Decoder errored on our own output — that IS a self-test
		// failure (the .fsm we produced isn't loadable). Surface as
		// a mismatch with the decoder error embedded in the txt.
		mismatchTxt := []byte("self-test failed: DecodeSnapshot rejected the produced .fsm: " + derr.Error())
		return SnapshotHeader{}, mismatchTxt, false, nil
	}

	if result.Header.LastCommitTS != opts.LastCommitTS {
		mismatchTxt := []byte(formatHeaderMismatch(opts.LastCommitTS, result.Header.LastCommitTS))
		return result.Header, mismatchTxt, false, nil
	}

	diff, derr := diffAdapterTrees(opts.InputRoot, scratchDir, opts.Adapters)
	if derr != nil {
		return result.Header, nil, false, errors.Wrap(derr, "diff scratch tree")
	}
	if len(diff) > 0 {
		return result.Header, []byte(strings.Join(diff, "\n") + "\n"), false, nil
	}
	return result.Header, nil, true, nil
}

// diffAdapterTrees returns a list of paths (relative to input/scratch
// root) where the two trees differ, restricted to the adapter subtrees
// enabled in adapters. MANIFEST.json itself is NOT compared — the scratch
// doesn't have one (DecodeSnapshot library doesn't emit it; the CLI
// wrapper does, codex P2 v1 #896 — header check above is the
// last_commit_ts substitute). Bounded to selfTestMaxMismatchPaths.
func diffAdapterTrees(inputRoot, scratchRoot string, adapters AdapterSet) ([]string, error) {
	subdirs := enabledAdapterSubdirs(adapters)
	var diffs []string
	for _, sub := range subdirs {
		paths, err := diffOneSubdir(filepath.Join(inputRoot, sub), filepath.Join(scratchRoot, sub), sub)
		if err != nil {
			return nil, err
		}
		diffs = append(diffs, paths...)
		if len(diffs) >= selfTestMaxMismatchPaths {
			diffs = diffs[:selfTestMaxMismatchPaths]
			diffs = append(diffs, "... (truncated; first "+strconv.Itoa(selfTestMaxMismatchPaths)+" paths shown)")
			return diffs, nil
		}
	}
	return diffs, nil
}

const selfTestMaxMismatchPaths = 64

// enabledAdapterSubdirs returns the top-level adapter subdir names for
// the enabled adapters, in canonical order for stable mismatch.txt output.
func enabledAdapterSubdirs(adapters AdapterSet) []string {
	var out []string
	for _, r := range adapterRunners() {
		if r.enabled(adapters) {
			out = append(out, r.name)
		}
	}
	return out
}

// diffOneSubdir walks aDir + bDir in parallel, returning paths (prefixed
// by relPrefix) that differ in presence, size, or bytes. Files are
// compared by streaming reads (NOT by loading whole bytes into memory)
// so a multi-GB S3 blob does not OOM the encoder (gemini high #904).
// Missing-on-one-side is a mismatch. The returned diffs are sorted
// alphabetically so mismatch.txt is deterministic across runs with
// identical inputs (claude v2 carry-over observation #904).
func diffOneSubdir(aDir, bDir, relPrefix string) ([]string, error) {
	aPaths, aErr := walkRegularFilePaths(aDir)
	if aErr != nil && !errors.Is(aErr, os.ErrNotExist) {
		return nil, errors.Wrapf(aErr, "walk input %s", aDir)
	}
	bPaths, bErr := walkRegularFilePaths(bDir)
	if bErr != nil && !errors.Is(bErr, os.ErrNotExist) {
		return nil, errors.Wrapf(bErr, "walk scratch %s", bDir)
	}

	var diffs []string
	for relPath, bFull := range bPaths {
		aFull, ok := aPaths[relPath]
		if !ok {
			diffs = append(diffs, relPrefix+"/"+relPath+" (missing in input)")
			continue
		}
		eq, derr := streamFilesEqual(aFull, bFull)
		if derr != nil {
			return nil, errors.Wrapf(derr, "compare %s vs %s", aFull, bFull)
		}
		if !eq {
			diffs = append(diffs, relPrefix+"/"+relPath+" (bytes differ)")
		}
		delete(aPaths, relPath)
	}
	for relPath := range aPaths {
		diffs = append(diffs, relPrefix+"/"+relPath+" (missing in scratch)")
	}
	sort.Strings(diffs)
	return diffs, nil
}

// walkRegularFilePaths returns a map of relative path → absolute path
// for every regular file under root. Replaces walkRegularFiles which
// eagerly read file bytes; this version only records paths so the diff
// can stream-compare per file (gemini high #904).
func walkRegularFilePaths(root string) (map[string]string, error) {
	out := map[string]string{}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !rootInfo.IsDir() {
		return nil, errors.Errorf("not a directory: %s", root)
	}
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return errors.WithStack(rerr)
		}
		out[filepath.ToSlash(rel)] = path
		return nil
	}); err != nil {
		return nil, errors.WithStack(err)
	}
	return out, nil
}

// streamCmpBufSize is the per-file read buffer for the streaming
// compare. 64 KiB matches Go's default bufio buffer and keeps the
// allocation small relative to the modal adapter file size.
const streamCmpBufSize = 64 * 1024

// streamFilesEqual reports whether the contents at aPath and bPath are
// byte-equal without loading either file fully into memory. A size
// mismatch short-circuits. Used by diffOneSubdir to bound the
// self-test's memory at O(streamCmpBufSize) per concurrent compare
// (gemini high #904).
func streamFilesEqual(aPath, bPath string) (bool, error) {
	aSize, err := fileSize(aPath)
	if err != nil {
		return false, err
	}
	bSize, err := fileSize(bPath)
	if err != nil {
		return false, err
	}
	if aSize != bSize {
		return false, nil
	}
	aFile, err := os.Open(aPath) //nolint:gosec // walking caller-provided dirs
	if err != nil {
		return false, errors.WithStack(err)
	}
	defer func() { _ = aFile.Close() }()
	bFile, err := os.Open(bPath) //nolint:gosec // walking caller-provided dirs
	if err != nil {
		return false, errors.WithStack(err)
	}
	defer func() { _ = bFile.Close() }()
	return streamReadersEqual(aFile, bFile)
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return info.Size(), nil
}

// streamReadersEqual compares two readers of equal length chunk-by-chunk
// and returns false on any difference, true on full match.
func streamReadersEqual(a, b io.Reader) (bool, error) {
	aBuf := make([]byte, streamCmpBufSize)
	bBuf := make([]byte, streamCmpBufSize)
	for {
		an, aErr := io.ReadFull(a, aBuf)
		bn, bErr := io.ReadFull(b, bBuf)
		if an != bn || !bytes.Equal(aBuf[:an], bBuf[:bn]) {
			return false, nil
		}
		if aErr == io.EOF || aErr == io.ErrUnexpectedEOF {
			return true, nil
		}
		if aErr != nil {
			return false, errors.WithStack(aErr)
		}
		if bErr != nil {
			return false, errors.WithStack(bErr)
		}
	}
}

func formatHeaderMismatch(want, got uint64) string {
	return "self-test failed: header.LastCommitTS mismatch (want " +
		strconv.FormatUint(want, 10) +
		", got " +
		strconv.FormatUint(got, 10) +
		")\n"
}

// sha256Writer wraps an io.Writer and tees every byte into a SHA-256
// hasher so the encoder gets a single-pass SHA256 of the produced .fsm
// without an extra buffer-pass. Used in the no-self-test streaming path.
type sha256Writer struct {
	w io.Writer
	h hash.Hash
}

func newSHA256Writer(w io.Writer) *sha256Writer {
	return &sha256Writer{w: w, h: sha256.New()}
}

func (s *sha256Writer) Write(p []byte) (int, error) {
	if _, err := s.h.Write(p); err != nil {
		// crypto/sha256 never errors on Write per stdlib contract.
		return 0, errors.WithStack(err)
	}
	n, err := s.w.Write(p)
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (s *sha256Writer) Sum() [32]byte {
	var out [32]byte
	copy(out[:], s.h.Sum(nil))
	return out
}

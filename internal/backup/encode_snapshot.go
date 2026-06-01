package backup

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"sort"
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
	// SelfTest enables the round-trip self-test. EncodeSnapshot buffers
	// the FSM in *bytes.Buffer, decodes from the buffer, and copies to
	// the caller's io.Writer only if the buffer survives DecodeSnapshot
	// (i.e. the bytes the encoder produced are loadable). When false,
	// the FSM streams straight to the writer with no extra buffering.
	SelfTest bool
	// SelfTestDecodeOptions are threaded into the scratch DecodeSnapshot
	// call. The CLI reads MANIFEST.json's Exclusions + DynamoDBLayout
	// and populates this so the self-test's scratch tree matches what
	// the original decoder would have produced.
	SelfTestDecodeOptions DecodeOptions

	// corruptBufferForTest is an unexported test-only hook that fires
	// against the internal *bytes.Buffer AFTER snapshotBuilder.WriteTo
	// returns but BEFORE the self-test DecodeSnapshot call (when
	// SelfTest=true). Same-package tests use it to inject corruption
	// reachable by the self-test but never reaching the io.Writer
	// passed to EncodeSnapshot (the write-then-rename invariant: a
	// self-test failure must not publish corrupt bytes — codex P2 v6
	// #896). External callers cannot set it (lowercase identifier).
	corruptBufferForTest func([]byte)
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
	// SHA256 of the produced .fsm (lowercase hex via SHA256Hex).
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
// enabled per-adapter encoders in canonicalAdapterFanOutOrder, optionally
// runs the round-trip self-test against the in-memory buffer, and writes
// the .fsm bytes to out. The .fsm bytes are NOT returned; they go to out.
//
// When opts.SelfTest=false the FSM streams straight to out with no extra
// buffering. When opts.SelfTest=true an internal *bytes.Buffer holds the
// FSM during the self-test; bytes are copied to out only after the
// self-test matches. Memory cost in self-test mode is one FSM-sized
// allocation on top of the existing sort working set.
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

// encodeBuffered is the SelfTest=true path: buffer, self-test against
// buffer, copy to out only on match. Corruption hook (if set) fires
// against the buffer between WriteTo and self-test so the self-test
// sees the corruption but out never does (codex P2 v6 #896).
func encodeBuffered(b *snapshotBuilder, opts EncodeOptions, enabled []string, out io.Writer) (EncodeResult, error) {
	var buf bytes.Buffer
	bytesWritten, err := b.WriteTo(&buf)
	if err != nil {
		return EncodeResult{}, errors.WithStack(err)
	}
	bufBytes := buf.Bytes()
	if opts.corruptBufferForTest != nil {
		opts.corruptBufferForTest(bufBytes)
	}

	header, mismatchTxt, matched, stErr := runSelfTest(bufBytes, opts)
	result := EncodeResult{
		Header:              header,
		BytesWritten:        bytesWritten,
		SHA256:              sha256.Sum256(bufBytes),
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
	if _, err := io.Copy(out, bytes.NewReader(bufBytes)); err != nil {
		return result, errors.Wrap(err, "copy buffered fsm to out")
	}
	return result, nil
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

// runSelfTest decodes fsmBytes into a unique scratch subdir, structurally
// diffs against opts.InputRoot, and returns (header, mismatchTxt, matched,
// err). matched=false with err=nil indicates a structural diff; matched=true
// with err=nil indicates success. err is non-nil only on infrastructure
// failure (mkdir, decoder error, walk error).
//
// The scratch subdir is removed via defer regardless of outcome. The
// caller cleans up <output>.mismatch.txt at the start of each run.
func runSelfTest(fsmBytes []byte, opts EncodeOptions) (SnapshotHeader, []byte, bool, error) {
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

	result, derr := DecodeSnapshot(bytes.NewReader(fsmBytes), decOpts)
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
			diffs = append(diffs, "... (truncated; first "+itoa(selfTestMaxMismatchPaths)+" paths shown)")
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
// by relPrefix) that differ in presence or bytes. Missing-on-one-side is
// a mismatch.
func diffOneSubdir(aDir, bDir, relPrefix string) ([]string, error) {
	aFiles, aErr := walkRegularFiles(aDir)
	if aErr != nil && !errors.Is(aErr, os.ErrNotExist) {
		return nil, errors.Wrapf(aErr, "walk input %s", aDir)
	}
	bFiles, bErr := walkRegularFiles(bDir)
	if bErr != nil && !errors.Is(bErr, os.ErrNotExist) {
		return nil, errors.Wrapf(bErr, "walk scratch %s", bDir)
	}

	var diffs []string
	aMap := map[string][]byte{}
	for path, body := range aFiles {
		aMap[path] = body
	}
	for path, bBody := range bFiles {
		aBody, ok := aMap[path]
		if !ok {
			diffs = append(diffs, relPrefix+"/"+path+" (missing in input)")
			continue
		}
		if !bytes.Equal(aBody, bBody) {
			diffs = append(diffs, relPrefix+"/"+path+" (bytes differ)")
		}
		delete(aMap, path)
	}
	// Anything remaining in aMap is present in input but not in scratch.
	remaining := make([]string, 0, len(aMap))
	for path := range aMap {
		remaining = append(remaining, relPrefix+"/"+path+" (missing in scratch)")
	}
	sort.Strings(remaining)
	diffs = append(diffs, remaining...)
	return diffs, nil
}

// walkRegularFiles returns a map of relative path -> file bytes for every
// regular file under root. Missing root is the empty map + os.ErrNotExist.
// Bounded by the per-adapter test fixtures the encoder runs against;
// production-scale dumps may want streaming compare, deferred until a
// real bottleneck shows up.
func walkRegularFiles(root string) (map[string][]byte, error) {
	out := map[string][]byte{}
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
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		body, rerr := os.ReadFile(path) //nolint:gosec // walking a caller-provided root, regular files only
		if rerr != nil {
			return errors.Wrap(rerr, path)
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return errors.WithStack(rerr)
		}
		out[filepath.ToSlash(rel)] = body
		return nil
	}); err != nil {
		return nil, errors.WithStack(err)
	}
	return out, nil
}

func formatHeaderMismatch(want, got uint64) string {
	return "self-test failed: header.LastCommitTS mismatch (want " + uitoa(want) + ", got " + uitoa(got) + ")\n"
}

// uitoaCap is the max decimal length of a uint64 (math.MaxUint64 has
// 20 digits). Constant so the cap is documented and lint-clean.
const uitoaCap = 20

func uitoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	buf := make([]byte, 0, uitoaCap)
	for v > 0 {
		buf = append(buf, byte('0'+v%10))
		v /= 10
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}

func itoa(v int) string {
	if v < 0 {
		return "-" + uitoa(uint64(-v))
	}
	return uitoa(uint64(v))
}

// sha256Writer wraps an io.Writer and tees every byte into a SHA-256
// hasher so the encoder gets a single-pass SHA256 of the produced .fsm
// without an extra buffer-pass. Used in the no-self-test streaming path.
type sha256Writer struct {
	w io.Writer
	h sha256w
}

type sha256w = hashSHA256

// hashSHA256 is an interface alias so we can satisfy the tiny hash.Hash
// surface (Write + Sum32) without importing hash explicitly.
type hashSHA256 interface {
	io.Writer
	Sum(b []byte) []byte
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

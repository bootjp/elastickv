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

// ErrSelfTestLowerLastCommitTS is returned when the operator-supplied
// T is below the manifest's last_commit_ts. The HLC ceiling invariant
// (CLAUDE.md "Timestamp Oracle") forbids lowering the ceiling on
// restore: a lower T would let a post-restart leader issue a read
// ts ≤ a restored row's commit ts.
//
// Enforced at two layers:
//   - CLI (`resolveLastCommitTS`) rejects --last-commit-ts T < manifest
//     before EncodeSnapshot is called (exit code 2).
//   - Library (`validateEncodeOptions`) rejects when the caller threads
//     `opts.ManifestLastCommitTS > 0` and `opts.LastCommitTS` is below
//     it — defense-in-depth for in-process callers (Phase 1 live
//     extractor, integration tests) that bypass the CLI.
//
// Callers can errors.Is on this sentinel to map to the right exit code
// (claude v3 doc bug #904 + claude v7 doc bug #904 + codex P2 v2 #904).
var ErrSelfTestLowerLastCommitTS = errors.New("backup: --last-commit-ts T < manifest.last_commit_ts (HLC ceiling regression)")

// ErrEncodeUnsupportedDynamoDBLayout is returned when an input dump
// declares `dynamodb_layout: "jsonl"` in MANIFEST.json. The DynamoDB
// reverse encoder only walks per-item files (items/*.json,
// items/*/*.json) and would silently skip every items/data-*.jsonl
// file, producing an .fsm with only table metadata and no items —
// a silent-data-loss restore artifact (codex P2 v7 #904). Fail closed
// until the encoder learns the JSONL layout (M7 / future milestone).
var ErrEncodeUnsupportedDynamoDBLayout = errors.New("backup: DynamoDB JSONL layout not supported by encoder")

// ErrRedisEncodeMultiDBUnsupported is returned when the input tree
// contains a Redis db_<N>/ for any N != 0, or contains multiple
// db_<N> directories. The current Redis MVCC key prefixes
// (!redis|str|, !redis|hll|, !redis|ttl|, …) carry NO database
// component, so feeding two distinct DBs into the same snapshot
// builder would either collide on same-named keys or silently merge
// both DBs under db_0 on restore (DecodeOptions.RedisDBIndex
// defaults to 0). Failing closed preserves correctness until Phase 1
// makes the native keys DB-aware (codex P2 v14 #904).
//
// v14 originally fanned out across db_<N> to address codex P1 v13's
// silent-data-loss concern; codex's v14 follow-up clarified that
// fan-out under the current key format produces mis-scoped output.
// The corrected fix replaces fan-out with fail-closed.
var ErrRedisEncodeMultiDBUnsupported = errors.New("backup: redis encoder requires single db_0 (multi-DB or non-zero DB not yet supported)")

// ErrEncodeAdapterData marks every error returned by an adapter
// encoder (Redis / DynamoDB / S3 / SQS) so callers can distinguish
// "the input tree contained content the encoder cannot translate"
// from "operator passed a bad flag". The encoder is offline-only —
// every adapter error originates from rejecting the content under
// opts.InputRoot (a malformed DynamoDB _schema.json, an S3 collision
// artifact the encoder cannot reverse, a SQS side-record with an
// unknown kind, …). These are data-correctness failures, not user
// errors; the CLI maps this sentinel to exit 2 so runbooks can branch
// on exit status to quarantine bad dump data (codex P2 v9 #904).
//
// Wrapped via errors.Mark inside runAdapterEncoders so the original
// adapter sentinel chain (ErrDDBEncodeInvalidSchema, …) is preserved
// for callers that errors.Is on the more specific type.
var ErrEncodeAdapterData = errors.New("backup: adapter encoder rejected input tree")

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
	// DynamoDBBundleJSONL is true when the input dump's MANIFEST.json
	// has `dynamodb_layout: "jsonl"`. The reverse encoder does not
	// support that layout — it would silently skip every
	// items/data-*.jsonl file and publish an .fsm with only table
	// metadata. Fail-closed via ErrEncodeUnsupportedDynamoDBLayout
	// when true (codex P2 v7 #904). When the encoder gains JSONL
	// support, this field will switch from a guard to a control.
	DynamoDBBundleJSONL bool

	// ManifestLastCommitTS is the floor LastCommitTS must not fall
	// below. When > 0, EncodeSnapshot fails-closed with
	// ErrSelfTestLowerLastCommitTS if LastCommitTS < ManifestLastCommitTS.
	// This is defense-in-depth for the CLI's pre-check (which already
	// rejects --last-commit-ts T < manifest), and it's the load-bearing
	// guard for future in-process library callers (Phase 1 live extractor,
	// integration tests) that bypass the CLI: a library caller that
	// forgets to compare against the manifest can no longer silently
	// publish a low-TS .fsm (codex P2 v2 #904). Callers that genuinely
	// have no manifest reference (synthetic test fixtures) leave this
	// at 0 to opt out of the check.
	ManifestLastCommitTS uint64
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

// SetSelfTestCorruptHookForTest installs a same-process hook that
// fires against the on-disk self-test buffer between WriteTo and the
// re-decode call. The hook can WriteAt into the file to inject
// corruption so the subsequent self-test mismatches deterministically.
//
// Production code MUST NOT call this; it is exclusively a test seam
// for callers OUTSIDE package backup (specifically the
// cmd/elastickv-snapshot-encode CLI tests, which need to drive a real
// end-to-end self-test mismatch to verify the stale-.fsm cleanup
// path — codex P2 v10 #904). In-package tests should set
// EncodeOptions.corruptBufferForTest directly.
func (o *EncodeOptions) SetSelfTestCorruptHookForTest(hook func(*os.File)) {
	o.corruptBufferForTest = hook
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

// validateEncodeOptions enforces the four pre-encode invariants:
// InputRoot non-empty + exists-as-directory, out non-nil, non-empty
// adapter selection, optional manifest-TS floor, and DDB JSONL guard.
// Split out so EncodeSnapshot stays under the cyclop threshold; the
// data-correctness checks live in validateEncodeOptionsData.
func validateEncodeOptions(opts EncodeOptions, out io.Writer) error {
	if opts.InputRoot == "" {
		return errors.New("backup: EncodeOptions.InputRoot is required")
	}
	// Stat the path so a typo'd or deleted directory surfaces here
	// rather than fan-out-no-op'ing every adapter and producing a
	// header-only .fsm (codex P2 v8 #904). CLI callers indirectly
	// catch this via os.Open(MANIFEST.json) before EncodeSnapshot,
	// but a library caller that passes a stale path needs the guard
	// at this layer.
	info, statErr := os.Stat(opts.InputRoot)
	if statErr != nil {
		return errors.Wrapf(statErr, "stat InputRoot %q", opts.InputRoot)
	}
	if !info.IsDir() {
		return errors.Errorf("backup: InputRoot %q is not a directory", opts.InputRoot)
	}
	if out == nil {
		return errors.New("backup: EncodeSnapshot out writer is nil")
	}
	if !opts.Adapters.DynamoDB && !opts.Adapters.S3 && !opts.Adapters.Redis && !opts.Adapters.SQS {
		// Zero AdapterSet would silently produce a header-only .fsm —
		// a "successful" empty restore artifact (codex v5 + claude v5 #904).
		return errors.New("backup: EncodeOptions.Adapters has no enabled adapter")
	}
	return validateEncodeOptionsData(opts)
}

// validateEncodeOptionsData covers the data-correctness pre-conditions:
// HLC ceiling floor and DynamoDB JSONL guard. Kept separate from the
// nil/empty-args checks so each function stays cyclop-clean.
func validateEncodeOptionsData(opts EncodeOptions) error {
	if opts.ManifestLastCommitTS > 0 && opts.LastCommitTS < opts.ManifestLastCommitTS {
		// Defense-in-depth HLC ceiling floor (codex P2 v2 #904).
		return errors.Wrapf(ErrSelfTestLowerLastCommitTS,
			"EncodeSnapshot opts.LastCommitTS %d < opts.ManifestLastCommitTS %d",
			opts.LastCommitTS, opts.ManifestLastCommitTS)
	}
	if opts.DynamoDBBundleJSONL && opts.Adapters.DynamoDB {
		// The DynamoDB reverse encoder only walks per-item files;
		// JSONL items would be silently skipped (codex P2 v7 #904).
		return errors.WithStack(ErrEncodeUnsupportedDynamoDBLayout)
	}
	return nil
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
// EncodeSnapshot does NOT read MANIFEST.json itself, but it WILL
// enforce a floor on opts.LastCommitTS when the caller threads the
// manifest value through opts.ManifestLastCommitTS — a low
// LastCommitTS returns ErrSelfTestLowerLastCommitTS BEFORE any bytes
// are written. The CLI's resolveLastCommitTS sets both fields to the
// reconciled values, and library callers SHOULD do the same. The
// check is opt-in (ManifestLastCommitTS=0 disables it) so synthetic
// test fixtures without a manifest reference can still call this
// directly (codex P2 v2 #904).
func EncodeSnapshot(opts EncodeOptions, out io.Writer) (EncodeResult, error) {
	if err := validateEncodeOptions(opts, out); err != nil {
		return EncodeResult{}, err
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

	hashTee := newSHA256Writer(tempFile)
	bytesWritten, err := b.WriteTo(hashTee)
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
	sha := hashTee.Sum()
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

// redisDBDirPrefix is the canonical "db_" prefix produced by the
// decoder for redis/db_<N>/ directories. Mirrored by encoder
// enumeration (encodeAllRedisDBs) so a multi-DB dump round-trips.
const redisDBDirPrefix = "db_"

// enumerateRedisDBs returns the sorted dbIndex values for which
// <inRoot>/redis/db_<N>/ exists as a directory. A missing redis/
// directory returns nil; the caller treats it as no-op (same convention
// as the per-DB encoder, which is a no-op when its db_<n> subdir is
// absent). Non-db_<N> entries (regular files, symlinks at the redis/
// level, non-numeric or non-canonical suffixes like "db_-1" or
// "db_01") are silently skipped — they cannot have been produced by
// the canonical decoder and are not the encoder's concern.
//
// Codex P1 v13 #904: replaces the prior hardcoded NewRedisEncoder(_, 0)
// in adapterRunners that silently dropped non-default DBs from any
// future Phase 1 multi-DB dump.
func enumerateRedisDBs(inRoot string) ([]int, error) {
	redisDir := filepath.Join(inRoot, "redis")
	if err := checkRedisRoot(redisDir); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(redisDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, errors.WithStack(err)
	}
	var indices []int
	for _, ent := range entries {
		idx, ok := parseRedisDBName(ent.Name())
		if !ok {
			continue
		}
		// Canonical db_<N> name; entry MUST be a directory.
		// Silently skipping a regular file or symlink at
		// redis/db_<N> would let a malformed dump publish a
		// header-only/partial FSM (codex P2 v14 #904 L427).
		if !ent.IsDir() {
			return nil, errors.Wrapf(ErrRedisEncodeNotDir,
				"redis/%s exists but is not a directory (mode=%s)",
				ent.Name(), ent.Type())
		}
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	return indices, nil
}

// checkRedisRoot stats <inRoot>/redis/ and rejects symlink / non-dir
// shapes. Missing is allowed (caller returns nil indices). Split out
// of enumerateRedisDBs to keep that function under the cyclop bound.
func checkRedisRoot(redisDir string) error {
	info, err := os.Lstat(redisDir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return errors.WithStack(err)
	case info.Mode()&os.ModeSymlink != 0:
		// Symlinked redis/ would let os.OpenRoot in the per-DB encoder
		// resolve outside the dump tree (mirrors the per-DB encoder's
		// symlink refusal on redis/db_<n>).
		return errors.Wrapf(ErrRedisEncodeNotDir, "redis path %q is a symlink", redisDir)
	case !info.IsDir():
		return errors.Wrapf(ErrRedisEncodeNotDir, "redis path %q is not a directory", redisDir)
	}
	return nil
}

// parseRedisDBName returns (dbIndex, true) when name matches the
// canonical db_<N> pattern (N is a non-negative decimal with no
// leading zeros). Non-matching names return (0, false) so the caller
// can skip them without erroring — they cannot have been produced by
// the canonical decoder. Reject non-canonical decimals so a
// hypothetical Phase 1 dumper cannot double-emit the same db under
// two distinct directory names.
//
// This is a pure name parser; the caller is responsible for
// validating the directory-entry shape (codex P2 v14 #904 L427
// shifted the IsDir check to enumerateRedisDBs so a regular file
// at redis/db_<N> fails closed instead of being silently skipped).
func parseRedisDBName(name string) (int, bool) {
	if !strings.HasPrefix(name, redisDBDirPrefix) {
		return 0, false
	}
	suffix := name[len(redisDBDirPrefix):]
	idx, err := strconv.Atoi(suffix)
	if err != nil || idx < 0 || strconv.Itoa(idx) != suffix {
		return 0, false
	}
	return idx, true
}

// encodeAllRedisDBs invokes NewRedisEncoder for redis/db_0/ when the
// input tree has exactly that DB (or no Redis content at all). A
// missing redis/ directory is a no-op. Any non-zero DB or the
// presence of multiple db_<N> directories fails closed with
// ErrRedisEncodeMultiDBUnsupported.
//
// Codex P1 v13 #904 originally asked for a per-DB fan-out to address
// the prior hardcoded db_0 dispatch silently dropping non-default
// DBs. Codex P2 v14 #904 (L452) clarified that fan-out under the
// current MVCC key prefixes (!redis|str|, !redis|hll|, !redis|ttl|,
// …, none of which carry a database component) would either collide
// on same-named keys across DBs or merge everything under db_0 at
// decode time. The corrected fix replaces the silent drop and the
// incorrect fan-out with a fail-closed sentinel until Phase 1
// makes the native keys DB-aware.
func encodeAllRedisDBs(b *snapshotBuilder, inRoot string) error {
	indices, err := enumerateRedisDBs(inRoot)
	if err != nil {
		return errors.Wrap(err, "redis encoder enumerate")
	}
	if len(indices) == 0 {
		return nil
	}
	if len(indices) > 1 || indices[0] != 0 {
		return errors.Wrapf(ErrRedisEncodeMultiDBUnsupported,
			"redis encoder enumerated db indices %v", indices)
	}
	if err := NewRedisEncoder(inRoot, 0).Encode(b); err != nil {
		return errors.Wrap(err, "redis encoder db_0")
	}
	return nil
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
		{"redis", func(s AdapterSet) bool { return s.Redis }, encodeAllRedisDBs},
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
//
// Adapter errors are marked with ErrEncodeAdapterData so the CLI can
// route them to exit-2 (data-correctness) rather than exit-1 (user
// error). The original adapter sentinel chain is preserved — callers
// that errors.Is on ErrDDBEncodeInvalidSchema,
// ErrS3EncodeUnsupportedCollision, etc. still see those (codex P2 v9
// #904; phantom-sentinel doc fix from claude v10 #904).
func runAdapterEncoders(b *snapshotBuilder, opts EncodeOptions) ([]string, error) {
	var enabled []string
	for _, r := range adapterRunners() {
		if !r.enabled(opts.Adapters) {
			continue
		}
		if err := r.encode(b, opts.InputRoot); err != nil {
			return nil, errors.WithStack(errors.Mark(err, ErrEncodeAdapterData))
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

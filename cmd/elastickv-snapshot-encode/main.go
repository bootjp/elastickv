// Command elastickv-snapshot-encode is the Phase 0b M6 snapshot encoder
// described in docs/design/2026_06_01_proposed_snapshot_encode_cli.md
// (parent: docs/design/2026_05_25_partial_snapshot_logical_encoder.md).
//
// It reads a vendor-independent per-adapter directory tree (produced by
// elastickv-snapshot-decode or by a future Phase 1 live extractor) and
// writes a native EKVPBBL1 .fsm a stopped node can load via the
// stop-replace-restart restore runbook (parent §"Restore via
// stop-replace-restart").
//
// The CLI is offline-only. It does not talk to a running cluster; the
// receiving cluster loads the output .fsm via its existing snapshot
// loader on next restart.
//
// Atomic publish: the .fsm is written to <output>.tmp-<random> first,
// fsync+close, then renamed to <output> only after the optional
// self-test matches. A self-test failure removes the temp file, so a
// known-bad .fsm never reaches the restore path (codex P2 v2 #896).
//
// version is stamped at build time via -ldflags "-X main.version=$(git rev-parse HEAD)".
// Test builds keep the literal "dev" so CLI-level tests can assert the
// field is present without depending on a release tag.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/backup"
	"github.com/cockroachdb/errors"
)

var version = "dev"

const (
	exitSuccess = 0
	exitUserErr = 1
	exitDataErr = 2
	// tempSuffixHexLen is the hex-character length of the random
	// suffix appended to <output>.tmp-<hex>; 16 hex chars = 8 bytes of
	// entropy = 2^64 collision space per --output path. The earlier
	// 8-hex/4-byte form was flagged as collision-prone in highly
	// concurrent CI environments (gemini medium #904); 8 bytes is the
	// same width crypto/rand.Read pads cryptographic nonces to.
	tempSuffixHexLen  = 16
	tempSuffixByteLen = tempSuffixHexLen / 2
	// mismatchTxtPerm + sidecar perm constants were removed in v25:
	// both writes now go through backup.OpenSidecarFile which fixes
	// the mode at 0o600 internally (codex P2 v25 #904 — no-follow
	// open required different syscall semantics than os.OpenFile +
	// O_TRUNC, so the perm now lives in the helper).
	encodeInfoFilePerm = 0o600
)

type config struct {
	inputPath           string
	outputPath          string
	adapters            backup.AdapterSet
	lastCommitTSPresent bool
	lastCommitTS        uint64
	selfTest            bool
	scratchRoot         string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	exitCode, err := run(os.Args[1:], logger)
	if err != nil {
		logger.Error("elastickv-snapshot-encode", "err", err)
	}
	os.Exit(exitCode)
}

func run(argv []string, logger *slog.Logger) (int, error) {
	cfg, err := parseFlags(argv)
	if err != nil {
		return exitUserErr, err
	}
	if err := encodeOne(cfg, logger); err != nil {
		return classifyEncodeError(err), err
	}
	return exitSuccess, nil
}

// classifyEncodeError maps the encodeOne return value to a CLI exit
// code. Data-correctness sentinels (HLC ceiling regression, JSONL
// layout, unsupported manifest exclusion flags, adapter scope
// mismatch with manifest, adapter rejecting input-tree contents,
// self-test mismatch, corrupt manifest) → exit 2; everything else
// → exit 1. Runbooks branch on exit status to triage bad-dump-data
// vs operator typos, so this mapping is part of the CLI contract.
//
// Sources of each sentinel:
//   - ErrSelfTestLowerLastCommitTS: CLI resolveLastCommitTS + library
//     validateEncodeOptionsData (codex P2 v2 #904)
//   - ErrEncodeUnsupportedDynamoDBLayout: validateEncodeOptionsData
//     (codex P2 v7 #904)
//   - ErrEncodeUnsupportedS3IncompleteUploads: validateEncodeOptionsUnsupportedFeatures
//     (codex P2 v21 #904)
//   - ErrEncodeUnsupportedS3Orphans: validateEncodeOptionsUnsupportedFeatures
//     (codex P2 v21 #904)
//   - ErrEncodeUnsupportedSQSPreserveVisibility: validateEncodeOptionsUnsupportedFeatures
//     (codex P2 v21 #904)
//   - ErrEncodeAdapterData: runAdapterEncoders mark on adapter
//     rejection (codex P2 v9 #904)
//   - errSelfTestMismatch: writeAndPublish self-test branch
//   - ErrInvalidManifest / ErrUnsupportedFormatVersion: readInputManifest
//     surfacing backup.ReadManifest sentinels (codex P2 v14 #904)
//   - errAdapterNotInManifest: validateAdaptersAgainstManifest when
//     a selected adapter has a nil manifest scope pointer (codex P2
//     v26 #904 scenario B; retracted v27 scenario A in v30 per codex
//     P1 v29 #904)
func classifyEncodeError(err error) int {
	switch {
	case errors.Is(err, backup.ErrSelfTestLowerLastCommitTS),
		errors.Is(err, backup.ErrEncodeUnsupportedDynamoDBLayout),
		errors.Is(err, backup.ErrEncodeUnsupportedS3IncompleteUploads),
		errors.Is(err, backup.ErrEncodeUnsupportedS3Orphans),
		errors.Is(err, backup.ErrEncodeUnsupportedSQSPreserveVisibility),
		errors.Is(err, backup.ErrEncodeAdapterData),
		errors.Is(err, errSelfTestMismatch),
		errors.Is(err, backup.ErrInvalidManifest),
		errors.Is(err, backup.ErrUnsupportedFormatVersion),
		errors.Is(err, errAdapterNotInManifest):
		return exitDataErr
	default:
		return exitUserErr
	}
}

// validateAdaptersAgainstManifest checks each enabled adapter
// against the nil/non-nil manifest scope pointer (manifest.Adapters.<X>).
// One failure mode, routed to exit 2:
//
//   - Manifest lists no scope (nil pointer) for an enabled adapter
//     (codex P2 v26 #904 scenario B). A truncated/wrong manifest
//     combined with the default `--adapter dynamodb,s3,redis,sqs`
//     would otherwise pick up a stale on-disk subdir for an adapter
//     the producer did not dump.
//
// Scenario A (non-nil scope but on-disk subdir missing) cannot be
// detected from the manifest alone because the decoder's
// populateAdapterScopes defers scope enumeration and always writes
// an empty &Adapter{} regardless of record count (codex P1 v29 #904
// pulled the v27/v28/v29 stat-and-readdir checks). Future work:
// add a SHA / record-count manifest field so scenario A becomes
// detectable. See checkOneAdapterScope's doc for the full per-shape
// decision matrix.
//
// A nil manifest.Adapters is treated as "manifest has no scopes for
// any adapter" — every enabled adapter trips the guard. Older
// manifests that omit the Adapters block deliberately are expected
// to pass `--adapter` set to only what they DO contain; that case
// is operator-driven and surfaces the same fail-closed error here.
func validateAdaptersAgainstManifest(selected backup.AdapterSet, m backup.Manifest, inputPath string) error {
	checks := []struct {
		name     string
		selected bool
		scope    *backup.Adapter
		subdir   string
	}{
		{"dynamodb", selected.DynamoDB, manifestAdapterField(m.Adapters, "dynamodb"), "dynamodb"},
		{"s3", selected.S3, manifestAdapterField(m.Adapters, "s3"), "s3"},
		{"redis", selected.Redis, manifestAdapterField(m.Adapters, "redis"), "redis"},
		{"sqs", selected.SQS, manifestAdapterField(m.Adapters, "sqs"), "sqs"},
	}
	for _, c := range checks {
		if err := checkOneAdapterScope(c.name, c.selected, c.scope, filepath.Join(inputPath, c.subdir)); err != nil {
			return err
		}
	}
	return nil
}

// manifestAdapterField returns the *Adapter for one adapter name from
// m.Adapters, or nil if m.Adapters or the specific adapter pointer is
// nil. Centralized so validateAdaptersAgainstManifest's table stays
// readable.
func manifestAdapterField(a *backup.Adapters, name string) *backup.Adapter {
	if a == nil {
		return nil
	}
	switch name {
	case "dynamodb":
		return a.DynamoDB
	case "s3":
		return a.S3
	case "redis":
		return a.Redis
	case "sqs":
		return a.SQS
	default:
		return nil
	}
}

// checkOneAdapterScope is the per-adapter half of
// validateAdaptersAgainstManifest. The decoder's populateAdapterScopes
// explicitly defers scope enumeration and writes `&Adapter{}` (empty)
// for every enabled adapter regardless of whether records were
// dumped, so the manifest's scope CONTENT cannot distinguish
// "this adapter had no records" from "this adapter had records but
// the decoder didn't enumerate them" (codex P1 v29 #904 corrected
// v27/v28/v29's over-eager subdir stat-and-readdir checks).
//
// The only sound nil/non-nil signal is the per-adapter POINTER
// (`manifest.Adapters.<X>`):
//
//   - scope == nil → fail (producer did NOT enable this adapter; any
//     on-disk subdir is stale and would otherwise be encoded under
//     the default `--adapter all`, codex P2 v26 #904 scenario B).
//   - scope != nil → pass (producer enabled the adapter; trust the
//     manifest contract regardless of the on-disk subdir's
//     presence/contents).
//
// Detecting truncated dumps (codex P2 v26 #904 scenario A: scope
// non-nil but on-disk subdir lost) needs SHA / record-count
// verification at the producer side; the manifest alone cannot
// surface it. Tracked as future work.
//
// subdirPath is intentionally unused now but kept in the signature so
// a future check that pairs the manifest with a SHA index doesn't
// need a call-site refactor.
func checkOneAdapterScope(name string, selected bool, scope *backup.Adapter, _ string) error {
	if !selected {
		return nil
	}
	if scope == nil {
		return errors.Wrapf(errAdapterNotInManifest,
			"adapter %q selected but MANIFEST.json has no scope for it (use --adapter to restrict, or re-dump including this adapter)",
			name)
	}
	return nil
}

func parseFlags(argv []string) (*config, error) {
	fs := flag.NewFlagSet("elastickv-snapshot-encode", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var (
		inputPath   string
		outputPath  string
		adapterCSV  string
		ltsRaw      string
		selfTest    bool
		scratchRoot string
	)
	fs.StringVar(&inputPath, "input", "", "Directory tree root produced by elastickv-snapshot-decode (required, must contain MANIFEST.json)")
	fs.StringVar(&outputPath, "output", "", "Destination .fsm file path (required)")
	fs.StringVar(&adapterCSV, "adapter", "dynamodb,s3,redis,sqs", "Comma-separated subset of adapters to encode")
	fs.StringVar(&ltsRaw, "last-commit-ts", "", "Override the manifest's last_commit_ts; must be >= manifest value (HLC ceiling can only rise)")
	fs.BoolVar(&selfTest, "self-test", false, "After encode, decode the produced .fsm and assert it structurally matches --input")
	fs.StringVar(&scratchRoot, "scratch-root", "", "Base directory for self-test scratch subdir (default os.TempDir); a unique encode-self-test-<random> subdir is always created underneath")

	if err := fs.Parse(argv); err != nil {
		return nil, errors.WithStack(err)
	}
	if inputPath == "" {
		return nil, errors.New("--input is required")
	}
	if outputPath == "" {
		return nil, errors.New("--output is required")
	}
	adapters, err := parseAdapterSet(adapterCSV)
	if err != nil {
		return nil, err
	}
	cfg := &config{
		inputPath:   inputPath,
		outputPath:  outputPath,
		adapters:    adapters,
		selfTest:    selfTest,
		scratchRoot: scratchRoot,
	}
	if ltsRaw != "" {
		ts, perr := parseLastCommitTS(ltsRaw)
		if perr != nil {
			return nil, perr
		}
		cfg.lastCommitTSPresent = true
		cfg.lastCommitTS = ts
	}
	return cfg, nil
}

// parseLastCommitTS parses --last-commit-ts as a uint64. Hex (0x prefix)
// or decimal accepted. Uses strconv.ParseUint strict parsing so trailing
// garbage is rejected — fmt.Sscanf would silently accept "0xffZZ" as
// 0xff and "100oops" as 100, which becomes a snapshot HLC ceiling that
// silently disagrees with what the operator typed (claude high #904,
// codex P2 #904). Negative or out-of-range surfaces as exit-1; the
// semantic check (T >= manifest) is exit-2.
func parseLastCommitTS(raw string) (uint64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("--last-commit-ts is empty")
	}
	const (
		base16     = 16
		base10     = 10
		uint64Bits = 64
	)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		ts, err := strconv.ParseUint(s[2:], base16, uint64Bits)
		if err != nil {
			return 0, errors.Wrap(err, "--last-commit-ts hex parse")
		}
		return ts, nil
	}
	ts, err := strconv.ParseUint(s, base10, uint64Bits)
	if err != nil {
		return 0, errors.Wrap(err, "--last-commit-ts decimal parse")
	}
	return ts, nil
}

// parseAdapterSet decodes a comma-separated adapter list (or "all").
// Mirrors the decoder's parser so a typo cannot silently disable an
// adapter. Unknown name → exit-1. A CSV that contains only separators
// or whitespace (e.g. `--adapter ' ,'`) is also rejected — without this
// guard a templated argument that expands to spaces would yield a
// zero AdapterSet and the encoder would publish a valid header-only
// .fsm (no adapters invoked), turning a bad argument into a silent
// empty restore artifact (codex P2 #904).
func parseAdapterSet(csv string) (backup.AdapterSet, error) {
	if csv == "" || csv == "all" {
		return backup.AdapterSet{DynamoDB: true, S3: true, Redis: true, SQS: true}, nil
	}
	var set backup.AdapterSet
	for _, raw := range strings.Split(csv, ",") {
		name := strings.TrimSpace(strings.ToLower(raw))
		if name == "" {
			continue
		}
		if err := applyAdapterName(name, &set); err != nil {
			return backup.AdapterSet{}, err
		}
	}
	if !set.DynamoDB && !set.S3 && !set.Redis && !set.SQS {
		return backup.AdapterSet{}, errors.Errorf("--adapter %q selects no adapters; use \"all\" or a comma-separated subset", csv)
	}
	return set, nil
}

// applyAdapterName sets the bit on s for one normalized adapter name,
// or returns an unknown-name error. Split out so parseAdapterSet stays
// under the cyclop threshold.
func applyAdapterName(name string, s *backup.AdapterSet) error {
	switch name {
	case "dynamodb":
		s.DynamoDB = true
	case "s3":
		s.S3 = true
	case "redis":
		s.Redis = true
	case "sqs":
		s.SQS = true
	default:
		return errors.Errorf("unknown adapter %q", name)
	}
	return nil
}

// errSelfTestMismatch is a typed sentinel so run() can map self-test diffs
// to exit-2 without coupling to the encoder's mismatch.txt format.
var errSelfTestMismatch = errors.New("backup: --self-test diff against --input")

// errAdapterNotInManifest is returned by validateAdaptersAgainstManifest
// when the user has enabled an adapter that the manifest doesn't list
// (nil pointer in manifest.Adapters.<X>). This is the codex P2 v26
// #904 scenario B: a stale on-disk subdir for an adapter the producer
// did not dump would otherwise be encoded under the default
// `--adapter dynamodb,s3,redis,sqs`. classifyEncodeError routes the
// sentinel to exit 2 (data-correctness).
//
// The earlier v27/v28/v29 attempts to also detect a missing on-disk
// subdir under a non-nil scope (codex P2 v26 scenario A) were retracted
// in v30 once codex P1 v29 #904 clarified that the decoder defers
// scope enumeration; the manifest can no longer distinguish "no
// records dumped" from "records dumped but scope not enumerated."
var errAdapterNotInManifest = errors.New("encode: adapter scope mismatch between MANIFEST.json and --adapter / on-disk tree")

func encodeOne(cfg *config, logger *slog.Logger) error {
	manifest, err := readInputManifest(cfg.inputPath)
	if err != nil {
		return err
	}
	if err := validateAdaptersAgainstManifest(cfg.adapters, manifest, cfg.inputPath); err != nil {
		return err
	}
	effectiveTS, overridden, err := resolveLastCommitTS(cfg, manifest.LastCommitTS)
	if err != nil {
		return err
	}
	encodeOpts := buildEncodeOptions(cfg, effectiveTS, manifest)

	mismatchPath := cfg.outputPath + ".mismatch.txt"
	_ = os.Remove(mismatchPath) // stale-mismatch cleanup (gemini medium v6 #896)
	// Do NOT pre-clean the sidecar here. The sidecar describes the
	// .fsm at cfg.outputPath; the .fsm is preserved when a run fails
	// in the adapter encoders (non-self-test exit-2 path), so wiping
	// its sidecar would leave the prior restore artifact without its
	// matching provenance metadata (codex P2 v17 #904). writeSidecar
	// uses O_CREATE|O_TRUNC, so the sidecar is atomically overwritten
	// on success and on self-test mismatch (where the .fsm is also
	// replaced or removed in lock-step). On adapter-encoder errors
	// neither writeSidecar nor removeStaleOutputFSM runs; the prior
	// .fsm + prior sidecar therefore stay paired.

	result, publishErr := writeAndPublish(cfg, encodeOpts, mismatchPath, logger)
	// Sidecar is written even on self-test mismatch so an operator
	// has both <output>.mismatch.txt AND <output>.encode_info.json
	// (with self_test.matched=false) for diagnostics. Only skipped
	// when the encode itself errored before any result was populated
	// (publishErr != nil && !errSelfTestMismatch) (codex P2 v6 #904).
	if publishErr == nil || errors.Is(publishErr, errSelfTestMismatch) {
		sidecarTruncated, serr := writeSidecar(cfg, manifest, effectiveTS, overridden, result)
		if serr != nil {
			// Surface the sidecar-write failure only if encode itself
			// succeeded; on mismatch the mismatch error takes priority.
			if publishErr == nil {
				// .fsm was just renamed into place by writeAndPublish
				// but the sidecar write failed → we have an orphan
				// .fsm without its matching provenance metadata.
				// Roll back to a consistent absent state. The sidecar
				// rollback is gated on sidecarTruncated so we don't
				// remove an operator-owned pre-existing entry that
				// OpenSidecarFile refused to clobber (claude/codex
				// P2 v31 added the rollback; codex P2 v33 #904 added
				// the truncation gate for hard-linked sidecars).
				rollbackOrphanFSMAndSidecar(cfg.outputPath, sidecarTruncated, logger)
				return errors.Wrap(serr, "write encode_info sidecar")
			}
			logger.Warn("write encode_info sidecar on mismatch", "err", serr)
		}
	}
	if publishErr != nil {
		return publishErr
	}
	logger.Info("encode complete",
		"output", cfg.outputPath,
		"bytes", result.BytesWritten,
		"self_test", cfg.selfTest,
		"adapters", strings.Join(result.AdaptersEnabled, ","),
	)
	return nil
}

// readInputManifest opens + decodes <input>/MANIFEST.json.
func readInputManifest(inputPath string) (backup.Manifest, error) {
	manifestPath := filepath.Join(inputPath, "MANIFEST.json")
	manifestFile, err := os.Open(manifestPath) //nolint:gosec // operator-supplied path
	if err != nil {
		return backup.Manifest{}, errors.Wrapf(err, "open %s", manifestPath)
	}
	defer func() { _ = manifestFile.Close() }()
	m, err := backup.ReadManifest(manifestFile)
	if err != nil {
		return backup.Manifest{}, errors.Wrap(err, "read manifest")
	}
	return m, nil
}

func buildEncodeOptions(cfg *config, effectiveTS uint64, manifest backup.Manifest) backup.EncodeOptions {
	encodeOpts := backup.EncodeOptions{
		InputRoot:            cfg.inputPath,
		Adapters:             cfg.adapters,
		LastCommitTS:         effectiveTS,
		ManifestLastCommitTS: manifest.LastCommitTS,
		DynamoDBBundleJSONL:  manifest.DynamoDBLayout == backup.DynamoDBLayoutJSONL,
		SelfTest:             cfg.selfTest,
	}
	// Thread manifest exclusions into the library guards (codex P2 v21
	// #904): the S3/SQS reverse encoders can't honor these today, so
	// failing closed here surfaces the unsupported-feature errors
	// before any bytes are written. The CLI's existing
	// buildSelfTestDecodeOptions also threads the same fields into
	// the scratch decode path so self-test sees a coherent picture.
	if manifest.Exclusions != nil {
		encodeOpts.S3IncludeIncompleteUploads = manifest.Exclusions.IncludeIncompleteUploads
		encodeOpts.S3IncludeOrphans = manifest.Exclusions.IncludeOrphans
		encodeOpts.PreserveSQSVisibility = manifest.Exclusions.PreserveSQSVisibility
	}
	if cfg.selfTest {
		encodeOpts.SelfTestDecodeOptions = buildSelfTestDecodeOptions(cfg, manifest)
	}
	return encodeOpts
}

// rollbackOrphanFSMAndSidecar removes both the just-published
// <output>.fsm and the partial <output>.encode_info.json after a
// sidecar-write failure on the encode success path. The pair was
// supposed to move together (the .fsm describes the data the sidecar
// records the provenance for); if the sidecar didn't land, the
// operator must not see a "successful" .fsm without its matching
// provenance metadata (claude / codex P2 v31 observation on PR #904).
//
// A prior successful encode at the same output path is unrecoverable
// — writeAndPublish's os.Rename already overwrote it before
// writeSidecar ran. The rollback brings the state to "no .fsm, no
// sidecar at this path", which is the same end state as "encode
// never ran." That's the cleanest consistent outcome the CLI can
// produce without filesystem transactions.
//
// Both os.Remove calls log-and-continue on non-ErrNotExist failures
// so the caller's primary sidecar-write error remains the dominant
// signal.
// rollbackOrphanFSMAndSidecar reverts an encode that succeeded in
// publishing the .fsm but failed in writing the sidecar. Always
// removes the just-renamed .fsm at outputPath. The sidecar at
// EncodeInfoSidecarPath(outputPath) is removed ONLY when
// sidecarTruncated is true — i.e., backup.OpenSidecarFile succeeded
// and either created a fresh file or truncated an existing
// single-link regular file. When sidecarTruncated is false the
// existing entry is operator-owned (symlink, hard link with
// Nlink > 1, FIFO, directory, etc.) that OpenSidecarFile refused to
// clobber, so this rollback must NOT destroy it either (codex P2
// v32 #904 / codex P2 v33 #904).
func rollbackOrphanFSMAndSidecar(outputPath string, sidecarTruncated bool, logger *slog.Logger) {
	if rerr := os.Remove(outputPath); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
		logger.Warn("rollback orphan .fsm after sidecar failure", "err", rerr)
	}
	if !sidecarTruncated {
		return
	}
	sidecarPath := backup.EncodeInfoSidecarPath(outputPath)
	if srerr := os.Remove(sidecarPath); srerr != nil && !errors.Is(srerr, os.ErrNotExist) {
		logger.Warn("rollback partial sidecar after write failure", "err", srerr)
	}
}

// writeMismatchTxt writes the self-test mismatch report to mismatchPath
// using the same no-follow/no-clobber discipline as the sidecar
// writer: an attacker pre-placing a symlink at
// <output>.mismatch.txt could otherwise redirect the
// truncate-and-write into a target of their choosing (codex P2 v25
// #904 — extending the sidecar guard to the sibling deterministic
// write path). On open failure the caller (writeAndPublish) logs at
// warn level and continues; the failure does NOT block the
// errSelfTestMismatch return so the mismatch error remains the
// dominant signal.
func writeMismatchTxt(mismatchPath string, body []byte) error {
	f, err := backup.OpenSidecarFile(mismatchPath)
	if err != nil {
		return errors.Wrap(err, "open mismatch.txt")
	}
	if _, werr := f.Write(body); werr != nil {
		_ = f.Close()
		return errors.Wrap(werr, "write mismatch.txt body")
	}
	if cerr := f.Close(); cerr != nil {
		return errors.Wrap(cerr, "close mismatch.txt")
	}
	return nil
}

// removeStaleOutputFSM removes outputPath ONLY when it exists as a
// regular file or a symlink. Both shapes satisfy the "no
// restore-visible FSM after self-test mismatch" contract: removing a
// regular file empties --output; removing a symlink unlinks the
// name (the target is preserved as a side effect — os.Remove on a
// symlink operates on the link, not the resolved target). A directory,
// device, FIFO, or socket at --output is left alone — those shapes
// were never valid restore targets, and os.Remove on a non-empty
// directory or device would be destructive in ways the mismatch
// contract does not require (codex P2 v14 #904 caught the directory
// case; codex P2 v19 #904 caught the symlink case where the prior
// IsRegular()-only check silently left the symlink resolving to the
// stale snapshot).
//
// Errors other than ErrNotExist are downgraded to warn-and-continue
// so the caller's primary mismatch error remains the dominant signal.
func removeStaleOutputFSM(outputPath string, logger *slog.Logger) {
	info, err := os.Lstat(outputPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Warn("stat stale .fsm on self-test mismatch", "err", err)
		}
		return
	}
	mode := info.Mode()
	isSymlink := mode&os.ModeSymlink != 0
	if !mode.IsRegular() && !isSymlink {
		logger.Warn("skip stale .fsm cleanup: --output is not a regular file or symlink",
			"path", outputPath, "mode", mode)
		return
	}
	if rerr := os.Remove(outputPath); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
		logger.Warn("remove stale .fsm on self-test mismatch", "err", rerr)
	}
}

// writeAndPublish writes the .fsm to a temp path, runs the optional
// self-test via EncodeSnapshot, and renames temp → output on success.
// On self-test failure: writes mismatch.txt, removes any stale
// <output>.fsm or symlink at <output> left by a prior successful
// run (codex P2 v10 #904 covered regular files, codex P2 v19 #904
// extended to symlinks; directories and special files are left
// alone per v14 L347), removes the temp file via the deferred
// cleanup, returns errSelfTestMismatch. See removeStaleOutputFSM
// for the per-shape decision matrix.
func writeAndPublish(cfg *config, encodeOpts backup.EncodeOptions, mismatchPath string, logger *slog.Logger) (backup.EncodeResult, error) {
	tempPath, err := tempOutputPath(cfg.outputPath)
	if err != nil {
		return backup.EncodeResult{}, err
	}
	result, err := encodeToTempFile(tempPath, encodeOpts)
	publishedTempPath := tempPath
	defer func() {
		if publishedTempPath != "" {
			_ = os.Remove(publishedTempPath)
		}
	}()
	if err != nil {
		return result, err
	}
	if cfg.selfTest && !result.SelfTestMatched {
		if werr := writeMismatchTxt(mismatchPath, result.SelfTestMismatchTxt); werr != nil {
			logger.Warn("write mismatch.txt", "err", werr)
		}
		// Remove the stale <output>.fsm if one exists from a prior
		// successful run AND is a regular file. encodeOne is about to
		// write a fresh <output>.encode_info.json with
		// self_test.matched=false and a NEW SHA pointing to the
		// unpublished temp snapshot; leaving old bytes on disk would
		// make the sidecar describe an FSM that does not exist and
		// violate the "self-test failure leaves no restore-visible
		// FSM" contract (codex P2 v10 #904).
		//
		// The mode-check guards against an --output that names a
		// directory (or any non-regular file): the normal publish
		// path would fail at os.Rename anyway, but the mismatch
		// cleanup must not destructively delete a directory the
		// operator passed in error (codex P2 v14 #904).
		removeStaleOutputFSM(cfg.outputPath, logger)
		return result, errors.Wrap(errSelfTestMismatch, "self-test diff (see "+mismatchPath+")")
	}
	if perr := publishAndFsync(tempPath, cfg.outputPath, logger); perr != nil {
		return result, perr
	}
	publishedTempPath = "" // rename succeeded; defer no-ops
	return result, nil
}

// publishAndFsync renames tempPath → outputPath and then fsyncs the
// parent directory. If the fsync fails, the just-renamed .fsm is
// removed so the operator does not see a non-durable "successful"
// .fsm (codex P2 v24 #904 added the fsync; codex P2 v32 #904 added
// the rollback). Split out of writeAndPublish to keep that function
// under the cyclop bound.
func publishAndFsync(tempPath, outputPath string, logger *slog.Logger) error {
	if err := os.Rename(tempPath, outputPath); err != nil {
		return errors.Wrap(err, "rename tmp -> output")
	}
	// fsync the parent dir so the rename's new directory entry is
	// durable. Without this, a power loss / host crash immediately
	// after a successful encode can lose the new <output> entry (or
	// resurrect the old one) on filesystems where rename durability
	// requires syncing the containing directory. Mirrors the repo
	// pattern used by internal/encryption/sidecar.go +
	// internal/raftengine/etcd/persistence.go.
	if err := fsyncParentDir(outputPath); err != nil {
		// Roll back so the operator doesn't see a non-durable
		// "successful" .fsm; restoring the consistent absent state
		// is the same outcome encodeOne enforces on sidecar-write
		// failures (codex P2 v32 #904).
		if rerr := os.Remove(outputPath); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			logger.Warn("rollback orphan .fsm after parent-dir fsync failure", "err", rerr)
		}
		return errors.Wrap(err, "fsync output dir after rename")
	}
	return nil
}

// fsyncParentDir opens the parent directory of path read-only and
// calls fsync on its file descriptor. On most POSIX filesystems this
// is what makes os.Rename durable. Errors other than path-traversal
// (which means the operator passed something weird like "" — already
// rejected upstream) bubble up so the caller can surface them.
//
// Mirrors syncDir in internal/encryption/sidecar.go and the etcd
// raftengine persistence helper; kept local here so the CLI binary
// doesn't depend on internal/encryption for a 6-line helper.
func fsyncParentDir(path string) error {
	dir := filepath.Dir(path)
	f, err := os.Open(dir) //nolint:gosec // dir is derived from operator-supplied --output path
	if err != nil {
		return errors.Wrapf(err, "open parent dir %q", dir)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		return errors.Wrapf(err, "fsync parent dir %q", dir)
	}
	return nil
}

// encodeToTempFile creates tempPath, runs EncodeSnapshot into it,
// fsync+close. Caller is responsible for the os.Remove cleanup on error.
// The temp file is created mode 0600 so the on-disk .fsm is not
// world-readable while the encode is in flight (claude v4 #904).
func encodeToTempFile(tempPath string, encodeOpts backup.EncodeOptions) (backup.EncodeResult, error) {
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, encodeInfoFilePerm) //nolint:gosec // operator-supplied path
	if err != nil {
		return backup.EncodeResult{}, errors.Wrapf(err, "create %s", tempPath)
	}
	result, err := backup.EncodeSnapshot(encodeOpts, tempFile)
	if err != nil {
		_ = tempFile.Close()
		return result, errors.Wrap(err, "EncodeSnapshot")
	}
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return result, errors.Wrap(err, "fsync tmp")
	}
	if err := tempFile.Close(); err != nil {
		return result, errors.Wrap(err, "close tmp")
	}
	return result, nil
}

// resolveLastCommitTS applies the parent doc's HLC-ceiling-only-rises
// rule. Returns the effective T, whether an override was applied, and a
// typed error on regression.
func resolveLastCommitTS(cfg *config, manifestTS uint64) (uint64, bool, error) {
	if !cfg.lastCommitTSPresent {
		return manifestTS, false, nil
	}
	if cfg.lastCommitTS < manifestTS {
		return 0, false, errors.Wrapf(backup.ErrSelfTestLowerLastCommitTS,
			"--last-commit-ts %d < manifest %d", cfg.lastCommitTS, manifestTS)
	}
	return cfg.lastCommitTS, true, nil
}

// buildSelfTestDecodeOptions translates manifest fields into the
// DecodeOptions the self-test feeds into DecodeSnapshot, so the scratch
// tree matches what the original decoder would have produced (codex P2
// v3 #896).
func buildSelfTestDecodeOptions(cfg *config, m backup.Manifest) backup.DecodeOptions {
	opts := backup.DecodeOptions{
		OutRoot:  cfg.scratchRoot,
		Adapters: cfg.adapters,
	}
	if m.Exclusions != nil {
		opts.IncludeIncompleteUploads = m.Exclusions.IncludeIncompleteUploads
		opts.IncludeOrphans = m.Exclusions.IncludeOrphans
		opts.PreserveSQSVisibility = m.Exclusions.PreserveSQSVisibility
		opts.IncludeSQSSideRecords = m.Exclusions.IncludeSQSSideRecords
		opts.RenameS3Collisions = m.Exclusions.RenameS3Collisions
	}
	if m.DynamoDBLayout == backup.DynamoDBLayoutJSONL {
		opts.DynamoDBBundleJSONL = true
	}
	return opts
}

// tempOutputPath returns <output>.tmp-<random> for the write-then-rename
// atomic publish. crypto/rand provides the suffix so concurrent encodes
// against the same --output cannot collide.
func tempOutputPath(output string) (string, error) {
	buf := make([]byte, tempSuffixByteLen)
	if _, err := rand.Read(buf); err != nil {
		return "", errors.Wrap(err, "rand suffix")
	}
	return output + ".tmp-" + hex.EncodeToString(buf), nil
}

// writeSidecar emits ENCODE_INFO.json next to the published .fsm
// (path-derived per gemini medium v2 #896). Returns (truncated, err):
// truncated is true iff backup.OpenSidecarFile succeeded — i.e., the
// existing path was truncated by THIS run (or a fresh file was
// created). When truncated is false, the caller MUST NOT roll back
// the sidecar path: any pre-existing entry there is operator-owned
// and OpenSidecarFile correctly refused to clobber it (codex P2 v33
// #904 — hard-linked sidecars in particular pass IsRegular but were
// refused via Nlink>1; v32's IsRegular-only rollback gate would
// have destroyed those).
func writeSidecar(cfg *config, m backup.Manifest, effectiveTS uint64, overridden bool, result backup.EncodeResult) (bool, error) {
	info := backup.NewEncodeInfo(time.Now())
	info.EncoderVersion = version
	info.InputRoot = cfg.inputPath
	info.OutputFSMPath = cfg.outputPath
	info.OutputFSMSHA256 = hex.EncodeToString(result.SHA256[:])
	info.LastCommitTS = effectiveTS
	info.LastCommitTSOverridden = overridden
	info.ManifestLastCommitTS = m.LastCommitTS
	info.ManifestClusterID = m.ClusterID
	info.AdaptersEnabled = result.AdaptersEnabled
	info.SelfTest = backup.EncodeInfoSelfTest{
		Ran:     result.SelfTestRan,
		Matched: result.SelfTestMatched,
	}
	sidecarPath := backup.EncodeInfoSidecarPath(cfg.outputPath)
	// 0o600 keeps ENCODE_INFO.json (which includes the source path,
	// cluster_id, and SHA-256 of the .fsm) from leaking to non-owner
	// users on multi-user backup hosts (claude v4 #904).
	//
	// backup.OpenSidecarFile refuses to follow a symlink at the
	// sidecar path, refuses to truncate a hard-linked or
	// non-regular file there, and (on unix) refuses to block on a
	// reader-less FIFO — all the clobber-attack vectors the adapter
	// dump writers already defend against. Without these guards an
	// attacker pre-placing a symlink at <output>.encode_info.json
	// could redirect the truncate-and-write into a target of their
	// choosing (codex P2 v25 #904).
	f, err := backup.OpenSidecarFile(sidecarPath)
	if err != nil {
		// Pre-existing entry refused (symlink/hard-link/non-regular).
		// Caller must NOT rollback the sidecar path; the file there
		// is operator-owned and was never touched by this run.
		return false, errors.Wrap(err, "open sidecar")
	}
	// From this point, f points at a truncated (zero-length) regular
	// file owned by this run. Any subsequent failure leaves partial
	// bytes (or empty) on disk — the caller's rollback removes them.
	if err := backup.WriteEncodeInfo(f, info); err != nil {
		_ = f.Close()
		return true, errors.Wrap(err, "WriteEncodeInfo")
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return true, errors.WithStack(err)
	}
	if err := f.Close(); err != nil {
		return true, errors.WithStack(err)
	}
	// fsync the parent dir so the new sidecar's directory entry is
	// durable alongside its bytes. Mirrors the rename path
	// (codex P2 v24 #904).
	if err := fsyncParentDir(sidecarPath); err != nil {
		return true, errors.Wrap(err, "fsync sidecar parent dir")
	}
	return true, nil
}

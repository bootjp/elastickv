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
	tempSuffixHexLen   = 16
	tempSuffixByteLen  = tempSuffixHexLen / 2
	mismatchTxtPerm    = 0o600
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
// layout, adapter rejecting input-tree contents, self-test mismatch,
// corrupt manifest) → exit 2; everything else → exit 1. Runbooks
// branch on exit status to triage bad-dump-data vs operator typos,
// so this mapping is part of the CLI contract.
//
// Sources of each sentinel:
//   - ErrSelfTestLowerLastCommitTS: CLI resolveLastCommitTS + library
//     validateEncodeOptionsData (codex P2 v2 #904)
//   - ErrEncodeUnsupportedDynamoDBLayout: validateEncodeOptionsData
//     (codex P2 v7 #904)
//   - ErrEncodeAdapterData: runAdapterEncoders mark on adapter
//     rejection (codex P2 v9 #904)
//   - errSelfTestMismatch: writeAndPublish self-test branch
//   - ErrInvalidManifest / ErrUnsupportedFormatVersion: readInputManifest
//     surfacing backup.ReadManifest sentinels (codex P2 v14 #904)
func classifyEncodeError(err error) int {
	switch {
	case errors.Is(err, backup.ErrSelfTestLowerLastCommitTS),
		errors.Is(err, backup.ErrEncodeUnsupportedDynamoDBLayout),
		errors.Is(err, backup.ErrEncodeAdapterData),
		errors.Is(err, errSelfTestMismatch),
		errors.Is(err, backup.ErrInvalidManifest),
		errors.Is(err, backup.ErrUnsupportedFormatVersion):
		return exitDataErr
	default:
		return exitUserErr
	}
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

func encodeOne(cfg *config, logger *slog.Logger) error {
	manifest, err := readInputManifest(cfg.inputPath)
	if err != nil {
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
		if serr := writeSidecar(cfg, manifest, effectiveTS, overridden, result); serr != nil {
			// Surface the sidecar-write failure only if encode itself
			// succeeded; on mismatch the mismatch error takes priority.
			if publishErr == nil {
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
	if cfg.selfTest {
		encodeOpts.SelfTestDecodeOptions = buildSelfTestDecodeOptions(cfg, manifest)
	}
	return encodeOpts
}

// removeStaleOutputFSM removes outputPath ONLY if it exists and is a
// regular file. A directory or special-file at the path is left alone
// (codex P2 v14 #904 — the prior unconditional os.Remove would have
// deleted an empty directory the operator passed in error to --output).
// Errors other than ErrNotExist are downgraded to warn-and-continue so
// the caller's primary mismatch error remains the dominant signal.
func removeStaleOutputFSM(outputPath string, logger *slog.Logger) {
	info, err := os.Lstat(outputPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Warn("stat stale .fsm on self-test mismatch", "err", err)
		}
		return
	}
	if !info.Mode().IsRegular() {
		logger.Warn("skip stale .fsm cleanup: --output is not a regular file",
			"path", outputPath, "mode", info.Mode())
		return
	}
	if rerr := os.Remove(outputPath); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
		logger.Warn("remove stale .fsm on self-test mismatch", "err", rerr)
	}
}

// writeAndPublish writes the .fsm to a temp path, runs the optional
// self-test via EncodeSnapshot, and renames temp → output on success.
// On self-test failure: writes mismatch.txt, removes any stale
// <output>.fsm left by a prior successful run (codex P2 v10 #904),
// removes the temp file via the deferred cleanup, returns
// errSelfTestMismatch.
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
		if werr := os.WriteFile(mismatchPath, result.SelfTestMismatchTxt, mismatchTxtPerm); werr != nil {
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
	if err := os.Rename(tempPath, cfg.outputPath); err != nil {
		return result, errors.Wrap(err, "rename tmp -> output")
	}
	publishedTempPath = "" // rename succeeded; defer no-ops
	return result, nil
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

// writeSidecar emits ENCODE_INFO.json next to the published .fsm.
// Path-derived per gemini medium v2 #896.
func writeSidecar(cfg *config, m backup.Manifest, effectiveTS uint64, overridden bool, result backup.EncodeResult) error {
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
	f, err := os.OpenFile(sidecarPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, encodeInfoFilePerm) //nolint:gosec // operator-supplied path
	if err != nil {
		return errors.WithStack(err)
	}
	if err := backup.WriteEncodeInfo(f, info); err != nil {
		_ = f.Close()
		return errors.Wrap(err, "WriteEncodeInfo")
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return errors.WithStack(err)
	}
	if err := f.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

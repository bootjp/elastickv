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
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	// suffix appended to <output>.tmp-<hex>; 8 hex chars = 4 bytes of
	// entropy, which gives ~4×10^9 collision space per --output path
	// (more than enough for concurrent encodes against the same path).
	tempSuffixHexLen   = 8
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
		// Errors from the encoder layer that represent data constraints
		// (HLC ceiling regression, self-test mismatch) are exit 2; other
		// errors are exit 1.
		if errors.Is(err, backup.ErrSelfTestLowerLastCommitTS) {
			return exitDataErr, err
		}
		if errors.Is(err, errSelfTestMismatch) {
			return exitDataErr, err
		}
		return exitUserErr, err
	}
	return exitSuccess, nil
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
// or decimal accepted. Negative or out-of-range surfaces as exit-1
// (flag-parse error); the semantic check (T >= manifest) is exit-2.
func parseLastCommitTS(raw string) (uint64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("--last-commit-ts is empty")
	}
	var ts uint64
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		if _, err := fmt.Sscanf(s[2:], "%x", &ts); err != nil {
			return 0, errors.Wrap(err, "--last-commit-ts hex parse")
		}
		return ts, nil
	}
	if _, err := fmt.Sscanf(s, "%d", &ts); err != nil {
		return 0, errors.Wrap(err, "--last-commit-ts decimal parse")
	}
	return ts, nil
}

// parseAdapterSet decodes a comma-separated adapter list (or "all").
// Mirrors the decoder's parser so a typo cannot silently disable an
// adapter. Unknown name → exit-1.
func parseAdapterSet(csv string) (backup.AdapterSet, error) {
	if csv == "" || csv == "all" {
		return backup.AdapterSet{DynamoDB: true, S3: true, Redis: true, SQS: true}, nil
	}
	var set backup.AdapterSet
	for _, raw := range strings.Split(csv, ",") {
		name := strings.TrimSpace(strings.ToLower(raw))
		switch name {
		case "dynamodb":
			set.DynamoDB = true
		case "s3":
			set.S3 = true
		case "redis":
			set.Redis = true
		case "sqs":
			set.SQS = true
		case "":
			continue
		default:
			return backup.AdapterSet{}, errors.Errorf("unknown adapter %q", name)
		}
	}
	return set, nil
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
	_ = os.Remove(mismatchPath) // stale-mismatch cleanup, gemini medium v6 #896

	result, err := writeAndPublish(cfg, encodeOpts, mismatchPath, logger)
	if err != nil {
		return err
	}
	if err := writeSidecar(cfg, manifest, effectiveTS, overridden, result); err != nil {
		return errors.Wrap(err, "write encode_info sidecar")
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
		InputRoot:    cfg.inputPath,
		Adapters:     cfg.adapters,
		LastCommitTS: effectiveTS,
		SelfTest:     cfg.selfTest,
	}
	if cfg.selfTest {
		encodeOpts.SelfTestDecodeOptions = buildSelfTestDecodeOptions(cfg, manifest)
	}
	return encodeOpts
}

// writeAndPublish writes the .fsm to a temp path, runs the optional
// self-test via EncodeSnapshot, and renames temp → output on success.
// On self-test failure: writes mismatch.txt, removes the temp file via
// the deferred cleanup, returns errSelfTestMismatch.
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
func encodeToTempFile(tempPath string, encodeOpts backup.EncodeOptions) (backup.EncodeResult, error) {
	tempFile, err := os.Create(tempPath) //nolint:gosec // operator-supplied path
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
	f, err := os.Create(sidecarPath) //nolint:gosec // operator-supplied path
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

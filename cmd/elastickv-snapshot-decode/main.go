// Command elastickv-snapshot-decode is the Phase 0a snapshot
// decoder described in
// docs/design/2026_04_29_proposed_snapshot_logical_decoder.md.
//
// It reads a Pebble snapshot (`.fsm`) emitted by the live FSM
// (store/lsm_store.go) and writes a vendor-independent per-adapter
// directory tree rooted at --output. The output tree carries a
// MANIFEST.json describing the dump and a sha256sum(1)-compatible
// CHECKSUMS file the operator can verify with stock UNIX tooling
// — `sha256sum -c CHECKSUMS` from the dump root suffices, with no
// elastickv binary involved.
//
// Phase 0a is offline-only. The tool needs a `.fsm` on disk; it
// does not talk to a running cluster. Phase 1 (separate design)
// adds a live-cluster extraction path that produces the same
// directory tree from a pinned read_ts.
package main

import (
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

// defaultOutputDirMode mirrors the permissions every backup-package
// encoder uses when it creates per-adapter subdirectories. Kept on
// the CLI side so an operator who wants a more restrictive umask can
// pre-create the output root before running the decoder; the
// MkdirAll call below is a no-op when the directory already exists.
const defaultOutputDirMode = 0o755

// version is stamped via -ldflags "-X main.version=<git-sha>" by
// the build pipeline. Phase 0a copies it into MANIFEST.json's
// elastickv_version field so a downstream restore-tool author
// knows which release-vintage of the elastickv source produced the
// dump.
var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "elastickv-snapshot-decode: %v\n", err)
		os.Exit(1)
	}
}

// run is the testable entrypoint: argv slice in, error out. Phase 0a's
// tests drive it directly (no os.Exit + no signal handling) so they
// can assert on the output tree shape.
func run(argv []string, logSink io.Writer) error {
	cfg, err := parseFlags(argv)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(logSink, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return decodeOne(cfg, logger)
}

// config carries the parsed CLI flags. Kept separate from
// backup.DecodeOptions so the CLI can layer flags the package does
// not need (cluster_id, source-path bookkeeping for MANIFEST,
// log-format) without contaminating the library.
type config struct {
	inputPath  string
	outputRoot string

	adapters backup.AdapterSet

	includeIncompleteUploads bool
	includeOrphans           bool
	preserveSQSVisibility    bool
	includeSQSSideRecords    bool
	renameCollisions         bool
	bundleJSONL              bool

	clusterID string
}

// parseFlags lifts argv into a config. The default adapter set is
// "everything"; --adapter overrides with a comma-separated list.
func parseFlags(argv []string) (*config, error) {
	fs := flag.NewFlagSet("elastickv-snapshot-decode", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // we surface errors via the returned error chain

	var (
		inputPath  string
		outputRoot string
		adapterCSV string
		bundleMode string
		clusterID  string

		includeIncompleteUploads bool
		includeOrphans           bool
		preserveSQSVisibility    bool
		includeSQSSideRecords    bool
		renameCollisions         bool
	)
	fs.StringVar(&inputPath, "input", "", "Path to the .fsm snapshot file (required)")
	fs.StringVar(&outputRoot, "output", "", "Destination directory tree root (required)")
	fs.StringVar(&adapterCSV, "adapter", "dynamodb,s3,redis,sqs", "Comma-separated subset of adapters to emit")
	fs.StringVar(&bundleMode, "dynamodb-bundle-mode", "per-item", "DynamoDB item layout: per-item | jsonl")
	fs.StringVar(&clusterID, "cluster-id", "", "Cluster identifier to stamp into MANIFEST.json (optional)")
	fs.BoolVar(&includeIncompleteUploads, "include-incomplete-uploads", false, "Emit in-flight S3 multipart uploads under _incomplete_uploads/")
	fs.BoolVar(&includeOrphans, "include-orphans", false, "Emit pre-generation S3 orphan blobs under _orphans/")
	fs.BoolVar(&preserveSQSVisibility, "preserve-sqs-visibility", false, "Carry live SQS visibility-state fields into the dump instead of zeroing them")
	fs.BoolVar(&includeSQSSideRecords, "include-sqs-side-records", false, "Emit SQS dedup/group/vis side records under _internals/")
	fs.BoolVar(&renameCollisions, "rename-collisions", false, "Rename user S3 keys ending in .elastickv-meta.json instead of failing")
	if err := fs.Parse(argv); err != nil {
		return nil, errors.WithStack(err)
	}
	if inputPath == "" {
		return nil, errors.New("--input is required")
	}
	if outputRoot == "" {
		return nil, errors.New("--output is required")
	}
	adapters, err := parseAdapterSet(adapterCSV)
	if err != nil {
		return nil, err
	}
	bundleJSONL, err := parseBundleMode(bundleMode)
	if err != nil {
		return nil, err
	}
	return &config{
		inputPath:                inputPath,
		outputRoot:               outputRoot,
		adapters:                 adapters,
		includeIncompleteUploads: includeIncompleteUploads,
		includeOrphans:           includeOrphans,
		preserveSQSVisibility:    preserveSQSVisibility,
		includeSQSSideRecords:    includeSQSSideRecords,
		renameCollisions:         renameCollisions,
		bundleJSONL:              bundleJSONL,
		clusterID:                clusterID,
	}, nil
}

// parseAdapterSet decodes a comma-separated adapter list.
// "all" is accepted as shorthand; unknown names surface as a hard
// error so a typo does not silently disable an adapter.
func parseAdapterSet(csv string) (backup.AdapterSet, error) {
	if csv == "" || csv == "all" {
		return backup.AllAdapters(), nil
	}
	var set backup.AdapterSet
	for _, raw := range strings.Split(csv, ",") {
		if err := applyAdapterName(strings.TrimSpace(raw), &set); err != nil {
			return backup.AdapterSet{}, err
		}
	}
	if set == (backup.AdapterSet{}) {
		return backup.AdapterSet{}, errors.New("--adapter selected zero adapters")
	}
	return set, nil
}

// applyAdapterName flips the matching field of set on. Extracted
// from parseAdapterSet so the parent function stays under the
// project's cyclop=10 ceiling.
func applyAdapterName(name string, set *backup.AdapterSet) error {
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
		// allow trailing comma
	default:
		return errors.Wrap(ErrUnknownAdapter, name)
	}
	return nil
}

// ErrUnknownAdapter is returned by parseAdapterSet when the CSV
// list contains a name we do not recognise. Surfaced as a typed
// sentinel so the CLI's flag-validation tests can assert on it
// without string matching.
var ErrUnknownAdapter = errors.New("unknown adapter name")

// ErrBundleModeInvalid is returned by parseBundleMode when the
// flag value is neither per-item nor jsonl.
var ErrBundleModeInvalid = errors.New("invalid --dynamodb-bundle-mode")

// parseBundleMode validates the --dynamodb-bundle-mode value. JSONL uses the
// encoder's default 64 MiB part size; the live producer additionally exposes a
// per-run size override.
func parseBundleMode(mode string) (bool, error) {
	switch mode {
	case "per-item", "":
		return false, nil
	case "jsonl":
		return true, nil
	default:
		return false, errors.Wrap(ErrBundleModeInvalid, mode)
	}
}

// decodeOne runs the dispatcher against the configured input and
// writes MANIFEST.json + CHECKSUMS into the output tree.
func decodeOne(cfg *config, logger *slog.Logger) error {
	if err := os.MkdirAll(cfg.outputRoot, defaultOutputDirMode); err != nil {
		return errors.WithStack(err)
	}
	in, err := os.Open(cfg.inputPath) //nolint:gosec // operator-supplied path
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = in.Close() }()

	res, err := backup.DecodeSnapshot(in, backup.DecodeOptions{
		OutRoot:                  cfg.outputRoot,
		Adapters:                 cfg.adapters,
		IncludeIncompleteUploads: cfg.includeIncompleteUploads,
		IncludeOrphans:           cfg.includeOrphans,
		RenameS3Collisions:       cfg.renameCollisions,
		PreserveSQSVisibility:    cfg.preserveSQSVisibility,
		IncludeSQSSideRecords:    cfg.includeSQSSideRecords,
		DynamoDBBundleJSONL:      cfg.bundleJSONL,
		WarnSink:                 warnSinkFor(logger),
	})
	if err != nil {
		return errors.Wrapf(err, "decode %s", cfg.inputPath)
	}
	logger.Info("snapshot decoded",
		"input", cfg.inputPath,
		"output", cfg.outputRoot,
		"entries", res.Counters.Total,
		"dynamodb", res.Counters.DynamoDB,
		"s3", res.Counters.S3,
		"redis", res.Counters.Redis,
		"sqs", res.Counters.SQS,
		"tombstone", res.Counters.Tombstone,
		"internal", res.Counters.Internal,
		"unknown", res.Counters.Unknown,
		"last_commit_ts", res.Header.LastCommitTS)

	if err := emitManifest(cfg, res); err != nil {
		return err
	}
	if err := backup.WriteChecksums(cfg.outputRoot); err != nil {
		return errors.Wrap(err, "write CHECKSUMS")
	}
	return nil
}

// emitManifest writes MANIFEST.json under cfg.outputRoot. Source
// metadata (FSM path and parsed snapshot_index) come from the CLI
// invocation; last_commit_ts comes from the snapshot header the
// dispatcher surfaces.
func emitManifest(cfg *config, res backup.DecodeResult) error {
	m := backup.NewPhase0SnapshotManifest(time.Now())
	m.ElastickvVersion = version
	m.ClusterID = cfg.clusterID
	m.LastCommitTS = res.Header.LastCommitTS
	if idx, err := backup.SnapshotIndexFromPath(cfg.inputPath); err == nil {
		m.SnapshotIndex = idx
	}
	m.Source = &backup.Source{FSMPath: cfg.inputPath}
	m.Adapters = populateAdapterScopes(cfg.adapters, res)
	m.Exclusions = &backup.Exclusions{
		IncludeIncompleteUploads: cfg.includeIncompleteUploads,
		IncludeOrphans:           cfg.includeOrphans,
		PreserveSQSVisibility:    cfg.preserveSQSVisibility,
		IncludeSQSSideRecords:    cfg.includeSQSSideRecords,
		RenameS3Collisions:       cfg.renameCollisions,
	}
	if cfg.bundleJSONL {
		m.DynamoDBLayout = backup.DynamoDBLayoutJSONL
	}
	// filepath.Join over `+ "/"` so the manifest path uses the
	// platform separator (gemini r1 medium on PR #810).
	out, err := os.Create(filepath.Join(cfg.outputRoot, "MANIFEST.json")) //nolint:gosec // operator-supplied path
	if err != nil {
		return errors.WithStack(err)
	}
	// Surface Close errors instead of swallowing them: on a
	// filesystem that delays writeback until Close (NFS, some
	// FUSE mounts), the write succeeds but the buffered-data
	// flush failure shows up only at Close time. Without this
	// surface, a manifest that was never durably written would
	// pass the cmd's error-return contract and the dump-tree
	// invariant ("MANIFEST.json is on disk") would silently
	// fail. Gemini r1 medium on PR #810. Sync runs before Close
	// so the explicit fsync is the authoritative durability
	// signal; Close's role is just to surface the kernel's
	// per-fd cleanup result.
	if err := backup.WriteManifest(out, m); err != nil {
		_ = out.Close()
		return errors.WithStack(err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return errors.WithStack(err)
	}
	if err := out.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// populateAdapterScopes returns a non-nil pointer for every enabled
// adapter. Scope-name enumeration (per-table, per-bucket, per-DB,
// per-queue) is a follow-up — Phase 0a's MANIFEST stamps an empty
// Adapter{} for each enabled adapter so the on-disk shape is
// already correct when scope enumeration lands.
//
// An adapter that was enabled but produced zero entries is still
// returned as a non-nil pointer with empty scope arrays. The
// "scope-everything-excluded" state is meaningful (operator passed
// --adapter dynamodb but the snapshot had no DynamoDB data) and
// distinct from "adapter not enabled".
func populateAdapterScopes(set backup.AdapterSet, _ backup.DecodeResult) *backup.Adapters {
	a := &backup.Adapters{}
	if set.DynamoDB {
		a.DynamoDB = &backup.Adapter{}
	}
	if set.S3 {
		a.S3 = &backup.Adapter{}
	}
	if set.Redis {
		a.Redis = &backup.Adapter{}
	}
	if set.SQS {
		a.SQS = &backup.Adapter{}
	}
	return a
}

// warnSinkFor adapts a *slog.Logger into the warn-sink callback
// the per-adapter encoders use. Event names ("redis_orphan_ttl",
// "ddb_orphan_items", ...) become the slog `event` field; the
// variadic key=value pairs are passed through verbatim so the
// stable adapter-side keys stay stable on the way out.
func warnSinkFor(logger *slog.Logger) func(event string, fields ...any) {
	return func(event string, fields ...any) {
		args := append([]any{"event", event}, fields...)
		logger.Warn("encoder warning", args...)
	}
}

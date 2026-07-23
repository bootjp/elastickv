// Command elastickv-snapshot-offload publishes and restores physical Raft/FSM
// snapshots through an object store.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/snapshotoffload"
	"github.com/cockroachdb/errors"
)

const (
	exitSuccess = 0
	exitUserErr = 1
	exitDataErr = 2

	commandPublish = "publish"
	commandRestore = "restore"
	storeLocal     = "local"
	storeS3        = "s3"
)

type storeFlags struct {
	storeKind                string
	localRoot                string
	s3Bucket                 string
	s3Region                 string
	s3Endpoint               string
	s3Profile                string
	s3PathStyle              bool
	s3ServerSideEncryption   string
	s3KMSKeyID               string
	s3DisableChecksumHeaders bool
}

type publishConfig struct {
	store         storeFlags
	dataDir       string
	prefix        string
	groupID       uint64
	sourceCluster string
	binaryVersion string
	spoolDir      string
}

type restoreConfig struct {
	store       storeFlags
	manifestKey string
	dataDir     string
	peerCSV     string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	code, err := run(context.Background(), os.Args[1:], os.Stdout, logger)
	if err != nil {
		logger.Error("elastickv-snapshot-offload", "err", err)
	}
	os.Exit(code)
}

func run(ctx context.Context, argv []string, stdout io.Writer, logger *slog.Logger) (int, error) {
	if len(argv) == 0 {
		return exitUserErr, errors.New("subcommand required: publish or restore")
	}
	switch argv[0] {
	case commandPublish:
		cfg, err := parsePublishFlags(argv[1:])
		if err != nil {
			return exitUserErr, err
		}
		if err := runPublish(ctx, cfg, stdout, logger); err != nil {
			return classifyError(err), err
		}
		return exitSuccess, nil
	case commandRestore:
		cfg, err := parseRestoreFlags(argv[1:])
		if err != nil {
			return exitUserErr, err
		}
		if err := runRestore(ctx, cfg, logger); err != nil {
			return classifyError(err), err
		}
		return exitSuccess, nil
	default:
		return exitUserErr, errors.Errorf("unknown subcommand %q", argv[0])
	}
}

func classifyError(err error) int {
	switch {
	case errors.Is(err, snapshotoffload.ErrIntegrity),
		errors.Is(err, snapshotoffload.ErrObjectNotFound),
		errors.Is(err, etcd.ErrExternalSnapshotRestoreInvalid),
		errors.Is(err, etcd.ErrExternalSnapshotRestoreSHA256):
		return exitDataErr
	default:
		return exitUserErr
	}
}

func parsePublishFlags(argv []string) (*publishConfig, error) {
	cfg := &publishConfig{}
	fs := flag.NewFlagSet("elastickv-snapshot-offload publish", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	addStoreFlags(fs, &cfg.store)
	fs.StringVar(&cfg.dataDir, "data-dir", "", "Source raft data directory containing a persisted snapshot (required)")
	fs.StringVar(&cfg.prefix, "prefix", "", "Object key prefix for published snapshot objects")
	fs.Uint64Var(&cfg.groupID, "group-id", 0, "Raft group ID recorded in the manifest")
	fs.StringVar(&cfg.sourceCluster, "source-cluster", "", "Source cluster identifier (required for group 0 manifests)")
	fs.StringVar(&cfg.binaryVersion, "binary-version", "", "Binary version recorded in the manifest")
	fs.StringVar(&cfg.spoolDir, "spool-dir", "", "Temporary spool directory for the payload stream")
	if err := fs.Parse(argv); err != nil {
		return nil, errors.WithStack(err)
	}
	if strings.TrimSpace(cfg.dataDir) == "" {
		return nil, errors.New("--data-dir is required")
	}
	if cfg.groupID == 0 && strings.TrimSpace(cfg.sourceCluster) == "" {
		return nil, errors.New("--source-cluster is required when --group-id is 0")
	}
	if err := validateStoreFlags(cfg.store); err != nil {
		return nil, err
	}
	return cfg, nil
}

func parseRestoreFlags(argv []string) (*restoreConfig, error) {
	cfg := &restoreConfig{}
	fs := flag.NewFlagSet("elastickv-snapshot-offload restore", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	addStoreFlags(fs, &cfg.store)
	fs.StringVar(&cfg.manifestKey, "manifest-key", "", "Object key of the snapshot manifest to restore (required)")
	fs.StringVar(&cfg.dataDir, "data-dir", "", "Fresh target raft data directory to create (required; must not already exist)")
	fs.StringVar(&cfg.peerCSV, "peers", "", "Comma-separated raft peers id=addr,id=addr (required)")
	if err := fs.Parse(argv); err != nil {
		return nil, errors.WithStack(err)
	}
	if strings.TrimSpace(cfg.manifestKey) == "" {
		return nil, errors.New("--manifest-key is required")
	}
	if strings.TrimSpace(cfg.dataDir) == "" {
		return nil, errors.New("--data-dir is required")
	}
	if strings.TrimSpace(cfg.peerCSV) == "" {
		return nil, errors.New("--peers is required")
	}
	if err := validateStoreFlags(cfg.store); err != nil {
		return nil, err
	}
	return cfg, nil
}

func addStoreFlags(fs *flag.FlagSet, cfg *storeFlags) {
	cfg.storeKind = storeLocal
	cfg.s3Region = "us-east-1"
	cfg.s3PathStyle = true
	fs.StringVar(&cfg.storeKind, "store", storeLocal, "Object store backend: local or s3")
	fs.StringVar(&cfg.localRoot, "local-root", "", "Local object store root when --store=local")
	fs.StringVar(&cfg.s3Bucket, "s3-bucket", "", "S3 bucket when --store=s3")
	fs.StringVar(&cfg.s3Region, "s3-region", cfg.s3Region, "S3 signing region")
	fs.StringVar(&cfg.s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint URL")
	fs.StringVar(&cfg.s3Profile, "s3-profile", "", "AWS shared config profile")
	fs.BoolVar(&cfg.s3PathStyle, "s3-path-style", cfg.s3PathStyle, "Use path-style S3 addressing")
	fs.StringVar(&cfg.s3ServerSideEncryption, "s3-sse", "", "Server-side encryption algorithm for uploaded objects, for example AES256 or aws:kms")
	fs.StringVar(&cfg.s3KMSKeyID, "s3-kms-key-id", "", "KMS key ID when --s3-sse=aws:kms")
	fs.BoolVar(&cfg.s3DisableChecksumHeaders, "s3-disable-checksum-headers", false, "Do not send S3 checksum headers; keep metadata and restore-time verification")
}

func validateStoreFlags(cfg storeFlags) error {
	switch cfg.storeKind {
	case storeLocal:
		if strings.TrimSpace(cfg.localRoot) == "" {
			return errors.New("--local-root is required when --store=local")
		}
	case storeS3:
		if strings.TrimSpace(cfg.s3Bucket) == "" {
			return errors.New("--s3-bucket is required when --store=s3")
		}
	default:
		return errors.Errorf("unknown --store %q", cfg.storeKind)
	}
	if strings.TrimSpace(cfg.s3KMSKeyID) != "" && strings.TrimSpace(cfg.s3ServerSideEncryption) == "" {
		return errors.New("--s3-kms-key-id requires --s3-sse")
	}
	return nil
}

func runPublish(ctx context.Context, cfg *publishConfig, stdout io.Writer, logger *slog.Logger) error {
	store, err := openObjectStore(ctx, cfg.store)
	if err != nil {
		return err
	}
	manifest, err := snapshotoffload.PublishPersistedSnapshot(ctx, snapshotoffload.PublishOptions{
		Store:         store,
		DataDir:       cfg.dataDir,
		Prefix:        cfg.prefix,
		GroupID:       cfg.groupID,
		SourceCluster: cfg.sourceCluster,
		BinaryVersion: cfg.binaryVersion,
		SpoolDir:      cfg.spoolDir,
	})
	if err != nil {
		return errors.Wrap(err, "publish persisted snapshot")
	}
	raw, _, err := manifest.MarshalCanonical()
	if err != nil {
		return errors.Wrap(err, "marshal snapshot manifest")
	}
	if _, err := stdout.Write(raw); err != nil {
		return errors.WithStack(err)
	}
	logger.Info("snapshot offload manifest published",
		"manifest_key", manifest.ManifestKey,
		"payload_key", manifest.Payload.Key,
		"group_id", manifest.GroupID,
		"index", manifest.SnapshotIndex,
		"term", manifest.SnapshotTerm,
	)
	return nil
}

func runRestore(ctx context.Context, cfg *restoreConfig, logger *slog.Logger) error {
	store, err := openObjectStore(ctx, cfg.store)
	if err != nil {
		return err
	}
	peers, err := parsePeers(cfg.peerCSV)
	if err != nil {
		return err
	}
	result, err := snapshotoffload.RestorePhysicalSnapshot(ctx, snapshotoffload.RestoreOptions{
		Store:       store,
		ManifestKey: cfg.manifestKey,
		DataDir:     cfg.dataDir,
		Peers:       peers,
	})
	if err != nil {
		return errors.Wrap(err, "restore physical snapshot")
	}
	logger.Info("snapshot offload restore data dir prepared",
		"data_dir", result.DataDir,
		"fsm", result.FSMPath,
		"snap", result.SnapPath,
		"crc32c", fmt.Sprintf("%08x", result.CRC32C),
		"payload_sha256", result.PayloadSHA256,
		"payload_bytes", result.PayloadBytes,
		"peers", result.Peers,
	)
	return nil
}

func openObjectStore(ctx context.Context, cfg storeFlags) (snapshotoffload.ObjectStore, error) {
	switch cfg.storeKind {
	case storeLocal:
		store, err := snapshotoffload.NewLocalStore(cfg.localRoot)
		if err != nil {
			return nil, errors.Wrap(err, "open local object store")
		}
		return store, nil
	case storeS3:
		store, err := snapshotoffload.NewS3Store(ctx, snapshotoffload.S3StoreConfig{
			Bucket:                 cfg.s3Bucket,
			Region:                 cfg.s3Region,
			Endpoint:               cfg.s3Endpoint,
			Profile:                cfg.s3Profile,
			ForcePathStyle:         cfg.s3PathStyle,
			ServerSideEncryption:   cfg.s3ServerSideEncryption,
			SSEKMSKeyID:            cfg.s3KMSKeyID,
			DisableChecksumHeaders: cfg.s3DisableChecksumHeaders,
		})
		if err != nil {
			return nil, errors.Wrap(err, "open s3 object store")
		}
		return store, nil
	default:
		return nil, errors.Errorf("unknown --store %q", cfg.storeKind)
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

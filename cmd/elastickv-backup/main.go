// Command elastickv-backup creates a point-in-time logical dump from a running
// elastickv cluster.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/backup"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	maxAdminTokenFileBytes = 4 << 10
	defaultBackupTTLMS     = 1_800_000
	defaultRPCDeadline     = 30 * time.Second
)

var version = "dev"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "elastickv-backup: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	address    string
	outputRoot string
	format     string
	tokenFile  string
	clusterID  string

	adapters backup.AdapterSet
	scopes   []backup.Scope
	ttl      time.Duration

	includeIncompleteUploads bool
	includeOrphans           bool
	preserveSQSVisibility    bool
	includeSQSSideRecords    bool
	renameCollisions         bool
	bundleJSONL              bool
	bundleSizeBytes          int64

	beginTimeout time.Duration
	endTimeout   time.Duration
	tlsCAFile    string
	tlsServer    string
	tlsSkip      bool
}

func run(argv []string, stdout, stderr io.Writer) (retErr error) {
	cfg, err := parseFlags(argv)
	if err != nil {
		return errors.Wrap(err, "run live backup")
	}
	token, err := loadAdminToken(cfg.tokenFile)
	if err != nil {
		return err
	}
	creds, err := loadTransportCredentials(cfg.tlsCAFile, cfg.tlsServer, cfg.tlsSkip)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(cfg.address,
		grpc.WithTransportCredentials(creds),
		internalutil.GRPCCallOptions(),
	)
	if err != nil {
		return errors.Wrap(err, "dial backup admin endpoint")
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, errors.WithStack(conn.Close()))
	}()

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	opts := liveBackupOptions(cfg)
	opts.WarnSink = warningSink(logger)
	result, err := backup.RunLiveBackup(ctx, &grpcLiveBackupRPC{client: pb.NewAdminClient(conn), token: token}, opts)
	if err != nil {
		return errors.Wrap(err, "create live backup")
	}
	if err := writeArchive(cfg, stdout); err != nil {
		return err
	}
	logger.Info("live backup complete",
		"output", cfg.outputRoot,
		"read_ts", result.ReadTS,
		"scopes", len(result.Scopes),
		"records", result.Counters.Total,
		"pin_renewals_total", result.PinRenewals,
		"format", cfg.format,
	)
	return nil
}

func parseFlags(argv []string) (*config, error) {
	if len(argv) == 0 || argv[0] != "dump" {
		return nil, errors.New("usage: elastickv-backup dump [flags]")
	}
	values, err := parseDumpFlagValues(argv[1:])
	if err != nil {
		return nil, err
	}
	return buildDumpConfig(values)
}

type dumpFlagValues struct {
	cfg        config
	adapterCSV string
	bundleMode string
	bundleSize string
	checksums  string
	ttlMS      uint64
	scopes     stringListFlag
}

func parseDumpFlagValues(argv []string) (*dumpFlagValues, error) {
	fs := flag.NewFlagSet("elastickv-backup dump", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	values := &dumpFlagValues{}
	bindDumpFlags(fs, values)
	if err := fs.Parse(argv); err != nil {
		return nil, errors.WithStack(err)
	}
	if len(fs.Args()) != 0 {
		return nil, errors.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	return values, nil
}

func bindDumpFlags(fs *flag.FlagSet, values *dumpFlagValues) {
	cfg := &values.cfg
	fs.StringVar(&cfg.address, "address", "", "Admin gRPC address")
	fs.StringVar(&cfg.outputRoot, "output-dir", "", "New destination directory")
	fs.StringVar(&cfg.format, "output-format", "directory", "directory, tar, or tar+zstd")
	fs.StringVar(&cfg.tokenFile, "admin-token-file", "", "Bearer-token file")
	fs.StringVar(&cfg.clusterID, "cluster-id", "", "Cluster identifier for MANIFEST.json")
	fs.StringVar(&values.adapterCSV, "adapter", "dynamodb,s3,redis,sqs", "Comma-separated adapter set")
	fs.Var(&values.scopes, "scope", "Adapter scope selection, for example dynamodb=orders,users")
	fs.Uint64Var(&values.ttlMS, "ttl-ms", defaultBackupTTLMS, "Requested pin TTL in milliseconds")
	fs.DurationVar(&cfg.beginTimeout, "begin-backup-deadline", defaultRPCDeadline, "Client deadline for BeginBackup")
	fs.DurationVar(&cfg.endTimeout, "end-backup-deadline", defaultRPCDeadline, "Cleanup deadline for EndBackup")
	fs.BoolVar(&cfg.includeIncompleteUploads, "include-incomplete-uploads", false, "Include in-flight S3 multipart uploads")
	fs.BoolVar(&cfg.includeOrphans, "include-orphans", false, "Include S3 orphan blobs")
	fs.BoolVar(&cfg.preserveSQSVisibility, "preserve-sqs-visibility", false, "Preserve SQS visibility state")
	fs.BoolVar(&cfg.includeSQSSideRecords, "include-sqs-side-records", false, "Include SQS internal side records")
	fs.BoolVar(&cfg.renameCollisions, "rename-collisions", false, "Rename colliding S3 logical paths")
	fs.StringVar(&values.checksums, "checksums", "sha256", "Checksum algorithm (sha256)")
	fs.StringVar(&values.bundleMode, "dynamodb-bundle-mode", "per-item", "per-item or jsonl")
	fs.StringVar(&values.bundleSize, "dynamodb-bundle-size", "64MiB", "Maximum JSONL part size")
	fs.StringVar(&cfg.tlsCAFile, "tls-ca-cert-file", "", "PEM CA file for the Admin endpoint")
	fs.StringVar(&cfg.tlsServer, "tls-server-name", "", "Expected TLS server name")
	fs.BoolVar(&cfg.tlsSkip, "tls-insecure-skip-verify", false, "Use TLS without certificate verification")
}

func buildDumpConfig(values *dumpFlagValues) (*config, error) {
	cfg := values.cfg
	var err error
	if err := validateDumpFlagValues(values); err != nil {
		return nil, err
	}
	cfg.ttl, err = dumpTTL(values.ttlMS)
	if err != nil {
		return nil, err
	}
	cfg.adapters, err = parseAdapterSet(values.adapterCSV)
	if err != nil {
		return nil, err
	}
	cfg.scopes, err = parseScopes(values.scopes)
	if err != nil {
		return nil, err
	}
	if err := validateRequestedScopeAdapters(cfg.adapters, cfg.scopes); err != nil {
		return nil, err
	}
	if err := populateDynamoDBBundleConfig(&cfg, values); err != nil {
		return nil, err
	}
	if err := backup.ValidateLiveBackupRestoreCompatibility(liveBackupOptions(&cfg)); err != nil {
		return nil, errors.Wrap(err, "validate native restore compatibility")
	}
	return &cfg, nil
}

func liveBackupOptions(cfg *config) backup.LiveBackupOptions {
	return backup.LiveBackupOptions{
		OutputRoot:               cfg.outputRoot,
		Adapters:                 cfg.adapters,
		Scopes:                   cfg.scopes,
		TTL:                      cfg.ttl,
		IncludeIncompleteUploads: cfg.includeIncompleteUploads,
		IncludeOrphans:           cfg.includeOrphans,
		PreserveSQSVisibility:    cfg.preserveSQSVisibility,
		IncludeSQSSideRecords:    cfg.includeSQSSideRecords,
		RenameS3Collisions:       cfg.renameCollisions,
		DynamoDBBundleJSONL:      cfg.bundleJSONL,
		DynamoDBBundleSizeBytes:  cfg.bundleSizeBytes,
		ElastickvVersion:         version,
		ClusterID:                cfg.clusterID,
		BeginTimeout:             cfg.beginTimeout,
		EndTimeout:               cfg.endTimeout,
	}
}

func validateDumpFlagValues(values *dumpFlagValues) error {
	if values.cfg.address == "" || values.cfg.outputRoot == "" {
		return errors.New("--address and --output-dir are required")
	}
	if values.checksums != backup.ChecksumAlgorithmSHA256 {
		return errors.Errorf("unsupported --checksums %q", values.checksums)
	}
	return validateOutputFormat(values.cfg.format)
}

func dumpTTL(ttlMS uint64) (time.Duration, error) {
	if ttlMS == 0 || ttlMS > uint64((time.Duration(1<<63-1))/time.Millisecond) {
		return 0, errors.New("--ttl-ms is outside the supported duration range")
	}
	return time.Duration(ttlMS) * time.Millisecond, nil //nolint:gosec // upper bound checked above.
}

func populateDynamoDBBundleConfig(cfg *config, values *dumpFlagValues) error {
	var err error
	cfg.bundleJSONL, err = parseBundleMode(values.bundleMode)
	if err != nil {
		return err
	}
	cfg.bundleSizeBytes, err = parseByteSize(values.bundleSize)
	return errors.Wrap(err, "--dynamodb-bundle-size")
}

type stringListFlag []string

func (f *stringListFlag) String() string { return strings.Join(*f, ";") }
func (f *stringListFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func parseAdapterSet(csv string) (backup.AdapterSet, error) {
	if csv == "" || csv == "all" {
		return backup.AllAdapters(), nil
	}
	var set backup.AdapterSet
	for _, raw := range strings.Split(csv, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if err := enableAdapter(&set, name); err != nil {
			return backup.AdapterSet{}, err
		}
	}
	if set == (backup.AdapterSet{}) {
		return backup.AdapterSet{}, errors.New("--adapter selected zero adapters")
	}
	return set, nil
}

func enableAdapter(set *backup.AdapterSet, name string) error {
	switch name {
	case "dynamodb":
		set.DynamoDB = true
	case "s3":
		set.S3 = true
	case "redis":
		set.Redis = true
	case "sqs":
		set.SQS = true
	default:
		return errors.Errorf("unknown adapter %q", name)
	}
	return nil
}

func parseScopes(flags []string) ([]backup.Scope, error) {
	seen := make(map[backup.Scope]struct{})
	for _, raw := range flags {
		adapter, names, ok := strings.Cut(raw, "=")
		adapter = strings.TrimSpace(adapter)
		if !ok || adapter == "" || names == "" {
			return nil, errors.Errorf("invalid --scope %q", raw)
		}
		for _, name := range strings.Split(names, ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				return nil, errors.Errorf("invalid empty scope in %q", raw)
			}
			seen[backup.Scope{Adapter: adapter, Name: name}] = struct{}{}
		}
	}
	out := make([]backup.Scope, 0, len(seen))
	for scope := range seen {
		out = append(out, scope)
	}
	sortScopes(out)
	return out, nil
}

func sortScopes(scopes []backup.Scope) {
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].Adapter != scopes[j].Adapter {
			return scopes[i].Adapter < scopes[j].Adapter
		}
		return scopes[i].Name < scopes[j].Name
	})
}

func validateRequestedScopeAdapters(set backup.AdapterSet, scopes []backup.Scope) error {
	for _, scope := range scopes {
		enabled := (scope.Adapter == "dynamodb" && set.DynamoDB) ||
			(scope.Adapter == "s3" && set.S3) ||
			(scope.Adapter == "redis" && set.Redis) ||
			(scope.Adapter == "sqs" && set.SQS)
		if !enabled {
			return errors.Errorf("--scope %s is not enabled by --adapter", scope)
		}
	}
	return nil
}

func parseBundleMode(mode string) (bool, error) {
	switch mode {
	case "", "per-item":
		return false, nil
	case "jsonl":
		return true, nil
	default:
		return false, errors.Errorf("invalid --dynamodb-bundle-mode %q", mode)
	}
}

func parseByteSize(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	multiplier := int64(1)
	for _, unit := range []struct {
		suffix string
		scale  int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}} {
		if strings.HasSuffix(raw, unit.suffix) {
			raw = strings.TrimSpace(strings.TrimSuffix(raw, unit.suffix))
			multiplier = unit.scale
			break
		}
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 || value > (1<<63-1)/multiplier {
		return 0, errors.Errorf("invalid byte size %q", raw)
	}
	return value * multiplier, nil
}

func validateOutputFormat(format string) error {
	switch format {
	case "directory", "tar", "tar+zstd":
		return nil
	default:
		return errors.Errorf("unsupported --output-format %q", format)
	}
}

func writeArchive(cfg *config, stdout io.Writer) error {
	switch cfg.format {
	case "directory":
		return nil
	case "tar":
		return errors.Wrap(backup.PackDumpTree(cfg.outputRoot, stdout, backup.ArchiveCompressionNone), "write tar backup")
	case "tar+zstd":
		return errors.Wrap(backup.PackDumpTree(cfg.outputRoot, stdout, backup.ArchiveCompressionZstd), "write zstd tar backup")
	default:
		return errors.Errorf("unsupported output format %q", cfg.format)
	}
}

func loadAdminToken(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	f, err := os.Open(path) //nolint:gosec // operator-selected token file
	if err != nil {
		return "", errors.WithStack(err)
	}
	body, err := io.ReadAll(io.LimitReader(f, maxAdminTokenFileBytes+1))
	if err != nil {
		_ = f.Close()
		return "", errors.WithStack(err)
	}
	if err := f.Close(); err != nil {
		return "", errors.WithStack(err)
	}
	if len(body) > maxAdminTokenFileBytes {
		return "", errors.New("admin token file exceeds 4 KiB")
	}
	token := strings.TrimSpace(string(body))
	if token == "" {
		return "", errors.New("admin token file is empty")
	}
	return token, nil
}

func loadTransportCredentials(caFile, serverName string, skipVerify bool) (credentials.TransportCredentials, error) {
	tlsRequested := caFile != "" || skipVerify
	if err := validateTLSFlagCombination(tlsRequested, caFile, serverName, skipVerify); err != nil {
		return nil, err
	}
	if !tlsRequested {
		return insecure.NewCredentials(), nil
	}
	conf := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName, InsecureSkipVerify: skipVerify} //nolint:gosec // explicit development flag.
	if caFile == "" {
		return credentials.NewTLS(conf), nil
	}
	pool, err := loadTLSCAPool(caFile)
	if err != nil {
		return nil, err
	}
	conf.RootCAs = pool
	return credentials.NewTLS(conf), nil
}

func validateTLSFlagCombination(tlsRequested bool, caFile, serverName string, skipVerify bool) error {
	switch {
	case !tlsRequested && serverName != "":
		return errors.New("--tls-server-name requires --tls-ca-cert-file or --tls-insecure-skip-verify")
	case caFile != "" && skipVerify:
		return errors.New("--tls-ca-cert-file and --tls-insecure-skip-verify are mutually exclusive")
	default:
		return nil
	}
}

func loadTLSCAPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile) //nolint:gosec // operator-selected CA file
	if err != nil {
		return nil, errors.WithStack(err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("TLS CA file contains no certificates")
	}
	return pool, nil
}

func warningSink(logger *slog.Logger) func(string, ...any) {
	return func(event string, fields ...any) {
		args := append([]any{"event", event}, fields...)
		logger.Warn("backup warning", args...)
	}
}

type grpcLiveBackupRPC struct {
	client pb.AdminClient
	token  string
}

func (r *grpcLiveBackupRPC) outgoing(ctx context.Context) context.Context {
	if r.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+r.token)
}

func (r *grpcLiveBackupRPC) BeginBackup(ctx context.Context, req *pb.BeginBackupRequest) (*pb.BeginBackupResponse, error) {
	resp, err := r.client.BeginBackup(r.outgoing(ctx), req)
	return resp, errors.Wrap(err, "BeginBackup RPC")
}

func (r *grpcLiveBackupRPC) RenewBackup(ctx context.Context, req *pb.RenewBackupRequest) (*pb.RenewBackupResponse, error) {
	resp, err := r.client.RenewBackup(r.outgoing(ctx), req)
	return resp, errors.Wrap(err, "RenewBackup RPC")
}

func (r *grpcLiveBackupRPC) EndBackup(ctx context.Context, req *pb.EndBackupRequest) (*pb.EndBackupResponse, error) {
	resp, err := r.client.EndBackup(r.outgoing(ctx), req)
	return resp, errors.Wrap(err, "EndBackup RPC")
}

func (r *grpcLiveBackupRPC) ListAdaptersAndScopes(ctx context.Context, req *pb.ListAdaptersAndScopesRequest) (*pb.ListAdaptersAndScopesResponse, error) {
	resp, err := r.client.ListAdaptersAndScopes(r.outgoing(ctx), req)
	return resp, errors.Wrap(err, "ListAdaptersAndScopes RPC")
}

func (r *grpcLiveBackupRPC) StreamBackup(ctx context.Context, req *pb.StreamBackupRequest, consume func(*pb.BackupKV) error) error {
	stream, err := r.client.StreamBackup(r.outgoing(ctx), req)
	if err != nil {
		return errors.Wrap(err, "StreamBackup RPC")
	}
	for {
		record, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "receive StreamBackup record")
		}
		if err := consume(record); err != nil {
			return err
		}
	}
}

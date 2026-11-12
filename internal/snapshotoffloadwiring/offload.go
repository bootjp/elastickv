package snapshotoffloadwiring

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/bootjp/elastickv/internal/snapshotoffload"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	snapshotOffloadMaxRedirects  = 10
	snapshotOffloadStaleSpoolAge = 24 * time.Hour
	snapshotOffloadSpoolDirPerm  = 0o755
)

// Physical snapshot object offload (design doc §4 / §7). Opt-in: the
// whole subsystem stays dormant unless --snapshotOffloadBucket (S3) or
// --snapshotOffloadLocalDir (filesystem) is set.
//
// The design requires that only a group's current leader publishes, so
// each group contributes both a cheap pre-check and a pre-commit
// leadership re-verification; the scheduler bounds the latter itself.
type Flags struct {
	Bucket         string
	LocalDir       string
	Prefix         string
	Region         string
	Endpoint       string
	Profile        string
	ForcePathStyle bool
	SSE            string
	SSEKMSKeyID    string
	Interval       time.Duration
	Jitter         time.Duration
	Concurrency    int
	PublishTimeout time.Duration
	SpoolDir       string
	SourceCluster  string
	// AllowInsecureEndpoint is the narrow development opt-in for a
	// plaintext offload endpoint. Off by default, and named so it cannot
	// be mistaken for a tuning knob.
	AllowInsecureEndpoint bool
	Retention             RetentionFlags
}

func (f *Flags) Register(fs *flag.FlagSet) {
	fs.StringVar(&f.Bucket, "snapshotOffloadBucket", "",
		"S3 bucket for physical snapshot offload; empty to disable")
	fs.StringVar(&f.LocalDir, "snapshotOffloadLocalDir", "",
		"filesystem root for physical snapshot offload; an alternative to --snapshotOffloadBucket, mainly for testing")
	fs.StringVar(&f.Prefix, "snapshotOffloadPrefix", "",
		"key prefix below which snapshot artifacts are written")
	fs.StringVar(&f.Region, "snapshotOffloadRegion", "",
		"AWS region for the snapshot offload bucket")
	fs.StringVar(&f.Endpoint, "snapshotOffloadEndpoint", "",
		"custom S3 endpoint for snapshot offload; empty uses the AWS default")
	fs.StringVar(&f.Profile, "snapshotOffloadProfile", "",
		"shared-credentials profile for snapshot offload")
	fs.BoolVar(&f.ForcePathStyle, "snapshotOffloadForcePathStyle", false,
		"use path-style addressing for the snapshot offload endpoint")
	fs.StringVar(&f.SSE, "snapshotOffloadServerSideEncryption", "",
		"server-side encryption mode for snapshot objects (AES256 or aws:kms)")
	fs.StringVar(&f.SSEKMSKeyID, "snapshotOffloadSSEKMSKeyId", "",
		"KMS key ARN when --snapshotOffloadServerSideEncryption is aws:kms")
	fs.DurationVar(&f.Interval, "snapshotOffloadInterval", snapshotoffload.DefaultSchedulerInterval,
		"how often to scan local groups for a publishable snapshot")
	fs.DurationVar(&f.Jitter, "snapshotOffloadJitter", 0,
		"random spread applied to the offload schedule; zero uses a quarter of the interval")
	fs.IntVar(&f.Concurrency, "snapshotOffloadConcurrency", snapshotoffload.DefaultSchedulerConcurrency,
		"maximum concurrent snapshot uploads for this process")
	fs.DurationVar(&f.PublishTimeout, "snapshotOffloadPublishTimeout", snapshotoffload.DefaultSchedulerPublishTimeout,
		"deadline for each physical snapshot publication")
	fs.StringVar(&f.SpoolDir, "snapshotOffloadSpoolDir", "",
		"directory for snapshot spool files; empty uses the data dir's filesystem")
	fs.StringVar(&f.SourceCluster, "snapshotOffloadSourceCluster", "",
		"source cluster identity recorded in every manifest; required when offload is enabled")
	fs.BoolVar(&f.AllowInsecureEndpoint, "snapshotOffloadAllowInsecureEndpoint", false,
		"allow a plaintext http:// --snapshotOffloadEndpoint. DEVELOPMENT ONLY: snapshot "+
			"payloads and session credentials would cross the network unencrypted.")
	f.Retention.register(fs)
}

// rejectPlaintextOffloadEndpoint refuses an http:// endpoint unless the
// operator explicitly opted in.
//
// The design and the runbook both require TLS for the external bucket, and
// this path carries whole snapshot payloads plus the session credentials used
// to write them. An endpoint that silently downgraded to plaintext is the kind
// of misconfiguration that is invisible until someone captures the traffic, so
// it fails startup instead.
func rejectPlaintextOffloadEndpoint(f Flags) error {
	endpoint := strings.TrimSpace(f.Endpoint)
	if endpoint == "" {
		return nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return errors.Wrapf(err, "parse --snapshotOffloadEndpoint %q", endpoint)
	}
	// A scheme-less host:port is ambiguous rather than known-plaintext, but
	// the AWS SDK resolves it as http, so it is refused too.
	allowedScheme := strings.EqualFold(parsed.Scheme, "https") ||
		(f.AllowInsecureEndpoint && strings.EqualFold(parsed.Scheme, "http"))
	if !allowedScheme || parsed.Hostname() == "" || parsed.Opaque != "" {
		return errors.Wrapf(snapshotoffload.ErrInvalidOptions,
			"--snapshotOffloadEndpoint %q is not a valid https URL; snapshot payloads and credentials "+
				"would cross the network unencrypted. Pass an https:// endpoint, or "+
				"--snapshotOffloadAllowInsecureEndpoint for local development only",
			endpoint)
	}
	return nil
}

func snapshotOffloadHTTPClient(base aws.HTTPClient, allowInsecure bool) (aws.HTTPClient, error) {
	var client http.Client
	switch resolved := base.(type) {
	case nil:
		client.Transport = awshttp.NewBuildableClient().GetTransport()
	case *awshttp.BuildableClient:
		client.Transport = resolved.GetTransport()
		client.Timeout = resolved.GetTimeout()
	case *http.Client:
		client = *resolved
	default:
		return nil, errors.Wrapf(snapshotoffload.ErrInvalidOptions,
			"unsupported snapshot offload SDK http client %T", base)
	}
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	client.Transport = snapshotOffloadHTTPSTransport{next: transport, allowInsecure: allowInsecure}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return checkSnapshotOffloadRedirect(req, via, allowInsecure)
	}
	return &client, nil
}

type snapshotOffloadHTTPSTransport struct {
	next          http.RoundTripper
	allowInsecure bool
}

func (t snapshotOffloadHTTPSTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !snapshotOffloadRequestAllowed(req, t.allowInsecure) {
		return nil, errors.Wrap(snapshotoffload.ErrInvalidOptions,
			"snapshot offload request must use https")
	}
	if t.next == nil {
		return nil, errors.Wrap(snapshotoffload.ErrInvalidOptions,
			"snapshot offload transport is required")
	}
	response, err := t.next.RoundTrip(req)
	return response, errors.Wrap(err, "snapshot offload request")
}

func snapshotOffloadRequestAllowed(req *http.Request, allowInsecure bool) bool {
	if req == nil || req.URL == nil {
		return false
	}
	return strings.EqualFold(req.URL.Scheme, "https") ||
		(allowInsecure && strings.EqualFold(req.URL.Scheme, "http"))
}

func checkSnapshotOffloadRedirect(req *http.Request, via []*http.Request, allowInsecure bool) error {
	if !snapshotOffloadRequestAllowed(req, allowInsecure) {
		return errors.Wrap(snapshotoffload.ErrInvalidOptions,
			"snapshot offload redirect must preserve https")
	}
	if len(via) >= snapshotOffloadMaxRedirects {
		return errors.New("snapshot offload stopped after 10 redirects")
	}
	if req.Response == nil {
		return errors.New("snapshot offload redirect response is required")
	}
	if req.Response.StatusCode != http.StatusTemporaryRedirect &&
		req.Response.StatusCode != http.StatusPermanentRedirect {
		return http.ErrUseLastResponse
	}
	if len(via) > 0 && !strings.EqualFold(via[len(via)-1].URL.Host, req.URL.Host) {
		return errors.Wrap(snapshotoffload.ErrInvalidOptions,
			"snapshot offload redirect must preserve the endpoint host")
	}
	return nil
}

func validateSnapshotOffloadPrefix(prefix string) error {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return nil
	}
	cleaned := path.Clean(prefix)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return errors.Wrapf(snapshotoffload.ErrInvalidOptions,
			"invalid --snapshotOffloadPrefix %q", prefix)
	}
	return nil
}

func validateSnapshotOffloadSpoolDir(f Flags) error {
	dir := strings.TrimSpace(f.SpoolDir)
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, snapshotOffloadSpoolDirPerm); err != nil {
		return errors.Wrap(err, "snapshot offload: create spool dir")
	}
	info, err := os.Stat(dir)
	if err != nil {
		return errors.Wrap(err, "snapshot offload: stat spool dir")
	}
	if !info.IsDir() {
		return errors.Wrapf(snapshotoffload.ErrInvalidOptions,
			"--snapshotOffloadSpoolDir %q is not a directory", dir)
	}
	return nil
}

// snapshotOffloadEnabled reports whether the operator configured a
// destination. Checked before any other offload flag is validated so a
// node that never opts in cannot fail startup on offload config.
func snapshotOffloadEnabled(f Flags) bool {
	return strings.TrimSpace(f.Bucket) != "" ||
		strings.TrimSpace(f.LocalDir) != ""
}

// buildSnapshotOffloadStore constructs the configured object store.
//
// Bucket and local dir are mutually exclusive: accepting both would
// leave which destination actually receives the artifacts ambiguous,
// and a backup written to the wrong place is discovered only when a
// restore is attempted.
func buildSnapshotOffloadStore(ctx context.Context, f Flags) (snapshotOffloadStore, error) {
	bucket := strings.TrimSpace(f.Bucket)
	localDir := strings.TrimSpace(f.LocalDir)
	if bucket != "" && localDir != "" {
		return nil, errors.Wrap(snapshotoffload.ErrInvalidOptions,
			"--snapshotOffloadBucket and --snapshotOffloadLocalDir are mutually exclusive")
	}
	if localDir != "" {
		store, err := snapshotoffload.NewLocalStore(localDir)
		if err != nil {
			return nil, errors.Wrap(err, "snapshot offload: local store")
		}
		return store, nil
	}
	if err := rejectPlaintextOffloadEndpoint(f); err != nil {
		return nil, err
	}
	store, err := snapshotoffload.NewS3Store(ctx, snapshotoffload.S3StoreConfig{
		Bucket:               bucket,
		Region:               strings.TrimSpace(f.Region),
		Endpoint:             strings.TrimSpace(f.Endpoint),
		Profile:              strings.TrimSpace(f.Profile),
		ForcePathStyle:       f.ForcePathStyle,
		ServerSideEncryption: strings.TrimSpace(f.SSE),
		SSEKMSKeyID:          strings.TrimSpace(f.SSEKMSKeyID),
		HTTPClientDecorator: func(base aws.HTTPClient) (aws.HTTPClient, error) {
			return snapshotOffloadHTTPClient(base, f.AllowInsecureEndpoint)
		},
		AllowVersionedBucketWithLifecycle: f.Retention.AllowVersionedLifecycle,
	})
	if err != nil {
		return nil, errors.Wrap(err, "snapshot offload: s3 store")
	}
	return store, nil
}

// cleanStaleOffloadSpool removes leftover spool files from every directory
// this process would publish through.
//
// Files younger than snapshotOffloadStaleSpoolAge may belong to another
// process sharing the configured spool directory and are left untouched.
func cleanStaleOffloadSpool(
	f Flags,
	groups []snapshotoffload.OffloadGroup,
	logger *slog.Logger,
) {
	configured := strings.TrimSpace(f.SpoolDir)
	seen := make(map[string]struct{})
	cutoff := time.Now().Add(-snapshotOffloadStaleSpoolAge)
	for _, group := range groups {
		dir := snapshotoffload.SpoolDirFor(configured, group.DataDir)
		if _, done := seen[dir]; done {
			continue
		}
		seen[dir] = struct{}{}
		removed, err := snapshotoffload.CleanStaleSpoolFiles(dir, cutoff)
		if err != nil {
			logger.Warn("snapshot offload: could not clean stale spool files",
				slog.String("spool_dir", dir), slog.Any("err", err))
		}
		if len(removed) > 0 {
			logger.Info("snapshot offload: removed stale spool files",
				slog.String("spool_dir", dir), slog.Int("files", len(removed)))
		}
	}
}

// Start wires and starts the scheduler when offload is
// configured. It returns an error rather than logging and continuing:
// an operator who configured a backup destination and got no backups
// is worse off than one whose node refused to start.
func Start(
	ctx context.Context,
	eg *errgroup.Group,
	f Flags,
	groups []snapshotoffload.OffloadGroup,
	version string,
	observer snapshotoffload.SchedulerObserver,
	logger *slog.Logger,
) (retErr error) {
	if f.Retention.Enabled && !snapshotOffloadEnabled(f) {
		return errors.Wrap(snapshotoffload.ErrInvalidOptions, "snapshot retention requires an offload destination")
	}
	if !snapshotOffloadEnabled(f) {
		return nil
	}
	if err := validateSnapshotOffloadPrefix(f.Prefix); err != nil {
		return err
	}
	if err := validateSnapshotOffloadSpoolDir(f); err != nil {
		return err
	}
	store, err := buildSnapshotOffloadStore(ctx, f)
	if err != nil {
		return err
	}
	handedOff := false
	defer func() {
		if !handedOff {
			retErr = errors.CombineErrors(retErr, closeSnapshotOffloadStore(store))
		}
	}()

	scheduler, err := snapshotoffload.NewScheduler(
		store,
		groups,
		strings.TrimSpace(f.Prefix),
		strings.TrimSpace(f.SourceCluster),
		version,
		snapshotOffloadSchedulerOptions(f, observer, logger)...,
	)
	if err != nil {
		return errors.Wrap(err, "snapshot offload: scheduler")
	}
	retention, err := buildSnapshotOffloadRetention(ctx, f, store, logger)
	if err != nil {
		return err
	}

	// Clean only aged leftovers before starting this process's publishers.
	cleanStaleOffloadSpool(f, groups, logger)

	logger.Info("snapshot offload enabled",
		slog.Int("groups", len(groups)),
		slog.Duration("interval", f.Interval),
		slog.Int("concurrency", f.Concurrency))

	eg.Go(func() error {
		// Run returns only on context cancellation; a failing group is
		// retried on the next tick rather than tearing the process
		// down, because an object-store outage must not stop serving.
		return runSnapshotOffloadWorkers(ctx, store, scheduler, retention)
	})
	handedOff = true
	return nil
}

func snapshotOffloadSchedulerOptions(f Flags, observer snapshotoffload.SchedulerObserver, logger *slog.Logger) []snapshotoffload.SchedulerOption {
	opts := []snapshotoffload.SchedulerOption{
		snapshotoffload.WithSchedulerInterval(f.Interval),
		snapshotoffload.WithSchedulerConcurrency(f.Concurrency),
		snapshotoffload.WithSchedulerPublishTimeout(f.PublishTimeout),
		snapshotoffload.WithSchedulerObserver(observer),
		snapshotoffload.WithSchedulerLogger(logger),
	}
	if f.Jitter > 0 {
		opts = append(opts, snapshotoffload.WithSchedulerJitter(f.Jitter))
	}
	if dir := strings.TrimSpace(f.SpoolDir); dir != "" {
		opts = append(opts, snapshotoffload.WithSchedulerSpoolDir(dir))
	}
	return opts
}

package main

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
	"github.com/bootjp/elastickv/internal/raftengine"
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
type snapshotOffloadFlags struct {
	bucket         string
	localDir       string
	prefix         string
	region         string
	endpoint       string
	profile        string
	forcePathStyle bool
	sse            string
	sseKMSKeyID    string
	interval       time.Duration
	jitter         time.Duration
	concurrency    int
	publishTimeout time.Duration
	spoolDir       string
	sourceCluster  string
	// allowInsecureEndpoint is the narrow development opt-in for a
	// plaintext offload endpoint. Off by default, and named so it cannot
	// be mistaken for a tuning knob.
	allowInsecureEndpoint bool
	retention             snapshotRetentionFlags
}

func (f *snapshotOffloadFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.bucket, "snapshotOffloadBucket", "",
		"S3 bucket for physical snapshot offload; empty to disable")
	fs.StringVar(&f.localDir, "snapshotOffloadLocalDir", "",
		"filesystem root for physical snapshot offload; an alternative to --snapshotOffloadBucket, mainly for testing")
	fs.StringVar(&f.prefix, "snapshotOffloadPrefix", "",
		"key prefix below which snapshot artifacts are written")
	fs.StringVar(&f.region, "snapshotOffloadRegion", "",
		"AWS region for the snapshot offload bucket")
	fs.StringVar(&f.endpoint, "snapshotOffloadEndpoint", "",
		"custom S3 endpoint for snapshot offload; empty uses the AWS default")
	fs.StringVar(&f.profile, "snapshotOffloadProfile", "",
		"shared-credentials profile for snapshot offload")
	fs.BoolVar(&f.forcePathStyle, "snapshotOffloadForcePathStyle", false,
		"use path-style addressing for the snapshot offload endpoint")
	fs.StringVar(&f.sse, "snapshotOffloadServerSideEncryption", "",
		"server-side encryption mode for snapshot objects (AES256 or aws:kms)")
	fs.StringVar(&f.sseKMSKeyID, "snapshotOffloadSSEKMSKeyId", "",
		"KMS key ARN when --snapshotOffloadServerSideEncryption is aws:kms")
	fs.DurationVar(&f.interval, "snapshotOffloadInterval", snapshotoffload.DefaultSchedulerInterval,
		"how often to scan local groups for a publishable snapshot")
	fs.DurationVar(&f.jitter, "snapshotOffloadJitter", 0,
		"random spread applied to the offload schedule; zero uses a quarter of the interval")
	fs.IntVar(&f.concurrency, "snapshotOffloadConcurrency", snapshotoffload.DefaultSchedulerConcurrency,
		"maximum concurrent snapshot uploads for this process")
	fs.DurationVar(&f.publishTimeout, "snapshotOffloadPublishTimeout", snapshotoffload.DefaultSchedulerPublishTimeout,
		"deadline for each physical snapshot publication")
	fs.StringVar(&f.spoolDir, "snapshotOffloadSpoolDir", "",
		"directory for snapshot spool files; empty uses the data dir's filesystem")
	fs.StringVar(&f.sourceCluster, "snapshotOffloadSourceCluster", "",
		"source cluster identity recorded in every manifest; required when offload is enabled")
	fs.BoolVar(&f.allowInsecureEndpoint, "snapshotOffloadAllowInsecureEndpoint", false,
		"allow a plaintext http:// --snapshotOffloadEndpoint. DEVELOPMENT ONLY: snapshot "+
			"payloads and session credentials would cross the network unencrypted.")
	f.retention.register(fs)
}

// rejectPlaintextOffloadEndpoint refuses an http:// endpoint unless the
// operator explicitly opted in.
//
// The design and the runbook both require TLS for the external bucket, and
// this path carries whole snapshot payloads plus the session credentials used
// to write them. An endpoint that silently downgraded to plaintext is the kind
// of misconfiguration that is invisible until someone captures the traffic, so
// it fails startup instead.
func rejectPlaintextOffloadEndpoint(f snapshotOffloadFlags) error {
	endpoint := strings.TrimSpace(f.endpoint)
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
		(f.allowInsecureEndpoint && strings.EqualFold(parsed.Scheme, "http"))
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

func validateSnapshotOffloadSpoolDir(f snapshotOffloadFlags) error {
	dir := strings.TrimSpace(f.spoolDir)
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
func snapshotOffloadEnabled(f snapshotOffloadFlags) bool {
	return strings.TrimSpace(f.bucket) != "" ||
		strings.TrimSpace(f.localDir) != ""
}

// buildSnapshotOffloadStore constructs the configured object store.
//
// Bucket and local dir are mutually exclusive: accepting both would
// leave which destination actually receives the artifacts ambiguous,
// and a backup written to the wrong place is discovered only when a
// restore is attempted.
func buildSnapshotOffloadStore(ctx context.Context, f snapshotOffloadFlags) (snapshotOffloadStore, error) {
	bucket := strings.TrimSpace(f.bucket)
	localDir := strings.TrimSpace(f.localDir)
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
		Region:               strings.TrimSpace(f.region),
		Endpoint:             strings.TrimSpace(f.endpoint),
		Profile:              strings.TrimSpace(f.profile),
		ForcePathStyle:       f.forcePathStyle,
		ServerSideEncryption: strings.TrimSpace(f.sse),
		SSEKMSKeyID:          strings.TrimSpace(f.sseKMSKeyID),
		HTTPClientDecorator: func(base aws.HTTPClient) (aws.HTTPClient, error) {
			return snapshotOffloadHTTPClient(base, f.allowInsecureEndpoint)
		},
		AllowVersionedBucketWithLifecycle: f.retention.allowVersionedLifecycle,
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
	f snapshotOffloadFlags,
	runtimes []*raftGroupRuntime,
	raftDir, raftID string,
	multi bool,
	logger *slog.Logger,
) {
	configured := strings.TrimSpace(f.spoolDir)
	seen := make(map[string]struct{})
	cutoff := time.Now().Add(-snapshotOffloadStaleSpoolAge)
	for _, group := range snapshotOffloadGroups(runtimes, raftDir, raftID, multi) {
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

// snapshotOffloadGroups uses snapshotEngine() for leadership callbacks so
// scheduler reads are synchronized with runtime shutdown.
func snapshotOffloadGroups(
	runtimes []*raftGroupRuntime, raftDir, raftID string, multi bool,
) []snapshotoffload.OffloadGroup {
	groups := make([]snapshotoffload.OffloadGroup, 0, len(runtimes))
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		groups = append(groups, snapshotoffload.OffloadGroup{
			GroupID:      rt.spec.ID,
			DataDir:      groupDataDir(raftDir, raftID, rt.spec.ID, multi),
			IsLeader:     snapshotOffloadIsLeader(rt),
			VerifyLeader: snapshotOffloadVerifyLeader(rt),
		})
	}
	return groups
}

func snapshotOffloadIsLeader(rt *raftGroupRuntime) func() bool {
	return func() bool {
		engine := rt.snapshotEngine()
		return engine != nil && engine.State() == raftengine.StateLeader
	}
}

// snapshotOffloadVerifyLeader is the §4 pre-commit re-verification: a
// multi-gigabyte spool takes long enough to lose an election, so
// leadership must hold at the instant the manifest commits, not merely
// when the snapshot was opened.
func snapshotOffloadVerifyLeader(rt *raftGroupRuntime) func(context.Context) error {
	return func(ctx context.Context) error {
		engine := rt.snapshotEngine()
		if engine == nil {
			return errors.Wrap(snapshotoffload.ErrInvalidOptions,
				"snapshot offload: raft engine closed")
		}
		verifier, ok := engine.(interface {
			VerifyLeader(context.Context) error
		})
		if !ok {
			return errors.Wrap(snapshotoffload.ErrInvalidOptions,
				"snapshot offload: raft engine cannot verify leadership")
		}
		return errors.Wrap(verifier.VerifyLeader(ctx), "snapshot offload: verify leadership")
	}
}

// startSnapshotOffload wires and starts the scheduler when offload is
// configured. It returns an error rather than logging and continuing:
// an operator who configured a backup destination and got no backups
// is worse off than one whose node refused to start.
func startSnapshotOffload(
	ctx context.Context,
	eg *errgroup.Group,
	f snapshotOffloadFlags,
	runtimes []*raftGroupRuntime,
	raftDir, raftID string,
	multi bool,
	observer snapshotoffload.SchedulerObserver,
	logger *slog.Logger,
) (retErr error) {
	if f.retention.enabled && !snapshotOffloadEnabled(f) {
		return errors.Wrap(snapshotoffload.ErrInvalidOptions, "snapshot retention requires an offload destination")
	}
	if !snapshotOffloadEnabled(f) {
		return nil
	}
	if err := validateSnapshotOffloadPrefix(f.prefix); err != nil {
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
		snapshotOffloadGroups(runtimes, raftDir, raftID, multi),
		strings.TrimSpace(f.prefix),
		strings.TrimSpace(f.sourceCluster),
		buildVersion(),
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
	cleanStaleOffloadSpool(f, runtimes, raftDir, raftID, multi, logger)

	logger.Info("snapshot offload enabled",
		slog.Int("groups", len(runtimes)),
		slog.Duration("interval", f.interval),
		slog.Int("concurrency", f.concurrency))

	eg.Go(func() error {
		// Run returns only on context cancellation; a failing group is
		// retried on the next tick rather than tearing the process
		// down, because an object-store outage must not stop serving.
		return runSnapshotOffloadWorkers(ctx, store, scheduler, retention)
	})
	handedOff = true
	return nil
}

func snapshotOffloadSchedulerOptions(f snapshotOffloadFlags, observer snapshotoffload.SchedulerObserver, logger *slog.Logger) []snapshotoffload.SchedulerOption {
	opts := []snapshotoffload.SchedulerOption{
		snapshotoffload.WithSchedulerInterval(f.interval),
		snapshotoffload.WithSchedulerConcurrency(f.concurrency),
		snapshotoffload.WithSchedulerPublishTimeout(f.publishTimeout),
		snapshotoffload.WithSchedulerObserver(observer),
		snapshotoffload.WithSchedulerLogger(logger),
	}
	if f.jitter > 0 {
		opts = append(opts, snapshotoffload.WithSchedulerJitter(f.jitter))
	}
	if dir := strings.TrimSpace(f.spoolDir); dir != "" {
		opts = append(opts, snapshotoffload.WithSchedulerSpoolDir(dir))
	}
	return opts
}

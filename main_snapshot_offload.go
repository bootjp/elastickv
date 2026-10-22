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
var (
	snapshotOffloadBucket = flag.String("snapshotOffloadBucket", "",
		"S3 bucket for physical snapshot offload; empty to disable")
	snapshotOffloadLocalDir = flag.String("snapshotOffloadLocalDir", "",
		"filesystem root for physical snapshot offload; an alternative to --snapshotOffloadBucket, mainly for testing")
	snapshotOffloadPrefix = flag.String("snapshotOffloadPrefix", "",
		"key prefix below which snapshot artifacts are written")
	snapshotOffloadRegion = flag.String("snapshotOffloadRegion", "",
		"AWS region for the snapshot offload bucket")
	snapshotOffloadEndpoint = flag.String("snapshotOffloadEndpoint", "",
		"custom S3 endpoint for snapshot offload; empty uses the AWS default")
	snapshotOffloadProfile = flag.String("snapshotOffloadProfile", "",
		"shared-credentials profile for snapshot offload")
	snapshotOffloadForcePathStyle = flag.Bool("snapshotOffloadForcePathStyle", false,
		"use path-style addressing for the snapshot offload endpoint")
	snapshotOffloadSSE = flag.String("snapshotOffloadServerSideEncryption", "",
		"server-side encryption mode for snapshot objects (AES256 or aws:kms)")
	snapshotOffloadSSEKMSKeyID = flag.String("snapshotOffloadSSEKMSKeyId", "",
		"KMS key ARN when --snapshotOffloadServerSideEncryption is aws:kms")
	snapshotOffloadInterval = flag.Duration("snapshotOffloadInterval", snapshotoffload.DefaultSchedulerInterval,
		"how often to scan local groups for a publishable snapshot")
	snapshotOffloadJitter = flag.Duration("snapshotOffloadJitter", 0,
		"random spread applied to the offload schedule; zero uses a quarter of the interval")
	snapshotOffloadConcurrency = flag.Int("snapshotOffloadConcurrency", snapshotoffload.DefaultSchedulerConcurrency,
		"maximum concurrent snapshot uploads for this process")
	snapshotOffloadPublishTimeout = flag.Duration("snapshotOffloadPublishTimeout", snapshotoffload.DefaultSchedulerPublishTimeout,
		"deadline for each physical snapshot publication")
	snapshotOffloadSpoolDir = flag.String("snapshotOffloadSpoolDir", "",
		"directory for snapshot spool files; empty uses the data dir's filesystem")
	snapshotOffloadSourceCluster = flag.String("snapshotOffloadSourceCluster", "",
		"source cluster identity recorded in every manifest; required when offload is enabled")
)

// snapshotOffloadAllowInsecureEndpoint is the narrow development opt-in for a
// plaintext offload endpoint. Off by default, and named so it cannot be
// mistaken for a tuning knob.
var snapshotOffloadAllowInsecureEndpoint = flag.Bool("snapshotOffloadAllowInsecureEndpoint", false,
	"allow a plaintext http:// --snapshotOffloadEndpoint. DEVELOPMENT ONLY: snapshot "+
		"payloads and session credentials would cross the network unencrypted.")

// rejectPlaintextOffloadEndpoint refuses an http:// endpoint unless the
// operator explicitly opted in.
//
// The design and the runbook both require TLS for the external bucket, and
// this path carries whole snapshot payloads plus the session credentials used
// to write them. An endpoint that silently downgraded to plaintext is the kind
// of misconfiguration that is invisible until someone captures the traffic, so
// it fails startup instead.
func rejectPlaintextOffloadEndpoint() error {
	endpoint := strings.TrimSpace(*snapshotOffloadEndpoint)
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
		(*snapshotOffloadAllowInsecureEndpoint && strings.EqualFold(parsed.Scheme, "http"))
	if !allowedScheme || parsed.Hostname() == "" || parsed.Opaque != "" {
		return errors.Wrapf(snapshotoffload.ErrInvalidOptions,
			"--snapshotOffloadEndpoint %q is not a valid https URL; snapshot payloads and credentials "+
				"would cross the network unencrypted. Pass an https:// endpoint, or "+
				"--snapshotOffloadAllowInsecureEndpoint for local development only",
			endpoint)
	}
	return nil
}

func snapshotOffloadHTTPClient(base aws.HTTPClient) (aws.HTTPClient, error) {
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
	allowInsecure := *snapshotOffloadAllowInsecureEndpoint
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

func validateSnapshotOffloadSpoolDir() error {
	dir := strings.TrimSpace(*snapshotOffloadSpoolDir)
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
func snapshotOffloadEnabled() bool {
	return strings.TrimSpace(*snapshotOffloadBucket) != "" ||
		strings.TrimSpace(*snapshotOffloadLocalDir) != ""
}

// buildSnapshotOffloadStore constructs the configured object store.
//
// Bucket and local dir are mutually exclusive: accepting both would
// leave which destination actually receives the artifacts ambiguous,
// and a backup written to the wrong place is discovered only when a
// restore is attempted.
func buildSnapshotOffloadStore(ctx context.Context) (snapshotOffloadStore, error) {
	bucket := strings.TrimSpace(*snapshotOffloadBucket)
	localDir := strings.TrimSpace(*snapshotOffloadLocalDir)
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
	if err := rejectPlaintextOffloadEndpoint(); err != nil {
		return nil, err
	}
	store, err := snapshotoffload.NewS3Store(ctx, snapshotoffload.S3StoreConfig{
		Bucket:                            bucket,
		Region:                            strings.TrimSpace(*snapshotOffloadRegion),
		Endpoint:                          strings.TrimSpace(*snapshotOffloadEndpoint),
		Profile:                           strings.TrimSpace(*snapshotOffloadProfile),
		ForcePathStyle:                    *snapshotOffloadForcePathStyle,
		ServerSideEncryption:              strings.TrimSpace(*snapshotOffloadSSE),
		SSEKMSKeyID:                       strings.TrimSpace(*snapshotOffloadSSEKMSKeyID),
		HTTPClientDecorator:               snapshotOffloadHTTPClient,
		AllowVersionedBucketWithLifecycle: *snapshotOffloadAllowVersionedLifecycle,
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
	runtimes []*raftGroupRuntime,
	raftDir, raftID string,
	multi bool,
	logger *slog.Logger,
) {
	configured := strings.TrimSpace(*snapshotOffloadSpoolDir)
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
			GroupID:      rt.spec.id,
			DataDir:      groupDataDir(raftDir, raftID, rt.spec.id, multi),
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
	runtimes []*raftGroupRuntime,
	raftDir, raftID string,
	multi bool,
	observer snapshotoffload.SchedulerObserver,
	logger *slog.Logger,
) (retErr error) {
	if *snapshotOffloadGCEnabled && !snapshotOffloadEnabled() {
		return errors.Wrap(snapshotoffload.ErrInvalidOptions, "snapshot retention requires an offload destination")
	}
	if !snapshotOffloadEnabled() {
		return nil
	}
	if err := validateSnapshotOffloadPrefix(*snapshotOffloadPrefix); err != nil {
		return err
	}
	if err := validateSnapshotOffloadSpoolDir(); err != nil {
		return err
	}
	store, err := buildSnapshotOffloadStore(ctx)
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
		strings.TrimSpace(*snapshotOffloadPrefix),
		strings.TrimSpace(*snapshotOffloadSourceCluster),
		buildVersion(),
		snapshotOffloadSchedulerOptions(observer, logger)...,
	)
	if err != nil {
		return errors.Wrap(err, "snapshot offload: scheduler")
	}
	retention, err := buildSnapshotOffloadRetention(ctx, store, logger)
	if err != nil {
		return err
	}

	// Clean only aged leftovers before starting this process's publishers.
	cleanStaleOffloadSpool(runtimes, raftDir, raftID, multi, logger)

	logger.Info("snapshot offload enabled",
		slog.Int("groups", len(runtimes)),
		slog.Duration("interval", *snapshotOffloadInterval),
		slog.Int("concurrency", *snapshotOffloadConcurrency))

	eg.Go(func() error {
		// Run returns only on context cancellation; a failing group is
		// retried on the next tick rather than tearing the process
		// down, because an object-store outage must not stop serving.
		return runSnapshotOffloadWorkers(ctx, store, scheduler, retention)
	})
	handedOff = true
	return nil
}

func snapshotOffloadSchedulerOptions(observer snapshotoffload.SchedulerObserver, logger *slog.Logger) []snapshotoffload.SchedulerOption {
	opts := []snapshotoffload.SchedulerOption{
		snapshotoffload.WithSchedulerInterval(*snapshotOffloadInterval),
		snapshotoffload.WithSchedulerConcurrency(*snapshotOffloadConcurrency),
		snapshotoffload.WithSchedulerPublishTimeout(*snapshotOffloadPublishTimeout),
		snapshotoffload.WithSchedulerObserver(observer),
		snapshotoffload.WithSchedulerLogger(logger),
	}
	if *snapshotOffloadJitter > 0 {
		opts = append(opts, snapshotoffload.WithSchedulerJitter(*snapshotOffloadJitter))
	}
	if dir := strings.TrimSpace(*snapshotOffloadSpoolDir); dir != "" {
		opts = append(opts, snapshotoffload.WithSchedulerSpoolDir(dir))
	}
	return opts
}

package snapshotoffloadwiring

import (
	"context"
	"encoding/pem"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/bootjp/elastickv/internal/snapshotoffload"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// testOffloadFlags returns the registered offload defaults with the given
// destination.
// defaultOffloadFlags returns the registered flag defaults.
func defaultOffloadFlags() Flags {
	var f Flags
	f.Register(flag.NewFlagSet("defaults", flag.ContinueOnError))
	return f
}

func testOffloadFlags(bucket, localDir string) Flags {
	f := defaultOffloadFlags()
	f.Bucket, f.LocalDir = bucket, localDir
	return f
}

// TestSnapshotOffloadRejectsAmbiguousDestination guards against
// accepting both a bucket and a local dir: which destination actually
// receives the artifacts would be ambiguous, and a backup written to
// the wrong place is discovered only when a restore is attempted.
func TestSnapshotOffloadRejectsAmbiguousDestination(t *testing.T) {
	cfg := testOffloadFlags("some-bucket", t.TempDir())
	require.True(t, snapshotOffloadEnabled(cfg))

	_, err := buildSnapshotOffloadStore(context.Background(), cfg)
	require.Error(t, err)
	require.True(t, errors.Is(err, snapshotoffload.ErrInvalidOptions))
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestSnapshotOffloadBuildsALocalStore(t *testing.T) {
	root := t.TempDir()
	cfg := testOffloadFlags("", root)

	store, err := buildSnapshotOffloadStore(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeSnapshotOffloadStore(store)) })
	require.NotNil(t, store)
	_, ok := store.(*snapshotoffload.LocalStore)
	require.True(t, ok)
}

// testLogger discards output so a test that exercises the enabled path
// does not spam the run.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRejectPlaintextOffloadEndpoint pins the transport requirement.
//
// The design and the runbook both require TLS for the external bucket, and this
// path carries whole snapshot payloads plus the credentials used to write them.
// An http:// endpoint was forwarded straight to the AWS client, so the
// downgrade was invisible until someone captured the traffic.
func TestRejectPlaintextOffloadEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
		allow    bool
		wantErr  bool
	}{
		{name: "https is accepted", endpoint: "https://s3.example.com", wantErr: false},
		{name: "uppercase HTTPS is accepted", endpoint: "HTTPS://s3.example.com", wantErr: false},
		{name: "empty means the AWS default, which is https", endpoint: "", wantErr: false},
		{name: "https without a host is refused", endpoint: "https://", wantErr: true},
		{name: "opaque https URL is refused", endpoint: "https:example.com", wantErr: true},
		{name: "malformed development endpoint is refused", endpoint: "http://", allow: true, wantErr: true},
		{name: "http is refused", endpoint: "http://s3.example.com", wantErr: true},
		{
			// The AWS SDK resolves a scheme-less endpoint as http, so it is
			// refused rather than assumed safe.
			name: "scheme-less host:port is refused", endpoint: "s3.example.com:9000", wantErr: true,
		},
		{
			name:     "http with the explicit development opt-in is allowed",
			endpoint: "http://127.0.0.1:9000", allow: true, wantErr: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testOffloadFlags("", "")
			cfg.Endpoint, cfg.AllowInsecureEndpoint = tc.endpoint, tc.allow
			err := rejectPlaintextOffloadEndpoint(cfg)
			if tc.wantErr {
				require.Error(t, err, "endpoint %q", tc.endpoint)
				require.ErrorIs(t, err, snapshotoffload.ErrInvalidOptions)
				return
			}
			require.NoError(t, err, "endpoint %q", tc.endpoint)
		})
	}
}

func TestSnapshotOffloadRedirectPolicy(t *testing.T) {
	t.Parallel()

	origin, err := http.NewRequestWithContext(context.Background(), http.MethodPut,
		"https://s3.example.com/object", nil)
	require.NoError(t, err)

	redirect := func(rawURL string, status int) *http.Request {
		t.Helper()
		u, parseErr := url.Parse(rawURL)
		require.NoError(t, parseErr)
		return &http.Request{
			Method:   http.MethodPut,
			URL:      u,
			Header:   make(http.Header),
			Response: &http.Response{StatusCode: status},
		}
	}

	httpsRedirect := redirect("https://s3.example.com/next", http.StatusTemporaryRedirect)
	require.NoError(t, checkSnapshotOffloadRedirect(httpsRedirect, []*http.Request{origin}, false))

	plaintextRedirect := redirect("http://s3.example.com/next", http.StatusTemporaryRedirect)
	require.ErrorIs(t, checkSnapshotOffloadRedirect(plaintextRedirect, []*http.Request{origin}, false),
		snapshotoffload.ErrInvalidOptions)

	unsupportedRedirect := redirect("https://s3.example.com/next", http.StatusFound)
	require.ErrorIs(t, checkSnapshotOffloadRedirect(unsupportedRedirect, []*http.Request{origin}, false),
		http.ErrUseLastResponse)

	crossHost := redirect("https://other.example.com/next", http.StatusPermanentRedirect)
	require.ErrorIs(t, checkSnapshotOffloadRedirect(crossHost, []*http.Request{origin}, false),
		snapshotoffload.ErrInvalidOptions)
}

func TestSnapshotOffloadInsecureClientStillRestrictsRedirects(t *testing.T) {
	base := &http.Client{Transport: &recordingRoundTripper{}}
	wrapped, err := snapshotOffloadHTTPClient(base, true)
	require.NoError(t, err)
	client, ok := wrapped.(*http.Client)
	require.True(t, ok)
	origin, err := http.NewRequestWithContext(context.Background(), http.MethodPut,
		"http://s3.example.com/object", nil)
	require.NoError(t, err)
	response, err := client.Do(origin)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	redirect := origin.Clone(context.Background())
	redirect.URL, err = url.Parse("http://other.s3.example.com/object")
	require.NoError(t, err)
	redirect.Response = &http.Response{StatusCode: http.StatusTemporaryRedirect}
	require.ErrorIs(t, client.CheckRedirect(redirect, []*http.Request{origin}),
		snapshotoffload.ErrInvalidOptions)
}

func TestSnapshotOffloadPreservesSDKCABundle(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
	}), 0o600))
	t.Setenv("AWS_CA_BUNDLE", caPath)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	var resolved aws.HTTPClient
	store, err := snapshotoffload.NewS3Store(context.Background(), snapshotoffload.S3StoreConfig{
		Bucket: "snapshot-test", Region: "us-east-1", Endpoint: server.URL,
		ForcePathStyle: true, ServerSideEncryption: "AES256",
		AccessKeyID: "test-key", SecretAccessKey: "test-secret",
		HTTPClientDecorator: func(base aws.HTTPClient) (aws.HTTPClient, error) {
			resolved = base
			return snapshotOffloadHTTPClient(base, false)
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resolved, "the SDK must supply the CA-configured client")
	_, found, err := store.HeadObject(context.Background(), "snapshot")
	require.NoError(t, err, "the guarded client must trust the SDK CA bundle")
	require.False(t, found)
}

type recordingRoundTripper struct {
	called bool
}

func (r *recordingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.called = true
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func TestSnapshotOffloadTransportRejectsSDKResolvedPlaintextEndpoint(t *testing.T) {
	t.Parallel()

	next := &recordingRoundTripper{}
	transport := snapshotOffloadHTTPSTransport{next: next}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPut,
		"http://s3.example.com/object", nil)
	require.NoError(t, err)

	_, err = transport.RoundTrip(request)
	require.ErrorIs(t, err, snapshotoffload.ErrInvalidOptions)
	require.False(t, next.called, "plaintext must be rejected before credentials or payload reach the transport")

	request.URL.Scheme = "https"
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	require.True(t, next.called)
	require.NoError(t, response.Body.Close())
}

func TestSnapshotOffloadRejectsEscapingPrefixBeforeScheduling(t *testing.T) {
	t.Parallel()

	for _, prefix := range []string{"..", "../backup", "snapshots/../../backup", "/../backup"} {
		require.ErrorIs(t, validateSnapshotOffloadPrefix(prefix), snapshotoffload.ErrInvalidOptions,
			"prefix %q", prefix)
	}
	for _, prefix := range []string{"", "/", "snapshots", "/snapshots/group-a/"} {
		require.NoError(t, validateSnapshotOffloadPrefix(prefix), "prefix %q", prefix)
	}
}

func TestSnapshotOffloadRejectsSpoolPathThatIsAFile(t *testing.T) {
	spoolFile := filepath.Join(t.TempDir(), "spool")
	require.NoError(t, os.WriteFile(spoolFile, []byte("not a directory"), 0o600))

	cfg := testOffloadFlags("", "")
	cfg.SpoolDir = spoolFile

	require.Error(t, validateSnapshotOffloadSpoolDir(cfg))
}

// TestSnapshotOffloadIsOptIn pins that a node which configured no
// destination does no offload work and cannot fail startup on offload
// configuration.
func TestSnapshotOffloadIsOptIn(t *testing.T) {
	cfg := testOffloadFlags("", "")
	require.False(t, snapshotOffloadEnabled(cfg))
	require.NoError(t, Start(
		context.Background(), nil, cfg, nil, "test", nil, testLogger(t)))
}

// TestStartSnapshotOffloadRejectsIncompleteConfiguration pins that a
// configured-but-invalid offload fails startup rather than logging and
// leaving the operator with no backups.
func TestStartSnapshotOffloadRejectsIncompleteConfiguration(t *testing.T) {
	cfg := testOffloadFlags("", t.TempDir())
	cfg.SourceCluster = "   " // whitespace-only: no identity

	err := Start(
		context.Background(), nil, cfg,
		[]snapshotoffload.OffloadGroup{{GroupID: 1, DataDir: t.TempDir()}},
		"test", nil, testLogger(t))
	require.Error(t, err)
	require.True(t, errors.Is(err, snapshotoffload.ErrInvalidOptions))
}

func TestSnapshotOffloadCleanupSparesRecentSharedSpoolFiles(t *testing.T) {
	spoolDir := t.TempDir()
	spoolFile := filepath.Join(spoolDir, "elastickv-snapshot-offload-shared.fsm")
	require.NoError(t, os.WriteFile(spoolFile, []byte("in flight"), 0o600))

	cfg := testOffloadFlags("", "")
	cfg.SpoolDir = spoolDir
	groups := []snapshotoffload.OffloadGroup{{GroupID: 1, DataDir: t.TempDir()}}

	cleanStaleOffloadSpool(cfg, groups, testLogger(t))
	require.FileExists(t, spoolFile)

	stale := time.Now().Add(-snapshotOffloadStaleSpoolAge - time.Hour)
	require.NoError(t, os.Chtimes(spoolFile, stale, stale))
	cleanStaleOffloadSpool(cfg, groups, testLogger(t))
	require.NoFileExists(t, spoolFile)
}

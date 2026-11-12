package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/adminbridge"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// Environment variables that the admin listener consults before
// falling back to the command-line flag values. Exposing secrets via
// env vars / file paths keeps them out of /proc/<pid>/cmdline.
const (
	envAdminSessionSigningKey         = "ELASTICKV_ADMIN_SESSION_SIGNING_KEY"
	envAdminSessionSigningKeyPrevious = "ELASTICKV_ADMIN_SESSION_SIGNING_KEY_PREVIOUS"
)

const (
	adminReadHeaderTimeout = 5 * time.Second
	adminWriteTimeout      = 10 * time.Second
	adminIdleTimeout       = 30 * time.Second
)

// adminBuildVersion is surfaced by both GET /admin/api/v1/cluster and the
// Admin gRPC GetNodeVersion RPC. Release builds may stamp it with:
//
//	-ldflags "-X main.adminBuildVersion=<version-or-git-sha>"
var adminBuildVersion = "dev"

const (
	buildInfoVCSRevisionKey = "vcs.revision"
	buildInfoVCSModifiedKey = "vcs.modified"
)

func buildVersion() string {
	if version := strings.TrimSpace(adminBuildVersion); version != "" && version != "dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		revision := ""
		modified := false
		for _, setting := range info.Settings {
			switch setting.Key {
			case buildInfoVCSRevisionKey:
				revision = strings.TrimSpace(setting.Value)
			case buildInfoVCSModifiedKey:
				modified = setting.Value == "true"
			}
		}
		if revision != "" {
			if modified {
				return revision + "-modified"
			}
			return revision
		}
	}
	return "dev"
}

// adminListenerConfig is the subset of startup inputs that goes into the
// admin listener. Collecting them in a struct keeps the main.go call site
// compact and makes unit testing the builder easier.
type adminListenerConfig struct {
	enabled                   bool
	listen                    string
	tlsCertFile               string
	tlsKeyFile                string
	allowPlaintextNonLoopback bool
	allowInsecureDevCookie    bool

	sessionSigningKey         string
	sessionSigningKeyPrevious string

	readOnlyAccessKeys []string
	fullAccessKeys     []string
}

// prepareAdminFromFlags owns the flag → config translation and credentials
// loading so run() does not inherit that complexity.
//
// When admin is disabled (the default) the function returns immediately
// without touching --s3CredentialsFile: pulling the admin feature into
// a hard dependency on that file would break deployments that never
// intended to use it.
// adminHTTPFlags configures the cookie/SigV4-authenticated admin dashboard
// listener and its keyviz cluster fan-out.
type adminHTTPFlags struct {
	enabled                       bool
	listen                        string
	tlsCertFile                   string
	tlsKeyFile                    string
	allowPlaintextNonLoopback     bool
	allowInsecureDevCookie        bool
	sessionSigningKey             string
	sessionSigningKeyFile         string
	sessionSigningKeyPrevious     string
	sessionSigningKeyPreviousFile string
	readOnlyAccessKeys            string
	fullAccessKeys                string
	// keyvizFanoutNodes lists peer admin endpoints (host:port or
	// scheme://host:port). When set, the admin keyviz handler aggregates
	// the local matrix with peer responses; when empty it serves the
	// single-node view. See
	// docs/design/2026_04_27_implemented_keyviz_cluster_fanout.md.
	keyvizFanoutNodes   string
	keyvizFanoutTimeout time.Duration
}

// keyvizFanoutDefaultTimeout matches design 9 open-question 2: 2 s
// per peer call. Operators on weird networks override via the flag.
const keyvizFanoutDefaultTimeout = 2 * time.Second

func (f *adminHTTPFlags) register(fs *flag.FlagSet) {
	fs.BoolVar(&f.enabled, "adminEnabled", false, "Enable the admin HTTP listener")
	fs.StringVar(&f.listen, "adminListen", "127.0.0.1:8080", "host:port for the admin HTTP listener (loopback by default)")
	fs.StringVar(&f.tlsCertFile, "adminTLSCertFile", "", "PEM-encoded TLS certificate for the admin listener")
	fs.StringVar(&f.tlsKeyFile, "adminTLSKeyFile", "", "PEM-encoded TLS private key for the admin listener")
	fs.BoolVar(&f.allowPlaintextNonLoopback, "adminAllowPlaintextNonLoopback", false, "Allow the admin listener to bind a non-loopback address without TLS (strongly discouraged)")
	fs.BoolVar(&f.allowInsecureDevCookie, "adminAllowInsecureDevCookie", false, "Mint admin cookies without the Secure attribute (local plaintext dev only)")
	fs.StringVar(&f.sessionSigningKey, "adminSessionSigningKey", "", "Cluster-shared base64 HS256 key (64 bytes decoded); prefer -adminSessionSigningKeyFile / ELASTICKV_ADMIN_SESSION_SIGNING_KEY so the value does not appear in /proc/<pid>/cmdline")
	fs.StringVar(&f.sessionSigningKeyFile, "adminSessionSigningKeyFile", "", "Path to a file containing the base64-encoded primary admin HS256 key; avoids leaking the secret via argv")
	fs.StringVar(&f.sessionSigningKeyPrevious, "adminSessionSigningKeyPrevious", "", "Optional previous admin HS256 key accepted only for verification during rotation; prefer -adminSessionSigningKeyPreviousFile")
	fs.StringVar(&f.sessionSigningKeyPreviousFile, "adminSessionSigningKeyPreviousFile", "", "Path to a file containing the base64-encoded previous admin HS256 key used for rotation")
	fs.StringVar(&f.readOnlyAccessKeys, "adminReadOnlyAccessKeys", "", "Comma-separated SigV4 access keys granted read-only admin access")
	fs.StringVar(&f.fullAccessKeys, "adminFullAccessKeys", "", "Comma-separated SigV4 access keys granted full-access admin role")
	fs.StringVar(&f.keyvizFanoutNodes, "keyvizFanoutNodes", "", "Comma-separated peer admin endpoints (host:port) for keyviz cluster-wide fan-out; empty disables")
	fs.DurationVar(&f.keyvizFanoutTimeout, "keyvizFanoutTimeout", keyvizFanoutDefaultTimeout, "Per-peer timeout for keyviz fan-out HTTP calls")
}

func prepareAdminFromFlags(
	ctx context.Context,
	lc *net.ListenConfig,
	f adminHTTPFlags,
	raftID string,
	s3CredentialsFile string,
	runtimes []*raftGroupRuntime,
	dynamoServer *adapter.DynamoDBServer,
	s3Server *adapter.S3Server,
	sqsServer *adapter.SQSServer,
	coordinate kv.Coordinator,
	connCache *kv.GRPCConnCache,
	keyvizSampler *keyviz.MemSampler,
	keyvizFanoutCfg adminbridge.KeyVizFanoutConfig,
) (*preparedAdminServer, error) {
	if !f.enabled {
		return nil, nil
	}
	staticCreds, err := loadS3StaticCredentials(s3CredentialsFile)
	if err != nil {
		return nil, errors.Wrapf(err, "load static credentials for admin listener")
	}
	// An admin listener with zero credentials would accept logins
	// only to reject every one of them with invalid_credentials, so a
	// missing or empty credentials file is a wiring bug rather than a
	// valid "locked down" state. Failing fast here also guards against
	// the typed-nil MapCredentialStore case inside NewServer (an
	// untyped `== nil` check cannot detect a nil-map-valued interface
	// on its own).
	if len(staticCreds) == 0 {
		return nil, errors.New("admin listener is enabled but no static credentials are configured; " +
			"set -s3CredentialsFile to a file with at least one entry")
	}
	primaryKey, err := resolveSigningKey(f.sessionSigningKey, f.sessionSigningKeyFile, envAdminSessionSigningKey)
	if err != nil {
		return nil, errors.Wrap(err, "resolve -adminSessionSigningKey")
	}
	previousKey, err := resolveSigningKey(f.sessionSigningKeyPrevious, f.sessionSigningKeyPreviousFile, envAdminSessionSigningKeyPrevious)
	if err != nil {
		return nil, errors.Wrap(err, "resolve -adminSessionSigningKeyPrevious")
	}
	cfg := adminListenerConfig{
		enabled:                   f.enabled,
		listen:                    f.listen,
		tlsCertFile:               f.tlsCertFile,
		tlsKeyFile:                f.tlsKeyFile,
		allowPlaintextNonLoopback: f.allowPlaintextNonLoopback,
		allowInsecureDevCookie:    f.allowInsecureDevCookie,
		sessionSigningKey:         primaryKey,
		sessionSigningKeyPrevious: previousKey,
		readOnlyAccessKeys:        parseCSV(f.readOnlyAccessKeys),
		fullAccessKeys:            parseCSV(f.fullAccessKeys),
	}
	clusterSrc := newClusterInfoSource(raftID, buildVersion(), runtimes)
	tablesSrc := adminbridge.NewDynamoTablesSource(dynamoServer)
	bucketsSrc := adminbridge.NewBucketsSource(s3Server)
	queuesSrc := adminbridge.NewSQSQueuesSource(sqsServer)
	forwarder, err := buildAdminLeaderForwarder(coordinate, connCache, raftID)
	if err != nil {
		return nil, errors.Wrap(err, "build admin leader forwarder")
	}
	leaderProbe := newAdminLeaderProbe(coordinate)
	return prepareAdminServer(ctx, lc, cfg, staticCreds, clusterSrc, tablesSrc, bucketsSrc, queuesSrc, forwarder, leaderProbe, keyvizSampler, keyvizFanoutCfg, buildVersion())
}

// newAdminLeaderProbe builds the LeaderProbe consumed by
// /admin/healthz/leader. It mirrors the verified-leader pattern S3 and
// DynamoDB use on their own /healthz/leader endpoints
// (adapter/s3.go:isVerifiedS3Leader,
// adapter/dynamodb.go:isVerifiedDynamoLeader): a cheap IsLeader check
// short-circuits non-leaders, and only nodes claiming leadership pay
// the ReadIndex round-trip that confirms the claim is still valid.
//
// Crucially the probe is scoped to the **default Raft group** (via
// coordinate.IsLeader / coordinate.VerifyLeader) — the same group the
// admin write paths key off (kv/sharded_coordinator.go), which the
// AdminForward proxy and the SQS admin write path
// (adapter/sqs_admin.go) both treat as authoritative. An earlier
// design returned true on any local-group leadership; in a multi-group
// deployment that could surface 200 on a node leading only a non-
// default group while admin writes there still 503'd or forwarded.
// P1 review on PR #689 caught this; using the coordinator keeps the
// healthz contract aligned with the actual admin-write leader.
//
// Returns nil when no coordinator is wired so the router answers
// /admin/healthz/leader with the standard JSON 404 (matches the
// "feature off" pattern Tables / Buckets / Queues already use).
func newAdminLeaderProbe(coordinate kv.Coordinator) admin.LeaderProbe {
	if coordinate == nil {
		return nil
	}
	return admin.LeaderProbeFunc(func(ctx context.Context) bool {
		if !coordinate.IsLeader() {
			return false
		}
		// VerifyLeader receives the request ctx (PR #748): a Caddy probe
		// or browser preflight that sets its own deadline now bounds the
		// ReadIndex round-trip, instead of falling back to
		// verifyLeaderEngineCtx's deadline-less 5s safety net (#745). The 5s bound
		// remains as defense-in-depth for callers without an upstream
		// ctx (lock resolver, HLC lease tick).
		return coordinate.VerifyLeader(ctx) == nil
	})
}

// buildAdminLeaderForwarder constructs the production LeaderForwarder
// for the dynamo HTTP handler when the wiring is complete enough to
// reach a remote leader. The bridge tolerates a nil connCache (and a
// nil coordinate) so single-node / leader-only builds — where the
// dashboard always hits a leader — can ship without paying the
// forwarder's wiring cost. tablesSrc itself can be nil for cluster-
// only builds; that's handled higher up by ServerDeps.Tables == nil.
func buildAdminLeaderForwarder(coordinate kv.Coordinator, connCache *kv.GRPCConnCache, nodeID string) (admin.LeaderForwarder, error) {
	if coordinate == nil || connCache == nil {
		// Returning (nil, nil) is the explicit "no forwarder" signal
		// — the handler falls back to 503 + Retry-After:1 on
		// ErrTablesNotLeader. The function-level doc comment above
		// describes this contract; the nilnil linter is not enabled
		// in .golangci.yaml so no suppression directive is needed
		// (Claude review on #648).
		return nil, nil
	}
	if nodeID == "" {
		// admin.NewGRPCForwardClient enforces this too; surfacing
		// it here keeps the misconfiguration message in the wiring
		// layer rather than buried under a Wrap chain.
		return nil, errors.New("admin forward bridge: --raftId is required")
	}
	return buildLeaderForwarder(coordinate, connCache, nodeID)
}

// buildAdminConfig translates flag values into an admin.Config.
func buildAdminConfig(in adminListenerConfig) admin.Config {
	return admin.Config{
		Enabled:                   in.enabled,
		Listen:                    in.listen,
		TLSCertFile:               in.tlsCertFile,
		TLSKeyFile:                in.tlsKeyFile,
		AllowPlaintextNonLoopback: in.allowPlaintextNonLoopback,
		SessionSigningKey:         in.sessionSigningKey,
		SessionSigningKeyPrevious: in.sessionSigningKeyPrevious,
		ReadOnlyAccessKeys:        in.readOnlyAccessKeys,
		FullAccessKeys:            in.fullAccessKeys,
		AllowInsecureDevCookie:    in.allowInsecureDevCookie,
	}
}

type preparedAdminServer struct {
	httpSrv    *http.Server
	listener   net.Listener
	cfg        admin.Config
	tlsEnabled bool
	version    string
}

func prepareAdminServer(
	ctx context.Context,
	lc *net.ListenConfig,
	cfg adminListenerConfig,
	creds map[string]string,
	cluster admin.ClusterInfoSource,
	tables admin.TablesSource,
	buckets admin.BucketsSource,
	queues admin.QueuesSource,
	forwarder admin.LeaderForwarder,
	leaderProbe admin.LeaderProbe,
	keyvizSampler *keyviz.MemSampler,
	keyvizFanoutCfg adminbridge.KeyVizFanoutConfig,
	version string,
) (*preparedAdminServer, error) {
	adminCfg := buildAdminConfig(cfg)
	enabled, err := checkAdminConfig(&adminCfg, cluster)
	if err != nil || !enabled {
		return nil, err
	}
	server, err := buildAdminHTTPServer(&adminCfg, creds, cluster, tables, buckets, queues, forwarder, leaderProbe, keyvizSampler, keyvizFanoutCfg)
	if err != nil {
		return nil, err
	}
	httpSrv := newAdminHTTPServer(server)
	listener, err := lc.Listen(ctx, "tcp", adminCfg.Listen)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on admin address %s", adminCfg.Listen)
	}
	tlsEnabled := strings.TrimSpace(adminCfg.TLSCertFile) != "" && strings.TrimSpace(adminCfg.TLSKeyFile) != ""
	if tlsEnabled {
		httpSrv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	actualAddr := listener.Addr().String()
	// Use the real bound address in log lines and in the lifecycle
	// task so the shutdown banner matches startup.
	boundCfg := adminCfg
	boundCfg.Listen = actualAddr
	return &preparedAdminServer{
		httpSrv:    httpSrv,
		listener:   listener,
		cfg:        boundCfg,
		tlsEnabled: tlsEnabled,
		version:    version,
	}, nil
}

func (p *preparedAdminServer) start(ctx context.Context, eg *errgroup.Group) {
	if p == nil || p.httpSrv == nil || p.listener == nil {
		return
	}
	registerAdminLifecycle(ctx, eg, p.httpSrv, p.listener, &p.cfg, p.tlsEnabled, p.version)
	p.listener = nil
}

func (p *preparedAdminServer) close() {
	if p == nil {
		return
	}
	if p.listener != nil {
		_ = p.listener.Close()
		p.listener = nil
	}
}

// checkAdminConfig validates adminCfg; returns (enabled=false, nil) when
// admin is disabled and requires no further work.
func checkAdminConfig(adminCfg *admin.Config, cluster admin.ClusterInfoSource) (bool, error) {
	if err := adminCfg.Validate(); err != nil {
		if !adminCfg.Enabled {
			return false, nil
		}
		return false, errors.Wrap(err, "admin config is invalid")
	}
	if !adminCfg.Enabled {
		return false, nil
	}
	if cluster == nil {
		return false, errors.New("admin: cluster info source is required")
	}
	return true, nil
}

func buildAdminHTTPServer(adminCfg *admin.Config, creds map[string]string, cluster admin.ClusterInfoSource, tables admin.TablesSource, buckets admin.BucketsSource, queues admin.QueuesSource, forwarder admin.LeaderForwarder, leaderProbe admin.LeaderProbe, keyvizSampler *keyviz.MemSampler, keyvizFanoutCfg adminbridge.KeyVizFanoutConfig) (*admin.Server, error) {
	primaryKeys, err := adminCfg.DecodedSigningKeys()
	if err != nil {
		return nil, errors.Wrap(err, "decode admin signing keys")
	}
	signer, err := admin.NewSigner(primaryKeys[0], nil)
	if err != nil {
		return nil, errors.Wrap(err, "build admin signer")
	}
	verifier, err := admin.NewVerifier(primaryKeys, nil)
	if err != nil {
		return nil, errors.Wrap(err, "build admin verifier")
	}
	staticFS, err := admin.StaticFS()
	if err != nil {
		return nil, errors.Wrap(err, "open embedded admin SPA")
	}
	server, err := admin.NewServer(admin.ServerDeps{
		Signer:              signer,
		Verifier:            verifier,
		Credentials:         admin.MapCredentialStore(creds),
		Roles:               adminCfg.RoleIndex(),
		ClusterInfo:         cluster,
		Tables:              tables,
		Buckets:             buckets,
		Queues:              queues,
		Forwarder:           forwarder,
		KeyViz:              adminbridge.KeyVizSource(keyvizSampler),
		KeyVizFanout:        adminbridge.NewKeyVizFanout(adminCfg.Listen, keyvizFanoutCfg),
		KeyVizHotKeys:       adminbridge.KeyVizHotKeysSource(keyvizSampler),
		KeyVizHotKeysFanout: adminbridge.NewKeyVizHotKeysFanout(adminCfg.Listen, keyvizFanoutCfg),
		StaticFS:            staticFS,
		LeaderProbe:         leaderProbe,
		AuthOpts: admin.AuthServiceOpts{
			InsecureCookie: adminCfg.AllowInsecureDevCookie,
		},
		Logger: slog.Default().With(slog.String("component", "admin")),
	})
	if err != nil {
		return nil, errors.Wrap(err, "build admin server")
	}
	return server, nil
}

func newAdminHTTPServer(server *admin.Server) *http.Server {
	return &http.Server{
		Handler:           server.Handler(),
		ReadHeaderTimeout: adminReadHeaderTimeout,
		WriteTimeout:      adminWriteTimeout,
		IdleTimeout:       adminIdleTimeout,
	}
}

// adminShutdownTimeout bounds how long the admin listener waits for
// in-flight requests on shutdown before the remaining connections are
// closed (monitoring.ShutdownHTTPServer). A variable so tests can shorten it.
var adminShutdownTimeout = 5 * time.Second

func registerAdminLifecycle(
	ctx context.Context,
	eg *errgroup.Group,
	httpSrv *http.Server,
	listener net.Listener,
	adminCfg *admin.Config,
	tlsEnabled bool,
	version string,
) {
	addr := adminCfg.Listen
	handlers := trackAdminHandlers(httpSrv)
	eg.Go(func() error {
		<-ctx.Done()
		slog.Info("shutting down admin listener", "address", addr, "reason", ctx.Err())
		err := monitoring.ShutdownHTTPServer(ctx, httpSrv, "admin", adminShutdownTimeout)
		handlers.join(adminHandlerJoinTimeout)
		return errors.WithStack(err)
	})
	eg.Go(func() error {
		slog.Info("starting admin listener", "address", addr, "tls", tlsEnabled, "version", version)
		var serveErr error
		if tlsEnabled {
			serveErr = httpSrv.ServeTLS(listener, adminCfg.TLSCertFile, adminCfg.TLSKeyFile)
		} else {
			serveErr = httpSrv.Serve(listener)
		}
		if serveErr == nil || errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, net.ErrClosed) {
			return nil
		}
		return errors.Wrapf(serveErr, "admin listener on %s stopped with error", addr)
	})
}

// newClusterInfoSource builds a ClusterInfoSource that reads from the
// runtime raftGroupRuntime slice. It lives here (rather than
// internal/admin) so the admin package stays free of main-package types.
//
// Membership is fetched via engine.Configuration(ctx); the call is
// best-effort — if it fails (for instance because the engine is in the
// middle of a leadership transition) we leave Members empty rather
// than fail the whole cluster snapshot.
func newClusterInfoSource(nodeID, version string, runtimes []*raftGroupRuntime) admin.ClusterInfoSource {
	return admin.ClusterInfoFunc(func(ctx context.Context) (admin.ClusterInfo, error) {
		groups := make([]admin.GroupInfo, 0, len(runtimes))
		for _, rt := range runtimes {
			// snapshotEngine takes engineMu.RLock so this
			// HTTP/gRPC handler running on its own goroutine
			// cannot race rt.Close() clearing the field on
			// shutdown. Same race class as the keyviz
			// leader-term publisher (P2 review on PR #720).
			engine := rt.snapshotEngine()
			if engine == nil {
				continue
			}
			status := engine.Status()
			// Seed as an empty-but-non-nil slice so a
			// Configuration() failure still JSON-encodes as `[]`
			// rather than `null`; API consumers that treat
			// members as an always-array field rely on this.
			members := []string{}
			if cfg, err := engine.Configuration(ctx); err == nil {
				members = make([]string, 0, len(cfg.Servers))
				for _, srv := range cfg.Servers {
					members = append(members, srv.ID)
				}
			}
			groups = append(groups, admin.GroupInfo{
				GroupID:  rt.spec.id,
				LeaderID: status.Leader.ID,
				IsLeader: status.State == raftengine.StateLeader,
				Members:  members,
			})
		}
		return admin.ClusterInfo{
			NodeID:  nodeID,
			Version: version,
			Groups:  groups,
		}, nil
	})
}

// resolveSigningKey picks the effective admin signing key from, in
// priority order: the --*File flag (file contents), the env var, and
// finally the --*Flag argv value. Preferring the file/env paths keeps
// the raw base64 out of /proc/<pid>/cmdline on Linux. Returns the empty
// string when every source is unset — callers that require a value
// (validated elsewhere) must handle that case themselves.
func resolveSigningKey(flagValue, filePath, envVar string) (string, error) {
	if strings.TrimSpace(filePath) != "" {
		b, err := os.ReadFile(filePath)
		if err != nil {
			return "", errors.Wrapf(err, "read admin signing key file %q", filePath)
		}
		return strings.TrimSpace(string(b)), nil
	}
	if v := strings.TrimSpace(os.Getenv(envVar)); v != "" {
		return v, nil
	}
	return strings.TrimSpace(flagValue), nil
}

// parseCSV splits a flag value like "a,b,c" into a slice with empty and
// whitespace-only entries dropped. It is not in shard_config.go because
// admin's comma-separated list format is simpler than raft groups.
func parseCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trim := strings.TrimSpace(p); trim != "" {
			out = append(out, trim)
		}
	}
	return out
}

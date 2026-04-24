package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/admin"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	adminReadHeaderTimeout = 5 * time.Second
	adminWriteTimeout      = 10 * time.Second
	adminIdleTimeout       = 30 * time.Second
	adminShutdownTimeout   = 5 * time.Second

	// adminBuildVersion is surfaced in GET /admin/api/v1/cluster. Until
	// we wire real ldflags-injected build info, a placeholder is fine.
	adminBuildVersion = "dev"
)

// buildVersion returns the elastickv binary version for admin purposes.
// It is intentionally a function, not a constant, so build tooling can
// link-replace it via -ldflags in the future.
func buildVersion() string { return adminBuildVersion }

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

// startAdminFromFlags is the single entrypoint main.run() uses to stand
// up the admin listener. It owns the flag → config translation and the
// credentials loading so run() does not inherit that complexity.
//
// When admin is disabled (the default) the function returns immediately
// without touching --s3CredentialsFile: pulling the admin feature into
// a hard dependency on that file would break deployments that never
// intended to use it.
func startAdminFromFlags(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, runtimes []*raftGroupRuntime) error {
	if !*adminEnabled {
		return nil
	}
	staticCreds, err := loadS3StaticCredentials(*s3CredsFile)
	if err != nil {
		return errors.Wrapf(err, "load static credentials for admin listener")
	}
	cfg := adminListenerConfig{
		enabled:                   *adminEnabled,
		listen:                    *adminListen,
		tlsCertFile:               *adminTLSCertFile,
		tlsKeyFile:                *adminTLSKeyFile,
		allowPlaintextNonLoopback: *adminAllowPlaintextNonLoopback,
		allowInsecureDevCookie:    *adminAllowInsecureDevCookie,
		sessionSigningKey:         *adminSessionSigningKey,
		sessionSigningKeyPrevious: *adminSessionSigningKeyPrevious,
		readOnlyAccessKeys:        parseCSV(*adminReadOnlyAccessKeys),
		fullAccessKeys:            parseCSV(*adminFullAccessKeys),
	}
	clusterSrc := newClusterInfoSource(*raftId, buildVersion(), runtimes)
	return startAdminServer(ctx, lc, eg, cfg, staticCreds, clusterSrc, buildVersion())
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

// startAdminServer validates the admin configuration, constructs the admin
// server, and attaches its lifecycle to eg. It is a no-op when the admin
// listener is disabled. Errors at this point are hard startup failures:
// the design doc mandates ハードエラーで起動失敗 for every invalid
// configuration, and we honour that uniformly.
func startAdminServer(
	ctx context.Context,
	lc *net.ListenConfig,
	eg *errgroup.Group,
	cfg adminListenerConfig,
	creds map[string]string,
	cluster admin.ClusterInfoSource,
	version string,
) error {
	adminCfg := buildAdminConfig(cfg)
	enabled, err := checkAdminConfig(&adminCfg, cluster)
	if err != nil || !enabled {
		return err
	}
	server, err := buildAdminHTTPServer(&adminCfg, creds, cluster)
	if err != nil {
		return err
	}
	httpSrv := newAdminHTTPServer(server, &adminCfg)
	listener, err := lc.Listen(ctx, "tcp", adminCfg.Listen)
	if err != nil {
		return errors.Wrapf(err, "failed to listen on admin address %s", adminCfg.Listen)
	}
	tlsEnabled := strings.TrimSpace(adminCfg.TLSCertFile) != "" && strings.TrimSpace(adminCfg.TLSKeyFile) != ""
	if tlsEnabled {
		httpSrv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	registerAdminLifecycle(ctx, eg, httpSrv, listener, &adminCfg, tlsEnabled, version)
	return nil
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

func buildAdminHTTPServer(adminCfg *admin.Config, creds map[string]string, cluster admin.ClusterInfoSource) (*admin.Server, error) {
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
	server, err := admin.NewServer(admin.ServerDeps{
		Signer:      signer,
		Verifier:    verifier,
		Credentials: admin.MapCredentialStore(creds),
		Roles:       adminCfg.RoleIndex(),
		ClusterInfo: cluster,
		StaticFS:    nil,
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

func newAdminHTTPServer(server *admin.Server, adminCfg *admin.Config) *http.Server {
	_ = adminCfg // reserved for future use (e.g. per-listener TLS overrides)
	return &http.Server{
		Handler:           server.Handler(),
		ReadHeaderTimeout: adminReadHeaderTimeout,
		WriteTimeout:      adminWriteTimeout,
		IdleTimeout:       adminIdleTimeout,
	}
}

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
	eg.Go(func() error {
		<-ctx.Done()
		slog.Info("shutting down admin listener", "address", addr, "reason", ctx.Err())
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adminShutdownTimeout)
		defer cancel()
		err := httpSrv.Shutdown(shutdownCtx)
		if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return nil
		}
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
			if rt == nil || rt.engine == nil {
				continue
			}
			status := rt.engine.Status()
			var members []string
			if cfg, err := rt.engine.Configuration(ctx); err == nil {
				members = make([]string, 0, len(cfg.Servers))
				for _, srv := range cfg.Servers {
					members = append(members, srv.ID)
				}
			}
			groups = append(groups, admin.GroupInfo{
				GroupID:  rt.spec.id,
				LeaderID: status.Leader.ID,
				IsLeader: strings.EqualFold(string(status.State), "leader"),
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

package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/raftengine"
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
func startAdminFromFlags(ctx context.Context, lc *net.ListenConfig, eg *errgroup.Group, runtimes []*raftGroupRuntime, dynamoServer *adapter.DynamoDBServer) error {
	if !*adminEnabled {
		return nil
	}
	staticCreds, err := loadS3StaticCredentials(*s3CredsFile)
	if err != nil {
		return errors.Wrapf(err, "load static credentials for admin listener")
	}
	// An admin listener with zero credentials would accept logins
	// only to reject every one of them with invalid_credentials, so a
	// missing or empty credentials file is a wiring bug rather than a
	// valid "locked down" state. Failing fast here also guards against
	// the typed-nil MapCredentialStore case inside NewServer (an
	// untyped `== nil` check cannot detect a nil-map-valued interface
	// on its own).
	if len(staticCreds) == 0 {
		return errors.New("admin listener is enabled but no static credentials are configured; " +
			"set -s3CredentialsFile to a file with at least one entry")
	}
	primaryKey, err := resolveSigningKey(*adminSessionSigningKey, *adminSessionSigningKeyFile, envAdminSessionSigningKey)
	if err != nil {
		return errors.Wrap(err, "resolve -adminSessionSigningKey")
	}
	previousKey, err := resolveSigningKey(*adminSessionSigningKeyPrevious, *adminSessionSigningKeyPreviousFile, envAdminSessionSigningKeyPrevious)
	if err != nil {
		return errors.Wrap(err, "resolve -adminSessionSigningKeyPrevious")
	}
	cfg := adminListenerConfig{
		enabled:                   *adminEnabled,
		listen:                    *adminListen,
		tlsCertFile:               *adminTLSCertFile,
		tlsKeyFile:                *adminTLSKeyFile,
		allowPlaintextNonLoopback: *adminAllowPlaintextNonLoopback,
		allowInsecureDevCookie:    *adminAllowInsecureDevCookie,
		sessionSigningKey:         primaryKey,
		sessionSigningKeyPrevious: previousKey,
		readOnlyAccessKeys:        parseCSV(*adminReadOnlyAccessKeys),
		fullAccessKeys:            parseCSV(*adminFullAccessKeys),
	}
	clusterSrc := newClusterInfoSource(*raftId, buildVersion(), runtimes)
	tablesSrc := newDynamoTablesSource(dynamoServer)
	_, err = startAdminServer(ctx, lc, eg, cfg, staticCreds, clusterSrc, tablesSrc, buildVersion())
	return err
}

// newDynamoTablesSource adapts *adapter.DynamoDBServer to the
// admin.TablesSource interface. The bridge stays in this file (rather
// than internal/admin) so the admin package stays free of the heavy
// adapter-package dependency tree (gRPC, Raft, store).
//
// Returns nil when dynamoServer is nil; admin.NewServer handles a nil
// Tables field by leaving the dynamo paths off the wire entirely,
// which is the right behaviour for builds that ship without the
// Dynamo adapter.
func newDynamoTablesSource(dynamoServer *adapter.DynamoDBServer) admin.TablesSource {
	if dynamoServer == nil {
		return nil
	}
	return &dynamoTablesBridge{server: dynamoServer}
}

// dynamoTablesBridge is the thin adapter that re-shapes the adapter's
// AdminTableSummary DTO into the admin package's DynamoTableSummary.
// The two structs are deliberately isomorphic so this translation
// does no allocation more than necessary; if a future GSI field is
// added on one side, the build breaks here, which is exactly the
// drift signal we want.
type dynamoTablesBridge struct {
	server *adapter.DynamoDBServer
}

func (b *dynamoTablesBridge) AdminListTables(ctx context.Context) ([]string, error) {
	return b.server.AdminListTables(ctx) //nolint:wrapcheck // pure pass-through; the adapter owns the error context.
}

func (b *dynamoTablesBridge) AdminDescribeTable(ctx context.Context, name string) (*admin.DynamoTableSummary, bool, error) {
	summary, exists, err := b.server.AdminDescribeTable(ctx, name)
	if err != nil {
		return nil, false, err //nolint:wrapcheck // adapter wraps internally.
	}
	if !exists {
		return nil, false, nil
	}
	return convertAdminTableSummary(summary), true, nil
}

func (b *dynamoTablesBridge) AdminCreateTable(ctx context.Context, principal admin.AuthPrincipal, in admin.CreateTableRequest) (*admin.DynamoTableSummary, error) {
	summary, err := b.server.AdminCreateTable(ctx, convertAdminPrincipal(principal), convertCreateTableInput(in))
	if err != nil {
		return nil, translateAdminTablesError(err)
	}
	return convertAdminTableSummary(summary), nil
}

func (b *dynamoTablesBridge) AdminDeleteTable(ctx context.Context, principal admin.AuthPrincipal, name string) error {
	if err := b.server.AdminDeleteTable(ctx, convertAdminPrincipal(principal), name); err != nil {
		return translateAdminTablesError(err)
	}
	return nil
}

// convertAdminPrincipal mirrors admin.AuthPrincipal onto the
// adapter's parallel struct. Both packages keep the principal type
// independent so the adapter stays free of internal/admin
// dependencies, but the role / access-key fields are deliberately
// 1:1 — any drift is a wiring bug, not a feature.
func convertAdminPrincipal(p admin.AuthPrincipal) adapter.AdminPrincipal {
	role := adapter.AdminRoleReadOnly
	if p.Role.AllowsWrite() {
		role = adapter.AdminRoleFull
	}
	return adapter.AdminPrincipal{AccessKey: p.AccessKey, Role: role}
}

// convertCreateTableInput translates the admin-handler request DTO
// into the adapter's parallel input struct. We do this here — not
// in the admin package — to keep `internal/admin` free of any
// adapter import.
func convertCreateTableInput(in admin.CreateTableRequest) adapter.AdminCreateTableInput {
	out := adapter.AdminCreateTableInput{
		TableName:    in.TableName,
		PartitionKey: adapter.AdminAttribute{Name: in.PartitionKey.Name, Type: in.PartitionKey.Type},
	}
	if in.SortKey != nil {
		out.SortKey = &adapter.AdminAttribute{Name: in.SortKey.Name, Type: in.SortKey.Type}
	}
	if len(in.GSI) == 0 {
		return out
	}
	out.GSI = make([]adapter.AdminCreateGSI, len(in.GSI))
	for i, g := range in.GSI {
		gsi := adapter.AdminCreateGSI{
			Name:             g.Name,
			PartitionKey:     adapter.AdminAttribute{Name: g.PartitionKey.Name, Type: g.PartitionKey.Type},
			ProjectionType:   g.Projection.Type,
			NonKeyAttributes: append([]string(nil), g.Projection.NonKeyAttributes...),
		}
		if g.SortKey != nil {
			gsi.SortKey = &adapter.AdminAttribute{Name: g.SortKey.Name, Type: g.SortKey.Type}
		}
		out.GSI[i] = gsi
	}
	return out
}

// translateAdminTablesError maps the adapter's error vocabulary
// onto the admin-package sentinels the HTTP handler matches against.
// Anything not recognised is forwarded as-is and answered with 500
// + a sanitised body, so a future adapter error mode does not leak
// raw text to clients while we are still adding the translation.
func translateAdminTablesError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, adapter.ErrAdminForbidden):
		return admin.ErrTablesForbidden
	case errors.Is(err, adapter.ErrAdminNotLeader):
		return admin.ErrTablesNotLeader
	case adapter.IsAdminTableAlreadyExists(err):
		return admin.ErrTablesAlreadyExists
	case adapter.IsAdminTableNotFound(err):
		return admin.ErrTablesNotFound
	case adapter.IsAdminValidation(err):
		msg := adapter.AdminErrorMessage(err)
		if msg == "" {
			msg = "validation failed"
		}
		return &admin.ValidationError{Message: msg}
	default:
		return err //nolint:wrapcheck // forwarded so the handler logs but does not surface it.
	}
}

func convertAdminTableSummary(in *adapter.AdminTableSummary) *admin.DynamoTableSummary {
	out := &admin.DynamoTableSummary{
		Name:         in.Name,
		PartitionKey: in.PartitionKey,
		SortKey:      in.SortKey,
		Generation:   in.Generation,
	}
	if len(in.GlobalSecondaryIndexes) == 0 {
		return out
	}
	out.GlobalSecondaryIndexes = make([]admin.DynamoGSISummary, len(in.GlobalSecondaryIndexes))
	for i, g := range in.GlobalSecondaryIndexes {
		out.GlobalSecondaryIndexes[i] = admin.DynamoGSISummary{
			Name:           g.Name,
			PartitionKey:   g.PartitionKey,
			SortKey:        g.SortKey,
			ProjectionType: g.ProjectionType,
		}
	}
	return out
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
//
// The returned address is the actual host:port the listener bound to; it
// differs from adminCfg.Listen only when the caller passed a port of 0,
// but tests rely on this to avoid the bind-close-rebind race that a
// pre-allocated free-port helper would otherwise introduce. When admin
// is disabled the returned address is empty.
func startAdminServer(
	ctx context.Context,
	lc *net.ListenConfig,
	eg *errgroup.Group,
	cfg adminListenerConfig,
	creds map[string]string,
	cluster admin.ClusterInfoSource,
	tables admin.TablesSource,
	version string,
) (string, error) {
	adminCfg := buildAdminConfig(cfg)
	enabled, err := checkAdminConfig(&adminCfg, cluster)
	if err != nil || !enabled {
		return "", err
	}
	server, err := buildAdminHTTPServer(&adminCfg, creds, cluster, tables)
	if err != nil {
		return "", err
	}
	httpSrv := newAdminHTTPServer(server)
	listener, err := lc.Listen(ctx, "tcp", adminCfg.Listen)
	if err != nil {
		return "", errors.Wrapf(err, "failed to listen on admin address %s", adminCfg.Listen)
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
	registerAdminLifecycle(ctx, eg, httpSrv, listener, &boundCfg, tlsEnabled, version)
	return actualAddr, nil
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

func buildAdminHTTPServer(adminCfg *admin.Config, creds map[string]string, cluster admin.ClusterInfoSource, tables admin.TablesSource) (*admin.Server, error) {
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
		Tables:      tables,
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

func newAdminHTTPServer(server *admin.Server) *http.Server {
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
			// Seed as an empty-but-non-nil slice so a
			// Configuration() failure still JSON-encodes as `[]`
			// rather than `null`; API consumers that treat
			// members as an always-array field rely on this.
			members := []string{}
			if cfg, err := rt.engine.Configuration(ctx); err == nil {
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

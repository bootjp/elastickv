// Command elastickv-admin serves the Elastickv admin Web UI described in
// docs/admin_ui_key_visualizer_design.md. Phase 0: token-protected passthrough
// of Admin.GetClusterOverview at /api/cluster/overview, no SPA yet.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
)

const (
	defaultBindAddr             = "127.0.0.1:8080"
	defaultNodesRefreshInterval = 15 * time.Second
	defaultGRPCRequestTimeout   = 10 * time.Second
	discoveryRPCTimeout         = 2 * time.Second
	// discoveryWaitBudget is how long a request handler is willing to wait
	// for the singleflight membership refresh before falling back to the
	// cached (or static seed) list. Kept well below defaultGRPCRequestTimeout
	// so a slow discovery cannot starve the subsequent per-node fan-out.
	discoveryWaitBudget = 3 * time.Second
	// membershipRefreshBudget caps the detached background refresh so it
	// cannot run forever even if every seed is slow. Sized for up to a few
	// sequential discoveryRPCTimeout attempts before the singleflight
	// collapses.
	membershipRefreshBudget = 10 * time.Second
	readHeaderTimeout       = 5 * time.Second
	readTimeout             = 30 * time.Second
	writeTimeout            = 30 * time.Second
	idleTimeout             = 120 * time.Second
	shutdownTimeout         = 5 * time.Second
	maxRequestBodyBytes     = 4 << 10
	// maxTokenFileBytes caps the admin-token file so a misconfigured path
	// pointing at a huge file (for example a log) cannot force the admin
	// process to allocate arbitrary memory before the bearer-token check.
	maxTokenFileBytes = 4 << 10
	// maxDiscoveredNodes bounds the member list returned by a peer's
	// GetClusterOverview so a malicious or misconfigured node cannot force
	// the admin binary to spawn unbounded goroutines / gRPC calls. A single
	// /api/cluster/overview fan-out dials every discovered node; the
	// per-conn cache is sized to match so a healthy cluster-wide query
	// reuses connections instead of thrashing the LRU.
	maxDiscoveredNodes = 512
	// maxCachedClients caps the fanout's cached gRPC connections so a
	// cluster with high node churn or a malicious discovery response
	// cannot leak file descriptors indefinitely. Equal to
	// maxDiscoveredNodes so a single overview fan-out fits without
	// eviction churn; still well below typical ulimits.
	maxCachedClients = maxDiscoveredNodes
)

var (
	bindAddr             = flag.String("bindAddr", defaultBindAddr, "HTTP bind address for the admin UI")
	nodes                = flag.String("nodes", "", "Comma-separated list of elastickv node gRPC addresses")
	nodeTokenFile        = flag.String("nodeTokenFile", "", "File containing the bearer token sent to nodes' Admin service")
	nodesRefreshInterval = flag.Duration("nodesRefreshInterval", defaultNodesRefreshInterval, "Duration to cache cluster membership before re-fetching")
	insecureNoAuth       = flag.Bool("adminInsecureNoAuth", false, "Skip bearer token authentication; development only")
	// Node gRPC is plaintext in Phase 0, so the admin binary defaults to
	// plaintext too. TLS is opt-in: set --nodeTLSCACertFile (preferred) or
	// --nodeTLSInsecureSkipVerify to switch to TLS. When the cluster turns
	// on TLS, operators flip the flag without code changes.
	nodeTLSCACertFile = flag.String("nodeTLSCACertFile", "", "PEM file with CA certificates used to verify nodes' gRPC TLS; setting this flag enables TLS dialing")
	nodeTLSServerName = flag.String("nodeTLSServerName", "", "Expected TLS server name when connecting to nodes (overrides the address host); only honoured when TLS is enabled")
	nodeTLSSkipVerify = flag.Bool("nodeTLSInsecureSkipVerify", false, "Dial nodes with TLS but skip certificate verification; development only. Implies TLS.")
	allowRemoteBind   = flag.Bool("allowRemoteBind", false, "Allow --bindAddr to listen on a non-loopback interface. The admin UI has no browser-facing auth; set this only when the UI is fronted by an authenticating reverse proxy.")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

type runConfig struct {
	seeds []string
	fan   *fanout
}

// initRun consolidates flag parsing and fanout construction so run() stays
// under the project's cyclop budget.
func initRun() (runConfig, error) {
	seeds := splitNodes(*nodes)
	if len(seeds) == 0 {
		return runConfig{}, errors.New("--nodes is required (comma-separated gRPC addresses)")
	}
	token, err := loadToken(*nodeTokenFile, *insecureNoAuth)
	if err != nil {
		return runConfig{}, err
	}
	if err := validateBindAddr(*bindAddr, *allowRemoteBind); err != nil {
		return runConfig{}, err
	}
	creds, err := loadTransportCredentials(*nodeTLSCACertFile, *nodeTLSServerName, *nodeTLSSkipVerify)
	if err != nil {
		return runConfig{}, err
	}
	fan := newFanout(seeds, token, *nodesRefreshInterval, creds)
	return runConfig{seeds: seeds, fan: fan}, nil
}

// buildMux wires the Phase 0 HTTP surface. Lives outside run() both for
// testability and to keep run() under the cyclop budget.
func buildMux(fan *fanout) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/cluster/overview", fan.handleOverview)
	mux.HandleFunc("/api/", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONError(w, http.StatusServiceUnavailable, "endpoint not implemented in phase 0")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("elastickv-admin: phase 0 — SPA not yet embedded\n"))
	})
	return mux
}

func run() error {
	cfg, err := initRun()
	if err != nil {
		return err
	}
	defer cfg.fan.Close()

	srv := &http.Server{
		Addr:              *bindAddr,
		Handler:           buildMux(cfg.fan),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("elastickv-admin listening on %s (seeds=%v)", *bindAddr, cfg.seeds)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return errors.Wrap(err, "shutdown")
		}
		return nil
	case err := <-errCh:
		return err
	}
}

// validateBindAddr rejects a non-loopback bind unless the operator has
// explicitly opted into --allowRemoteBind. The admin binary performs no
// browser-side authentication in Phase 0 while holding a privileged node
// admin token, so a misconfigured 0.0.0.0:8080 would expose that token-gated
// cluster view to anyone on the network.
func validateBindAddr(addr string, allow bool) error {
	if allow {
		return nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.Wrapf(err, "invalid --bindAddr %q", addr)
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return fmt.Errorf("--bindAddr %q has an empty host; pass an explicit loopback host like 127.0.0.1 or set --allowRemoteBind when fronted by an auth proxy", addr)
	}
	ip := net.ParseIP(host)
	switch {
	case host == "localhost":
		return nil
	case ip != nil && ip.IsLoopback():
		return nil
	}
	return fmt.Errorf("--bindAddr %q is not loopback; set --allowRemoteBind to expose the admin UI remotely (the UI has no browser-side auth — do so only behind an auth proxy)", addr)
}

func splitNodes(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func loadToken(path string, insecureMode bool) (string, error) {
	if path == "" {
		if insecureMode {
			return "", nil
		}
		return "", errors.New("--nodeTokenFile is required; pass --adminInsecureNoAuth for insecure dev mode")
	}
	if insecureMode {
		return "", errors.New("--adminInsecureNoAuth and --nodeTokenFile are mutually exclusive")
	}
	tok, err := internalutil.LoadBearerTokenFile(path, maxTokenFileBytes, "admin token")
	if err != nil {
		return "", errors.Wrap(err, "load admin token")
	}
	return tok, nil
}

// loadTransportCredentials builds the gRPC TransportCredentials used to dial
// nodes. Phase 0 nodes expose a plaintext gRPC server, so the default is
// insecure credentials — if neither --nodeTLSCACertFile nor
// --nodeTLSInsecureSkipVerify is set, the admin binary dials plaintext.
// Passing either flag opts into TLS; --nodeTLSServerName is honoured only
// alongside a TLS opt-in.
func loadTransportCredentials(
	caFile, serverName string,
	skipVerify bool,
) (credentials.TransportCredentials, error) {
	tlsRequested := caFile != "" || skipVerify
	if !tlsRequested {
		if serverName != "" {
			return nil, errors.New("--nodeTLSServerName requires TLS; set --nodeTLSCACertFile or --nodeTLSInsecureSkipVerify")
		}
		return insecure.NewCredentials(), nil
	}
	if caFile != "" && skipVerify {
		return nil, errors.New("--nodeTLSCACertFile and --nodeTLSInsecureSkipVerify are mutually exclusive")
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         serverName,
		InsecureSkipVerify: skipVerify, //nolint:gosec // gated behind --nodeTLSInsecureSkipVerify; dev-only.
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, errors.Wrap(err, "read node TLS CA file")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("no certificates parsed from --nodeTLSCACertFile")
		}
		cfg.RootCAs = pool
	}
	return credentials.NewTLS(cfg), nil
}

type nodeClient struct {
	addr   string
	conn   *grpc.ClientConn
	client pb.AdminClient

	// refcount, evicted, and closed are protected by fanout.mu. They let the
	// cache evict entries while RPCs are in flight: eviction removes the
	// entry from the map and marks it evicted, and the conn is closed only
	// once the last borrower calls release. closed guards against a second
	// release on an already-closed client so the public contract (extra
	// release() calls are no-ops) holds even when refcount transiently
	// bounces back to zero.
	refcount int
	evicted  bool
	closed   bool
}

type membership struct {
	addrs     []string
	fetchedAt time.Time
}

type fanout struct {
	seeds []string
	// seedSet is a pre-computed lookup over seeds for evictOneLocked's
	// "skip seed entries" check. Seeds are immutable after construction so
	// rebuilding the map on every cache-full eviction (under f.mu) is pure
	// waste — Gemini flagged the per-call allocation.
	seedSet         map[string]struct{}
	token           string
	refreshInterval time.Duration
	creds           credentials.TransportCredentials

	mu      sync.Mutex
	clients map[string]*nodeClient
	members *membership
	closed  bool

	// refreshGroup deduplicates concurrent membership refresh RPCs so a burst
	// of browser requests immediately after cache expiry collapses into a
	// single GetClusterOverview call against one seed.
	refreshGroup singleflight.Group

	// dialGroup deduplicates concurrent grpc.NewClient calls for the same
	// address. Without it, N goroutines that all miss the cache for the
	// same addr would each run a parallel dial (DNS/parsing/setup); only
	// one is kept. With singleflight, only one dial runs and every waiter
	// gets the same *grpc.ClientConn — refcount is bumped per waiter
	// before they each return.
	dialGroup singleflight.Group
}

// errFanoutClosed is returned by clientFor when Close has already run, so
// callers can treat it as a graceful shutdown signal instead of bubbling up as
// a generic map-panic.
var errFanoutClosed = errors.New("admin fanout is closed")

func newFanout(
	seeds []string,
	token string,
	refreshInterval time.Duration,
	creds credentials.TransportCredentials,
) *fanout {
	if refreshInterval <= 0 {
		refreshInterval = defaultNodesRefreshInterval
	}
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	seedSet := make(map[string]struct{}, len(seeds))
	for _, s := range seeds {
		seedSet[s] = struct{}{}
	}
	return &fanout{
		seeds:           seeds,
		seedSet:         seedSet,
		token:           token,
		refreshInterval: refreshInterval,
		creds:           creds,
		clients:         make(map[string]*nodeClient),
	}
}

func (f *fanout) Close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	// Shutdown is an intentional cancellation of any in-flight RPCs; close
	// connections eagerly and let borrowers see the cancel. Borrowers that
	// still hold leases will observe the conn as closed on their next call.
	// Mark each client closed inside the lock so the deferred release path
	// does not attempt a double-close, then collect the *grpc.ClientConn
	// references and run conn.Close() outside the lock — Close() can do
	// network I/O and waits for the gRPC client transport to drain, which
	// would block any concurrent clientFor / invalidateClient / RPC waiting
	// on f.mu for the entire shutdown window.
	conns := make([]struct {
		addr string
		conn *grpc.ClientConn
	}, 0, len(f.clients))
	for _, c := range f.clients {
		if c.closed {
			continue
		}
		c.closed = true
		conns = append(conns, struct {
			addr string
			conn *grpc.ClientConn
		}{addr: c.addr, conn: c.conn})
	}
	// Replace with an empty map rather than nil so the remaining
	// closed-guarded accessors can still iterate or lookup without panicking
	// while still releasing the client references for GC.
	f.clients = map[string]*nodeClient{}
	f.mu.Unlock()

	for _, e := range conns {
		if err := e.conn.Close(); err != nil {
			log.Printf("elastickv-admin: close gRPC connection to %s: %v", e.addr, err)
		}
	}
}

// clientFor returns a leased nodeClient that callers must release once they
// finish the RPC (release is the second return value, always non-nil and safe
// to call). The cache is bounded by maxCachedClients; if the cache is full,
// one entry is evicted — prefer non-seed victims, fall back to any entry when
// the cache is saturated with seeds. Evicted entries stop accepting new leases
// but their underlying *grpc.ClientConn is kept alive until every outstanding
// borrower has released; this prevents an eviction from canceling a healthy
// concurrent GetClusterOverview.
//
// The dial step (grpc.NewClient) runs outside f.mu through a singleflight
// keyed by addr — concurrent dials for the same addr collapse into one,
// avoiding wasted DNS/parsing work plus the post-dial close-the-loser
// dance. NewClient itself is non-blocking but parses the target and may
// trigger synchronous DNS depending on resolver config, so holding the
// global mutex for that wall-clock time would serialize concurrent
// clientFor calls for distinct addrs.
func (f *fanout) clientFor(addr string) (*nodeClient, func(), error) {
	if c, release, err, ok := f.cacheLookup(addr); ok {
		return c, release, err
	}
	conn, err := f.dialDeduped(addr)
	if err != nil {
		return nil, func() {}, err
	}
	return f.installOrAttach(addr, conn)
}

// cacheLookup returns (client, release, err, true) when either the cache hit
// or the fanout-closed branch fires; the caller can short-circuit. Returns
// (_,_,_,false) when the caller still needs to dial.
func (f *fanout) cacheLookup(addr string) (*nodeClient, func(), error, bool) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, func() {}, errFanoutClosed, true
	}
	if c, ok := f.clients[addr]; ok {
		c.refcount++
		release := f.releaseFunc(c)
		f.mu.Unlock()
		return c, release, nil, true
	}
	f.mu.Unlock()
	return nil, nil, nil, false
}

// dialDeduped runs grpc.NewClient inside the dialGroup singleflight so
// concurrent first-time dials for addr collapse to one conn.
func (f *fanout) dialDeduped(addr string) (*grpc.ClientConn, error) {
	v, err, _ := f.dialGroup.Do(addr, func() (any, error) {
		return grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(f.creds),
			internalutil.GRPCCallOptions(),
		)
	})
	if err != nil {
		return nil, errors.Wrapf(err, "dial %s", addr)
	}
	conn, ok := v.(*grpc.ClientConn)
	if !ok {
		return nil, fmt.Errorf("dial %s: unexpected singleflight value type %T", addr, v)
	}
	return conn, nil
}

// installOrAttach installs the just-dialed conn into the cache or, if a
// concurrent waiter beat us to it, takes a lease on the existing entry and
// closes the orphaned conn (when its pointer differs from the cached entry).
func (f *fanout) installOrAttach(addr string, conn *grpc.ClientConn) (*nodeClient, func(), error) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		if err := conn.Close(); err != nil {
			log.Printf("elastickv-admin: close orphaned dial for %s after shutdown: %v", addr, err)
		}
		return nil, func() {}, errFanoutClosed
	}
	// If another waiter already installed a cache entry, take a lease on it.
	// Two cases: (a) singleflight collapsed concurrent dials so the cached
	// entry's conn IS this conn (same pointer) — must NOT Close it because
	// the cache holds the only reference; (b) a non-concurrent earlier dial
	// installed a different conn before our Do call — our just-dialed conn
	// is orphaned and must be closed to avoid leaking fds/goroutines.
	if c, ok := f.clients[addr]; ok {
		c.refcount++
		release := f.releaseFunc(c)
		shouldClose := c.conn != conn
		f.mu.Unlock()
		if shouldClose {
			if err := conn.Close(); err != nil {
				log.Printf("elastickv-admin: close orphaned dial for %s: %v", addr, err)
			}
		}
		return c, release, nil
	}
	var evicted *grpc.ClientConn
	if len(f.clients) >= maxCachedClients {
		evicted = f.evictOneLocked()
	}
	c := &nodeClient{addr: addr, conn: conn, client: pb.NewAdminClient(conn), refcount: 1}
	f.clients[addr] = c
	release := f.releaseFunc(c)
	f.mu.Unlock()
	if evicted != nil {
		if err := evicted.Close(); err != nil {
			log.Printf("elastickv-admin: evict-close: %v", err)
		}
	}
	return c, release, nil
}

// releaseFunc returns the closer used to drop a lease. On the last release
// of an evicted client the underlying connection is finally closed; that
// Close() runs after f.mu is dropped because grpc.ClientConn.Close can do
// network I/O and waits for the transport to drain — holding the global
// fanout mutex across that would block any concurrent clientFor /
// invalidateClient / RPC waiting on f.mu. Extra release() calls after the
// conn is already closed are safe no-ops.
func (f *fanout) releaseFunc(c *nodeClient) func() {
	return func() {
		f.mu.Lock()
		if c.refcount > 0 {
			c.refcount--
		}
		var toClose *grpc.ClientConn
		if c.refcount == 0 && c.evicted && !c.closed {
			c.closed = true
			toClose = c.conn
		}
		f.mu.Unlock()

		if toClose == nil {
			return
		}
		if err := toClose.Close(); err != nil {
			log.Printf("elastickv-admin: deferred close for %s: %v", c.addr, err)
		}
	}
}

// evictOneLocked removes exactly one entry from f.clients. Prefers non-seed
// entries; falls back to any entry if none are eligible (for example when
// len(seeds) >= maxCachedClients). Returns the *grpc.ClientConn that needs
// closing (or nil if the entry has outstanding leases or was already
// closed) — caller must run Close() outside f.mu. Closing is deferred to
// the last release (see releaseFunc) when leases are still held.
func (f *fanout) evictOneLocked() *grpc.ClientConn {
	var fallback string
	var fallbackClient *nodeClient
	for victim, vc := range f.clients {
		if fallback == "" {
			fallback, fallbackClient = victim, vc
		}
		if _, keep := f.seedSet[victim]; keep {
			continue
		}
		return f.retireLocked(victim, vc)
	}
	if fallbackClient != nil {
		return f.retireLocked(fallback, fallbackClient)
	}
	return nil
}

// retireLocked removes a client from the cache and, if no lease is currently
// held, marks it for closing. Returns the connection that needs to be closed
// (or nil) so the caller can run conn.Close() outside f.mu — Close() blocks
// on transport teardown and must not run with the global fanout mutex held.
// Otherwise the connection stays open until the last borrower releases, so
// an evicted entry never cancels an in-flight RPC. Idempotent — double-retiring
// or retiring after the last release is a no-op. Caller must hold f.mu.
func (f *fanout) retireLocked(addr string, c *nodeClient) *grpc.ClientConn {
	delete(f.clients, addr)
	if c.evicted {
		return nil
	}
	c.evicted = true
	if c.refcount > 0 || c.closed {
		return nil
	}
	c.closed = true
	return c.conn
}

// invalidateClient drops a cached connection — used when a peer returns
// Unavailable so the next request re-dials or skips the removed node. The
// connection stays open until the last borrower releases, so invalidating
// does not cancel other goroutines' in-flight RPCs.
func (f *fanout) invalidateClient(addr string) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.members = nil
	var toClose *grpc.ClientConn
	if c, ok := f.clients[addr]; ok {
		toClose = f.retireLocked(addr, c)
	}
	f.mu.Unlock()

	if toClose != nil {
		if err := toClose.Close(); err != nil {
			log.Printf("elastickv-admin: invalidate %s: close: %v", addr, err)
		}
	}
}

func (f *fanout) outgoingCtx(parent context.Context) context.Context {
	if f.token == "" {
		return parent
	}
	return metadata.AppendToOutgoingContext(parent, "authorization", "Bearer "+f.token)
}

// currentTargets returns the list of node addresses to fan out to. If the
// membership cache is fresh it is returned directly; otherwise the admin binary
// queries seeds via GetClusterOverview and caches the resulting member list
// for refreshInterval. Concurrent refreshes are collapsed through singleflight
// so a burst of requests after cache expiry hits only one seed. The shared
// refresh runs on a detached background context bounded by
// membershipRefreshBudget so one caller canceling (e.g., browser tab close)
// does not abort the work for every other concurrent waiter. On total failure
// the admin binary falls back to the static seed list so a single unreachable
// seed does not take the admin offline.
func (f *fanout) currentTargets(ctx context.Context) []string {
	f.mu.Lock()
	if f.members != nil && time.Since(f.members.fetchedAt) < f.refreshInterval {
		addrs := append([]string(nil), f.members.addrs...)
		f.mu.Unlock()
		return addrs
	}
	f.mu.Unlock()

	ch := f.refreshGroup.DoChan("members", func() (any, error) {
		bgCtx, cancel := context.WithTimeout(context.Background(), membershipRefreshBudget)
		defer cancel()
		return f.refreshMembership(bgCtx), nil
	})
	select {
	case r := <-ch:
		// refreshMembership always returns a []string today, but explicitly
		// check the assertion so a future return-type change turns into a
		// loud, non-panicking fallback to seeds instead of a silent crash.
		if addrs, ok := r.Val.([]string); ok {
			return addrs
		}
		log.Printf("elastickv-admin: membership refresh returned unexpected type %T; falling back to seeds", r.Val)
		return append([]string(nil), f.seeds...)
	case <-ctx.Done():
		// Caller bailed. Give them whatever targets we can assemble without
		// blocking: the last cached membership if we have one, else seeds.
		// The detached refresh continues in the background and will populate
		// the cache for the next request.
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.members != nil {
			return append([]string(nil), f.members.addrs...)
		}
		return append([]string(nil), f.seeds...)
	}
}

// refreshMembership performs the actual discovery RPC. It honours the caller's
// context for overall cancellation but derives a short per-seed timeout from
// discoveryRPCTimeout so a slow first seed does not stall the whole request.
func (f *fanout) refreshMembership(ctx context.Context) []string {
	for _, seed := range f.seeds {
		cli, release, err := f.clientFor(seed)
		if err != nil {
			log.Printf("elastickv-admin: dial seed %s: %v", seed, err)
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, discoveryRPCTimeout)
		resp, err := cli.client.GetClusterOverview(f.outgoingCtx(rpcCtx), &pb.GetClusterOverviewRequest{})
		cancel()
		release()
		if err != nil {
			if status.Code(err) == codes.Unavailable {
				f.invalidateClient(seed)
			}
			log.Printf("elastickv-admin: discover membership via %s: %v", seed, err)
			continue
		}
		addrs := membersFrom(seed, resp)
		f.mu.Lock()
		f.members = &membership{addrs: addrs, fetchedAt: time.Now()}
		f.mu.Unlock()
		return append([]string(nil), addrs...)
	}

	log.Printf("elastickv-admin: all seeds unreachable for membership refresh; falling back to static seed list")
	return append([]string(nil), f.seeds...)
}

// membersFrom extracts a deduplicated address list from a cluster overview
// response, always including the node that answered so the answering seed is
// still queried even if it omits itself from members. The result is capped at
// maxDiscoveredNodes so a malicious or misconfigured peer cannot inflate the
// fan-out.
//
// Deduplication keys on NodeID when available, falling back to the raw
// grpc_address otherwise. This prevents a fan-out from querying the same
// node twice when the seed address (e.g. "localhost:50051") and the node's
// self-advertised address (e.g. "127.0.0.1:50051") are different aliases
// for the same process — Codex flagged that the previous address-only
// dedup distorted overview results in that case.
//
// Initial slice capacity is bounded by maxDiscoveredNodes (rather than
// len(members)+1) so a misbehaving peer that returns 10× the cap does not
// force a giant allocation just to truncate immediately afterward.
func membersFrom(seed string, resp *pb.GetClusterOverviewResponse) []string {
	acc := newDiscoveryAccumulator(len(resp.GetMembers()) + 1)

	// Add the seed under the responding node's ID so a later entry for that
	// same NodeID (most likely resp.Self.GrpcAddress, an alias of the seed)
	// is deduped instead of producing a duplicate fan-out target.
	self := resp.GetSelf()
	var selfID string
	if self != nil {
		selfID = self.GetNodeId()
	}
	acc.add(selfID, seed)

	// When the response advertises a different self.GrpcAddress, only add it
	// when we have no NodeID to anchor the seed to (legacy nodes); otherwise
	// the dedup above already covers it.
	if self != nil && selfID == "" {
		acc.add("", self.GetGrpcAddress())
	}

	for _, m := range resp.GetMembers() {
		acc.add(m.GetNodeId(), m.GetGrpcAddress())
	}
	if acc.truncated {
		log.Printf("elastickv-admin: discovery response exceeded %d nodes; truncating (peer=%s)", maxDiscoveredNodes, seed)
	}
	return acc.out
}

// discoveryAccumulator dedups (NodeID, address) pairs while building the
// fan-out target list. Extracted from membersFrom so the surrounding loop
// stays under the cyclop budget.
type discoveryAccumulator struct {
	out       []string
	seenAddr  map[string]struct{}
	seenID    map[string]struct{}
	truncated bool
}

func newDiscoveryAccumulator(suggestedCap int) *discoveryAccumulator {
	if suggestedCap > maxDiscoveredNodes {
		suggestedCap = maxDiscoveredNodes
	}
	return &discoveryAccumulator{
		out:      make([]string, 0, suggestedCap),
		seenAddr: map[string]struct{}{},
		seenID:   map[string]struct{}{},
	}
}

// add records a fan-out target keyed by its NodeID (when known) and address.
// Returns silently when the entry is empty, a duplicate, or would push the
// list past maxDiscoveredNodes; the caller can read truncated to log a
// truncation event once.
func (a *discoveryAccumulator) add(id, addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	if _, dup := a.seenAddr[addr]; dup {
		return
	}
	if id != "" {
		if _, dup := a.seenID[id]; dup {
			return
		}
	}
	if len(a.out) >= maxDiscoveredNodes {
		a.truncated = true
		return
	}
	a.seenAddr[addr] = struct{}{}
	if id != "" {
		a.seenID[id] = struct{}{}
	}
	a.out = append(a.out, addr)
}

// perNodeResult wraps a fan-out response from one node. Data is stored as
// json.RawMessage so it can be filled with a protojson-encoded protobuf
// message — encoding/json would lose the proto3 field-name mapping and
// well-known-type handling.
type perNodeResult struct {
	Node  string          `json:"node"`
	OK    bool            `json:"ok"`
	Error string          `json:"error,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// marshalProto encodes a protobuf message with the JSON mapping that preserves
// proto3 field names and well-known-type semantics.
var protoMarshaler = protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: false}

func marshalProto(m proto.Message) (json.RawMessage, error) {
	raw, err := protoMarshaler.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "protojson marshal")
	}
	return raw, nil
}

func (f *fanout) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	// Split the discovery and per-node fan-out budgets. Reusing one ctx for
	// both lets a slow membership refresh consume the entire deadline and
	// leave the fan-out with an already-canceled context, so separate them.
	discoveryCtx, discoveryCancel := context.WithTimeout(r.Context(), discoveryWaitBudget)
	targets := f.currentTargets(discoveryCtx)
	discoveryCancel()

	ctx, cancel := context.WithTimeout(r.Context(), defaultGRPCRequestTimeout)
	defer cancel()
	results := make([]perNodeResult, len(targets))
	var wg sync.WaitGroup
	for i, addr := range targets {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			entry := perNodeResult{Node: addr}
			cli, release, err := f.clientFor(addr)
			if err != nil {
				entry.Error = err.Error()
				results[i] = entry
				return
			}
			defer release()
			resp, err := cli.client.GetClusterOverview(f.outgoingCtx(ctx), &pb.GetClusterOverviewRequest{})
			if err != nil {
				if status.Code(err) == codes.Unavailable {
					f.invalidateClient(addr)
				}
				entry.Error = err.Error()
				results[i] = entry
				return
			}
			data, mErr := marshalProto(resp)
			if mErr != nil {
				entry.Error = errors.Wrap(mErr, "marshal response").Error()
				results[i] = entry
				return
			}
			entry.OK = true
			entry.Data = data
			results[i] = entry
		}(i, addr)
	}
	wg.Wait()

	writeJSON(w, http.StatusOK, map[string]any{"nodes": results})
}

// maxResponseBodyBytes caps writeJSON's encode buffer. Worst-case sizing:
// fan-out hits at most maxDiscoveredNodes (=512) nodes, each returning a
// GetClusterOverview proto. The proto is dominated by the members list
// (≤maxDiscoveredNodes entries × ~few-hundred bytes each) plus the group
// leaders map (one entry per Raft group; clusters carry tens, not hundreds),
// so the per-node JSON is bounded around ~150 KiB and the aggregated body is
// bounded around 75 MiB even before deduplication. The 128 MiB cap below
// comfortably covers that worst case while still rejecting clearly
// oversized payloads; operators running clusters where the overview
// legitimately exceeds this can raise the constant. Keep this aligned with
// handleOverview's fan-out cap so a misbehaving node cannot force unbounded
// memory growth.
const maxResponseBodyBytes = 128 << 20

// writeJSONBufferPool reuses encode buffers across requests so a steady stream
// of /api/* calls doesn't churn the heap with per-request allocations. The
// pool stores *bytes.Buffer; each user resets and bounds the buffer.
var writeJSONBufferPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// writeJSON marshals body into a pooled, size-capped buffer first, so an
// encoding failure can still surface as a 500 instead of a truncated body
// under a committed 2xx header. The cap (maxResponseBodyBytes) bounds memory
// even if a misbehaving downstream returns an oversized payload.
func writeJSON(w http.ResponseWriter, code int, body any) {
	buf, ok := writeJSONBufferPool.Get().(*bytes.Buffer)
	if !ok {
		buf = new(bytes.Buffer)
	}
	defer func() {
		// Drop very large buffers rather than retaining them in the pool —
		// keeps steady-state memory close to the typical response size.
		const maxRetainBytes = 1 << 20
		if buf.Cap() > maxRetainBytes {
			return
		}
		buf.Reset()
		writeJSONBufferPool.Put(buf)
	}()
	buf.Reset()

	limited := &cappedWriter{w: buf, max: maxResponseBodyBytes}
	if err := json.NewEncoder(limited).Encode(body); err != nil || limited.exceeded {
		if limited.exceeded {
			log.Printf("elastickv-admin: response exceeded %d-byte cap; returning 500", maxResponseBodyBytes)
		} else {
			log.Printf("elastickv-admin: encode JSON response: %v", err)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		const fallback = `{"code":500,"message":"internal server error"}` + "\n"
		if _, werr := w.Write([]byte(fallback)); werr != nil {
			log.Printf("elastickv-admin: write fallback response: %v", werr)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if _, err := w.Write(buf.Bytes()); err != nil {
		log.Printf("elastickv-admin: write JSON response: %v", err)
	}
}

// cappedWriter wraps an io.Writer and refuses writes once `written` would
// exceed `max`. Used by writeJSON so json.Encoder stops streaming bytes into
// the buffer past the cap; the encoder reports the short-write and writeJSON
// returns a 500 instead of an oversized body.
type cappedWriter struct {
	w        *bytes.Buffer
	max      int
	written  int
	exceeded bool
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	if c.exceeded {
		return 0, errors.New("response body cap exceeded")
	}
	if c.written+len(p) > c.max {
		c.exceeded = true
		return 0, fmt.Errorf("response body would exceed %d bytes", c.max)
	}
	n, err := c.w.Write(p)
	c.written += n
	if err != nil {
		return n, errors.Wrap(err, "buffer write")
	}
	return n, nil
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"code": code, "message": msg})
}

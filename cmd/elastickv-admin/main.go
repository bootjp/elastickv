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

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
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
	// maxCachedClients caps the fanout's cached gRPC connections so a cluster
	// with high node churn or a malicious discovery response cannot leak file
	// descriptors indefinitely. Sized to cover tested cluster sizes while
	// staying well below typical ulimits.
	maxCachedClients = 256
	// maxDiscoveredNodes bounds the member list returned by a peer's
	// GetClusterOverview so a malicious or misconfigured node cannot force
	// the admin binary to spawn unbounded goroutines / gRPC calls.
	maxDiscoveredNodes = 512
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
	seeds           []string
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
	return &fanout{
		seeds:           seeds,
		token:           token,
		refreshInterval: refreshInterval,
		creds:           creds,
		clients:         make(map[string]*nodeClient),
	}
}

func (f *fanout) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	// Shutdown is an intentional cancellation of any in-flight RPCs; close
	// connections eagerly and let borrowers see the cancel. Borrowers that
	// still hold leases will observe the conn as closed on their next call.
	// Mark each client closed so the deferred release path does not attempt
	// a double-close.
	for _, c := range f.clients {
		if c.closed {
			continue
		}
		c.closed = true
		if err := c.conn.Close(); err != nil {
			log.Printf("elastickv-admin: close gRPC connection to %s: %v", c.addr, err)
		}
	}
	// Replace with an empty map rather than nil so the remaining
	// closed-guarded accessors can still iterate or lookup without panicking
	// while still releasing the client references for GC.
	f.clients = map[string]*nodeClient{}
}

// clientFor returns a leased nodeClient that callers must release once they
// finish the RPC (release is the second return value, always non-nil and safe
// to call). The cache is bounded by maxCachedClients; if the cache is full,
// one entry is evicted — prefer non-seed victims, fall back to any entry when
// the cache is saturated with seeds. Evicted entries stop accepting new leases
// but their underlying *grpc.ClientConn is kept alive until every outstanding
// borrower has released; this prevents an eviction from canceling a healthy
// concurrent GetClusterOverview.
func (f *fanout) clientFor(addr string) (*nodeClient, func(), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, func() {}, errFanoutClosed
	}
	if c, ok := f.clients[addr]; ok {
		c.refcount++
		return c, f.releaseFunc(c), nil
	}
	if len(f.clients) >= maxCachedClients {
		f.evictOneLocked()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(f.creds))
	if err != nil {
		return nil, func() {}, errors.Wrapf(err, "dial %s", addr)
	}
	c := &nodeClient{addr: addr, conn: conn, client: pb.NewAdminClient(conn), refcount: 1}
	f.clients[addr] = c
	return c, f.releaseFunc(c), nil
}

// releaseFunc returns the closer used to drop a lease. On the last release
// of an evicted client the underlying connection is finally closed. Extra
// release() calls after the conn is already closed are safe no-ops.
func (f *fanout) releaseFunc(c *nodeClient) func() {
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if c.refcount > 0 {
			c.refcount--
		}
		if c.refcount == 0 && c.evicted && !c.closed {
			c.closed = true
			if err := c.conn.Close(); err != nil {
				log.Printf("elastickv-admin: deferred close for %s: %v", c.addr, err)
			}
		}
	}
}

// evictOneLocked removes exactly one entry from f.clients. Prefers non-seed
// entries; falls back to any entry if none are eligible (for example when
// len(seeds) >= maxCachedClients). The underlying connection is closed only
// if no borrowers still hold a lease; otherwise closing is deferred to the
// last release (see releaseFunc). Caller must hold f.mu.
func (f *fanout) evictOneLocked() {
	seeds := make(map[string]struct{}, len(f.seeds))
	for _, s := range f.seeds {
		seeds[s] = struct{}{}
	}
	var fallback string
	var fallbackClient *nodeClient
	for victim, vc := range f.clients {
		if fallback == "" {
			fallback, fallbackClient = victim, vc
		}
		if _, keep := seeds[victim]; keep {
			continue
		}
		f.retireLocked(victim, vc)
		return
	}
	if fallbackClient != nil {
		f.retireLocked(fallback, fallbackClient)
	}
}

// retireLocked removes a client from the cache and, if no lease is currently
// held, closes its connection. Otherwise the connection stays open until the
// last borrower releases, so an evicted entry never cancels an in-flight
// RPC. Idempotent — double-retiring or retiring after the last release is a
// no-op. Caller must hold f.mu.
func (f *fanout) retireLocked(addr string, c *nodeClient) {
	delete(f.clients, addr)
	if c.evicted {
		return
	}
	c.evicted = true
	if c.refcount > 0 || c.closed {
		return
	}
	c.closed = true
	if err := c.conn.Close(); err != nil {
		log.Printf("elastickv-admin: retire %s: close: %v", addr, err)
	}
}

// invalidateClient drops a cached connection — used when a peer returns
// Unavailable so the next request re-dials or skips the removed node. The
// connection stays open until the last borrower releases, so invalidating
// does not cancel other goroutines' in-flight RPCs.
func (f *fanout) invalidateClient(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.members = nil
	if c, ok := f.clients[addr]; ok {
		f.retireLocked(addr, c)
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
func membersFrom(seed string, resp *pb.GetClusterOverviewResponse) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(resp.GetMembers())+1)
	truncated := false
	add := func(addr string) {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			return
		}
		if _, dup := seen[addr]; dup {
			return
		}
		if len(out) >= maxDiscoveredNodes {
			truncated = true
			return
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	add(seed)
	if self := resp.GetSelf(); self != nil {
		add(self.GetGrpcAddress())
	}
	for _, m := range resp.GetMembers() {
		add(m.GetGrpcAddress())
	}
	if truncated {
		log.Printf("elastickv-admin: discovery response exceeded %d nodes; truncating (peer=%s)", maxDiscoveredNodes, seed)
	}
	return out
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

// writeJSON marshals body into a buffer first, so an encoding failure can
// still surface as a 500 instead of a truncated body under a committed 2xx
// header. The admin API response bodies are small (bounded by rows/routes
// caps in later phases), so buffering is safe.
func writeJSON(w http.ResponseWriter, code int, body any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(body); err != nil {
		log.Printf("elastickv-admin: encode JSON response: %v", err)
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

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"code": code, "message": msg})
}

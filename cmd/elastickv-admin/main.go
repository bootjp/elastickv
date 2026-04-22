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
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

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
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	seeds := splitNodes(*nodes)
	if len(seeds) == 0 {
		return errors.New("--nodes is required (comma-separated gRPC addresses)")
	}

	token, err := loadToken(*nodeTokenFile, *insecureNoAuth)
	if err != nil {
		return err
	}

	creds, err := loadTransportCredentials(*nodeTLSCACertFile, *nodeTLSServerName, *nodeTLSSkipVerify)
	if err != nil {
		return err
	}

	fan := newFanout(seeds, token, *nodesRefreshInterval, creds)
	defer fan.Close()

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

	srv := &http.Server{
		Addr:              *bindAddr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("elastickv-admin listening on %s (seeds=%v)", *bindAddr, seeds)
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
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", errors.Wrap(err, "resolve token path")
	}
	info, err := os.Stat(abs)
	if err != nil {
		return "", errors.Wrap(err, "stat token file")
	}
	if info.Size() > maxTokenFileBytes {
		return "", fmt.Errorf("token file %s is %d bytes; maximum is %d — refusing to load",
			abs, info.Size(), maxTokenFileBytes)
	}
	// Size is bounded above, so materializing the file is safe.
	b, err := os.ReadFile(abs)
	if err != nil {
		return "", errors.Wrap(err, "read token file")
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", errors.New("token file is empty")
	}
	return token, nil
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
	for _, c := range f.clients {
		if err := c.conn.Close(); err != nil {
			log.Printf("elastickv-admin: close gRPC connection to %s: %v", c.addr, err)
		}
	}
	// Replace with an empty map rather than nil so the remaining
	// closed-guarded accessors can still iterate or lookup without panicking
	// while still releasing the client references for GC.
	f.clients = map[string]*nodeClient{}
}

func (f *fanout) clientFor(addr string) (*nodeClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, errFanoutClosed
	}
	if c, ok := f.clients[addr]; ok {
		return c, nil
	}
	// Bound the cache so a high-churn cluster or a stream of hostile
	// discovery responses cannot leak file descriptors. Evict any one entry
	// (map iteration is randomized) to make room; the evicted target will be
	// re-dialed on demand if it comes back. Never evict an address that is in
	// the active seeds list.
	if len(f.clients) >= maxCachedClients {
		seeds := map[string]struct{}{}
		for _, s := range f.seeds {
			seeds[s] = struct{}{}
		}
		for victim, vc := range f.clients {
			if _, keep := seeds[victim]; keep {
				continue
			}
			delete(f.clients, victim)
			if err := vc.conn.Close(); err != nil {
				log.Printf("elastickv-admin: evict %s: close: %v", victim, err)
			}
			break
		}
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(f.creds))
	if err != nil {
		return nil, errors.Wrapf(err, "dial %s", addr)
	}
	c := &nodeClient{addr: addr, conn: conn, client: pb.NewAdminClient(conn)}
	f.clients[addr] = c
	return c, nil
}

// invalidateClient drops a cached connection — used when a peer returns
// Unavailable so the next request re-dials or skips the removed node.
func (f *fanout) invalidateClient(addr string) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	c, ok := f.clients[addr]
	delete(f.clients, addr)
	f.members = nil
	f.mu.Unlock()
	if ok {
		if err := c.conn.Close(); err != nil {
			log.Printf("elastickv-admin: close gRPC connection to %s: %v", addr, err)
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
		addrs, _ := r.Val.([]string)
		return addrs
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
		cli, err := f.clientFor(seed)
		if err != nil {
			log.Printf("elastickv-admin: dial seed %s: %v", seed, err)
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, discoveryRPCTimeout)
		resp, err := cli.client.GetClusterOverview(f.outgoingCtx(rpcCtx), &pb.GetClusterOverviewRequest{})
		cancel()
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
			cli, err := f.clientFor(addr)
			if err != nil {
				entry.Error = err.Error()
				results[i] = entry
				return
			}
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

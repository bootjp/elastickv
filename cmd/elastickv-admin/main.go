// Command elastickv-admin serves the Elastickv admin Web UI described in
// docs/admin_ui_key_visualizer_design.md. Phase 0: token-protected passthrough
// of Admin.GetClusterOverview at /api/cluster/overview, no SPA yet.
package main

import (
	"context"
	"encoding/json"
	"errors"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	defaultBindAddr             = "127.0.0.1:8080"
	defaultNodesRefreshInterval = 15 * time.Second
	defaultGRPCRequestTimeout   = 10 * time.Second
	readHeaderTimeout           = 5 * time.Second
	shutdownTimeout             = 5 * time.Second
)

var (
	bindAddr             = flag.String("bindAddr", defaultBindAddr, "HTTP bind address for the admin UI")
	nodes                = flag.String("nodes", "", "Comma-separated list of elastickv node gRPC addresses")
	nodeTokenFile        = flag.String("nodeTokenFile", "", "File containing the bearer token sent to nodes' Admin service")
	nodesRefreshInterval = flag.Duration("nodesRefreshInterval", defaultNodesRefreshInterval, "Duration to cache cluster membership before re-fetching")
	insecureNoAuth       = flag.Bool("adminInsecureNoAuth", false, "Skip bearer token authentication; development only")
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

	fan := newFanout(seeds, token, *nodesRefreshInterval)
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
			return fmt.Errorf("shutdown: %w", err)
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
		return "", fmt.Errorf("resolve token path: %w", err)
	}
	b, err := os.ReadFile(abs)
	if err != nil {
		return "", fmt.Errorf("read token file: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", errors.New("token file is empty")
	}
	return token, nil
}

type nodeClient struct {
	addr   string
	conn   *grpc.ClientConn
	client pb.AdminClient
}

type fanout struct {
	seeds           []string
	token           string
	refreshInterval time.Duration

	mu      sync.Mutex
	clients map[string]*nodeClient
}

func newFanout(seeds []string, token string, refreshInterval time.Duration) *fanout {
	if refreshInterval <= 0 {
		refreshInterval = defaultNodesRefreshInterval
	}
	return &fanout{
		seeds:           seeds,
		token:           token,
		refreshInterval: refreshInterval,
		clients:         make(map[string]*nodeClient),
	}
}

func (f *fanout) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.clients {
		_ = c.conn.Close()
	}
	f.clients = nil
}

func (f *fanout) clientFor(addr string) (*nodeClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	c := &nodeClient{addr: addr, conn: conn, client: pb.NewAdminClient(conn)}
	f.clients[addr] = c
	return c, nil
}

func (f *fanout) outgoingCtx(parent context.Context) context.Context {
	if f.token == "" {
		return parent
	}
	return metadata.AppendToOutgoingContext(parent, "authorization", "Bearer "+f.token)
}

func (f *fanout) handleOverview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), defaultGRPCRequestTimeout)
	defer cancel()

	type perNode struct {
		Node  string                         `json:"node"`
		OK    bool                           `json:"ok"`
		Error string                         `json:"error,omitempty"`
		Data  *pb.GetClusterOverviewResponse `json:"data,omitempty"`
	}

	results := make([]perNode, len(f.seeds))
	var wg sync.WaitGroup
	for i, addr := range f.seeds {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			entry := perNode{Node: addr}
			cli, err := f.clientFor(addr)
			if err != nil {
				entry.Error = err.Error()
				results[i] = entry
				return
			}
			resp, err := cli.client.GetClusterOverview(f.outgoingCtx(ctx), &pb.GetClusterOverviewRequest{})
			if err != nil {
				entry.Error = err.Error()
				results[i] = entry
				return
			}
			entry.OK = true
			entry.Data = resp
			results[i] = entry
		}(i, addr)
	}
	wg.Wait()

	writeJSON(w, http.StatusOK, map[string]any{"nodes": results})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"code": code, "message": msg})
}

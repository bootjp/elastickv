package monitoring

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/cockroachdb/errors"
)

// NewPprofHandler returns an http.Handler that serves the Go runtime profiling
// endpoints under /debug/pprof/.
func NewPprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// NewPprofServer creates an HTTP server for the pprof debug endpoints,
// optionally protected by bearer-token authentication.
func NewPprofServer(bearerToken string) *http.Server {
	return &http.Server{
		Handler:           ProtectHandler(NewPprofHandler(), bearerToken),
		ReadHeaderTimeout: metricsReadHeaderTimeout,
	}
}

// PprofShutdownTask returns an errgroup task that stops the pprof server on
// context cancellation: a graceful drain bounded by metricsShutdownTimeout,
// then a forced close, so a long profile request (`/debug/pprof/profile?
// seconds=30`) cannot hold the shutdown open or turn it into a failure.
func PprofShutdownTask(ctx context.Context, server *http.Server, address string) func() error {
	return func() error {
		if server == nil {
			return nil
		}
		<-ctx.Done()
		slog.Info("Shutting down pprof server", "address", address, "reason", ctx.Err())
		return ShutdownHTTPServer(ctx, server, "pprof", metricsShutdownTimeout)
	}
}

// PprofServeTask returns an errgroup task that serves the pprof endpoint until
// shutdown.
func PprofServeTask(server *http.Server, listener net.Listener, address string) func() error {
	return func() error {
		if server == nil || listener == nil {
			return nil
		}
		slog.Info("Starting pprof server", "address", address)
		err := server.Serve(listener)
		if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	}
}

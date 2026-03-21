package monitoring

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	metricsReadHeaderTimeout = time.Second
	metricsShutdownTimeout   = 5 * time.Second
)

// AddressRequiresToken reports whether the address is exposed beyond loopback
// and therefore requires bearer-token protection.
func AddressRequiresToken(addr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return true
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "0.0.0.0" || host == "::" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsLoopback()
}

// ProtectHandler wraps a metrics handler with optional bearer-token authentication.
func ProtectHandler(handler http.Handler, bearerToken string) http.Handler {
	if handler == nil {
		return nil
	}
	token := strings.TrimSpace(bearerToken)
	if token == "" {
		return handler
	}
	expectedTokenHash := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		parts := strings.Fields(authHeader)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		providedToken := parts[1]
		providedTokenHash := sha256.Sum256([]byte(providedToken))
		if subtle.ConstantTimeCompare(providedTokenHash[:], expectedTokenHash[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

// NewMetricsServer constructs an HTTP server for the protected metrics handler.
func NewMetricsServer(handler http.Handler, bearerToken string) *http.Server {
	if handler == nil {
		return nil
	}
	return &http.Server{
		Handler:           ProtectHandler(handler, bearerToken),
		ReadHeaderTimeout: metricsReadHeaderTimeout,
	}
}

// MetricsShutdownTask returns an errgroup task that stops the metrics server on context cancellation.
func MetricsShutdownTask(ctx context.Context, server *http.Server, address string) func() error {
	return func() error {
		if server == nil {
			return nil
		}
		<-ctx.Done()
		slog.Info("Shutting down metrics server", "address", address, "reason", ctx.Err())
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), metricsShutdownTimeout)
		defer cancel()
		err := server.Shutdown(shutdownCtx)
		if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	}
}

// MetricsServeTask returns an errgroup task that serves the metrics endpoint until shutdown.
func MetricsServeTask(server *http.Server, listener net.Listener, address string) func() error {
	return func() error {
		if server == nil || listener == nil {
			return nil
		}
		slog.Info("Starting metrics server", "address", address)
		err := server.Serve(listener)
		if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return errors.WithStack(err)
	}
}

package monitoring

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
)

// ShutdownHTTPServer drains server gracefully for at most timeout and then
// forces the remaining connections closed with http.Server.Close. A request
// that never finishes on its own (a pprof profile that was asked for 30
// seconds, a client that stalls mid-body) would otherwise hold Shutdown
// open past the bound, and a graceful stop requested by a signal must
// never wait on it. A forced close after the bound is the expected
// outcome of that bounded shutdown and is reported as success; only a
// Close that itself fails, or a Shutdown failure other than the deadline,
// is returned. A nil server is a no-op. The returned error is wrapped.
func ShutdownHTTPServer(ctx context.Context, server *http.Server, name string, timeout time.Duration) error {
	if server == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	err := server.Shutdown(shutdownCtx)
	switch {
	case err == nil, errors.Is(err, http.ErrServerClosed), errors.Is(err, net.ErrClosed):
		return nil
	case errors.Is(err, context.DeadlineExceeded):
		slog.WarnContext(ctx, "http server graceful shutdown did not finish in time, closing remaining connections",
			slog.String("server", name),
			slog.Duration("timeout", timeout),
		)
		if closeErr := server.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			return errors.Wrapf(closeErr, "close %s server after shutdown timeout", name)
		}
		return nil
	default:
		return errors.Wrapf(err, "shut down %s server", name)
	}
}

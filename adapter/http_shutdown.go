package adapter

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/bootjp/elastickv/monitoring"
)

// httpShutdownTimeout bounds how long an HTTP adapter's Stop waits for
// in-flight requests to finish before closing the remaining connections. A
// client that stalls mid-request (for example an S3 upload that never sends
// its body) would otherwise hold http.Server.Shutdown open indefinitely,
// which in the server process means the shutdown errgroup never drains and
// the WAL/store cleanup behind it never runs. A variable so tests can
// shorten it.
var httpShutdownTimeout = 10 * time.Second

// shutdownHTTPServer drains srv gracefully for at most httpShutdownTimeout
// and then forces the remaining connections closed
// (monitoring.ShutdownHTTPServer). It is safe to call more than once: a
// second call finds no listeners and no connections and returns at once.
// Stop has no error to return, so a failure is logged.
func shutdownHTTPServer(srv *http.Server, name string) {
	if err := monitoring.ShutdownHTTPServer(context.Background(), srv, name, httpShutdownTimeout); err != nil {
		slog.Warn("http adapter shutdown failed",
			slog.String("adapter", name),
			slog.Any("err", err),
		)
	}
}

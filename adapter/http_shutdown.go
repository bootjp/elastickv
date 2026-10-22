package adapter

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
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

// httpHandlerJoinTimeout bounds how long an HTTP adapter's Stop waits,
// once the shutdown bound forced the remaining connections closed, for the
// handlers still running on them. A variable so tests can shorten it.
var httpHandlerJoinTimeout = 2 * time.Second

// httpConnTracker counts the connections an http.Server serves, from the
// accept until the goroutine serving one has returned from its handler.
// http.Server.Close closes the connections but does not wait for their
// handlers, so without it Stop would let the caller close the stores under
// a handler still unwinding out of a store call or a Raft dispatch. The
// count follows ConnState, which net/http reports as StateClosed (or
// StateHijacked) only after the handler returned. All methods are safe on
// a nil tracker, which has nothing to wait for.
type httpConnTracker struct {
	gate  *inflightGate
	conns sync.Map // net.Conn -> struct{}, the connections counted in gate
}

// trackHTTPConns installs the tracker as srv's ConnState hook.
func trackHTTPConns(srv *http.Server) *httpConnTracker {
	t := &httpConnTracker{gate: newInflightGate()}
	srv.ConnState = t.connState
	return t
}

func (t *httpConnTracker) connState(conn net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		// Serve has returned before the join starts, so the gate never
		// refuses here; a refused connection is simply not counted.
		if t.gate.enter() {
			t.conns.Store(conn, struct{}{})
		}
	case http.StateClosed, http.StateHijacked:
		if _, ok := t.conns.LoadAndDelete(conn); ok {
			t.gate.exit()
		}
	case http.StateActive, http.StateIdle:
	}
}

// join waits up to timeout for every counted connection to be done, and
// reports whether they were. Concurrent and later calls share the deadline
// the first one set (see inflightGate.drain).
func (t *httpConnTracker) join(timeout time.Duration) bool {
	if t == nil {
		return true
	}
	return t.gate.drain(timeout)
}

// shutdownHTTPServer drains srv gracefully for at most httpShutdownTimeout
// and then forces the remaining connections closed
// (monitoring.ShutdownHTTPServer), then waits, for at most
// httpHandlerJoinTimeout, for the handlers still running on the closed
// connections (conns). It is safe to call more than once: a second call
// finds no listeners and no connections and returns once the first call's
// wait is over. Stop has no error to return, so a failure is logged.
func shutdownHTTPServer(srv *http.Server, conns *httpConnTracker, name string) {
	if err := monitoring.ShutdownHTTPServer(context.Background(), srv, name, httpShutdownTimeout); err != nil {
		slog.Warn("http adapter shutdown failed",
			slog.String("adapter", name),
			slog.Any("err", err),
		)
	}
	if !conns.join(httpHandlerJoinTimeout) {
		slog.Warn("http adapter handlers still running on closed connections, continuing shutdown",
			slog.String("adapter", name),
			slog.Int64("in_flight", conns.gate.remaining()),
			slog.Duration("timeout", httpHandlerJoinTimeout),
		)
	}
}

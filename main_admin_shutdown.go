package main

import (
	"log/slog"
	"net/http"
	"sync"
	"time"
)

var adminHandlerJoinTimeout = 2 * time.Second

type adminHandlerDrain struct {
	mu       sync.Mutex
	active   int
	stopping bool
	done     chan struct{}
}

func trackAdminHandlers(server *http.Server) *adminHandlerDrain {
	d := &adminHandlerDrain{done: make(chan struct{})}
	handler := server.Handler
	if handler == nil {
		handler = http.DefaultServeMux
	}
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !d.enter() {
			http.Error(w, "server is stopping", http.StatusServiceUnavailable)
			return
		}
		defer d.exit()
		handler.ServeHTTP(w, r)
	})
	return d
}

func (d *adminHandlerDrain) enter() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopping {
		return false
	}
	d.active++
	return true
}

func (d *adminHandlerDrain) exit() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active--
	if d.stopping && d.active == 0 {
		close(d.done)
	}
}

func (d *adminHandlerDrain) join(timeout time.Duration) {
	d.mu.Lock()
	if !d.stopping {
		d.stopping = true
		if d.active == 0 {
			close(d.done)
		}
	}
	d.mu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-d.done:
	case <-timer.C:
		slog.Warn("admin handlers still running after forced connection close", "timeout", timeout)
	}
}

package runtimehealth

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

const observationTimeout = time.Second

// Handler never exposes peer topology, index values, or underlying storage errors.
func (p *Probe) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/livez" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/readyz" && r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), observationTimeout)
		defer cancel()
		result := p.Current(ctx)
		w.Header().Set("Content-Type", "application/json")
		code := http.StatusOK
		if !result.Ready {
			code = http.StatusServiceUnavailable
		}
		w.WriteHeader(code)
		if r.Method != http.MethodHead {
			_ = json.NewEncoder(w).Encode(result)
		}
	})
}

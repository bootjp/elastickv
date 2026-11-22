package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/serverwiring"
	"github.com/stretchr/testify/require"
)

type rampProfiler struct {
	t                  *testing.T
	directory, address string
	busy               atomic.Bool
	wg                 sync.WaitGroup
}

func newRampProfiler(t *testing.T) *rampProfiler {
	t.Helper()
	p := &rampProfiler{t: t, directory: os.Getenv("ELASTICKV_LOAD_PROFILE_DIR")}
	if p.directory == "" {
		return p
	}
	require.NoError(t, os.MkdirAll(p.directory, 0750))
	server, listener, _, err := serverwiring.PreparePprofServer(t.Context(), &net.ListenConfig{}, "127.0.0.1:0", "")
	require.NoError(t, err)
	p.address = "http://" + listener.Addr().String() + "/debug/pprof/"
	previous := runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	t.Cleanup(func() {
		p.wait()
		runtime.SetMutexProfileFraction(previous)
		runtime.SetBlockProfileRate(0)
		require.NoError(t, server.Close())
	})
	go func() { _ = server.Serve(listener) }()
	t.Logf("LOAD profile_endpoint=%s sampling=enabled process=three-nodes-only", p.address)
	return p
}

func (p *rampProfiler) capture(label string) {
	if p.directory == "" {
		return
	}
	if only := os.Getenv("ELASTICKV_LOAD_PROFILE_ONLY"); only != "" && only != label {
		return
	}
	if !p.busy.CompareAndSwap(false, true) {
		p.t.Logf("LOAD profile_skipped label=%s reason=busy", label)
		return
	}
	p.wg.Go(func() {
		defer p.busy.Store(false)
		started := time.Now()
		for _, kind := range []string{"profile?seconds=10", "heap", "mutex", "block"} {
			if err := p.download(label, kind); err != nil {
				p.t.Errorf("capture %s/%s: %v", label, kind, err)
			}
		}
		p.t.Logf("LOAD profile label=%s started=%s elapsed_ms=%d", label, started.UTC().Format(time.RFC3339Nano), time.Since(started).Milliseconds())
	})
}

func (p *rampProfiler) download(label, kind string) error {
	ctx, cancel := context.WithTimeout(p.t.Context(), 20*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.address+kind, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("profile HTTP %d", response.StatusCode)
	}
	name := kind
	if name == "profile?seconds=10" {
		name = "cpu"
	}
	file, err := os.Create(filepath.Join(p.directory, label+"-"+name+".pprof"))
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, response.Body)
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}
func (p *rampProfiler) wait() { p.wg.Wait() }

func (p *rampProfiler) watchJob(catalog *distribution.CatalogStore, id uint64) func() {
	if p.directory == "" {
		return func() {}
	}
	ctx, cancel := context.WithCancel(p.t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				job, found, err := catalog.SplitJob(ctx, id)
				if err == nil && found && job.Phase == distribution.SplitJobPhaseFence && !job.TransferPreflightDone {
					p.capture("preflight")
					return
				}
			}
		}
	}()
	return func() { cancel(); <-done }
}

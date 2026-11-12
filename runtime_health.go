package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/bootjp/elastickv/internal/runtimehealth"
	"github.com/cockroachdb/errors"
)

const healthShutdownTimeout = 5 * time.Second

type runtimeHealthListener interface {
	Listen(context.Context, string, string) (net.Listener, error)
}

func startRuntimeHealth(ctx context.Context, lc runtimeHealthListener, cancel context.CancelFunc, address, localID string) (*runtimehealth.Probe, func(), error) {
	probe := runtimehealth.New(localID)
	if address == "" {
		return probe, func() {}, nil
	}
	listener, err := lc.Listen(ctx, "tcp", address)
	if err != nil {
		return nil, nil, errors.Wrap(err, "listen for private runtime health")
	}
	server := &http.Server{Handler: probe.Handler(), ReadHeaderTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	go probe.Run(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			slog.Error("private health listener failed", "error", serveErr)
			cancel()
		}
	}()
	stop := func() {
		shutdown, cancelShutdown := context.WithTimeout(context.Background(), healthShutdownTimeout)
		defer cancelShutdown()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
		<-done
	}
	return probe, stop, nil
}

func publishRuntimeHealth(probe *runtimehealth.Probe, runtimes []*raftGroupRuntime) {
	sources := make([]runtimehealth.Source, 0, len(runtimes))
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		sources = append(sources, runtimehealth.Source{GroupID: rt.Spec.ID, Engine: func() runtimehealth.Engine {
			engine, ok := rt.snapshotEngine().(runtimehealth.Engine)
			if !ok {
				return nil
			}
			return engine
		}})
	}
	probe.SetSources(sources)
}

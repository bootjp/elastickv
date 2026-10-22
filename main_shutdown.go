package main

import (
	"context"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	shutdownTransferTimeout    = 5 * time.Second
	shutdownTransferRetryDelay = 50 * time.Millisecond
	shutdownGRPCGracePeriod    = 5 * time.Second
	shutdownVoterSuffrage      = "voter"
)

type shutdownLeadershipController interface {
	raftengine.LeaderView
	raftengine.ConfigReader
	TransferLeadershipToServerIfEligible(context.Context, []raftengine.TransferTarget, uint64) error
}

func stopGRPCServer(server *grpc.Server, gracePeriod time.Duration) {
	// Raft and catalog streams can remain open indefinitely. Give in-flight
	// requests time to finish, then release streams so storage cleanup can run.
	forceStop := time.AfterFunc(gracePeriod, server.Stop)
	defer forceStop.Stop()
	server.GracefulStop()
}

func startSignalShutdown(ctx context.Context, eg *errgroup.Group, cancel context.CancelFunc, runtimes []*raftGroupRuntime) func() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	eg.Go(func() error {
		shutdownOnSignal(ctx, cancel, runtimes, signals, shutdownTransferTimeout)
		return nil
	})
	return func() { signal.Stop(signals) }
}

func shutdownOnSignal(ctx context.Context, cancel context.CancelFunc, runtimes []*raftGroupRuntime, signals <-chan os.Signal, timeout time.Duration) {
	select {
	case <-ctx.Done():
		return
	case sig := <-signals:
		slog.Info("shutdown signal received", "signal", sig)
	}
	defer cancel()
	// Keep the server context alive until the transfers finish: canceling it
	// first would close the Raft transport needed to elect the successors.
	transferCtx, stop := context.WithTimeout(ctx, timeout)
	defer stop()
	transferLeadershipOnShutdown(transferCtx, runtimes)
}

func transferLeadershipOnShutdown(ctx context.Context, runtimes []*raftGroupRuntime) {
	var transfers sync.WaitGroup
	for _, rt := range runtimes {
		engine := rt.snapshotEngine()
		admin, ok := engine.(shutdownLeadershipController)
		if !ok || admin.State() != raftengine.StateLeader {
			continue
		}
		transfers.Go(func() {
			slog.Info("transferring leadership before shutdown", "group", rt.spec.id)
			if err := transferGroupLeadershipOnShutdown(ctx, admin); err != nil {
				slog.Warn("shutdown leadership transfer failed; continuing shutdown", "group", rt.spec.id, "error", err)
				return
			}
			slog.Info("shutdown leadership transfer completed", "group", rt.spec.id)
		})
	}
	// All groups share one deadline, so an unavailable group does not delay
	// healthy groups or multiply the shutdown budget by the shard count.
	transfers.Wait()
}

func transferGroupLeadershipOnShutdown(ctx context.Context, admin shutdownLeadershipController) error {
	ticker := time.NewTicker(shutdownTransferRetryDelay)
	defer ticker.Stop()
	for admin.State() == raftengine.StateLeader {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		cfg, err := admin.Configuration(ctx)
		if err != nil {
			return errors.Wrap(err, "read shutdown transfer membership")
		}
		candidates := shutdownTransferCandidates(cfg, admin.Leader().ID)
		if len(candidates) == 0 {
			return errors.WithStack(raftengine.ErrLeadershipTransferNoHealthyTarget)
		}
		// Eligibility excludes inactive replicas and learners. Let Raft catch
		// up the selected voter during transfer rather than imposing a lag cap.
		err = admin.TransferLeadershipToServerIfEligible(ctx, candidates, math.MaxUint64)
		if err == nil || errors.Is(err, raftengine.ErrNotLeader) {
			return nil
		}
		// A pending config change, another transfer, or a replica that is not
		// active yet may settle while the shutdown deadline still has time.
		select {
		case <-ctx.Done():
			return errors.Wrap(errors.Join(err, ctx.Err()), "shutdown transfer deadline")
		case <-ticker.C:
		}
	}
	return nil
}

func shutdownTransferCandidates(cfg raftengine.Configuration, localID string) []raftengine.TransferTarget {
	candidates := make([]raftengine.TransferTarget, 0, len(cfg.Servers))
	for _, server := range cfg.Servers {
		if server.ID != localID && server.Suffrage == shutdownVoterSuffrage {
			candidates = append(candidates, raftengine.TransferTarget{ID: server.ID, Address: server.Address})
		}
	}
	return candidates
}

package main

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	shutdownTransferTimeout    = 5 * time.Second
	shutdownTransferRetryDelay = 50 * time.Millisecond
	shutdownVoterSuffrage      = "voter"
)

type shutdownLeadershipController interface {
	raftengine.LeaderView
	raftengine.ConfigReader
	TransferLeadershipToServerIfEligible(context.Context, []raftengine.TransferTarget, uint64) error
}

// shutdownHandoff keeps startup cancellation immediate, then switches the
// existing signal handler to request a handoff once startup has completed.
type shutdownHandoff struct {
	mu      sync.Mutex
	request context.CancelFunc
	cancel  context.CancelFunc
}

func newShutdownHandoff(cancel context.CancelFunc) *shutdownHandoff {
	return &shutdownHandoff{request: cancel, cancel: cancel}
}

func (h *shutdownHandoff) requestShutdown() {
	h.mu.Lock()
	request := h.request
	h.mu.Unlock()
	request()
}

func (h *shutdownHandoff) arm(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ctx.Err() != nil {
		return
	}
	requests := make(chan struct{})
	h.request = sync.OnceFunc(func() { close(requests) })
	eg.Go(func() error {
		shutdownAfterRequest(ctx, h.cancel, runtimes, requests, shutdownTransferTimeout)
		return nil
	})
}

func shutdownAfterRequest(ctx context.Context, cancel context.CancelFunc, runtimes []*raftGroupRuntime, requests <-chan struct{}, timeout time.Duration) {
	select {
	case <-ctx.Done():
		return
	case <-requests:
	}
	defer cancel()
	// Keep the server context alive until the transfers finish: canceling it
	// first would close the Raft transport needed to elect the successors.
	transferCtx, stop := context.WithTimeout(ctx, timeout)
	defer stop()
	done := make(chan struct{})
	go func() {
		transferLeadershipOnShutdown(transferCtx, runtimes)
		close(done)
	}()
	// Logging may be backpressured. It must not hold root cancellation or
	// the errgroup beyond the handoff deadline.
	select {
	case <-done:
	case <-transferCtx.Done():
	}
}

func transferLeadershipOnShutdown(ctx context.Context, runtimes []*raftGroupRuntime) {
	ticker := time.NewTicker(shutdownTransferRetryDelay)
	defer ticker.Stop()
	for ctx.Err() == nil && transferShutdownLeaderRound(ctx, runtimes) {
		// Revisit followers after each round: they may have acquired a new
		// leadership while another group's handoff was still running.
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func transferShutdownLeaderRound(ctx context.Context, runtimes []*raftGroupRuntime) bool {
	var transfers sync.WaitGroup
	foundLeader := false
	for _, rt := range runtimes {
		engine := rt.snapshotEngine()
		admin, ok := engine.(shutdownLeadershipController)
		if !ok || admin.State() != raftengine.StateLeader {
			continue
		}
		foundLeader = true
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
	return foundLeader
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

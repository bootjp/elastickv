package main

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/keyviz"
	"golang.org/x/sync/errgroup"
)

// keyVizSamplerForCoordinator wraps a *MemSampler in the
// keyviz.Sampler interface understood by ShardedCoordinator. A nil
// sampler returns a typed-nil interface value, so the coordinator's
// `if c.sampler == nil` guard fires and the dispatch hot path skips
// Observe with a single branch.
func keyVizSamplerForCoordinator(s *keyviz.MemSampler) keyviz.Sampler {
	if s == nil {
		return nil
	}
	return s
}

// startKeyVizLeaderTermPublisher polls each Raft group's current term
// at the sampler's flush cadence and publishes it via
// MemSampler.SetLeaderTerm so subsequent column flushes stamp
// MatrixRow.LeaderTerm. The poll cadence is the same as the flush
// step because every flush column should observe a fresh term —
// publishing more often costs RLocks for no benefit; publishing less
// often opens a window where the column inherits a stale term from
// the previous flush.
//
// Skip the goroutine entirely when the sampler is disabled or when
// no runtimes are wired (single-process tests / cmd/client). With no
// publisher running, MatrixRow.LeaderTerm stays zero and the fan-out
// aggregator falls back to the legacy max-merge — no behavior change
// versus PR #709.
func startKeyVizLeaderTermPublisher(ctx context.Context, eg *errgroup.Group, s *keyviz.MemSampler, runtimes []*clusterbootstrap.GroupRuntime) {
	if s == nil || len(runtimes) == 0 {
		return
	}
	eg.Go(func() error {
		step := s.Step()
		if step <= 0 {
			step = keyviz.DefaultStep
		}
		t := time.NewTicker(step)
		defer t.Stop()
		// Publish once immediately so the very first flush column sees
		// a non-zero term — without this, the column built between
		// startup and the first ticker fire would carry LeaderTerm=0
		// for every group, which the fan-out merge interprets as the
		// legacy max-merge fallback.
		publishLeaderTerms(s, runtimes)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				publishLeaderTerms(s, runtimes)
			}
		}
	})
}

// groupTermSnapshot pairs a Raft group ID with the term observed
// from its engine at one publish moment. Pulled out as its own type
// so publishLeaderTermsFromSnapshots can be tested without a real
// raftengine.Engine fake (the interface is too wide to mock cheaply
// for a unit test of this 5-line publication step).
type groupTermSnapshot struct {
	groupID uint64
	term    uint64
}

func publishLeaderTerms(s *keyviz.MemSampler, runtimes []*clusterbootstrap.GroupRuntime) {
	snaps := make([]groupTermSnapshot, 0, len(runtimes))
	for _, rt := range runtimes {
		// snapshotEngine takes engineMu.RLock so a concurrent
		// rt.Close() (which clears rt.engine while holding the
		// write lock) cannot race the publisher's read. On
		// startup-error paths that fire cleanup before
		// joining all goroutines, this lock prevents the
		// race-detector failure and the undefined-behavior
		// nil-pointer dereference Codex round-1/round-2 P2
		// flagged on PR #720.
		engine := clusterbootstrap.RuntimeCore(rt).SnapshotEngine()
		if engine == nil {
			continue
		}
		snaps = append(snaps, groupTermSnapshot{groupID: rt.Spec.ID, term: engine.Status().Term})
	}
	publishLeaderTermsFromSnapshots(s, snaps)
}

// publishLeaderTermsFromSnapshots applies a precomputed
// (groupID, term) set to the sampler. Split out of
// publishLeaderTerms so unit tests can exercise the publish step
// without standing up a full raftengine.Engine.
func publishLeaderTermsFromSnapshots(s *keyviz.MemSampler, snaps []groupTermSnapshot) {
	if s == nil {
		return
	}
	for _, sn := range snaps {
		s.SetLeaderTerm(sn.groupID, sn.term)
	}
}

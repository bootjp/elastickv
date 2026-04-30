package main

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftengine"
)

// sqsLeadershipController is the subset of raftengine.Admin the
// SQS leadership-refusal hook needs. Defined as a small interface
// (rather than taking raftengine.Admin directly) so the test
// double doesn't have to satisfy the full Admin surface.
type sqsLeadershipController interface {
	State() raftengine.State
	TransferLeadership(ctx context.Context) error
	RegisterLeaderAcquiredCallback(fn func()) (deregister func())
}

// installSQSLeadershipRefusal registers a per-group leader-acquired
// observer that refuses leadership of any Raft group hosting a
// partitioned FIFO queue when this binary does NOT advertise the
// htfifo capability. Implements §8 of
// docs/design/2026_04_26_proposed_sqs_split_queue_fifo.md.
//
// # What it protects against
//
// A node rolled BACK to a binary that lacks the HT-FIFO data
// plane but is still elected leader of a Raft group hosting a
// partitioned queue would (1) scan the legacy single-prefix
// keyspace and report no messages (false-empty reads), and (2)
// accept SendMessage writes under the legacy keyspace, hiding
// them from the partition-aware fanout reader. The §11 PR 2
// "PartitionCount > 1 rejected" gate prevents NEW partitioned
// queues from being created in such a cluster, but does nothing
// for queues created BEFORE the rollback. The leadership-refusal
// hook closes that gap by stepping the affected node down via
// TransferLeadership; the cluster picks a peer that still
// advertises htfifo, and the partitioned queue stays correct.
//
// When it does NOT fire
//
//   - The binary advertises htfifo (advertisesHTFIFO=true). The
//     happy-path runtime — every binary past PR 4-B-3b advertises.
//   - No partitioned queue maps to gid (partitionedGroups[gid] is
//     false). Groups that only host non-partitioned data have no
//     partitioned-keyspace contract to break.
//
// Both no-op cases return a no-op deregister so callers can defer
// uniformly.
//
// Lifecycle
//
//   - Startup check: if engine is currently leader at install time,
//     refuse() runs immediately. Otherwise the hook waits for the
//     next leader-acquired transition.
//   - Per-acquisition: RegisterLeaderAcquiredCallback fires on
//     every previous!=Leader → status==Leader edge.
//
// # Concurrency
//
// refuse() offloads TransferLeadership to a goroutine because the
// leader-acquired callback contract is non-blocking — a synchronous
// admin RPC inside the callback would stall refreshStatus. The
// goroutine uses ctx so a coordinator shutdown cancels any
// in-flight transfer.
func installSQSLeadershipRefusal(
	ctx context.Context,
	admin sqsLeadershipController,
	gid uint64,
	partitionedGroups map[uint64]bool,
	advertisesHTFIFO bool,
	logger *slog.Logger,
) func() {
	if admin == nil || !partitionedGroups[gid] || advertisesHTFIFO {
		return func() {}
	}
	if logger == nil {
		logger = slog.Default()
	}
	refuse := func() {
		logger.Warn("sqs: refusing leadership — partitioned queue requires htfifo capability",
			"group", gid)
		// Non-blocking by contract — TransferLeadership submits
		// an admin request that may block on the raft loop. A
		// nested goroutine is the documented pattern for the
		// callback contract.
		go func() {
			if err := admin.TransferLeadership(ctx); err != nil {
				logger.Warn("sqs: TransferLeadership failed",
					"group", gid, "err", err)
			}
		}()
	}
	if admin.State() == raftengine.StateLeader {
		// Startup: this node is already leader. Refuse now so
		// the cluster picks an htfifo-capable peer immediately
		// rather than waiting for a future re-election.
		refuse()
	}
	return admin.RegisterLeaderAcquiredCallback(refuse)
}

// partitionedGroupSet flattens the operator's --sqsFifoPartitionMap
// into a set of group IDs that have at least one partitioned FIFO
// queue. The leadership-refusal hook consults this set per group
// to decide whether the policy check applies.
//
// parseSQSFifoGroupList canonicalises group references as uint64
// strings at config-load time, so the ParseUint call here cannot
// fail in production. A malformed entry is logged-and-skipped
// rather than panicked; the affected group simply won't get the
// refusal hook (the broader config validation in
// validateSQSFifoPartitionMap would have already rejected an
// unknown group, so reaching this branch implies a programmer
// bypassed validation — which is a test-only concern).
func partitionedGroupSet(partitionMap map[string]sqsFifoQueueRouting, logger *slog.Logger) map[uint64]bool {
	if len(partitionMap) == 0 {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	out := make(map[uint64]bool)
	for queue, routing := range partitionMap {
		for _, groupRef := range routing.groups {
			id, err := strconv.ParseUint(groupRef, 10, 64)
			if err != nil {
				logger.Warn("sqs: leadership-refusal: skipping non-uint64 group reference (config validation bypass?)",
					"queue", queue, "group_ref", groupRef, "err", err)
				continue
			}
			out[id] = true
		}
	}
	return out
}

// sqsAdvertisesHTFIFO reports whether this binary's /sqs_health
// endpoint lists the htfifo capability. Wraps the package-internal
// adapter constant so main.go's leadership-refusal install site
// reads the canonical source of truth without exporting the
// constant itself.
func sqsAdvertisesHTFIFO() bool {
	return adapter.AdvertisesHTFIFO()
}

// installSQSLeadershipRefusalAcrossGroups iterates every shard
// runtime and installs the SQS leadership-refusal hook for any
// group hosting a partitioned FIFO queue. Returns a composite
// deregister that fires every per-group deregister so the
// caller can defer it on shutdown.
//
// The hook is a no-op for groups that don't host a partitioned
// queue, and for binaries that advertise htfifo. The composite
// deregister is therefore safe to defer unconditionally — if
// there is nothing to refuse, every per-group deregister is a
// no-op and the outer wrapper returns immediately.
func installSQSLeadershipRefusalAcrossGroups(
	ctx context.Context,
	runtimes []*raftGroupRuntime,
	partitionMap map[string]sqsFifoQueueRouting,
	advertisesHTFIFO bool,
	logger *slog.Logger,
) func() {
	partGroups := partitionedGroupSet(partitionMap, logger)
	if len(partGroups) == 0 {
		return func() {}
	}
	deregisters := make([]func(), 0, len(runtimes))
	for _, rt := range runtimes {
		if rt == nil || rt.engine == nil {
			continue
		}
		admin, ok := rt.engine.(sqsLeadershipController)
		if !ok {
			// Engine implementation lacks Admin surface — log and
			// skip rather than refusing to start. This branch is
			// hit only by test doubles or future engines without
			// the leader-acquired observer; the etcd engine
			// satisfies the interface by construction.
			if logger != nil {
				logger.Warn("sqs: skipping leadership-refusal install for group "+
					"— engine does not implement leader-acquired observer",
					"group", rt.spec.id)
			}
			continue
		}
		dereg := installSQSLeadershipRefusal(
			ctx, admin, rt.spec.id, partGroups, advertisesHTFIFO, logger)
		deregisters = append(deregisters, dereg)
	}
	return func() {
		for _, d := range deregisters {
			d()
		}
	}
}

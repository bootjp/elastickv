package kv

import (
	"context"
	"log/slog"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// rawFenceRetryAttempts bounds how often a fenced raw write is re-dispatched
// with a fresh timestamp. A fence rejection means another entry with a later
// timestamp applied first; the fresh timestamp is above it unless that race
// repeats.
const rawFenceRetryAttempts = 3

// WithCommitTSFenceActivation makes this coordinator, while it leads a group,
// propose the raw and PREPARE apply-order fence activation for that group
// until the activation has applied (--commitTSFence). Enable it only once
// every node runs a binary that understands the activation entry.
func (c *ShardedCoordinator) WithCommitTSFenceActivation() *ShardedCoordinator {
	if c != nil {
		c.commitTSFenceActivation = true
	}
	return c
}

// activateCommitTSFencesAsync runs one activation pass in the background so
// a stalled group cannot delay the HLC lease renewal that shares the tick.
// At most one pass runs at a time.
func (c *ShardedCoordinator) activateCommitTSFencesAsync(ctx context.Context) {
	if c == nil || !c.commitTSFenceActivation {
		return
	}
	if !c.commitTSFenceActivationInFlight.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.commitTSFenceActivationInFlight.Store(false)
		c.activateCommitTSFences(ctx)
	}()
}

// activateCommitTSFences proposes the activation on every group this node
// leads whose store has not applied it yet. A group whose store shows the
// marker is remembered and not read again.
func (c *ShardedCoordinator) activateCommitTSFences(ctx context.Context) {
	for gid, group := range c.groups {
		if _, done := c.commitTSFenceActiveGroups.Load(gid); done {
			continue
		}
		if group == nil || group.Engine == nil || group.Store == nil || group.Engine.State() != raftengine.StateLeader {
			continue
		}
		enabled, err := CommitTSFenceEnabled(ctx, group.Store)
		if err != nil {
			c.logger().WarnContext(ctx, "commit-ts fence activation: read marker", slog.Uint64("group_id", gid), slog.Any("err", err))
			continue
		}
		if enabled {
			c.commitTSFenceActiveGroups.Store(gid, struct{}{})
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, hlcRenewalProposalTimeout)
		// The wrap-aware proposer chain, as for HLC lease entries, so the
		// entry follows the group's Raft envelope cutover.
		_, err = group.Proposer().Propose(pctx, EncodeCommitTSFenceActivation())
		cancel()
		if err != nil {
			c.logger().WarnContext(ctx, "commit-ts fence activation: propose", slog.Uint64("group_id", gid), slog.Any("err", err))
		}
	}
}

// retryableRawFenceConflict reports whether a raw dispatch that failed with
// err can be re-dispatched with a fresh timestamp. Only a write conflict
// qualifies (a fenced raw entry, or one below a newer version, which is the
// same reordering seen per key); nothing of a rejected raw entry applied,
// and a raw write carries no read set. A multi-group raw dispatch is not
// retried, because the other groups' entries may have applied. A route
// write-timestamp floor rejection also reads as a write conflict; it keeps
// its own contract (the caller sees it) and is not retried here.
func (c *ShardedCoordinator) retryableRawFenceConflict(reqs *OperationGroup[OP], groupCount int, err error) bool {
	return errors.Is(err, store.ErrWriteConflict) &&
		!errors.Is(err, ErrRouteWriteTimestampTooLow) &&
		groupCount == 1 && c.canRetryRawVersionGC(reqs)
}

// dispatchRawWithFenceRetry dispatches a raw request, and re-dispatches it
// with a fresh timestamp when the apply rejected it as out of timestamp
// order (retryableRawFenceConflict).
func (c *ShardedCoordinator) dispatchRawWithFenceRetry(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, int, error) {
	var (
		resp       *CoordinateResponse
		groupCount int
		err        error
	)
	for attempt := 0; attempt <= rawFenceRetryAttempts; attempt++ {
		var handled bool
		groupCount = 0
		resp, handled, err = c.dispatchBeforeShardRouting(ctx, reqs)
		if !handled {
			resp, groupCount, err = c.dispatchNonTxn(ctx, reqs)
		}
		if err == nil || !c.retryableRawFenceConflict(reqs, groupCount, err) {
			return resp, groupCount, err
		}
	}
	return resp, groupCount, err
}

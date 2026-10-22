package adapter

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
)

// Delta and migration work share both the timeout and its rotation. Separate
// rotations still starve migration when any delta handler exhausts every tick.
func (c *DeltaCompactor) compactBackgroundRotation(ctx context.Context, readTS uint64, snapshotAt time.Time) error {
	if !c.ttlInlineMu.TryLock() {
		return nil
	}
	defer c.ttlInlineMu.Unlock()
	plans := c.ttlInlineMigrationTickPlans()
	c.pruneTTLInlineMigrationPlanStates(plans)
	var handlers []collectionDeltaHandler
	if c.coord.IsLeader() {
		handlers = c.allHandlers()
	}
	total := len(handlers) + len(plans)
	start := c.rotationStart(&c.backgroundCursor, total)
	var combined error
	for offset := range total {
		if ctx.Err() != nil {
			break
		}
		index := (start + offset) % total
		err := c.runBackgroundSlot(ctx, handlers, plans, index, readTS, snapshotAt)
		c.advanceRotation(&c.backgroundCursor, index, total)
		if err != nil && !errors.Is(err, context.Canceled) {
			combined = errors.CombineErrors(combined, err)
		}
	}
	return errors.WithStack(errors.CombineErrors(combined, ctx.Err()))
}

func (c *DeltaCompactor) runBackgroundSlot(ctx context.Context, handlers []collectionDeltaHandler, plans []ttlInlineMigrationTickPlan, index int, readTS uint64, snapshotAt time.Time) error {
	if index < len(handlers) {
		return c.compactHandlerSafely(ctx, handlers[index], readTS)
	}
	plan := plans[index-len(handlers)]
	return c.migrateTTLInlineScanPlan(ctx, plan.handler, plan.plan, readTS, snapshotAt)
}

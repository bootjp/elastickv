package adapter

import (
	"context"
	"log/slog"
	"time"

	"github.com/bootjp/elastickv/monitoring"
)

// ttlInlineMigrationTally counts what the TTL-inline migrator did with the
// anchors it read.
type ttlInlineMigrationTally struct {
	// keysScanned counts the anchors read, i.e. the rows whose user key the
	// handler could extract.
	keysScanned int
	// keysMigrated counts the anchors rewritten by a dispatched batch.
	keysMigrated int
	// keysSkipped counts the anchors left alone because this node did not
	// lead their user key's route.
	keysSkipped int
	// keysFailed counts the anchors whose rewrite could not be built.
	keysFailed int
}

func (t *ttlInlineMigrationTally) add(o ttlInlineMigrationTally) {
	t.keysScanned += o.keysScanned
	t.keysMigrated += o.keysMigrated
	t.keysSkipped += o.keysSkipped
	t.keysFailed += o.keysFailed
}

// ttlInlineMigrationPass is one pass of a scan plan over its prefix: the pages
// from the start of the prefix through the page that reaches its end.
type ttlInlineMigrationPass struct {
	ttlInlineMigrationTally
	// full reports that the pass started at the beginning of the prefix and
	// its group stayed in every tick's plans since. A pass that resumes a
	// cursor left before its group dropped out of the plans is not full.
	full      bool
	pages     int
	startedAt time.Time
}

// clean reports that the pass read every anchor of its plan and found each one
// already in the inline format: it read the whole prefix, rewrote nothing, and
// left nothing unchecked.
func (p ttlInlineMigrationPass) clean() bool {
	return p.full && p.foundNothing()
}

// foundNothing reports that the pass rewrote, skipped and failed no anchor.
func (p ttlInlineMigrationPass) foundNothing() bool {
	return p.keysMigrated == 0 && p.keysSkipped == 0 && p.keysFailed == 0
}

// outcome is the pass's monitoring outcome: clean, partial (found nothing but
// did not read the whole prefix), or not_clean.
func (p ttlInlineMigrationPass) outcome() string {
	switch {
	case p.clean():
		return monitoring.RedisTTLInlinePassClean
	case p.foundNothing():
		return monitoring.RedisTTLInlinePassPartial
	default:
		return monitoring.RedisTTLInlinePassNotClean
	}
}

// ttlInlineMigrationPlanState is a scan plan's pass bookkeeping.
type ttlInlineMigrationPlanState struct {
	// pass is the open pass, nil between passes.
	pass *ttlInlineMigrationPass
	// lastClean reports that the plan's previous pass was clean. It keeps
	// repeated clean passes out of the info log.
	lastClean bool
}

// openTTLInlineMigrationPassLocked returns the open pass of the plan named
// name, starting one if needed. A page that starts at the beginning of the
// prefix always opens a new pass unless the open pass has not completed a page
// yet. A pass the page opens starts at the page's snapshot time, no later than
// its read timestamp was acquired. Callers hold cursorMu.
func (c *DeltaCompactor) openTTLInlineMigrationPassLocked(name string, page ttlInlineMigrationPage) *ttlInlineMigrationPass {
	state := c.ttlInlinePlans[name]
	if state == nil {
		state = &ttlInlineMigrationPlanState{}
		c.ttlInlinePlans[name] = state
	}
	if state.pass == nil || (page.fromStart && state.pass.pages > 0) {
		state.pass = &ttlInlineMigrationPass{full: page.fromStart, startedAt: page.snapshotAt}
	}
	return state.pass
}

// noteTTLInlineMigrationPageFailed adds the keys a failed page already
// rewrote to its plan's pass. The page itself does not count: its cursor stays,
// and the next tick reads it again.
func (c *DeltaCompactor) noteTTLInlineMigrationPageFailed(name string, page ttlInlineMigrationPage, migrated int) {
	if migrated == 0 {
		return
	}
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	c.openTTLInlineMigrationPassLocked(name, page).keysMigrated += migrated
}

// finishTTLInlineMigrationPage adds a migrated page to its plan's pass and
// moves the plan's cursor past it. When the page reached the end of the prefix
// it closes the pass and returns it with done set, and whether the plan's
// previous pass was clean.
func (c *DeltaCompactor) finishTTLInlineMigrationPage(
	name string,
	page ttlInlineMigrationPage,
	tally ttlInlineMigrationTally,
) (pass ttlInlineMigrationPass, prevClean bool, done bool) {
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	open := c.openTTLInlineMigrationPassLocked(name, page)
	open.add(tally)
	open.pages++
	if !c.advanceCursorLocked(name, page.cursorKey, page.truncated) {
		return ttlInlineMigrationPass{}, false, false
	}
	state := c.ttlInlinePlans[name]
	pass, prevClean = *open, state.lastClean
	state.pass = nil
	state.lastClean = pass.clean()
	return pass, prevClean, true
}

// pruneTTLInlineMigrationPlanStates drops the bookkeeping of plans this tick
// no longer has, such as a group whose leadership moved to another node. The
// plan's cursor stays, so migration resumes where it stopped if the group
// comes back; the pass it resumes is not full and so cannot be clean.
func (c *DeltaCompactor) pruneTTLInlineMigrationPlanStates(plans []ttlInlineMigrationTickPlan) {
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	if len(c.ttlInlinePlans) == 0 {
		return
	}
	live := make(map[string]struct{}, len(plans))
	for _, p := range plans {
		live[p.plan.cursorName] = struct{}{}
	}
	for name := range c.ttlInlinePlans {
		if _, ok := live[name]; !ok {
			delete(c.ttlInlinePlans, name)
		}
	}
}

// reportTTLInlineMigrationPass logs a completed pass and hands it to the
// observer. A pass that is not clean, or the first clean pass after one that
// was not, is logged at info; repeated clean passes are logged at debug so an
// idle migrator does not log every tick. group_id 0 is the whole-prefix scan
// used when the coordinator does not report its local leader groups.
func (c *DeltaCompactor) reportTTLInlineMigrationPass(
	ctx context.Context,
	h ttlInlineMigrationHandler,
	plan ttlInlineMigrationScanPlan,
	pass ttlInlineMigrationPass,
	prevClean bool,
) {
	clean := pass.clean()
	level := slog.LevelInfo
	if clean && prevClean {
		level = slog.LevelDebug
	}
	c.logger.Log(ctx, level, "ttl inline migrator: pass complete",
		"type", h.typeName,
		"group_id", plan.groupID,
		"clean", clean,
		"full_pass", pass.full,
		"pages", pass.pages,
		"keys_scanned", pass.keysScanned,
		"keys_migrated", pass.keysMigrated,
		"keys_skipped", pass.keysSkipped,
		"keys_failed", pass.keysFailed,
		"duration", time.Since(pass.startedAt),
	)
	if c.ttlInlineObserver != nil {
		c.ttlInlineObserver.ObserveRedisTTLInlineMigrationPass(h.typeName, plan.groupID, pass.outcome(), pass.startedAt)
	}
}

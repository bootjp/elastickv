package adapter

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// LegacyMetadataReasonCounts describes one owner group's physical legacy
// base-metadata rows observed during a complete scan plan pass.
type LegacyMetadataReasonCounts struct {
	Served            uint64
	OwnerHasVersion   uint64
	OwnerLedElsewhere uint64
	StagedVisibility  uint64
}

// LegacyMetadataPassSnapshot is diagnostic only, not a Phase-3 gate verdict.
// A snapshot is published only after a full pass under one catalog version
// and one local leader term.
type LegacyMetadataPassSnapshot struct {
	GroupID         uint64
	Plan            string
	CatalogVersion  uint64
	LeaderTerm      uint64
	StartedAt       time.Time
	CompletionEpoch time.Time
	Counts          map[uint64]LegacyMetadataReasonCounts
}

type legacyMetadataPublishedPass struct {
	snapshot LegacyMetadataPassSnapshot
	scanner  ttlInlineDiagnosticScanner
}

func isLegacyMetadataMigrationHandler(name string) bool {
	switch name {
	case ttlInlineHashPlan, ttlInlineSetPlan, ttlInlineZSetPlan, ttlInlineStreamPlan:
		return true
	default:
		return false
	}
}

func (p *ttlInlineMigrationPass) addLegacyMetadataPage(page ttlInlineMigrationPage) {
	if !page.diagnostic {
		return
	}
	if p.legacyCounts == nil {
		p.legacyCounts = make(map[uint64]LegacyMetadataReasonCounts)
	}
	for _, row := range page.legacyRows {
		counts := p.legacyCounts[row.OwnerGroupID]
		switch row.Reason {
		case kv.LegacyMetadataServed:
			counts.Served++
		case kv.LegacyMetadataOwnerHasVersion:
			counts.OwnerHasVersion++
		case kv.LegacyMetadataOwnerLedElsewhere:
			counts.OwnerLedElsewhere++
		case kv.LegacyMetadataStaged:
			counts.StagedVisibility++
		}
		p.legacyCounts[row.OwnerGroupID] = counts
	}
}

func (c *DeltaCompactor) acceptLegacyMetadataPage(plan ttlInlineMigrationScanPlan, page ttlInlineMigrationPage) bool {
	if !page.diagnostic {
		return true
	}
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	if state := c.ttlInlinePlans[plan.cursorName]; state != nil && state.pass != nil && state.pass.pages > 0 &&
		(state.pass.catalogVersion != page.catalogVersion || state.pass.leaderTerm != page.leaderTerm) {
		c.discardLegacyMetadataPassLocked(plan.cursorName)
		return false
	}
	if published, ok := c.legacyMetadataPasses[plan.cursorName]; ok &&
		(published.snapshot.CatalogVersion != page.catalogVersion || published.snapshot.LeaderTerm != page.leaderTerm) {
		delete(c.legacyMetadataPasses, plan.cursorName)
	}
	return true
}

func (c *DeltaCompactor) checkLegacyMetadataPageIdentity(ctx context.Context, plan ttlInlineMigrationScanPlan, page ttlInlineMigrationPage) error {
	if !page.diagnostic {
		return nil
	}
	scanner, ok := plan.scanner.(ttlInlineDiagnosticScanner)
	if !ok {
		return errors.WithStack(kv.ErrReadRouteVersionUnavailable)
	}
	version, term, err := scanner.GroupOwnedScanIdentity(ctx, plan.groupID)
	if err != nil {
		return errors.WithStack(err)
	}
	if version != page.catalogVersion || term != page.leaderTerm {
		return errors.WithStack(kv.ErrReadRouteVersionUnavailable)
	}
	return nil
}

func (c *DeltaCompactor) discardLegacyMetadataPass(plan ttlInlineMigrationScanPlan) {
	if _, ok := plan.scanner.(ttlInlineDiagnosticScanner); !ok {
		return
	}
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	c.discardLegacyMetadataPassLocked(plan.cursorName)
}

func (c *DeltaCompactor) discardLegacyMetadataPassLocked(name string) {
	delete(c.ttlInlinePlans, name)
	delete(c.legacyMetadataPasses, name)
	delete(c.cursors, name)
}

func (c *DeltaCompactor) publishLegacyMetadataPass(ctx context.Context, h ttlInlineMigrationHandler,
	plan ttlInlineMigrationScanPlan, pass ttlInlineMigrationPass) {
	scanner, ok := plan.scanner.(ttlInlineDiagnosticScanner)
	if !ok || !isLegacyMetadataMigrationHandler(h.typeName) || !pass.full ||
		pass.catalogVersion == 0 || pass.leaderTerm == 0 {
		return
	}
	version, term, err := scanner.GroupOwnedScanIdentity(ctx, plan.groupID)
	if err != nil || version != pass.catalogVersion || term != pass.leaderTerm {
		c.discardLegacyMetadataPass(plan)
		return
	}
	snapshot := LegacyMetadataPassSnapshot{
		GroupID: plan.groupID, Plan: h.typeName, CatalogVersion: pass.catalogVersion,
		LeaderTerm: pass.leaderTerm, StartedAt: pass.startedAt, CompletionEpoch: time.Now().UTC(),
		Counts: cloneLegacyMetadataCounts(pass.legacyCounts),
	}
	c.cursorMu.Lock()
	c.legacyMetadataPasses[plan.cursorName] = legacyMetadataPublishedPass{snapshot: snapshot, scanner: scanner}
	c.cursorMu.Unlock()
	c.logger.Log(ctx, slog.LevelInfo, "ttl inline migrator: legacy metadata pass complete",
		"type", snapshot.Plan, "group_id", snapshot.GroupID, "catalog_version", snapshot.CatalogVersion,
		"leader_term", snapshot.LeaderTerm, "completion_epoch", snapshot.CompletionEpoch, "owner_counts", snapshot.Counts)
}

func cloneLegacyMetadataCounts(in map[uint64]LegacyMetadataReasonCounts) map[uint64]LegacyMetadataReasonCounts {
	out := make(map[uint64]LegacyMetadataReasonCounts, len(in))
	for group, counts := range in {
		out[group] = counts
	}
	return out
}

// LegacyMetadataPassSnapshots returns only snapshots whose group still has the
// same local leader term and catalog version. It does not perform M4 gating.
func (c *DeltaCompactor) LegacyMetadataPassSnapshots(ctx context.Context) []LegacyMetadataPassSnapshot {
	c.cursorMu.Lock()
	entries := make(map[string]legacyMetadataPublishedPass, len(c.legacyMetadataPasses))
	for name, entry := range c.legacyMetadataPasses {
		entries[name] = entry
	}
	c.cursorMu.Unlock()
	result := make([]LegacyMetadataPassSnapshot, 0, len(entries))
	for name, entry := range entries {
		if !c.legacyMetadataPassStillValid(ctx, name, entry) {
			continue
		}
		copy := entry.snapshot
		copy.Counts = cloneLegacyMetadataCounts(copy.Counts)
		result = append(result, copy)
	}
	slices.SortFunc(result, func(a, b LegacyMetadataPassSnapshot) int {
		if a.GroupID < b.GroupID {
			return -1
		}
		if a.GroupID > b.GroupID {
			return 1
		}
		if a.Plan < b.Plan {
			return -1
		}
		if a.Plan > b.Plan {
			return 1
		}
		return 0
	})
	return result
}

func (c *DeltaCompactor) legacyMetadataPassStillValid(ctx context.Context, name string, entry legacyMetadataPublishedPass) bool {
	if ctx.Err() != nil {
		return false
	}
	version, term, err := entry.scanner.GroupOwnedScanIdentity(ctx, entry.snapshot.GroupID)
	if ctx.Err() != nil {
		return false
	}
	valid := err == nil && version == entry.snapshot.CatalogVersion && term == entry.snapshot.LeaderTerm
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	current, stillPublished := c.legacyMetadataPasses[name]
	if !stillPublished || !current.snapshot.CompletionEpoch.Equal(entry.snapshot.CompletionEpoch) {
		return false
	}
	if !valid {
		delete(c.legacyMetadataPasses, name)
	}
	return valid
}

var ErrLegacyMetadataPassIncomplete = errors.New("complete current legacy metadata pass set unavailable")

const (
	ttlInlineHashPlan   = "ttl-inline-hash"
	ttlInlineSetPlan    = "ttl-inline-set"
	ttlInlineZSetPlan   = "ttl-inline-zset"
	ttlInlineStreamPlan = "ttl-inline-stream"
)

var legacyMetadataPlanNames = [...]string{
	ttlInlineHashPlan, ttlInlineSetPlan, ttlInlineZSetPlan, ttlInlineStreamPlan,
}

func validLegacyMetadataPassHeader(pass LegacyMetadataPassSnapshot, groupID uint64) bool {
	return pass.GroupID == groupID && isLegacyMetadataMigrationHandler(pass.Plan) &&
		pass.CatalogVersion != 0 && pass.LeaderTerm != 0 && !pass.StartedAt.IsZero() &&
		!pass.CompletionEpoch.IsZero() && !pass.CompletionEpoch.Before(pass.StartedAt)
}

// CompleteLegacyMetadataGroupPasses is diagnostic, not M4 admission. A
// successful result has one completed pass for every base-metadata family,
// all under the same still-current catalog version and local leader term.
func (c *DeltaCompactor) CompleteLegacyMetadataGroupPasses(ctx context.Context, groupID uint64) ([]LegacyMetadataPassSnapshot, error) {
	if c == nil || groupID == 0 {
		return nil, errors.WithStack(ErrLegacyMetadataPassIncomplete)
	}
	all := c.LegacyMetadataPassSnapshots(ctx)
	selected, err := selectCompleteLegacyMetadataPasses(all, groupID)
	if err != nil {
		return nil, err
	}
	if err := c.recheckCompleteLegacyMetadataPasses(ctx, groupID, selected); err != nil {
		return nil, err
	}
	return selected, nil
}

func selectCompleteLegacyMetadataPasses(all []LegacyMetadataPassSnapshot, groupID uint64) ([]LegacyMetadataPassSnapshot, error) {
	byPlan := make(map[string]LegacyMetadataPassSnapshot, len(legacyMetadataPlanNames))
	for _, snapshot := range all {
		if snapshot.GroupID == groupID {
			byPlan[snapshot.Plan] = snapshot
		}
	}
	selected := make([]LegacyMetadataPassSnapshot, 0, len(legacyMetadataPlanNames))
	for _, name := range legacyMetadataPlanNames {
		snapshot, ok := byPlan[name]
		if !ok || !validLegacyMetadataPassHeader(snapshot, groupID) {
			return nil, errors.WithStack(ErrLegacyMetadataPassIncomplete)
		}
		if len(selected) != 0 && (snapshot.CatalogVersion != selected[0].CatalogVersion || snapshot.LeaderTerm != selected[0].LeaderTerm) {
			return nil, errors.WithStack(ErrLegacyMetadataPassIncomplete)
		}
		selected = append(selected, snapshot)
	}
	return selected, nil
}

func (c *DeltaCompactor) recheckCompleteLegacyMetadataPasses(ctx context.Context, groupID uint64, selected []LegacyMetadataPassSnapshot) error {
	c.cursorMu.Lock()
	entries := make([]legacyMetadataPublishedPass, 0, len(selected))
	for _, snapshot := range selected {
		entry, ok := c.legacyMetadataPasses[ttlInlineMigrationGroupCursorName(snapshot.Plan, groupID)]
		if !ok || !entry.snapshot.CompletionEpoch.Equal(snapshot.CompletionEpoch) {
			c.cursorMu.Unlock()
			return errors.WithStack(ErrLegacyMetadataPassIncomplete)
		}
		entries = append(entries, entry)
	}
	c.cursorMu.Unlock()
	version, term, err := entries[0].scanner.GroupOwnedScanIdentity(ctx, groupID)
	if err != nil || version != selected[0].CatalogVersion || term != selected[0].LeaderTerm {
		return errors.Join(ErrLegacyMetadataPassIncomplete, err)
	}
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	for n, snapshot := range selected {
		current, ok := c.legacyMetadataPasses[ttlInlineMigrationGroupCursorName(snapshot.Plan, groupID)]
		if !ok || !current.snapshot.CompletionEpoch.Equal(entries[n].snapshot.CompletionEpoch) {
			return errors.WithStack(ErrLegacyMetadataPassIncomplete)
		}
	}
	return nil
}

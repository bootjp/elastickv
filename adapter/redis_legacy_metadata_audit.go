package adapter

import (
	"bytes"
	"context"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	legacyMetadataAuditPageSize     = 512
	legacyMetadataAuditDefaultPages = 1024
	legacyMetadataAuditMaxPages     = 65536
)

type LegacyMetadataAuditStatus uint8

const (
	LegacyMetadataAuditComplete LegacyMetadataAuditStatus = iota + 1
	LegacyMetadataAuditIncomplete
	LegacyMetadataAuditRestarted
)

// LegacyMetadataAuditResult is diagnostic only. Non-complete results contain
// no passes, even when pages were classified before a fence or budget failed.
type LegacyMetadataAuditResult struct {
	Status         LegacyMetadataAuditStatus
	GroupID        uint64
	ReadTS         uint64
	CatalogVersion uint64
	LeaderTerm     uint64
	PagesScanned   uint32
	Passes         []LegacyMetadataPassSnapshot
}

type LegacyMetadataAuditSource interface {
	AuditLegacyMetadata(context.Context, uint64, uint32) (LegacyMetadataAuditResult, error)
}

type legacyMetadataAuditScanner interface {
	GroupOwnedScanIdentity(context.Context, uint64) (uint64, uint64, error)
}

// LegacyMetadataAuditor reads only the local group's physical metadata ranges.
// Latest creates one admitted snapshot and keeps its read pin until Close.
type LegacyMetadataAuditor struct {
	store   *kv.ShardStore
	coord   kv.Coordinator
	tracker *kv.ActiveTimestampTracker
}

func NewLegacyMetadataAuditor(st *kv.ShardStore, coord kv.Coordinator, tracker *kv.ActiveTimestampTracker) *LegacyMetadataAuditor {
	return &LegacyMetadataAuditor{store: st, coord: coord, tracker: tracker}
}

func (a *LegacyMetadataAuditor) AuditLegacyMetadata(ctx context.Context, groupID uint64, maxPages uint32) (LegacyMetadataAuditResult, error) {
	if a == nil || a.store == nil || a.coord == nil || a.tracker == nil {
		return LegacyMetadataAuditResult{}, errors.New("legacy metadata audit is unavailable")
	}
	request := kv.NewReadRequest(ctx, a.coord, a.store, a.tracker)
	defer request.Close()
	snapshot, err := request.Latest(request.Context(), "legacy metadata audit")
	if err != nil {
		return LegacyMetadataAuditResult{}, errors.WithStack(err)
	}
	return runLegacyMetadataAudit(request.Context(), a.store, groupID, snapshot.Timestamp(), maxPages,
		func(ctx context.Context, start, end []byte) (kv.GroupOwnedScanPage, error) {
			return request.ScanGroupOwnedPage(ctx, snapshot, groupID, start, end, legacyMetadataAuditPageSize)
		})
}

type legacyMetadataAuditRange struct {
	name   string
	prefix []byte
}

var legacyMetadataAuditRanges = [...]legacyMetadataAuditRange{
	{ttlInlineHashPlan, []byte(store.HashMetaPrefix)},
	{ttlInlineSetPlan, []byte(store.SetMetaPrefix)},
	{ttlInlineZSetPlan, []byte(store.ZSetMetaPrefix)},
	{ttlInlineStreamPlan, []byte(store.StreamMetaPrefix)},
}

func runLegacyMetadataAudit(ctx context.Context, scanner legacyMetadataAuditScanner, groupID, readTS uint64, maxPages uint32,
	readPage func(context.Context, []byte, []byte) (kv.GroupOwnedScanPage, error)) (LegacyMetadataAuditResult, error) {
	if maxPages == 0 {
		maxPages = legacyMetadataAuditDefaultPages
	}
	version, term, err := scanner.GroupOwnedScanIdentity(ctx, groupID)
	if err != nil {
		return LegacyMetadataAuditResult{}, errors.WithStack(err)
	}
	result := LegacyMetadataAuditResult{GroupID: groupID, ReadTS: readTS, CatalogVersion: version, LeaderTerm: term}
	started := time.Now().UTC()
	for _, plan := range legacyMetadataAuditRanges {
		pass, status, scanErr := scanLegacyAuditPlan(ctx, scanner, &result, plan, started, maxPages, readPage)
		if scanErr != nil {
			return LegacyMetadataAuditResult{}, scanErr
		}
		if status != 0 {
			return auditDiscardedResult(result, status), nil
		}
		result.Passes = append(result.Passes, pass)
	}
	if auditIdentityChanged(ctx, scanner, groupID, version, term) {
		return auditDiscardedResult(result, LegacyMetadataAuditRestarted), nil
	}
	result.Status = LegacyMetadataAuditComplete
	return result, nil
}

func scanLegacyAuditPlan(ctx context.Context, scanner legacyMetadataAuditScanner, result *LegacyMetadataAuditResult,
	plan legacyMetadataAuditRange, started time.Time, maxPages uint32,
	readPage func(context.Context, []byte, []byte) (kv.GroupOwnedScanPage, error)) (LegacyMetadataPassSnapshot, LegacyMetadataAuditStatus, error) {
	pass := LegacyMetadataPassSnapshot{GroupID: result.GroupID, Plan: plan.name, CatalogVersion: result.CatalogVersion,
		LeaderTerm: result.LeaderTerm, StartedAt: started, Counts: make(map[uint64]LegacyMetadataReasonCounts)}
	end := store.PrefixScanEnd(plan.prefix)
	start := plan.prefix
	for {
		if result.PagesScanned >= maxPages {
			return pass, LegacyMetadataAuditIncomplete, nil
		}
		page, status, err := auditReadPage(ctx, scanner, *result, start, end, readPage)
		if err != nil || status != 0 {
			return pass, status, err
		}
		result.PagesScanned++
		if err := addLegacyAuditRows(pass.Counts, page.LegacyRows); err != nil {
			return pass, 0, err
		}
		if len(page.Resume) == 0 {
			break
		}
		if !bytes.HasPrefix(page.Resume, plan.prefix) || bytes.Compare(page.Resume, start) < 0 || bytes.Compare(page.Resume, end) >= 0 {
			return pass, 0, errors.New("invalid legacy metadata scan cursor")
		}
		start = append(bytes.Clone(page.Resume), cursorNextByte)
	}
	pass.CompletionEpoch = time.Now().UTC()
	return pass, 0, nil
}

func auditReadPage(ctx context.Context, scanner legacyMetadataAuditScanner, result LegacyMetadataAuditResult, start, end []byte,
	readPage func(context.Context, []byte, []byte) (kv.GroupOwnedScanPage, error)) (kv.GroupOwnedScanPage, LegacyMetadataAuditStatus, error) {
	page, err := readPage(ctx, start, end)
	if ctx.Err() != nil {
		return kv.GroupOwnedScanPage{}, 0, errors.WithStack(ctx.Err())
	}
	if err != nil {
		if auditIdentityChanged(ctx, scanner, result.GroupID, result.CatalogVersion, result.LeaderTerm) {
			return kv.GroupOwnedScanPage{}, LegacyMetadataAuditRestarted, nil
		}
		return kv.GroupOwnedScanPage{}, 0, errors.WithStack(err)
	}
	if page.CatalogVersion != result.CatalogVersion || page.LeaderTerm != result.LeaderTerm ||
		auditIdentityChanged(ctx, scanner, result.GroupID, result.CatalogVersion, result.LeaderTerm) {
		return kv.GroupOwnedScanPage{}, LegacyMetadataAuditRestarted, nil
	}
	return page, 0, nil
}

func addLegacyAuditRows(countsByOwner map[uint64]LegacyMetadataReasonCounts, rows []kv.LegacyMetadataRow) error {
	for _, row := range rows {
		counts := countsByOwner[row.OwnerGroupID]
		switch row.Reason {
		case kv.LegacyMetadataServed:
			counts.Served++
		case kv.LegacyMetadataOwnerHasVersion:
			counts.OwnerHasVersion++
		case kv.LegacyMetadataOwnerLedElsewhere:
			counts.OwnerLedElsewhere++
		case kv.LegacyMetadataStaged:
			counts.StagedVisibility++
		default:
			return errors.New("unknown legacy metadata classification")
		}
		countsByOwner[row.OwnerGroupID] = counts
	}
	return nil
}

func auditIdentityChanged(ctx context.Context, scanner legacyMetadataAuditScanner, groupID, version, term uint64) bool {
	currentVersion, currentTerm, err := scanner.GroupOwnedScanIdentity(ctx, groupID)
	return err != nil || currentVersion != version || currentTerm != term
}

func auditDiscardedResult(result LegacyMetadataAuditResult, status LegacyMetadataAuditStatus) LegacyMetadataAuditResult {
	result.Status = status
	result.Passes = nil
	return result
}

package autosplit

import (
	"sort"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
)

// SnapshotSource is the narrow keyviz snapshot surface the observe-only
// autosplit reader needs.
type SnapshotSource interface {
	Snapshot(from, to time.Time) []keyviz.MatrixColumn
}

// SnapshotReadConfig controls one off-path autosplit sampler read.
type SnapshotReadConfig struct {
	Step             time.Duration
	CandidateWindows int
	LastProcessedAt  time.Time
	Now              time.Time
}

// SnapshotReadResult is the committed keyviz material consumed by the detector.
type SnapshotReadResult struct {
	Windows           []ColumnWindow
	NewestCommittedAt time.Time
	SnapshotFrom      time.Time
	SnapshotTo        time.Time
	SkippedInvalid    int
}

// ObserveSnapshot reads committed keyviz windows and runs the pure detector.
// Callers may log Result.Decisions in observe-only mode; this helper never
// mutates the route catalog and never calls SplitRange.
func ObserveSnapshot(
	cfg Config,
	state *DetectorState,
	routes []distribution.RouteDescriptor,
	source SnapshotSource,
	readCfg SnapshotReadConfig,
) (Result, SnapshotReadResult) {
	read := ReadCommittedWindows(source, readCfg)
	result := Evaluate(cfg, state, Input{
		Routes:  routes,
		Windows: read.Windows,
		Now:     read.SnapshotTo,
	})
	return result, read
}

// ReadCommittedWindows converts a time-range keyviz snapshot into detector
// windows, excluding columns that have already been processed and columns whose
// committed lower boundary is not proven.
func ReadCommittedWindows(source SnapshotSource, cfg SnapshotReadConfig) SnapshotReadResult {
	if source == nil {
		return SnapshotReadResult{}
	}
	step := cfg.Step
	if step <= 0 {
		step = keyviz.DefaultStep
	}
	candidateWindows := cfg.CandidateWindows
	if candidateWindows <= 0 {
		candidateWindows = defaultCandidateWindows
	}
	now := cfg.Now
	if now.IsZero() {
		now = time.Now()
	}

	from := now.Add(-time.Duration(candidateWindows+1) * step)
	if !cfg.LastProcessedAt.IsZero() {
		from = cfg.LastProcessedAt.Add(-step)
	}
	cols := source.Snapshot(from, now)
	windows, newest, skipped := CommittedWindowsFromColumns(cols, cfg.LastProcessedAt)
	return SnapshotReadResult{
		Windows:           windows,
		NewestCommittedAt: newest,
		SnapshotFrom:      from,
		SnapshotTo:        now,
		SkippedInvalid:    skipped,
	}
}

// CommittedWindowsFromColumns normalizes raw keyviz columns into detector
// windows. WindowStart is authoritative when present. For older in-memory
// columns without WindowStart, the immediately previous column boundary is the
// only accepted fallback.
func CommittedWindowsFromColumns(cols []keyviz.MatrixColumn, lastProcessedAt time.Time) ([]ColumnWindow, time.Time, int) {
	ordered := append([]keyviz.MatrixColumn(nil), cols...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].At.Before(ordered[j].At)
	})

	var newest time.Time
	windows := make([]ColumnWindow, 0, len(ordered))
	skipped := 0
	for i, col := range ordered {
		if col.At.After(newest) {
			newest = col.At
		}
		if !col.At.After(lastProcessedAt) {
			continue
		}
		start := col.WindowStart
		if start.IsZero() && i > 0 && ordered[i-1].At.Before(col.At) {
			start = ordered[i-1].At
		}
		if start.IsZero() || !start.Before(col.At) {
			skipped++
			continue
		}
		windows = append(windows, ColumnWindow{
			Column:   col,
			Duration: col.At.Sub(start),
		})
	}
	return windows, newest, skipped
}

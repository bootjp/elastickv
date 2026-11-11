package autosplit

import (
	"sort"
	"time"

	"github.com/bootjp/elastickv/keyviz"
)

// CommittedWindowsFromColumns normalizes raw keyviz columns into detector
// windows. WindowStart is authoritative when present. For older in-memory
// columns without WindowStart, the immediately previous column boundary is the
// only accepted fallback. Columns whose lower boundary is not proven are
// returned as zero-duration reset sentinels so the detector clears stale
// confidence instead of carrying it across an unknown interval.
func CommittedWindowsFromColumns(cols []keyviz.MatrixColumn, lastProcessedAt time.Time) ([]ColumnWindow, time.Time, int) {
	ordered := append([]keyviz.MatrixColumn(nil), cols...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].At.Before(ordered[j].At)
	})

	var newest time.Time
	windows := make([]ColumnWindow, 0, len(ordered))
	skipped := 0
	lastBoundary := lastProcessedAt
	for i, col := range ordered {
		if col.At.After(newest) {
			newest = col.At
		}
		if !col.At.After(lastProcessedAt) {
			continue
		}
		start := committedWindowStart(ordered, i)
		if start.IsZero() || !start.Before(col.At) {
			skipped++
			windows = append(windows, ColumnWindow{Column: keyviz.MatrixColumn{At: col.At}})
			lastBoundary = col.At
			continue
		}
		if needsBoundaryReset(lastBoundary, start) {
			windows = append(windows, ColumnWindow{Column: keyviz.MatrixColumn{At: col.At}})
			lastBoundary = col.At
			continue
		}
		windows = append(windows, ColumnWindow{
			Column:   col,
			Duration: col.At.Sub(start),
		})
		lastBoundary = col.At
	}
	return windows, newest, skipped
}

func committedWindowStart(cols []keyviz.MatrixColumn, i int) time.Time {
	start := cols[i].WindowStart
	if start.IsZero() && i > 0 && cols[i-1].At.Before(cols[i].At) {
		start = cols[i-1].At
	}
	return start
}

func needsBoundaryReset(lastBoundary, start time.Time) bool {
	return !lastBoundary.IsZero() && !start.Equal(lastBoundary)
}

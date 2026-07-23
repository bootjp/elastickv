package autosplit

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

type fakeSnapshotSource struct {
	cols []keyviz.MatrixColumn
	from time.Time
	to   time.Time
}

func (f *fakeSnapshotSource) Snapshot(from, to time.Time) []keyviz.MatrixColumn {
	f.from = from
	f.to = to
	out := make([]keyviz.MatrixColumn, 0, len(f.cols))
	for _, col := range f.cols {
		if !from.IsZero() && col.At.Before(from) {
			continue
		}
		if !to.IsZero() && !col.At.Before(to) {
			continue
		}
		out = append(out, col)
	}
	return out
}

func TestCommittedWindowsPreferExplicitWindowStart(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_100, 0)
	cols := []keyviz.MatrixColumn{
		readerColumn(at.Add(time.Minute), at, nil),
		readerColumn(at.Add(3*time.Minute), at.Add(time.Minute), nil),
	}

	windows, newest, skipped := CommittedWindowsFromColumns(cols, time.Time{})

	require.Equal(t, at.Add(3*time.Minute), newest)
	require.Zero(t, skipped)
	require.Len(t, windows, 2)
	require.Equal(t, time.Minute, windows[0].Duration)
	require.Equal(t, 2*time.Minute, windows[1].Duration)
}

func TestCommittedWindowsFallbackRequiresPreviousBoundary(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_200, 0)
	cols := []keyviz.MatrixColumn{
		{At: at, Rows: []keyviz.MatrixRow{readerRow(20)}},
		{At: at.Add(time.Minute), Rows: []keyviz.MatrixRow{readerRow(20)}},
	}

	windows, newest, skipped := CommittedWindowsFromColumns(cols, time.Time{})

	require.Equal(t, at.Add(time.Minute), newest)
	require.Equal(t, 1, skipped)
	require.Len(t, windows, 2)
	require.Equal(t, at, windows[0].Column.At)
	require.Zero(t, windows[0].Duration)
	require.Equal(t, at.Add(time.Minute), windows[1].Column.At)
	require.Equal(t, time.Minute, windows[1].Duration)
}

func TestReadCommittedWindowsFetchesFromLastProcessedMinusStep(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_300, 0)
	source := &fakeSnapshotSource{cols: []keyviz.MatrixColumn{
		readerColumn(at, at.Add(-time.Minute), nil),
		readerColumn(at.Add(time.Minute), at, nil),
		readerColumn(at.Add(2*time.Minute), at.Add(time.Minute), nil),
	}}

	result := ReadCommittedWindows(source, SnapshotReadConfig{
		Step:             time.Minute,
		CandidateWindows: 3,
		LastProcessedAt:  at.Add(time.Minute),
		Now:              at.Add(3 * time.Minute),
	})

	require.Equal(t, at, source.from)
	require.Equal(t, at.Add(3*time.Minute), source.to)
	require.Equal(t, at.Add(2*time.Minute), result.NewestCommittedAt)
	require.Len(t, result.Windows, 1)
	require.Equal(t, at.Add(2*time.Minute), result.Windows[0].Column.At)
}

func TestObserveSnapshotProcessesSilentGapAsReset(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_400, 0)
	source := &fakeSnapshotSource{cols: []keyviz.MatrixColumn{
		readerColumn(at, at.Add(-time.Minute), []keyviz.MatrixRow{
			readerRow(60),
		}),
		readerColumn(at.Add(time.Minute), at, nil),
		readerColumn(at.Add(2*time.Minute), at.Add(time.Minute), []keyviz.MatrixRow{
			readerRow(60),
		}),
	}}
	state := NewDetectorState()
	cfg := testConfig()
	cfg.CandidateWindows = 2

	result, read := ObserveSnapshot(cfg, state, []distribution.RouteDescriptor{
		testRoute(1, 1, "a", "z"),
	}, source, SnapshotReadConfig{
		Step:             time.Minute,
		CandidateWindows: 2,
		Now:              at.Add(3 * time.Minute),
	})

	require.Len(t, read.Windows, 3)
	require.Empty(t, result.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)
	require.Equal(t, at.Add(2*time.Minute), state.RouteStatus(1).LastProcessedAt)
}

func TestObserveSnapshotResetsWhenHistoryNoLongerReachesWatermark(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_500, 0)
	route := testRoute(1, 1, "a", "z")
	state := NewDetectorState()
	cfg := testConfig()
	cfg.CandidateWindows = 3
	warmup := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			hotWindow(at.Add(-time.Minute)),
			hotWindow(at),
		},
		Now: at,
	})
	require.Empty(t, warmup.Decisions)
	require.Equal(t, 2, state.RouteStatus(1).ConsecutiveOver)

	source := &fakeSnapshotSource{cols: []keyviz.MatrixColumn{
		readerColumn(at.Add(10*time.Minute), at.Add(9*time.Minute), []keyviz.MatrixRow{
			readerRow(60),
		}),
		readerColumn(at.Add(11*time.Minute), at.Add(10*time.Minute), []keyviz.MatrixRow{
			readerRow(60),
		}),
	}}

	result, read := ObserveSnapshot(cfg, state, []distribution.RouteDescriptor{route}, source, SnapshotReadConfig{
		Step:             time.Minute,
		CandidateWindows: 3,
		LastProcessedAt:  at,
		Now:              at.Add(12 * time.Minute),
	})

	require.Len(t, read.Windows, 2)
	require.Zero(t, read.Windows[0].Duration)
	require.Equal(t, at.Add(10*time.Minute), read.Windows[0].Column.At)
	require.Empty(t, result.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)
	require.Equal(t, at.Add(11*time.Minute), state.RouteStatus(1).LastProcessedAt)
	requireEvent(t, result.Events, 0, SkipReasonInvalidWindow)
}

func TestObserveSnapshotResetsWhenWindowOverlapsWatermark(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_550, 0)
	route := testRoute(1, 1, "a", "z")
	state := NewDetectorState()
	cfg := testConfig()
	cfg.CandidateWindows = 3
	warmup := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			hotWindow(at.Add(-time.Minute)),
			hotWindow(at),
		},
		Now: at,
	})
	require.Empty(t, warmup.Decisions)
	require.Equal(t, 2, state.RouteStatus(1).ConsecutiveOver)

	source := &fakeSnapshotSource{cols: []keyviz.MatrixColumn{
		readerColumn(at.Add(2*time.Minute), at.Add(-time.Minute), []keyviz.MatrixRow{
			readerRow(60),
		}),
		readerColumn(at.Add(3*time.Minute), at.Add(2*time.Minute), []keyviz.MatrixRow{
			readerRow(60),
		}),
	}}

	result, read := ObserveSnapshot(cfg, state, []distribution.RouteDescriptor{route}, source, SnapshotReadConfig{
		Step:             time.Minute,
		CandidateWindows: 3,
		LastProcessedAt:  at,
		Now:              at.Add(4 * time.Minute),
	})

	require.Len(t, read.Windows, 2)
	require.Zero(t, read.Windows[0].Duration)
	require.Equal(t, at.Add(2*time.Minute), read.Windows[0].Column.At)
	require.Empty(t, result.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)
	require.Equal(t, at.Add(3*time.Minute), state.RouteStatus(1).LastProcessedAt)
	requireEvent(t, result.Events, 0, SkipReasonInvalidWindow)
}

func TestObserveSnapshotResetsOnInvalidCommittedColumn(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_600, 0)
	route := testRoute(1, 1, "a", "z")
	state := NewDetectorState()
	cfg := testConfig()
	cfg.CandidateWindows = 3
	warmup := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			hotWindow(at.Add(-time.Minute)),
			hotWindow(at),
		},
		Now: at,
	})
	require.Empty(t, warmup.Decisions)
	require.Equal(t, 2, state.RouteStatus(1).ConsecutiveOver)

	source := &fakeSnapshotSource{cols: []keyviz.MatrixColumn{
		{At: at.Add(time.Minute), Rows: []keyviz.MatrixRow{readerRow(60)}},
		readerColumn(at.Add(2*time.Minute), at.Add(time.Minute), []keyviz.MatrixRow{
			readerRow(60),
		}),
	}}

	result, read := ObserveSnapshot(cfg, state, []distribution.RouteDescriptor{route}, source, SnapshotReadConfig{
		Step:             time.Minute,
		CandidateWindows: 3,
		LastProcessedAt:  at,
		Now:              at.Add(3 * time.Minute),
	})

	require.Equal(t, 1, read.SkippedInvalid)
	require.Len(t, read.Windows, 2)
	require.Zero(t, read.Windows[0].Duration)
	require.Empty(t, result.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)
	require.Equal(t, at.Add(2*time.Minute), state.RouteStatus(1).LastProcessedAt)
	requireEvent(t, result.Events, 0, SkipReasonInvalidWindow)
}

func readerColumn(at, start time.Time, rows []keyviz.MatrixRow) keyviz.MatrixColumn {
	return keyviz.MatrixColumn{
		WindowStart: start,
		At:          at,
		Rows:        rows,
	}
}

func readerRow(writes uint64) keyviz.MatrixRow {
	return keyviz.MatrixRow{
		RouteID:        1,
		RaftGroupID:    1,
		Start:          []byte("a"),
		End:            []byte("m"),
		SubBucket:      0,
		SubBucketCount: 2,
		Writes:         writes,
	}
}

package autosplit

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

func TestAggregateSubBucketRowsBeforeScoring(t *testing.T) {
	t.Parallel()
	at := time.Unix(100, 0)
	route := testRoute(7, 2, "a", "z")
	window := testWindow(at, time.Minute,
		testRow(7, 2, 0, 4, "a", "b", 20, 0),
		testRow(7, 2, 1, 4, "b", "c", 20, 0),
		testRow(7, 2, 2, 4, "c", "d", 20, 0),
		keyviz.MatrixRow{RouteID: 99, RaftGroupID: 2, Aggregate: true, Writes: 1000},
	)

	loads := AggregateColumnRows(window.Column)
	require.Equal(t, RouteLoad{Writes: 60}, loads[RouteKey{RouteID: 7, RaftGroupID: 2}])
	require.NotContains(t, loads, RouteKey{RouteID: 99, RaftGroupID: 2})

	state := NewDetectorState()
	result := Evaluate(Config{
		WriteWeight:       1,
		ReadWeight:        0,
		ThresholdOpsMin:   50,
		CandidateWindows:  1,
		MaxSplitsPerCycle: 1,
	}, state, Input{Routes: []distribution.RouteDescriptor{route}, Windows: []ColumnWindow{window}, Now: at})

	require.Len(t, result.Decisions, 1)
	require.Equal(t, uint64(7), result.Decisions[0].RouteID)
	require.Equal(t, []byte("c"), result.Decisions[0].SplitKey)
	require.InDelta(t, 60, result.Decisions[0].ScoreOpsMin, 0.0001)
	requireEvent(t, result.Events, 99, SkipReasonAggregateRow)
}

func TestReadWeightContributesToScore(t *testing.T) {
	t.Parallel()
	at := time.Unix(150, 0)
	route := testRoute(2, 3, "a", "z")
	window := testWindow(at, time.Minute,
		testRow(2, 3, 0, 2, "a", "m", 0, 20),
		testRow(2, 3, 1, 2, "m", "z", 0, 20),
	)

	result := Evaluate(Config{
		WriteWeight:       0,
		ReadWeight:        1,
		ThresholdOpsMin:   30,
		CandidateWindows:  1,
		MaxSplitsPerCycle: 1,
	}, NewDetectorState(), Input{Routes: []distribution.RouteDescriptor{route}, Windows: []ColumnWindow{window}, Now: at})

	require.Len(t, result.Decisions, 1)
	require.InDelta(t, 40, result.Decisions[0].ScoreOpsMin, 0.0001)
}

func TestDecisionReportsSmoothedScoreWithoutChangingPerColumnPromotion(t *testing.T) {
	t.Parallel()
	start := time.Unix(160, 0)
	route := testRoute(2, 3, "a", "z")
	windows := []ColumnWindow{
		testWindow(start.Add(time.Minute), time.Minute,
			testRow(2, 3, 0, 2, "a", "m", 50, 0),
			testRow(2, 3, 1, 2, "m", "z", 50, 0),
		),
		testWindow(start.Add(3*time.Minute), 2*time.Minute,
			testRow(2, 3, 0, 2, "a", "m", 200, 0),
			testRow(2, 3, 1, 2, "m", "z", 200, 0),
		),
	}

	result := Evaluate(Config{
		WriteWeight:       1,
		ReadWeight:        0,
		ThresholdOpsMin:   50,
		CandidateWindows:  2,
		MaxSplitsPerCycle: 1,
	}, NewDetectorState(), Input{Routes: []distribution.RouteDescriptor{route}, Windows: windows, Now: start.Add(3 * time.Minute)})

	require.Len(t, result.Decisions, 1)
	require.InDelta(t, 200, result.Decisions[0].PerColumnScoreOpsMin, 0.0001)
	require.InDelta(t, 500.0/3.0, result.Decisions[0].ScoreOpsMin, 0.0001)
}

func TestInvalidWindowIsSkipped(t *testing.T) {
	t.Parallel()
	at := time.Unix(175, 0)
	route := testRoute(1, 1, "a", "z")
	window := testWindow(at, 0,
		testRow(1, 1, 0, 2, "a", "m", 100, 0),
		testRow(1, 1, 1, 2, "m", "z", 100, 0),
	)

	result := Evaluate(testConfig(), NewDetectorState(), Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{window},
		Now:     at,
	})

	require.Empty(t, result.Decisions)
	requireEvent(t, result.Events, 0, SkipReasonInvalidWindow)
}

func TestInvalidWindowResetsActiveRouteConfidence(t *testing.T) {
	t.Parallel()
	at := time.Unix(180, 0)
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 3

	result := Evaluate(cfg, NewDetectorState(), Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			hotWindow(at),
			hotWindow(at.Add(time.Minute)),
			testWindow(at.Add(2*time.Minute), 0,
				testRow(1, 1, 0, 2, "a", "m", 100, 0),
				testRow(1, 1, 1, 2, "m", "z", 100, 0),
			),
			hotWindow(at.Add(3 * time.Minute)),
		},
		Now: at.Add(3 * time.Minute),
	})

	require.Empty(t, result.Decisions)
	requireEvent(t, result.Events, 0, SkipReasonInvalidWindow)
}

func TestWindowsAreEvaluatedChronologicallyWithoutMutatingInput(t *testing.T) {
	t.Parallel()
	at := time.Unix(190, 0)
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 2
	windows := []ColumnWindow{
		hotWindow(at.Add(time.Minute)),
		hotWindow(at.Add(2 * time.Minute)),
		coldWindow(at),
	}

	result := Evaluate(cfg, NewDetectorState(), Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: windows,
		Now:     at.Add(2 * time.Minute),
	})

	require.Len(t, result.Decisions, 1)
	require.Equal(t, at.Add(time.Minute), windows[0].Column.At)
	require.Equal(t, at.Add(2*time.Minute), windows[1].Column.At)
	require.Equal(t, at, windows[2].Column.At)
}

func TestNonActiveRouteResetsConfidence(t *testing.T) {
	t.Parallel()
	at := time.Unix(200, 0)
	state := NewDetectorState()
	cfg := testConfig()
	cfg.CandidateWindows = 2

	active := testRoute(1, 1, "a", "z")
	first := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{active},
		Windows: []ColumnWindow{hotWindow(at)},
		Now:     at,
	})
	require.Empty(t, first.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)

	migrating := active
	migrating.State = distribution.RouteStateMigratingSource
	reset := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{migrating},
		Windows: []ColumnWindow{hotWindow(at.Add(time.Minute))},
		Now:     at.Add(time.Minute),
	})
	require.Empty(t, reset.Decisions)
	require.Equal(t, 0, state.RouteStatus(1).ConsecutiveOver)
	requireEvent(t, reset.Events, 1, SkipReasonNonActiveState)

	again := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{active},
		Windows: []ColumnWindow{hotWindow(at.Add(2 * time.Minute))},
		Now:     at.Add(2 * time.Minute),
	})
	require.Empty(t, again.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)
}

func TestNonActiveRouteAdvancesSkippedBufferedColumns(t *testing.T) {
	t.Parallel()
	at := time.Unix(225, 0)
	state := NewDetectorState()
	active := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 2

	first := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{active},
		Windows: []ColumnWindow{hotWindow(at)},
		Now:     at,
	})
	require.Empty(t, first.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)

	migrating := active
	migrating.State = distribution.RouteStateMigratingSource
	skipped := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{migrating},
		Windows: []ColumnWindow{hotWindow(at.Add(time.Minute))},
		Now:     at.Add(time.Minute),
	})
	require.Empty(t, skipped.Decisions)
	require.Equal(t, 0, state.RouteStatus(1).ConsecutiveOver)
	require.Equal(t, at.Add(time.Minute), state.RouteStatus(1).LastProcessedAt)
	requireEvent(t, skipped.Events, 1, SkipReasonNonActiveState)

	replayed := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{active},
		Windows: []ColumnWindow{
			hotWindow(at.Add(time.Minute)),
			hotWindow(at.Add(2 * time.Minute)),
		},
		Now: at.Add(2 * time.Minute),
	})
	require.Empty(t, replayed.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)
}

func TestApplyRouteStateAdvancesProcessedWatermark(t *testing.T) {
	t.Parallel()
	at := time.Unix(250, 0)
	state := NewDetectorState()
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 2

	first := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{hotWindow(at)},
		Now:     at,
	})
	require.Empty(t, first.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)

	state.ApplyRouteState(1, distribution.RouteStateMigratingSource, at.Add(time.Minute))
	require.Equal(t, 0, state.RouteStatus(1).ConsecutiveOver)
	require.Equal(t, at.Add(time.Minute), state.RouteStatus(1).LastProcessedAt)

	result := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			hotWindow(at.Add(time.Minute)),
			hotWindow(at.Add(2 * time.Minute)),
		},
		Now: at.Add(2 * time.Minute),
	})

	require.Empty(t, result.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)
}

func TestConsecutiveWindowsPromoteAndBelowThresholdResets(t *testing.T) {
	t.Parallel()
	at := time.Unix(300, 0)
	state := NewDetectorState()
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 3

	result := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			hotWindow(at),
			hotWindow(at.Add(time.Minute)),
			coldWindow(at.Add(2 * time.Minute)),
			hotWindow(at.Add(3 * time.Minute)),
			hotWindow(at.Add(4 * time.Minute)),
		},
		Now: at.Add(4 * time.Minute),
	})
	require.Empty(t, result.Decisions)
	require.Equal(t, 2, state.RouteStatus(1).ConsecutiveOver)

	promoted := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{hotWindow(at.Add(5 * time.Minute))},
		Now:     at.Add(5 * time.Minute),
	})
	require.Len(t, promoted.Decisions, 1)
	require.Equal(t, 3, state.RouteStatus(1).ConsecutiveOver)
}

func TestDuplicateColumnsAreSkippedAcrossEvaluations(t *testing.T) {
	t.Parallel()
	at := time.Unix(325, 0)
	state := NewDetectorState()
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 2

	first := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{hotWindow(at)},
		Now:     at,
	})
	require.Empty(t, first.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)

	duplicate := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{hotWindow(at)},
		Now:     at.Add(30 * time.Second),
	})
	require.Empty(t, duplicate.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)

	next := Evaluate(cfg, state, Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{hotWindow(at.Add(time.Minute))},
		Now:     at.Add(time.Minute),
	})
	require.Len(t, next.Decisions, 1)
	require.Equal(t, 2, state.RouteStatus(1).ConsecutiveOver)
}

func TestStaleInvalidWindowDoesNotResetConfidence(t *testing.T) {
	t.Parallel()
	at := time.Unix(330, 0)
	state := NewDetectorState()
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 3

	warmup := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			hotWindow(at),
			hotWindow(at.Add(time.Minute)),
		},
		Now: at.Add(time.Minute),
	})
	require.Empty(t, warmup.Decisions)
	require.Equal(t, 2, state.RouteStatus(1).ConsecutiveOver)

	result := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			testWindow(at, 0,
				testRow(1, 1, 0, 2, "a", "m", 100, 0),
				testRow(1, 1, 1, 2, "m", "z", 100, 0),
			),
			hotWindow(at.Add(2 * time.Minute)),
		},
		Now: at.Add(2 * time.Minute),
	})

	require.Len(t, result.Decisions, 1)
	require.Equal(t, 3, state.RouteStatus(1).ConsecutiveOver)
	requireEvent(t, result.Events, 0, SkipReasonInvalidWindow)
}

func TestBufferedColdColumnAfterHotRunSuppressesDecision(t *testing.T) {
	t.Parallel()
	at := time.Unix(350, 0)
	state := NewDetectorState()
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 3

	result := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			hotWindow(at),
			hotWindow(at.Add(time.Minute)),
			hotWindow(at.Add(2 * time.Minute)),
			coldWindow(at.Add(3 * time.Minute)),
		},
		Now: at.Add(3 * time.Minute),
	})

	require.Empty(t, result.Decisions)
	require.Equal(t, 0, state.RouteStatus(1).ConsecutiveOver)
}

func TestCooldownBlocksPromotion(t *testing.T) {
	t.Parallel()
	at := time.Unix(400, 0)
	state := NewDetectorState()
	state.SetCooldown(1, at.Add(time.Minute))
	route := testRoute(1, 1, "a", "z")

	result := Evaluate(testConfig(), state, Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{hotWindow(at)},
		Now:     at,
	})

	require.Empty(t, result.Decisions)
	require.Equal(t, 0, state.RouteStatus(1).ConsecutiveOver)
	requireEvent(t, result.Events, 1, SkipReasonCooldown)
}

func TestCooldownDropsBufferedWindowsFromCooldownPeriod(t *testing.T) {
	t.Parallel()
	at := time.Unix(450, 0)
	state := NewDetectorState()
	state.SetCooldown(1, at.Add(3*time.Minute))
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 2

	result := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			hotWindow(at.Add(time.Minute)),
			hotWindow(at.Add(2 * time.Minute)),
			hotWindow(at.Add(4 * time.Minute)),
		},
		Now: at.Add(4 * time.Minute),
	})

	require.Empty(t, result.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)
	requireEvent(t, result.Events, 1, SkipReasonCooldown)
}

func TestCooldownDropsWindowThatStartedBeforeDeadline(t *testing.T) {
	t.Parallel()
	at := time.Unix(460, 0)
	state := NewDetectorState()
	state.SetCooldown(1, at.Add(3*time.Minute))
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 1

	result := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{
			testWindow(at.Add(4*time.Minute), 2*time.Minute,
				testRow(1, 1, 0, 2, "a", "m", 60, 0),
				testRow(1, 1, 1, 2, "m", "z", 60, 0),
			),
		},
		Now: at.Add(4 * time.Minute),
	})

	require.Empty(t, result.Decisions)
	require.Equal(t, 0, state.RouteStatus(1).ConsecutiveOver)
	requireEvent(t, result.Events, 1, SkipReasonCooldown)
}

func TestStateGCRemovesRetiredRoutes(t *testing.T) {
	t.Parallel()
	at := time.Unix(475, 0)
	state := NewDetectorState()
	state.SetCooldown(2, at.Add(time.Hour))

	Evaluate(testConfig(), state, Input{
		Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
		Windows: nil,
		Now:     at,
	})

	require.True(t, state.RouteStatus(2).CooldownUntil.IsZero())
}

func TestRouteCapUsesCycleLocalReservation(t *testing.T) {
	t.Parallel()
	at := time.Unix(500, 0)
	routes := []distribution.RouteDescriptor{
		testRoute(1, 1, "a", "m"),
		testRoute(2, 1, "m", "z"),
	}
	windows := []ColumnWindow{testWindow(at, time.Minute,
		testRow(1, 1, 0, 2, "a", "g", 100, 0),
		testRow(1, 1, 1, 2, "g", "m", 100, 0),
		testRow(2, 1, 0, 2, "m", "t", 100, 0),
		testRow(2, 1, 1, 2, "t", "z", 100, 0),
	)}
	cfg := testConfig()
	cfg.MaxRoutes = 3
	cfg.MaxSplitsPerCycle = 2

	result := Evaluate(cfg, NewDetectorState(), Input{Routes: routes, Windows: windows, Now: at})

	require.Len(t, result.Decisions, 1)
	require.Equal(t, uint64(1), result.Decisions[0].RouteID)
	requireEvent(t, result.Events, 2, SkipReasonRouteCap)
}

func TestRouteCapUsesFullLiveRouteCountWithLocalSubset(t *testing.T) {
	t.Parallel()
	at := time.Unix(525, 0)
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.MaxRoutes = 2

	result := Evaluate(cfg, NewDetectorState(), Input{
		Routes:         []distribution.RouteDescriptor{route},
		Windows:        []ColumnWindow{hotWindow(at)},
		Now:            at,
		LiveRouteCount: 2,
	})

	require.Empty(t, result.Decisions)
	requireEvent(t, result.Events, 1, SkipReasonRouteCap)
}

func TestDefaultConfigSetsRouteCap(t *testing.T) {
	t.Parallel()

	require.Equal(t, defaultMaxRoutes, DefaultConfig().MaxRoutes)
}

func TestZeroMaxRoutesUsesDefaultRouteCap(t *testing.T) {
	t.Parallel()
	at := time.Unix(550, 0)
	routes := make([]distribution.RouteDescriptor, 0, defaultMaxRoutes)
	for routeID := uint64(1); routeID <= defaultMaxRoutes; routeID++ {
		routes = append(routes, testRoute(routeID, 1, "a", "z"))
	}
	cfg := testConfig()
	cfg.MaxRoutes = 0

	result := Evaluate(cfg, NewDetectorState(), Input{
		Routes:  routes,
		Windows: []ColumnWindow{hotWindow(at)},
		Now:     at,
	})

	require.Empty(t, result.Decisions)
	requireEvent(t, result.Events, 1, SkipReasonRouteCap)
}

func TestMaxSplitsPerCycleBudget(t *testing.T) {
	t.Parallel()
	at := time.Unix(600, 0)
	routes := []distribution.RouteDescriptor{
		testRoute(1, 1, "a", "m"),
		testRoute(2, 1, "m", "z"),
	}
	windows := []ColumnWindow{testWindow(at, time.Minute,
		testRow(1, 1, 0, 2, "a", "g", 100, 0),
		testRow(1, 1, 1, 2, "g", "m", 100, 0),
		testRow(2, 1, 0, 2, "m", "t", 100, 0),
		testRow(2, 1, 1, 2, "t", "z", 100, 0),
	)}
	cfg := testConfig()
	cfg.MaxSplitsPerCycle = 1

	result := Evaluate(cfg, NewDetectorState(), Input{Routes: routes, Windows: windows, Now: at})

	require.Len(t, result.Decisions, 1)
	require.Equal(t, uint64(1), result.Decisions[0].RouteID)
	requireEvent(t, result.Events, 2, SkipReasonBudgetExhausted)
}

func TestNoSplitKeyForSingleBucketEvidence(t *testing.T) {
	t.Parallel()
	at := time.Unix(700, 0)
	route := testRoute(1, 1, "a", "z")
	window := testWindow(at, time.Minute,
		testRow(1, 1, 0, 1, "a", "z", 100, 0),
	)

	result := Evaluate(testConfig(), NewDetectorState(), Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{window},
		Now:     at,
	})

	require.Empty(t, result.Decisions)
	requireEvent(t, result.Events, 1, SkipReasonNoSplitKey)
}

func TestP50BoundarySelection(t *testing.T) {
	t.Parallel()
	at := time.Unix(800, 0)
	tests := []struct {
		name       string
		route      distribution.RouteDescriptor
		rows       []keyviz.MatrixRow
		wantKey    []byte
		wantOrigin SplitOrigin
	}{
		{
			name:  "interior median uses hi",
			route: testRoute(1, 1, "a", "e"),
			rows: []keyviz.MatrixRow{
				testRow(1, 1, 0, 4, "a", "b", 10, 0),
				testRow(1, 1, 1, 4, "b", "c", 90, 0),
			},
			wantKey:    []byte("c"),
			wantOrigin: SplitOriginP50Mid,
		},
		{
			name:  "first bucket uses interior hi",
			route: testRoute(1, 1, "a", "e"),
			rows: []keyviz.MatrixRow{
				testRow(1, 1, 0, 4, "a", "b", 90, 0),
				testRow(1, 1, 1, 4, "b", "c", 10, 0),
			},
			wantKey:    []byte("b"),
			wantOrigin: SplitOriginP50FirstBucketHi,
		},
		{
			name:  "last finite bucket falls back to lo",
			route: testRoute(1, 1, "a", "e"),
			rows: []keyviz.MatrixRow{
				testRow(1, 1, 0, 4, "a", "b", 10, 0),
				testRow(1, 1, 3, 4, "d", "e", 90, 0),
			},
			wantKey:    []byte("d"),
			wantOrigin: SplitOriginP50LastBucketLo,
		},
		{
			name:  "rightmost interior bucket keeps finite hi",
			route: testRouteNilEnd(1, 1, "a"),
			rows: []keyviz.MatrixRow{
				testRow(1, 1, 0, 4, "a", "b", 10, 0),
				testRow(1, 1, 1, 4, "b", "c", 70, 0),
				testRowNilEnd(1, 1, 3, 4, "d", 20, 0),
			},
			wantKey:    []byte("c"),
			wantOrigin: SplitOriginP50Mid,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			window := testWindow(at, time.Minute, tc.rows...)
			result := Evaluate(testConfig(), NewDetectorState(), Input{
				Routes:  []distribution.RouteDescriptor{tc.route},
				Windows: []ColumnWindow{window},
				Now:     at,
			})
			require.Len(t, result.Decisions, 1)
			require.Equal(t, tc.wantKey, result.Decisions[0].SplitKey)
			require.Equal(t, tc.wantOrigin, result.Decisions[0].SplitOrigin)
		})
	}
}

func TestP50CoalescesDuplicateSubBucketRows(t *testing.T) {
	t.Parallel()
	at := time.Unix(850, 0)
	route := testRoute(1, 1, "a", "z")
	window := testWindow(at, time.Minute,
		testRow(1, 1, 0, 2, "a", "m", 1000, 0),
		testRow(1, 1, 0, 2, "a", "m", 900, 0),
		testRow(1, 1, 1, 2, "m", "z", 100, 0),
	)

	result := Evaluate(testConfig(), NewDetectorState(), Input{
		Routes:  []distribution.RouteDescriptor{route},
		Windows: []ColumnWindow{window},
		Now:     at,
	})

	require.Len(t, result.Decisions, 1)
	require.Equal(t, []byte("m"), result.Decisions[0].SplitKey)
	require.InDelta(t, 1900, result.Decisions[0].LeftLoad, 0.0001)
	require.InDelta(t, 100, result.Decisions[0].RightLoad, 0.0001)
}

func TestTopKeyIsolationUsesAlignedLowerBoundBeforeP50(t *testing.T) {
	t.Parallel()
	at := time.Unix(900, 0)
	route := testRoute(1, 1, "a", "z")

	for _, sample := range []struct {
		rate   int
		writes uint64
	}{{rate: 1, writes: 100}, {rate: 16, writes: 1600}} {
		t.Run("sample_rate_"+time.Duration(sample.rate).String(), func(t *testing.T) {
			t.Parallel()
			window := topKeyWindow(at, time.Minute, route, sample.writes, keyviz.KeyvizHotKeysSnapshot{
				SampledN:   100,
				SampleRate: sample.rate,
				Capacity:   100,
				Entries: []keyviz.KeyvizHotKeyEntry{
					{Key: []byte("m"), Count: 91},
				},
			})
			cfg := testConfig()
			cfg.TopKeyAbsoluteFloor = 50 * float64(sample.rate)

			result := Evaluate(cfg, NewDetectorState(), Input{
				Routes: []distribution.RouteDescriptor{route}, Windows: []ColumnWindow{window}, Now: at,
			})

			require.Len(t, result.Decisions, 1)
			require.Equal(t, SplitOriginIsolationCompound, result.Decisions[0].SplitOrigin)
			require.Equal(t, []byte("m"), result.Decisions[0].SplitKey)
			require.Equal(t, []byte{'m', 0}, result.Decisions[0].SecondSplitKey)
			require.Equal(t, 2, result.Decisions[0].RouteDelta)
			require.Empty(t, result.Events)
		})
	}
}

func TestTopKeyIsolationDeclinesConservativelyAndFallsBackToP50(t *testing.T) {
	t.Parallel()
	at := time.Unix(1000, 0)
	route := testRoute(1, 1, "a", "z")

	tests := []struct {
		name       string
		snapshot   keyviz.KeyvizHotKeysSnapshot
		floor      float64
		wantReason IsolationDeclineReason
	}{
		{
			name: "absolute floor",
			snapshot: keyviz.KeyvizHotKeysSnapshot{SampledN: 100, SampleRate: 1, Capacity: 100,
				Entries: []keyviz.KeyvizHotKeyEntry{{Key: []byte("m"), Count: 91}}},
			floor:      100,
			wantReason: IsolationDeclineAbsoluteFloor,
		},
		{
			name: "space saving uncertainty",
			snapshot: keyviz.KeyvizHotKeysSnapshot{SampledN: 100, SampleRate: 1, Capacity: 10,
				Entries: []keyviz.KeyvizHotKeyEntry{{Key: []byte("m"), Count: 85}}},
			floor:      50,
			wantReason: IsolationDeclineTopKErrorBound,
		},
		{
			name: "degraded queue",
			snapshot: keyviz.KeyvizHotKeysSnapshot{SampledN: 100, SampleRate: 1, Capacity: 100, DroppedSamples: 1,
				Entries: []keyviz.KeyvizHotKeyEntry{{Key: []byte("m"), Count: 100}}},
			floor:      50,
			wantReason: IsolationDeclineTopKDegraded,
		},
		{
			name:       "empty sketch",
			snapshot:   keyviz.KeyvizHotKeysSnapshot{SampleRate: 1, Capacity: 100},
			floor:      50,
			wantReason: IsolationDeclineTopKInsufficient,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			window := topKeyWindow(at, time.Minute, route, 100, tc.snapshot)
			cfg := testConfig()
			cfg.TopKeyAbsoluteFloor = tc.floor

			result := Evaluate(cfg, NewDetectorState(), Input{
				Routes: []distribution.RouteDescriptor{route}, Windows: []ColumnWindow{window}, Now: at,
			})

			require.Len(t, result.Decisions, 1)
			require.Equal(t, SplitOriginP50FirstBucketHi, result.Decisions[0].SplitOrigin)
			requireIsolationEvent(t, result.Events, 1, tc.wantReason)
		})
	}
}

func TestTopKeySnapshotRequiresExactCommittedWindow(t *testing.T) {
	t.Parallel()
	at := time.Unix(1100, 0)
	route := testRoute(1, 1, "a", "z")
	window := topKeyWindow(at, time.Minute, route, 100, keyviz.KeyvizHotKeysSnapshot{
		SampledN: 100, SampleRate: 1, Capacity: 100,
		Entries: []keyviz.KeyvizHotKeyEntry{{Key: []byte("m"), Count: 91}},
	})
	window.Column.HotKeys[0].WindowStart = window.Column.WindowStart.Add(time.Second)

	result := Evaluate(testConfig(), NewDetectorState(), Input{
		Routes: []distribution.RouteDescriptor{route}, Windows: []ColumnWindow{window}, Now: at,
	})

	require.Len(t, result.Decisions, 1)
	require.Equal(t, SplitOriginP50FirstBucketHi, result.Decisions[0].SplitOrigin)
}

func TestTopKeyIsolationBoundaryForms(t *testing.T) {
	t.Parallel()
	at := time.Unix(1200, 0)
	tests := []struct {
		name       string
		route      distribution.RouteDescriptor
		hotKey     []byte
		wantOrigin SplitOrigin
		wantKey    []byte
		wantSecond []byte
		wantDelta  int
		wantNone   bool
	}{
		{name: "compound", route: testRoute(1, 1, "a", "z"), hotKey: []byte("m"),
			wantOrigin: SplitOriginIsolationCompound, wantKey: []byte("m"), wantSecond: []byte{'m', 0}, wantDelta: 2},
		{name: "lower edge", route: testRoute(1, 1, "m", "z"), hotKey: []byte("m"),
			wantOrigin: SplitOriginIsolationSingleLowerEdge, wantKey: []byte{'m', 0}, wantDelta: 1},
		{name: "lower edge unbounded", route: testRouteNilEnd(1, 1, "m"), hotKey: []byte("m"),
			wantOrigin: SplitOriginIsolationSingleLowerEdge, wantKey: []byte{'m', 0}, wantDelta: 1},
		{name: "upper edge", route: testRoute(1, 1, "a", "m\x00"), hotKey: []byte("m"),
			wantOrigin: SplitOriginIsolationSingleUpperEdge, wantKey: []byte("m"), wantDelta: 1},
		{name: "already isolated", route: testRoute(1, 1, "m", "m\x00"), hotKey: []byte("m"), wantNone: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			window := topKeyOnlyWindow(at, time.Minute, tc.route, 100, tc.hotKey)
			result := Evaluate(testConfig(), NewDetectorState(), Input{
				Routes: []distribution.RouteDescriptor{tc.route}, Windows: []ColumnWindow{window}, Now: at,
			})
			if tc.wantNone {
				require.Empty(t, result.Decisions)
				return
			}
			require.Len(t, result.Decisions, 1)
			require.Equal(t, tc.wantOrigin, result.Decisions[0].SplitOrigin)
			require.Equal(t, tc.wantKey, result.Decisions[0].SplitKey)
			require.Equal(t, tc.wantSecond, result.Decisions[0].SecondSplitKey)
			require.Equal(t, tc.wantDelta, result.Decisions[0].RouteDelta)
		})
	}
}

func TestCompoundIsolationHonorsActualRouteDelta(t *testing.T) {
	t.Parallel()
	at := time.Unix(1300, 0)
	routes := []distribution.RouteDescriptor{
		testRoute(1, 1, "a", "m"),
		testRoute(2, 1, "m", "z"),
	}
	window := topKeyOnlyWindow(at, time.Minute, routes[0], 100, []byte("g"))
	cfg := testConfig()
	cfg.MaxRoutes = 3

	result := Evaluate(cfg, NewDetectorState(), Input{Routes: routes, Windows: []ColumnWindow{window}, Now: at})

	require.Empty(t, result.Decisions)
	requireEvent(t, result.Events, 1, SkipReasonRouteCap)
}

func TestEvidenceFenceRejectsPreLeadershipAndStraddlingWindows(t *testing.T) {
	t.Parallel()
	base := time.Unix(1400, 0)
	route := testRoute(1, 1, "a", "z")
	cfg := testConfig()
	cfg.CandidateWindows = 2
	fence := EvidenceFence{
		ProcessedThrough:     base,
		WindowStartNotBefore: base.Add(30 * time.Second),
	}
	windows := []ColumnWindow{
		hotWindowWithStart(base, base.Add(-time.Minute)),
		hotWindowWithStart(base.Add(time.Minute), base),
		hotWindowWithStart(base.Add(2*time.Minute), base.Add(time.Minute)),
	}
	state := NewDetectorState()

	first := Evaluate(cfg, state, Input{
		Routes: []distribution.RouteDescriptor{route}, Windows: windows,
		EvidenceFences: map[uint64]EvidenceFence{1: fence}, Now: base.Add(2 * time.Minute),
	})

	require.Empty(t, first.Decisions)
	require.Equal(t, 1, state.RouteStatus(1).ConsecutiveOver)
	requireEvent(t, first.Events, 1, SkipReasonLeadershipFence)

	final := Evaluate(cfg, state, Input{
		Routes:         []distribution.RouteDescriptor{route},
		Windows:        []ColumnWindow{hotWindowWithStart(base.Add(3*time.Minute), base.Add(2*time.Minute))},
		EvidenceFences: map[uint64]EvidenceFence{1: fence}, Now: base.Add(3 * time.Minute),
	})

	require.Len(t, final.Decisions, 1)
}

func testConfig() Config {
	return Config{
		WriteWeight:       1,
		ReadWeight:        0,
		ThresholdOpsMin:   50,
		CandidateWindows:  1,
		MaxSplitsPerCycle: 1,
	}
}

func testRoute(routeID, groupID uint64, start, end string) distribution.RouteDescriptor {
	return distribution.RouteDescriptor{
		RouteID: routeID,
		Start:   []byte(start),
		End:     []byte(end),
		GroupID: groupID,
		State:   distribution.RouteStateActive,
	}
}

func testRouteNilEnd(routeID, groupID uint64, start string) distribution.RouteDescriptor {
	return distribution.RouteDescriptor{
		RouteID: routeID,
		Start:   []byte(start),
		GroupID: groupID,
		State:   distribution.RouteStateActive,
	}
}

func testWindow(at time.Time, duration time.Duration, rows ...keyviz.MatrixRow) ColumnWindow {
	return ColumnWindow{
		Column: keyviz.MatrixColumn{
			At:   at,
			Rows: rows,
		},
		Duration: duration,
	}
}

func topKeyWindow(
	at time.Time,
	duration time.Duration,
	route distribution.RouteDescriptor,
	writes uint64,
	snapshot keyviz.KeyvizHotKeysSnapshot,
) ColumnWindow {
	window := testWindow(at, duration,
		testRow(route.RouteID, route.GroupID, 0, 2, string(route.Start), "m", writes/2, 0),
		testRow(route.RouteID, route.GroupID, 1, 2, "m", string(route.End), writes-writes/2, 0),
	)
	window.Column.WindowStart = at.Add(-duration)
	snapshot.RouteID = route.RouteID
	snapshot.WindowStart = window.Column.WindowStart
	snapshot.WindowEnd = at
	window.Column.HotKeys = []keyviz.KeyvizHotKeysSnapshot{snapshot}
	return window
}

func topKeyOnlyWindow(
	at time.Time,
	duration time.Duration,
	route distribution.RouteDescriptor,
	writes uint64,
	hotKey []byte,
) ColumnWindow {
	window := testWindow(at, duration, keyviz.MatrixRow{
		RouteID: route.RouteID, RaftGroupID: route.GroupID, Start: route.Start, End: route.End,
		SubBucketCount: 1, Writes: writes,
	})
	window.Column.WindowStart = at.Add(-duration)
	window.Column.HotKeys = []keyviz.KeyvizHotKeysSnapshot{{
		RouteID: route.RouteID, WindowStart: window.Column.WindowStart, WindowEnd: at,
		SampledN: 100, SampleRate: 1, Capacity: 100,
		Entries: []keyviz.KeyvizHotKeyEntry{{Key: hotKey, Count: 91}},
	}}
	return window
}

func hotWindow(at time.Time) ColumnWindow {
	return testWindow(at, time.Minute,
		testRow(1, 1, 0, 2, "a", "m", 60, 0),
		testRow(1, 1, 1, 2, "m", "z", 60, 0),
	)
}

func hotWindowWithStart(at, start time.Time) ColumnWindow {
	window := hotWindow(at)
	window.Column.WindowStart = start
	window.Duration = at.Sub(start)
	return window
}

func coldWindow(at time.Time) ColumnWindow {
	return testWindow(at, time.Minute,
		testRow(1, 1, 0, 2, "a", "m", 10, 0),
		testRow(1, 1, 1, 2, "m", "z", 10, 0),
	)
}

func testRow(routeID, groupID uint64, subBucket, subBucketCount int, start, end string, writes, reads uint64) keyviz.MatrixRow {
	return keyviz.MatrixRow{
		RouteID:        routeID,
		RaftGroupID:    groupID,
		Start:          []byte(start),
		End:            []byte(end),
		SubBucket:      subBucket,
		SubBucketCount: subBucketCount,
		Writes:         writes,
		Reads:          reads,
	}
}

func testRowNilEnd(routeID, groupID uint64, subBucket, subBucketCount int, start string, writes, reads uint64) keyviz.MatrixRow {
	return keyviz.MatrixRow{
		RouteID:        routeID,
		RaftGroupID:    groupID,
		Start:          []byte(start),
		SubBucket:      subBucket,
		SubBucketCount: subBucketCount,
		Writes:         writes,
		Reads:          reads,
	}
}

func requireEvent(t *testing.T, events []Event, routeID uint64, reason SkipReason) {
	t.Helper()
	for _, event := range events {
		if event.RouteID == routeID && event.Reason == reason {
			return
		}
	}
	t.Fatalf("missing event route_id=%d reason=%s in %#v", routeID, reason, events)
}

func requireIsolationEvent(t *testing.T, events []Event, routeID uint64, reason IsolationDeclineReason) {
	t.Helper()
	for _, event := range events {
		if event.RouteID == routeID && event.IsolationReason == reason {
			return
		}
	}
	t.Fatalf("missing isolation event route_id=%d reason=%s in %#v", routeID, reason, events)
}

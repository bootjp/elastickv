package autosplit

import (
	"bytes"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestRapidConsecutivePromotionAndSplitBounds(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		candidateWindows := rapid.IntRange(1, 5).Draw(rt, "candidate_windows")
		hot := rapid.SliceOfN(rapid.Bool(), 1, 24).Draw(rt, "hot_columns")
		base := time.Unix(2_000, 0)
		windows := make([]ColumnWindow, 0, len(hot))
		finalRun := 0
		for i, over := range hot {
			writes := uint64(10)
			if over {
				writes = 100
				finalRun++
			} else {
				finalRun = 0
			}
			at := base.Add(time.Duration(i+1) * time.Minute)
			windows = append(windows, testWindow(at, time.Minute,
				testRow(1, 1, 0, 2, "a", "m", writes/2, 0),
				testRow(1, 1, 1, 2, "m", "z", writes-writes/2, 0),
			))
		}
		cfg := testConfig()
		cfg.CandidateWindows = candidateWindows
		state := NewDetectorState()
		result := Evaluate(cfg, state, Input{
			Routes:  []distribution.RouteDescriptor{testRoute(1, 1, "a", "z")},
			Windows: windows,
			Now:     windows[len(windows)-1].Column.At,
		})

		require.Equal(rt, finalRun, state.RouteStatus(1).ConsecutiveOver)
		require.Equal(rt, finalRun >= candidateWindows, len(result.Decisions) == 1)
		for _, decision := range result.Decisions {
			require.Greater(rt, bytes.Compare(decision.SplitKey, []byte("a")), 0)
			require.Less(rt, bytes.Compare(decision.SplitKey, []byte("z")), 0)
		}
	})
}

func TestRapidCooldownAndCycleBudget(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		routeCount := rapid.IntRange(1, 12).Draw(rt, "route_count")
		budget := rapid.IntRange(1, 5).Draw(rt, "budget")
		cooling := rapid.Bool().Draw(rt, "cooling")
		at := time.Unix(3_000, 0)
		routes := make([]distribution.RouteDescriptor, 0, routeCount)
		rows := make([]keyviz.MatrixRow, 0, routeCount*2)
		state := NewDetectorState()
		routeID := uint64(0)
		for range routeCount {
			routeID++
			routes = append(routes, testRoute(routeID, 1, "a", "z"))
			rows = append(rows,
				testRow(routeID, 1, 0, 2, "a", "m", 60, 0),
				testRow(routeID, 1, 1, 2, "m", "z", 60, 0),
			)
			if cooling {
				state.SetCooldown(routeID, at.Add(time.Minute))
			}
		}
		cfg := testConfig()
		cfg.MaxRoutes = routeCount + budget + 10
		cfg.MaxSplitsPerCycle = budget
		result := Evaluate(cfg, state, Input{
			Routes:  routes,
			Windows: []ColumnWindow{testWindow(at, time.Minute, rows...)},
			Now:     at,
		})

		require.LessOrEqual(rt, len(result.Decisions), budget)
		if cooling {
			require.Empty(rt, result.Decisions)
		}
	})
}

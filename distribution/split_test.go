package distribution

import "testing"

// Test range statistics collection and split triggering.
func TestRangeStatsAndSplit(t *testing.T) {
    var notified bool
    eng := NewEngine(3, func(left, right Range) {
        notified = true
    })

    eng.RecordRequest("a")
    eng.RecordRequest("a")

    stats := eng.GetStats()
    if len(stats) != 1 || stats[0].Count != 2 {
        t.Fatalf("expected single range count 2, got %+v", stats)
    }

    // Third request should trigger a split
    eng.RecordRequest("a")
    if !notified {
        t.Fatalf("expected split notification")
    }
    if len(eng.Ranges()) != 2 {
        t.Fatalf("expected 2 ranges after split, got %d", len(eng.Ranges()))
    }

    // Stats should now have two ranges with reset counts
    stats = eng.GetStats()
    if len(stats) != 2 {
        t.Fatalf("expected stats for 2 ranges, got %d", len(stats))
    }

    // find range containing "a"
    var count int
    for _, s := range stats {
        if (s.Start == "" || "a" >= s.Start) && (s.End == "" || "a" < s.End) {
            count = s.Count
        }
    }
    if count != 0 {
        t.Fatalf("expected count reset after split, got %d", count)
    }
}

// Ensure metadata updates after manual SplitRange call.
func TestManualSplitRange(t *testing.T) {
    eng := NewEngine(100, nil)
    eng.SplitRange("a")
    if len(eng.Ranges()) != 2 {
        t.Fatalf("expected 2 ranges after manual split, got %d", len(eng.Ranges()))
    }
}


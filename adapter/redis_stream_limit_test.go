package adapter

import (
	"math"
	"strings"
	"testing"
)

// TestScanStreamEntriesLimit exercises the boundary cases of the limit
// helper used by scanStreamEntriesAt. The cases mirror the Copilot review
// concern about int overflow on 32-bit targets and corrupted meta.Length
// values producing negative scan limits that ScanAt would then interpret
// as "no limit".
func TestScanStreamEntriesLimit(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		expected int64
		want     int
	}{
		{"zero → unlimited (matches ScanAt convention)", 0, 0},
		{"negative (corrupted meta) → unlimited, not negative limit", -1, 0},
		{"small stream adds 64 slack", 100, 164},
		{"large legit stream above old 100k cap passes through", 200_000, 200_064},
		{"MaxInt64 triggers overflow guard → unlimited", math.MaxInt64, 0},
		{"near-MaxInt + slack wraps → overflow guard returns 0", math.MaxInt - 1, 0},
		{"MaxInt - slack passes through at MaxInt", math.MaxInt - 64, math.MaxInt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scanStreamEntriesLimit(tc.expected)
			if got != tc.want {
				t.Fatalf("scanStreamEntriesLimit(%d): want %d, got %d", tc.expected, tc.want, got)
			}
		})
	}
}

// TestEstimateXAddTrimCount guards two clamps on the trim-count capacity
// hint: (a) int-overflow on a corrupted meta.Length, (b) the
// maxWideColumnItems ceiling that prevents make() from being asked for a
// 16 EiB allocation on 64-bit targets when the diff exceeds what fits
// comfortably in one Raft txn. Gemini-flagged HIGH after the
// math.MaxInt clamp landed in the previous round.
func TestEstimateXAddTrimCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		maxLen        int
		currentLength int64
		want          int
	}{
		{"maxLen unset (-1) → 0", -1, 100, 0},
		{"under cap → 0", 10, 5, 0},
		{"one below cap (add fills it) → 0", 10, 9, 0},
		{"at cap (add exceeds by 1) → 1", 10, 10, 1},
		{"over cap → excess count", 10, 20, 11},
		{"MAXLEN 0 on empty stream → 1 (the just-added entry)", 0, 0, 1},
		{"MAXLEN 0 on populated stream → whole length + 1", 0, 99, 100},
		{"diff equal to maxWideColumnItems passes through", 0, int64(maxWideColumnItems) - 1, maxWideColumnItems},
		{"diff above maxWideColumnItems clamps to maxWideColumnItems", 0, int64(maxWideColumnItems) + 5, maxWideColumnItems},
		// currentLength = MaxInt64: currentLength+1 overflows to a negative
		// int64 (MinInt64), which the "nextLen <= maxLen" early return
		// then catches. Returning 0 on this corrupted input is strictly
		// safer than feeding make() a wrapped negative; the scan path
		// still runs at the store-imposed page limit.
		{"MaxInt64 length → safe 0 (arithmetic overflow early-returns)", 10, math.MaxInt64, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := estimateXAddTrimCount(tc.maxLen, tc.currentLength)
			if got != tc.want {
				t.Fatalf("estimateXAddTrimCount(%d, %d): want %d, got %d",
					tc.maxLen, tc.currentLength, tc.want, got)
			}
		})
	}
}

// TestBumpStreamID exercises the ID-space overflow guard directly — it
// avoids the time-dependent nowMs branch in nextXAddID so the seq/ms
// carry logic is testable deterministically. Nextxn tests for the '*'
// path live in the integration suite.
func TestBumpStreamID(t *testing.T) {
	t.Parallel()

	// Normal seq bump.
	ms, seq, err := bumpStreamID(100, 5)
	if err != nil || ms != 100 || seq != 6 {
		t.Fatalf("normal bump: want (100, 6, nil), got (%d, %d, %v)", ms, seq, err)
	}

	// seq at MaxUint64 carries to ms+1, seq=0.
	ms, seq, err = bumpStreamID(100, ^uint64(0))
	if err != nil || ms != 101 || seq != 0 {
		t.Fatalf("seq-at-max carry: want (101, 0, nil), got (%d, %d, %v)", ms, seq, err)
	}

	// Both ms and seq at MaxUint64: ID space exhausted, error.
	_, _, err = bumpStreamID(^uint64(0), ^uint64(0))
	if err == nil {
		t.Fatal("both at max: expected ID-space-exhausted error, got nil")
	}
	if !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("both at max: error should mention 'exhausted', got %q", err.Error())
	}
}

// TestNextXAddID_Monotonic: with a lastMs deliberately far in the future
// (so nowMs < lastMs), nextXAddID MUST advance past the given ID rather
// than reset to nowMs-0. Guards the monotonicity contract against a
// backwards clock step or a corrupted meta with a very large LastMs.
func TestNextXAddID_Monotonic(t *testing.T) {
	t.Parallel()

	const farFuture = uint64(1_000_000_000_000_000) // ~year 33658
	id, err := nextXAddID(true, farFuture, 5, "*")
	if err != nil {
		t.Fatalf("future lastMs: unexpected error %v", err)
	}
	// Must be 1000000000000000-6 (carry seq).
	if id != "1000000000000000-6" {
		t.Fatalf("future lastMs: want 1000000000000000-6, got %s", id)
	}

	// With seq at MaxUint64 in the future-ms case, should carry to ms+1.
	id, err = nextXAddID(true, farFuture, ^uint64(0), "*")
	if err != nil {
		t.Fatalf("future lastMs seq-at-max: unexpected error %v", err)
	}
	if id != "1000000000000001-0" {
		t.Fatalf("future lastMs seq-at-max: want 1000000000000001-0, got %s", id)
	}

	// Both maxed → exhausted.
	_, err = nextXAddID(true, ^uint64(0), ^uint64(0), "*")
	if err == nil || !strings.Contains(err.Error(), "exhausted") {
		t.Fatalf("both maxed: want exhausted error, got %v", err)
	}
}

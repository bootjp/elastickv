package adapter

import (
	"math"
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

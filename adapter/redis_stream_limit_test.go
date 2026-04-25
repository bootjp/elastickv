package adapter

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"

	cockerrors "github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// TestParseXReadCountArg_Clamp guards Gemini's medium concern: a client
// XREAD COUNT well above maxWideColumnItems must be silently clamped so
// the server cannot pre-allocate a maxInt-sized []redisStreamEntry slice
// or pull more entries than the equivalent uncapped scan.
func TestParseXReadCountArg_Clamp(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want int
		ok   bool
	}{
		{"valid small", "10", 10, true},
		{"at cap", strconv.Itoa(maxWideColumnItems), maxWideColumnItems, true},
		{"one above cap clamps", strconv.Itoa(maxWideColumnItems + 1), maxWideColumnItems, true},
		{"MaxInt32 clamps", strconv.Itoa(math.MaxInt32), maxWideColumnItems, true},
		// 0 / negative / non-numeric are still rejected.
		{"zero rejected", "0", 0, false},
		{"negative rejected", "-1", 0, false},
		{"garbage rejected", "abc", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseXReadCountArg([][]byte{[]byte("COUNT"), []byte(tc.raw)}, 0)
			if tc.ok && err != nil {
				t.Fatalf("parseXReadCountArg(%q): unexpected error %v", tc.raw, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("parseXReadCountArg(%q): expected error, got count=%d", tc.raw, got)
			}
			if tc.ok && got != tc.want {
				t.Fatalf("parseXReadCountArg(%q): want %d, got %d", tc.raw, tc.want, got)
			}
		})
	}
}

// TestParseRangeStreamCount_Clamp guards the matching XRANGE / XREVRANGE
// COUNT clamp. The negative -1 sentinel (no COUNT) must pass through
// unchanged so rangeStreamNewLayout's unbounded path still trips its
// maxWideColumnItems guard.
func TestParseRangeStreamCount_Clamp(t *testing.T) {
	t.Parallel()

	noCount := [][]byte{[]byte("XRANGE"), []byte("k"), []byte("-"), []byte("+")}
	got, err := parseRangeStreamCount(noCount)
	if err != nil {
		t.Fatalf("no COUNT: unexpected error %v", err)
	}
	if got != -1 {
		t.Fatalf("no COUNT: want -1 sentinel, got %d", got)
	}

	cases := []struct {
		name string
		raw  string
		want int
		ok   bool
	}{
		{"valid small", "10", 10, true},
		{"zero passes (not negative)", "0", 0, true},
		{"at cap", strconv.Itoa(maxWideColumnItems), maxWideColumnItems, true},
		{"one above cap clamps", strconv.Itoa(maxWideColumnItems + 1), maxWideColumnItems, true},
		{"MaxInt32 clamps", strconv.Itoa(math.MaxInt32), maxWideColumnItems, true},
		{"negative rejected", "-1", 0, false},
		{"garbage rejected", "xx", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := [][]byte{[]byte("XRANGE"), []byte("k"), []byte("-"), []byte("+"), []byte("COUNT"), []byte(tc.raw)}
			got, err := parseRangeStreamCount(args)
			if tc.ok && err != nil {
				t.Fatalf("parseRangeStreamCount(%q): unexpected error %v", tc.raw, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("parseRangeStreamCount(%q): expected error, got count=%d", tc.raw, got)
			}
			if tc.ok && got != tc.want {
				t.Fatalf("parseRangeStreamCount(%q): want %d, got %d", tc.raw, tc.want, got)
			}
		})
	}
}

// TestIsXReadIterCtxError covers the four error shapes the XREAD
// busy-poll loop must silently translate to "empty iteration" rather than
// surface to the client as -ERR: bare context sentinels, cockroachdb/errors
// wraps, and gRPC status codes (DeadlineExceeded / Canceled). Anything
// else must be reported faithfully so genuine errors (auth, wrong type,
// transport failures) still reach the client.
func TestIsXReadIterCtxError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"bare DeadlineExceeded", context.DeadlineExceeded, true},
		{"bare Canceled", context.Canceled, true},
		{"cockroach-wrapped DeadlineExceeded", cockerrors.WithStack(context.DeadlineExceeded), true},
		{"cockroach-wrapped Canceled", cockerrors.WithStack(context.Canceled), true},
		{"grpc DeadlineExceeded status", status.Error(codes.DeadlineExceeded, "iter ctx fired"), true},
		{"grpc Canceled status", status.Error(codes.Canceled, "iter ctx cancelled"), true},
		// Genuine non-ctx errors must NOT be swallowed by the helper.
		{"grpc Unavailable", status.Error(codes.Unavailable, "leader gone"), false},
		{"grpc Internal", status.Error(codes.Internal, "boom"), false},
		{"plain non-ctx error", cockerrors.New("wrong type"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isXReadIterCtxError(tc.err); got != tc.want {
				t.Fatalf("isXReadIterCtxError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIsKnownInternalKey_StreamPrefixNarrowed guards Codex P2: the stream
// internal namespace must list its two concrete prefixes (!stream|meta|
// and !stream|entry|) rather than the !stream| umbrella, so a user key
// such as "!stream|foo" continues to flow through redisStrKey() in
// txnContext.load instead of being misclassified as an internal key.
func TestIsKnownInternalKey_StreamPrefixNarrowed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		key  []byte
		want bool
	}{
		// True internal stream keys: classified as internal.
		{"stream meta key is internal", []byte("!stream|meta|\x00\x00\x00\x03foo"), true},
		{"stream entry key is internal", []byte("!stream|entry|\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"), true},
		// User keys that happen to start with "!stream|" must NOT be
		// classified as internal — they need redisStrKey() namespacing.
		{"user key !stream|foo is NOT internal", []byte("!stream|foo"), false},
		{"user key !stream|metadata is NOT internal", []byte("!stream|metadata"), false},
		{"user key !stream|entryless is NOT internal", []byte("!stream|entryless"), false},
		// Sanity: other internal namespaces still classify correctly.
		{"!redis| is internal", []byte("!redis|str|foo"), true},
		{"!hs| is internal", []byte("!hs|fld|foo"), true},
		// And bare user keys are not internal.
		{"plain user key", []byte("hello"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isKnownInternalKey(tc.key); got != tc.want {
				t.Fatalf("isKnownInternalKey(%q) = %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}

// TestSafeUnixMilliToUint64 guards Gemini's medium concern: a system
// clock set before the Unix epoch makes time.Now().UnixMilli() return a
// negative int64; a naive uint64 cast wraps to a value near
// math.MaxUint64 that wedges every subsequent XADD '*' (the future-ms
// branch in nextXAddID would chase that pathological value forever). The
// helper must clamp at 0 so the lastMs/lastSeq monotonic carry takes
// over.
func TestSafeUnixMilliToUint64(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   int64
		want uint64
	}{
		{"zero", 0, 0},
		{"positive epoch ms", 1_777_000_000_000, 1_777_000_000_000},
		{"max int64", math.MaxInt64, uint64(math.MaxInt64)},
		// Negative values represent a clock set before the Unix epoch
		// (1970-01-01). All must clamp at 0.
		{"minus one", -1, 0},
		{"large negative", -1_000_000_000_000, 0},
		{"min int64", math.MinInt64, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := safeUnixMilliToUint64(tc.in); got != tc.want {
				t.Fatalf("safeUnixMilliToUint64(%d): want %d, got %d", tc.in, tc.want, got)
			}
		})
	}
}

package backup

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
)

// TestEncodeSnapshotLibraryRoundTrip pins the public library entrypoint:
// EncodeSnapshot writes a .fsm to the supplied io.Writer; running
// DecodeSnapshot on those bytes into a scratch dir produces an
// equivalent adapter tree. No CLI involved. Codex P2 v2 #896 — encoder
// entrypoint exposure.
func TestEncodeSnapshotLibraryRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	// One tiny SQS queue fixture is enough to exercise the SQS slice
	// end-to-end via the new library wrapper; the per-adapter tree
	// shape is already covered by the M5-1/M5-2 tests.
	const queue = "lib-rt"
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"lib-rt","fifo":false,"partition_count":1,"generation":1}`),
		[][]byte{
			[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000000,"sequence_number":0}`),
		},
	)

	var buf bytes.Buffer
	result, err := EncodeSnapshot(EncodeOptions{
		InputRoot:    in,
		Adapters:     AdapterSet{SQS: true},
		LastCommitTS: 0xDEADBEEF,
	}, &buf)
	if err != nil {
		t.Fatalf("EncodeSnapshot: %v", err)
	}
	if result.SelfTestRan {
		t.Errorf("SelfTestRan = true, want false (SelfTest opt was false)")
	}
	if result.BytesWritten == 0 {
		t.Errorf("BytesWritten = 0")
	}
	if len(result.AdaptersEnabled) != 1 || result.AdaptersEnabled[0] != "sqs" {
		t.Errorf("AdaptersEnabled = %v, want [sqs]", result.AdaptersEnabled)
	}

	// Decode the produced bytes into a scratch tree.
	scratch := t.TempDir()
	decResult, err := DecodeSnapshot(bytes.NewReader(buf.Bytes()), DecodeOptions{
		OutRoot:  scratch,
		Adapters: AdapterSet{SQS: true},
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot of EncodeSnapshot output failed: %v", err)
	}
	if decResult.Header.LastCommitTS != 0xDEADBEEF {
		t.Errorf("decoded header.LastCommitTS = %x, want 0xDEADBEEF", decResult.Header.LastCommitTS)
	}
}

// TestEncodeSnapshotSelfTestMatchesInput pins the happy-path self-test
// against a tree that has already been canonicalized by one decode pass
// (so the input matches what DecodeSnapshot would write back, modulo
// the encoder's idempotency). The full encode -> decode -> encode chain
// is the gold-standard round trip the parent design mandates.
func TestEncodeSnapshotSelfTestMatchesInput(t *testing.T) {
	t.Parallel()
	rawIn := t.TempDir()
	const queue = "selftest-match"
	writeSQSQueue(t, rawIn, queue,
		[]byte(`{"format_version":1,"name":"selftest-match","fifo":false,"partition_count":1,"generation":1}`),
		[][]byte{
			[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000000,"sequence_number":0}`),
		},
	)

	// Canonicalize: encode rawIn, decode it back to canonicalIn. The
	// resulting tree is what the encoder's self-test will produce in
	// the scratch dir, so a second encode against it must match.
	canonicalIn := t.TempDir()
	var canonicalBuf bytes.Buffer
	if _, err := EncodeSnapshot(EncodeOptions{
		InputRoot:    rawIn,
		Adapters:     AdapterSet{SQS: true},
		LastCommitTS: 0xCAFE,
	}, &canonicalBuf); err != nil {
		t.Fatalf("canonical encode: %v", err)
	}
	if _, err := DecodeSnapshot(bytes.NewReader(canonicalBuf.Bytes()), DecodeOptions{
		OutRoot:  canonicalIn,
		Adapters: AdapterSet{SQS: true},
	}); err != nil {
		t.Fatalf("canonical decode: %v", err)
	}

	scratchBase := t.TempDir()
	var buf bytes.Buffer
	result, err := EncodeSnapshot(EncodeOptions{
		InputRoot:    canonicalIn,
		Adapters:     AdapterSet{SQS: true},
		LastCommitTS: 0xCAFE,
		SelfTest:     true,
		SelfTestDecodeOptions: DecodeOptions{
			OutRoot:  scratchBase,
			Adapters: AdapterSet{SQS: true},
		},
	}, &buf)
	if err != nil {
		t.Fatalf("EncodeSnapshot: %v", err)
	}
	if !result.SelfTestRan || !result.SelfTestMatched {
		t.Errorf("SelfTestRan=%v Matched=%v, want both true; mismatch=%s", result.SelfTestRan, result.SelfTestMatched, string(result.SelfTestMismatchTxt))
	}
	if buf.Len() == 0 {
		t.Errorf("bytes were not copied to out after successful self-test")
	}
	if result.Header.LastCommitTS != 0xCAFE {
		t.Errorf("Header.LastCommitTS = %x, want 0xCAFE", result.Header.LastCommitTS)
	}
}

// flipBytesPastHeaderHelper returns a corruption hook that flips bytes
// every 13 bytes starting at offset 200 in the buffered self-test temp
// file — far enough past the EKVPBBL1 header + lastCommitTS that the
// decoder trips on a malformed entry length. Extracted from the test
// body so the test body itself stays under the cyclop threshold.
func flipBytesPastHeaderHelper(t *testing.T) func(*os.File) {
	t.Helper()
	return func(f *os.File) {
		info, err := f.Stat()
		if err != nil {
			t.Fatalf("temp Stat: %v", err)
		}
		const headerSkip = 200
		if info.Size() <= headerSkip {
			t.Fatalf("temp file too small to corrupt past header: %d bytes", info.Size())
		}
		buf := make([]byte, info.Size()-headerSkip)
		if _, err := f.ReadAt(buf, headerSkip); err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		for i := 0; i < len(buf); i += 13 {
			buf[i] ^= 0xFF
		}
		if _, err := f.WriteAt(buf, headerSkip); err != nil {
			t.Fatalf("WriteAt: %v", err)
		}
	}
}

// TestEncodeSnapshotSelfTestDetectsCorruption pins that the unexported
// corruptBufferForTest hook lets the self-test catch corruption in the
// internal buffer. The corruption must be reachable by the self-test
// decode but MUST NOT reach the supplied io.Writer (the write-then-
// rename invariant — codex P2 v6 #896).
func TestEncodeSnapshotSelfTestDetectsCorruption(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "selftest-corrupt"
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"selftest-corrupt","fifo":false,"partition_count":1,"generation":1}`),
		[][]byte{
			[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000000,"sequence_number":0}`),
		},
	)

	scratchBase := t.TempDir()
	var out bytes.Buffer
	corrupt := flipBytesPastHeaderHelper(t)
	result, err := EncodeSnapshot(EncodeOptions{
		InputRoot:    in,
		Adapters:     AdapterSet{SQS: true},
		LastCommitTS: 0xCAFE,
		SelfTest:     true,
		SelfTestDecodeOptions: DecodeOptions{
			OutRoot:  scratchBase,
			Adapters: AdapterSet{SQS: true},
		},
		corruptBufferForTest: corrupt,
	}, &out)
	if err != nil {
		t.Fatalf("EncodeSnapshot: %v", err)
	}
	if !result.SelfTestRan {
		t.Fatalf("SelfTestRan = false")
	}
	if result.SelfTestMatched {
		t.Errorf("SelfTestMatched = true with corruption injected; want false")
	}
	if len(result.SelfTestMismatchTxt) == 0 {
		t.Errorf("SelfTestMismatchTxt is empty; expected a mismatch report")
	}
	// CRITICAL: the corrupt bytes must NEVER reach out. The
	// write-then-rename atomic-publish discipline requires that a
	// self-test failure publishes nothing.
	if out.Len() != 0 {
		t.Errorf("out.Len = %d, want 0 (no bytes should reach out on self-test failure)", out.Len())
	}
}

// TestEncodeSnapshotRequiresInputRoot rejects EncodeOptions with no
// InputRoot — a simple guard so the constructor errors surface early.
func TestEncodeSnapshotRequiresInputRoot(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if _, err := EncodeSnapshot(EncodeOptions{}, &buf); err == nil {
		t.Fatalf("EncodeSnapshot with empty InputRoot succeeded; want error")
	}
}

// TestEncodeSnapshotRejectsLowManifestFloor pins codex P2 v2: the
// library-level HLC floor check fails-closed when opts.LastCommitTS
// is below opts.ManifestLastCommitTS. Defense-in-depth for the CLI's
// resolveLastCommitTS — a future in-process caller (Phase 1 live
// extractor) cannot silently publish a low-TS .fsm.
func TestEncodeSnapshotRejectsLowManifestFloor(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	var buf bytes.Buffer
	_, err := EncodeSnapshot(EncodeOptions{
		InputRoot:            in,
		Adapters:             AdapterSet{SQS: true},
		LastCommitTS:         500,
		ManifestLastCommitTS: 1000, // floor; LastCommitTS is below
	}, &buf)
	if err == nil {
		t.Fatalf("EncodeSnapshot with LastCommitTS < ManifestLastCommitTS succeeded; want error")
	}
	if !errors.Is(err, ErrSelfTestLowerLastCommitTS) {
		t.Errorf("err = %v, want errors.Is ErrSelfTestLowerLastCommitTS", err)
	}
	if buf.Len() != 0 {
		t.Errorf("buf.Len = %d, want 0 (no bytes should be written on floor regression)", buf.Len())
	}
}

// TestEncodeSnapshotManifestFloorOptOut pins that ManifestLastCommitTS=0
// disables the check (synthetic test fixtures, library callers without a
// manifest reference). The existing TestEncodeSnapshotLibraryRoundTrip
// implicitly relies on this opt-out.
func TestEncodeSnapshotManifestFloorOptOut(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "floor-opt-out"
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"floor-opt-out","fifo":false,"partition_count":1,"generation":1}`),
		[][]byte{
			[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000000,"sequence_number":0}`),
		},
	)
	var buf bytes.Buffer
	_, err := EncodeSnapshot(EncodeOptions{
		InputRoot:            in,
		Adapters:             AdapterSet{SQS: true},
		LastCommitTS:         500,
		ManifestLastCommitTS: 0, // opt-out
	}, &buf)
	if err != nil {
		t.Fatalf("EncodeSnapshot with opt-out floor failed: %v", err)
	}
}

// TestEncodeSnapshotRejectsZeroAdapterSet pins claude v5 + codex v5
// carry-over: a library caller that forgets to thread Adapters into
// EncodeOptions gets a fail-closed error rather than a silently empty
// header-only .fsm. The CLI's parseAdapterSet already rejects this for
// flag-driven entry; this test pins the library-level guard.
func TestEncodeSnapshotRejectsZeroAdapterSet(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	var buf bytes.Buffer
	_, err := EncodeSnapshot(EncodeOptions{
		InputRoot:    in,
		Adapters:     AdapterSet{}, // explicit zero
		LastCommitTS: 1,
	}, &buf)
	if err == nil {
		t.Fatalf("EncodeSnapshot with empty AdapterSet succeeded; want error")
	}
	if buf.Len() != 0 {
		t.Errorf("buf.Len = %d, want 0 (no bytes should be written on guard rejection)", buf.Len())
	}
}

// TestEncodeInfoSidecarPath pins the path-derivation rule for the
// sidecar (gemini medium v2 #896): one .fsm path produces one distinct
// sidecar path; two .fsm files in the same dir produce two distinct
// sidecars (no collision).
func TestEncodeInfoSidecarPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	a := filepath.Join(dir, "a.fsm")
	b := filepath.Join(dir, "b.fsm")
	sa := EncodeInfoSidecarPath(a)
	sb := EncodeInfoSidecarPath(b)
	if sa == sb {
		t.Fatalf("sidecar paths collided: %s == %s", sa, sb)
	}
	// Verify each ends with the expected suffix.
	if got, want := filepath.Base(sa), "a.fsm.encode_info.json"; got != want {
		t.Errorf("sidecar(a) basename = %q, want %q", got, want)
	}
	if got, want := filepath.Base(sb), "b.fsm.encode_info.json"; got != want {
		t.Errorf("sidecar(b) basename = %q, want %q", got, want)
	}
	// Both writable next to their .fsm (no OS-level collision).
	for _, p := range []string{sa, sb} {
		if err := os.WriteFile(p, []byte("{}"), 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
}

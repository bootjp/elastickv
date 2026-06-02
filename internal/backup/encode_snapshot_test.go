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

// TestEncodeSnapshotRejectsMissingInputRoot pins codex P2 v8 #904: a
// non-existent or non-directory InputRoot must be rejected before any
// adapter runs. Otherwise each enabled adapter treats its missing
// top-level subdirectory as a no-op, the call "succeeds", and the
// caller gets a header-only .fsm — a silent empty-restore artifact.
// CLI callers don't hit this path (they open MANIFEST.json first),
// but library callers can pass a stale path, so the guard belongs in
// EncodeSnapshot itself.
func TestEncodeSnapshotRejectsMissingInputRoot(t *testing.T) {
	t.Parallel()
	t.Run("non-existent path", func(t *testing.T) {
		t.Parallel()
		missing := filepath.Join(t.TempDir(), "does-not-exist")
		var buf bytes.Buffer
		_, err := EncodeSnapshot(EncodeOptions{
			InputRoot:    missing,
			Adapters:     AdapterSet{SQS: true},
			LastCommitTS: 1,
		}, &buf)
		if err == nil {
			t.Fatalf("EncodeSnapshot with non-existent InputRoot succeeded; want error")
		}
		if buf.Len() != 0 {
			t.Errorf("buf.Len = %d, want 0 (no bytes should be written for missing InputRoot)", buf.Len())
		}
	})
	t.Run("regular file", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		filePath := filepath.Join(dir, "not-a-dir")
		if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		var buf bytes.Buffer
		_, err := EncodeSnapshot(EncodeOptions{
			InputRoot:    filePath,
			Adapters:     AdapterSet{SQS: true},
			LastCommitTS: 1,
		}, &buf)
		if err == nil {
			t.Fatalf("EncodeSnapshot with file-as-InputRoot succeeded; want error")
		}
		if buf.Len() != 0 {
			t.Errorf("buf.Len = %d, want 0 (no bytes should be written for non-directory InputRoot)", buf.Len())
		}
	})
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

// TestEncodeSnapshotRejectsDynamoDBJSONLLayout pins codex P2 v7 #904:
// the DynamoDB reverse encoder does not support the JSONL bundle
// layout, so a caller that threads DynamoDBBundleJSONL=true must be
// rejected with ErrEncodeUnsupportedDynamoDBLayout before any bytes
// are written. The CLI hits this path automatically when MANIFEST.json
// has `dynamodb_layout: "jsonl"`; library callers that mirror that
// thread the field themselves.
func TestEncodeSnapshotRejectsDynamoDBJSONLLayout(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	var buf bytes.Buffer
	_, err := EncodeSnapshot(EncodeOptions{
		InputRoot:           in,
		Adapters:            AdapterSet{DynamoDB: true},
		LastCommitTS:        1,
		DynamoDBBundleJSONL: true,
	}, &buf)
	if err == nil {
		t.Fatalf("EncodeSnapshot with DynamoDBBundleJSONL accepted; want error")
	}
	if !errors.Is(err, ErrEncodeUnsupportedDynamoDBLayout) {
		t.Errorf("err = %v, want errors.Is ErrEncodeUnsupportedDynamoDBLayout", err)
	}
	if buf.Len() != 0 {
		t.Errorf("buf.Len = %d, want 0 (no bytes should be written when JSONL is rejected)", buf.Len())
	}
}

// TestEncodeSnapshotJSONLOnlyRejectedWhenDDBEnabled pins that the JSONL
// guard fires only when DynamoDB is in the adapter set — a caller that
// happens to set DynamoDBBundleJSONL=true while encoding ONLY Redis (or
// any other adapter) is unaffected. Prevents the guard from becoming
// over-zealous for callers who simply mirror the manifest field.
func TestEncodeSnapshotJSONLOnlyRejectedWhenDDBEnabled(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "no-ddb"
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"no-ddb","fifo":false,"partition_count":1,"generation":1}`),
		[][]byte{
			[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000000,"sequence_number":0}`),
		},
	)
	var buf bytes.Buffer
	_, err := EncodeSnapshot(EncodeOptions{
		InputRoot:           in,
		Adapters:            AdapterSet{SQS: true}, // DDB NOT in scope
		LastCommitTS:        1,
		DynamoDBBundleJSONL: true, // would be rejected if DDB were enabled
	}, &buf)
	if err != nil {
		t.Fatalf("EncodeSnapshot rejected JSONL flag when DDB not in scope: %v", err)
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

// TestEnumerateRedisDBsMissingDir pins codex P1 v13 #904: a missing
// redis/ directory returns nil indices (no-op), matching the per-DB
// encoder's "missing db_<n> = nothing to encode" convention.
func TestEnumerateRedisDBsMissingDir(t *testing.T) {
	t.Parallel()
	indices, err := enumerateRedisDBs(t.TempDir())
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if indices != nil {
		t.Errorf("indices = %v, want nil for missing redis/", indices)
	}
}

// TestEnumerateRedisDBsMixedEntries pins codex P1 v13 #904: only
// canonical db_<N> entries are kept; non-numeric, negative, leading-
// zero, empty-suffix, wrong-prefix, and non-directory entries are
// silently skipped. The returned slice is sorted ascending.
func TestEnumerateRedisDBsMixedEntries(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	for _, name := range []string{"db_0", "db_1", "db_5"} {
		if err := os.MkdirAll(filepath.Join(in, "redis", name), 0o755); err != nil {
			t.Fatalf("MkdirAll %s: %v", name, err)
		}
	}
	// Entries that MUST be skipped:
	//   db_garbage  — non-numeric suffix
	//   db_-1       — negative
	//   db_01       — non-canonical leading zero
	//   db_         — empty suffix
	//   notdb_2     — wrong prefix
	for _, name := range []string{"db_garbage", "db_-1", "db_01", "db_", "notdb_2"} {
		if err := os.MkdirAll(filepath.Join(in, "redis", name), 0o755); err != nil {
			t.Fatalf("MkdirAll %s: %v", name, err)
		}
	}
	// A regular file under redis/ must be skipped (not enumerable).
	if err := os.WriteFile(filepath.Join(in, "redis", "README"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile README: %v", err)
	}
	indices, err := enumerateRedisDBs(in)
	if err != nil {
		t.Fatalf("enumerateRedisDBs: %v", err)
	}
	want := []int{0, 1, 5}
	if len(indices) != len(want) {
		t.Fatalf("indices = %v, want %v", indices, want)
	}
	for i, v := range want {
		if indices[i] != v {
			t.Errorf("indices[%d] = %d, want %d", i, indices[i], v)
		}
	}
}

// TestEnumerateRedisDBsRedisIsRegularFile pins fail-closed when the
// "redis" path inside the dump is a regular file rather than a
// directory — distinct from the missing case.
func TestEnumerateRedisDBsRedisIsRegularFile(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	if err := os.WriteFile(filepath.Join(in, "redis"), []byte("not a dir"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := enumerateRedisDBs(in)
	if !errors.Is(err, ErrRedisEncodeNotDir) {
		t.Errorf("err = %v, want errors.Is ErrRedisEncodeNotDir", err)
	}
}

// TestEncodeSnapshotRedisRejectsNonZeroDB pins codex P2 v14 #904
// L452: the Redis MVCC key prefixes (!redis|str|, !redis|hll|,
// !redis|ttl|, …) carry no database component, so feeding a
// non-zero DB through the encoder would mis-scope the produced
// .fsm — same-named keys collide and a db_3-only self-test would
// decode under db_0. Until Phase 1 makes native keys DB-aware,
// non-zero-DB inputs MUST fail closed.
//
// The fixture places a single string under redis/db_3/ ONLY.
// EncodeSnapshot must reject with ErrRedisEncodeMultiDBUnsupported
// and write no bytes. (v14 originally attempted to fan out per DB;
// codex's L452 follow-up established the correct fix is fail-closed.)
func TestEncodeSnapshotRedisRejectsNonZeroDB(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	encKey := EncodeSegment([]byte("k3"))
	db3Strings := filepath.Join(in, "redis", "db_3", "strings")
	if err := os.MkdirAll(db3Strings, 0o755); err != nil {
		t.Fatalf("MkdirAll db_3/strings: %v", err)
	}
	if err := os.WriteFile(filepath.Join(db3Strings, encKey+".bin"), []byte("v3"), 0o600); err != nil {
		t.Fatalf("WriteFile db_3 string: %v", err)
	}
	var buf bytes.Buffer
	_, err := EncodeSnapshot(EncodeOptions{
		InputRoot:    in,
		Adapters:     AdapterSet{Redis: true},
		LastCommitTS: 1,
	}, &buf)
	if err == nil {
		t.Fatalf("EncodeSnapshot accepted db_3-only Redis input; want ErrRedisEncodeMultiDBUnsupported")
	}
	if !errors.Is(err, ErrRedisEncodeMultiDBUnsupported) {
		t.Errorf("err = %v, want errors.Is ErrRedisEncodeMultiDBUnsupported", err)
	}
	// Marked as adapter-data so the CLI routes it to exit-2.
	if !errors.Is(err, ErrEncodeAdapterData) {
		t.Errorf("err = %v, want errors.Is ErrEncodeAdapterData (mark from runAdapterEncoders)", err)
	}
	if buf.Len() != 0 {
		t.Errorf("buf.Len = %d, want 0 (no bytes should be written on multi-DB rejection)", buf.Len())
	}
}

// TestEncodeSnapshotRedisRejectsMultipleDBs pins the multi-DB case:
// redis/db_0 + redis/db_3 → ErrRedisEncodeMultiDBUnsupported (the
// fan-out would collide on same-named keys or merge both DBs under
// db_0 on restore; codex P2 v14 #904 L452).
func TestEncodeSnapshotRedisRejectsMultipleDBs(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	for _, name := range []string{"db_0", "db_3"} {
		if err := os.MkdirAll(filepath.Join(in, "redis", name, "strings"), 0o755); err != nil {
			t.Fatalf("MkdirAll %s: %v", name, err)
		}
	}
	var buf bytes.Buffer
	_, err := EncodeSnapshot(EncodeOptions{
		InputRoot:    in,
		Adapters:     AdapterSet{Redis: true},
		LastCommitTS: 1,
	}, &buf)
	if err == nil {
		t.Fatalf("EncodeSnapshot accepted db_0 + db_3; want ErrRedisEncodeMultiDBUnsupported")
	}
	if !errors.Is(err, ErrRedisEncodeMultiDBUnsupported) {
		t.Errorf("err = %v, want errors.Is ErrRedisEncodeMultiDBUnsupported", err)
	}
	// Marked as adapter-data so the CLI routes it to exit-2 (mirrors
	// TestEncodeSnapshotRedisRejectsNonZeroDB; claude v17 parity).
	if !errors.Is(err, ErrEncodeAdapterData) {
		t.Errorf("err = %v, want errors.Is ErrEncodeAdapterData (mark from runAdapterEncoders)", err)
	}
	if buf.Len() != 0 {
		t.Errorf("buf.Len = %d, want 0 (no bytes should be written on multi-DB rejection)", buf.Len())
	}
}

// TestEnumerateRedisDBsRejectsNonDirDBEntry pins codex P2 v14 #904
// L427: when a canonical db_<N> name resolves to a regular file
// (or symlink) instead of a directory, enumerateRedisDBs must fail
// closed with ErrRedisEncodeNotDir — silently skipping would let a
// malformed dump publish a header-only/partial FSM.
func TestEnumerateRedisDBsRejectsNonDirDBEntry(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	if err := os.MkdirAll(filepath.Join(in, "redis"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// redis/db_2 is a regular file — name matches the canonical
	// pattern but the entry shape is wrong.
	if err := os.WriteFile(filepath.Join(in, "redis", "db_2"), []byte("not a dir"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := enumerateRedisDBs(in)
	if !errors.Is(err, ErrRedisEncodeNotDir) {
		t.Errorf("err = %v, want errors.Is ErrRedisEncodeNotDir", err)
	}
}

// TestEncodeSnapshotMarksAdapterDataErrors pins codex P2 v9 #904: when
// an adapter encoder rejects the input tree's contents (e.g. a
// malformed DynamoDB _schema.json), EncodeSnapshot must surface the
// failure as ErrEncodeAdapterData so the CLI can route it to exit-2
// (data-correctness) rather than exit-1 (operator/flag error).
// Crucially, errors.Mark preserves the original sentinel chain, so a
// caller that errors.Is on the per-adapter sentinel
// (ErrDDBEncodeInvalidSchema here) still gets a match — the marking
// is additive.
func TestEncodeSnapshotMarksAdapterDataErrors(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	// Empty table_name triggers ErrDDBEncodeInvalidSchema inside the
	// DynamoDB encoder (encode_dynamodb.go:120).
	writeDDBSchema(t, in, "tbl",
		[]byte(`{"format_version":1,"table_name":"","primary_key":{"hash_key":{"name":"id","type":"S"}}}`))
	var buf bytes.Buffer
	_, err := EncodeSnapshot(EncodeOptions{
		InputRoot:    in,
		Adapters:     AdapterSet{DynamoDB: true},
		LastCommitTS: 1,
	}, &buf)
	if err == nil {
		t.Fatalf("EncodeSnapshot with malformed schema succeeded; want error")
	}
	if !errors.Is(err, ErrEncodeAdapterData) {
		t.Errorf("err = %v, want errors.Is ErrEncodeAdapterData", err)
	}
	// Inner sentinel must still be reachable so existing per-adapter
	// errors.Is callers are unaffected by the additional mark.
	if !errors.Is(err, ErrDDBEncodeInvalidSchema) {
		t.Errorf("err = %v, want errors.Is ErrDDBEncodeInvalidSchema (mark must preserve inner chain)", err)
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

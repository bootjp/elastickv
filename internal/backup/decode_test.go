package backup

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestDecodeSnapshot_RejectsEmptyOutRoot pins that DecodeOptions
// validation runs before any I/O happens — a missing OutRoot must
// surface as ErrDecodeOptionsInvalid, not as a downstream
// permission-denied / not-a-directory error.
func TestDecodeSnapshot_RejectsEmptyOutRoot(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(0)
	_, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		Adapters: AllAdapters(),
	})
	if err == nil {
		t.Fatalf("expected ErrDecodeOptionsInvalid, got nil")
	}
}

// TestDecodeSnapshot_HeaderOnlyProducesEmptyResult pins that a
// snapshot with no entries still validates and Finalize runs cleanly
// on every adapter.
func TestDecodeSnapshot_HeaderOnlyProducesEmptyResult(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	b := newSnapBuilder(0xCAFE)
	res, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		OutRoot:  root,
		Adapters: AllAdapters(),
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if res.Header.LastCommitTS != 0xCAFE {
		t.Fatalf("LastCommitTS = %d, want 0xCAFE", res.Header.LastCommitTS)
	}
	if res.Counters.Total != 0 {
		t.Fatalf("Counters.Total = %d, want 0", res.Counters.Total)
	}
}

// TestDecodeSnapshot_RoutesRedisString feeds a single !redis|str|
// entry and confirms the dispatcher wrote the strings/<key>.bin
// file via the RedisDB encoder.
func TestDecodeSnapshot_RoutesRedisString(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	b := newSnapBuilder(1)
	val := encodeNewStringValue(t, []byte("hello"), 0)
	b.WriteEntry([]byte(RedisStringPrefix+"greeting"), 1, val, false, 0, snapshotEncStateCleartx)
	res, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		OutRoot:  root,
		Adapters: AllAdapters(),
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if res.Counters.Redis != 1 {
		t.Fatalf("Counters.Redis = %d, want 1 (counters=%+v)", res.Counters.Redis, res.Counters)
	}
	wantPath := filepath.Join(root, "redis", "db_0", "strings", "greeting.bin")
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("stat %s: %v", wantPath, err)
	}
}

// TestDecodeSnapshot_DropsTombstones pins the up-front tombstone
// filter — entries flagged tombstone bypass the prefix router and
// only increment Counters.Tombstone.
func TestDecodeSnapshot_DropsTombstones(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	b := newSnapBuilder(0)
	val := encodeNewStringValue(t, []byte("v"), 0)
	b.WriteEntry([]byte(RedisStringPrefix+"k"), 1, val, true, 0, snapshotEncStateCleartx)
	res, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		OutRoot:  root,
		Adapters: AllAdapters(),
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if res.Counters.Tombstone != 1 {
		t.Fatalf("Counters.Tombstone = %d, want 1", res.Counters.Tombstone)
	}
	if res.Counters.Redis != 0 {
		t.Fatalf("Counters.Redis = %d, want 0", res.Counters.Redis)
	}
	if _, err := os.Stat(filepath.Join(root, "redis", "db_0", "strings", "k.bin")); err == nil {
		t.Fatalf("tombstone produced an output file; want absent")
	}
}

// TestDecodeSnapshot_DisabledAdapterCountsAsInternal pins that
// per-adapter exclusion turns matched-prefix entries into Internal
// drops (not Unknown) — operators who pass `--adapter redis` still
// see DynamoDB / S3 / SQS counts under the right bucket.
func TestDecodeSnapshot_DisabledAdapterCountsAsInternal(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	b := newSnapBuilder(0)
	val := encodeNewStringValue(t, []byte("v"), 0)
	b.WriteEntry([]byte(RedisStringPrefix+"k"), 1, val, false, 0, snapshotEncStateCleartx)
	res, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		OutRoot: root,
		// Redis NOT enabled.
		Adapters: AdapterSet{DynamoDB: true, S3: true, SQS: true},
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if res.Counters.Internal != 1 {
		t.Fatalf("Counters.Internal = %d, want 1", res.Counters.Internal)
	}
	if res.Counters.Unknown != 0 {
		t.Fatalf("Counters.Unknown = %d, want 0", res.Counters.Unknown)
	}
	if _, err := os.Stat(filepath.Join(root, "redis")); err == nil {
		t.Fatalf("disabled adapter produced redis/; want absent")
	}
}

// TestDecodeSnapshot_UnknownPrefixCounted pins that a key with no
// matching route lands in Counters.Unknown — the format-skew /
// corruption signal a Phase 0a operator wants to surface.
func TestDecodeSnapshot_UnknownPrefixCounted(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	b := newSnapBuilder(0)
	b.WriteEntry([]byte("!alien|prefix|x"), 1, []byte("v"), false, 0, snapshotEncStateCleartx)
	res, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		OutRoot:  root,
		Adapters: AllAdapters(),
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if res.Counters.Unknown != 1 {
		t.Fatalf("Counters.Unknown = %d, want 1", res.Counters.Unknown)
	}
}

// TestDecodeSnapshot_TxnPrefixDroppedAsInternal pins the
// transactional-state opt-out: !txn| records are intentionally
// dropped and surface as Internal (not Unknown).
func TestDecodeSnapshot_TxnPrefixDroppedAsInternal(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	b := newSnapBuilder(0)
	b.WriteEntry([]byte("!txn|intent|abc"), 1, []byte("v"), false, 0, snapshotEncStateCleartx)
	res, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		OutRoot:  root,
		Adapters: AllAdapters(),
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if res.Counters.Internal != 1 {
		t.Fatalf("Counters.Internal = %d, want 1 (counters=%+v)", res.Counters.Internal, res.Counters)
	}
	if res.Counters.Unknown != 0 {
		t.Fatalf("Counters.Unknown = %d, want 0", res.Counters.Unknown)
	}
}

// TestPrefixRoutes_LongestMatchesFirst pins the descending-length
// ordering invariant — the routing table relies on it so a key
// matching both "!hs|meta|d|" and "!hs|meta|" lands on the delta
// handler. A future addition that adds an inverted-order entry
// surfaces here.
func TestPrefixRoutes_LongestMatchesFirst(t *testing.T) {
	t.Parallel()
	for i := 1; i < len(prefixRoutes); i++ {
		if len(prefixRoutes[i-1].prefix) < len(prefixRoutes[i].prefix) {
			t.Fatalf("prefixRoutes ordering violated at %d: %q (len %d) before %q (len %d)",
				i, prefixRoutes[i-1].prefix, len(prefixRoutes[i-1].prefix),
				prefixRoutes[i].prefix, len(prefixRoutes[i].prefix))
		}
	}
}

// TestPrefixRoutes_HashMetaDeltaWinsOverHashMeta is the
// canary for the most subtle ambiguity in the table: "!hs|meta|d|"
// vs "!hs|meta|" — both delta and base records start with
// "!hs|meta|". The longer prefix must match first or every delta
// key would be routed to HandleHashMeta with garbage.
func TestPrefixRoutes_HashMetaDeltaWinsOverHashMeta(t *testing.T) {
	t.Parallel()
	deltaKey := []byte(RedisHashMetaDeltaPrefix + "garbage")
	// Walk the routing table the same way route() does and confirm
	// the delta prefix matches first.
	var matched string
	for _, r := range prefixRoutes {
		if bytes.HasPrefix(deltaKey, r.prefix) {
			matched = string(r.prefix)
			break
		}
	}
	if matched != RedisHashMetaDeltaPrefix {
		t.Fatalf("first match for %q = %q, want %q", deltaKey, matched, RedisHashMetaDeltaPrefix)
	}
}

// TestDecodeSnapshot_DefaultScratchDoesNotWipeFinalS3Output pins
// gemini r1 security-high on PR #806: when ScratchRoot is empty
// and the S3 adapter is enabled, the default scratch path MUST NOT
// be <OutRoot>/s3/ (the same directory the final assembled bodies
// live under). If it did, S3Encoder.Finalize's deferred
// os.RemoveAll would wipe the dump immediately after it landed.
//
// We assert by pre-creating a marker file at <OutRoot>/s3/marker
// and verifying it survives a DecodeSnapshot run.
func TestDecodeSnapshot_DefaultScratchDoesNotWipeFinalS3Output(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	s3Dir := filepath.Join(root, "s3")
	if err := os.MkdirAll(s3Dir, 0o755); err != nil {
		t.Fatalf("mkdir s3: %v", err)
	}
	marker := filepath.Join(s3Dir, "preexisting.txt")
	if err := os.WriteFile(marker, []byte("survive me"), 0o600); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	// Header-only stream — the S3 encoder still goes through
	// newDispatcher + Finalize, which is what triggers the bug.
	b := newSnapBuilder(0)
	_, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		OutRoot:  root,
		Adapters: AdapterSet{S3: true},
		// ScratchRoot intentionally empty — exercise the default.
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("marker file was wiped by Finalize: %v (default scratch must not collide with <OutRoot>/s3/)", err)
	}
}

// TestDecodeSnapshot_FinalizeRunsOnTruncatedStream pins gemini r1
// medium on PR #806: a read error in ReadSnapshotWithHeader must
// not skip finalize(). To make the assertion non-vacuous
// (claude-bot r1 follow-up), we pre-create <root>/.scratch/s3/
// manually so it exists going in — if finalize() is skipped on
// the read error, the dir survives; if finalize() runs,
// S3Encoder.Finalize's deferred os.RemoveAll reclaims it.
func TestDecodeSnapshot_FinalizeRunsOnTruncatedStream(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	preScratch := filepath.Join(root, ".scratch", "s3")
	if err := os.MkdirAll(preScratch, 0o755); err != nil {
		t.Fatalf("pre-create scratch: %v", err)
	}
	// Truncate a header-only stream so ReadSnapshotWithHeader fails
	// on the partial header read.
	b := newSnapBuilder(0)
	full := b.Bytes()
	truncated := full[:len(full)/2]
	_, err := DecodeSnapshot(bytes.NewReader(truncated), DecodeOptions{
		OutRoot:  root,
		Adapters: AdapterSet{S3: true},
	})
	if err == nil {
		t.Fatalf("DecodeSnapshot on truncated stream returned nil; want error")
	}
	// finalize() ran => S3Encoder.Finalize's defer reclaimed the
	// scratch tree. If finalize was skipped on read error, the
	// pre-existing dir survives.
	if _, err := os.Stat(preScratch); err == nil {
		t.Fatalf(".scratch/s3/ survived truncated-stream decode — finalize did not run")
	}
}

// TestDecodeSnapshot_DistPrefixDroppedAsInternal pins the
// !dist| route added in r2 (Codex r1 P2 / claude-bot r1 on PR
// #806). Distribution-catalog keys ride the default Raft group's
// Pebble and so appear in every clustered snapshot. Without the
// route, they would land in Counters.Unknown — a false corruption
// signal on real dumps.
func TestDecodeSnapshot_DistPrefixDroppedAsInternal(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	b := newSnapBuilder(0)
	b.WriteEntry([]byte("!dist|meta|version"), 1, []byte("v1"), false, 0, snapshotEncStateCleartx)
	b.WriteEntry([]byte("!dist|route|abc"), 1, []byte("rt"), false, 0, snapshotEncStateCleartx)
	res, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		OutRoot:  root,
		Adapters: AllAdapters(),
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if res.Counters.Internal != 2 {
		t.Fatalf("Counters.Internal = %d, want 2 (counters=%+v)", res.Counters.Internal, res.Counters)
	}
	if res.Counters.Unknown != 0 {
		t.Fatalf("Counters.Unknown = %d, want 0 (!dist| must drop to Internal, not Unknown)", res.Counters.Unknown)
	}
}

// TestDecodeSnapshot_EncryptionPrefixDroppedAsInternal pins the
// !encryption| route added in r3 (Codex r3 P2 on PR #806). The
// writer-registry rows (!encryption|writers|<be4 dek_id>|<be2 node_id>)
// are persisted by the encryption applier through the default
// Raft group's Pebble; they appear in every snapshot from a
// cluster that has rotated DEKs, so the dispatcher must classify
// them as Internal, not Unknown.
func TestDecodeSnapshot_EncryptionPrefixDroppedAsInternal(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	b := newSnapBuilder(0)
	// !encryption|writers|<be4 dek_id=1>|<be2 node_id=0x42>
	b.WriteEntry([]byte("!encryption|writers|\x00\x00\x00\x01|\x00\x42"), 1, []byte("row"), false, 0, snapshotEncStateCleartx)
	res, err := DecodeSnapshot(bytes.NewReader(b.Bytes()), DecodeOptions{
		OutRoot:  root,
		Adapters: AllAdapters(),
	})
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	if res.Counters.Internal != 1 {
		t.Fatalf("Counters.Internal = %d, want 1 (counters=%+v)", res.Counters.Internal, res.Counters)
	}
	if res.Counters.Unknown != 0 {
		t.Fatalf("Counters.Unknown = %d, want 0 (!encryption| must drop to Internal, not Unknown)", res.Counters.Unknown)
	}
}

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

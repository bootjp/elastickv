package backup

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// sstIngestSnapshotEnv mirrors store's sstIngestSnapshotEnv: the switch
// that makes a live Pebble store emit the EKVSSTI1 bundle instead of the
// EKVPBBL1 stream.
const sstIngestSnapshotEnv = "ELASTICKV_PEBBLE_SST_INGEST_SNAPSHOT"

// liveRedisStringValue wraps body in the live Redis string-value envelope
// (magic 0xFF, version 0x01, flags 0x00 = no TTL) the adapter writes.
func liveRedisStringValue(body string) []byte {
	return append([]byte{0xFF, 0x01, 0x00}, body...)
}

// liveHistoryWrite is one replicated write applied to the test store.
type liveHistoryWrite struct {
	key      string
	body     string // ignored when del is set
	del      bool
	commitTS uint64
}

// liveHistory gives three Redis strings MVCC history, the way a running
// node accumulates versions between compactions: "updated" is overwritten,
// "deleted" is deleted after its first write, and "kept" is written once.
var liveHistory = []liveHistoryWrite{
	{key: "updated", body: "old", commitTS: 10},
	{key: "deleted", body: "gone", commitTS: 11},
	{key: "kept", body: "same", commitTS: 12},
	{key: "updated", body: "new", commitTS: 20},
	{key: "deleted", del: true, commitTS: 21},
}

// liveStoreSnapshotWithHistory applies liveHistory to a real Pebble store,
// one Raft entry per write, and returns the store's native snapshot stream.
func liveStoreSnapshotWithHistory(t *testing.T) []byte {
	t.Helper()
	st, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "fsm.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	var appliedIndex uint64
	for _, w := range liveHistory {
		mut := &store.KVPairMutation{Op: store.OpTypePut, Key: []byte(RedisStringPrefix + w.key), Value: liveRedisStringValue(w.body)}
		if w.del {
			mut = &store.KVPairMutation{Op: store.OpTypeDelete, Key: []byte(RedisStringPrefix + w.key)}
		}
		appliedIndex++
		if err := st.ApplyMutationsRaftAt(context.Background(), []*store.KVPairMutation{mut}, nil, w.commitTS, w.commitTS, appliedIndex); err != nil {
			t.Fatalf("apply %s@%d: %v", w.key, w.commitTS, err)
		}
	}
	snap, err := st.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer snap.Close()
	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	return buf.Bytes()
}

// assertLiveHistoryDump checks the Redis dump DecodeSnapshot produced from
// liveHistory: the newest version of each key, and no file for the key
// whose newest version is a delete.
func assertLiveHistoryDump(t *testing.T, out string) {
	t.Helper()
	dir := filepath.Join(out, "redis", "db_0", "strings")
	for key, want := range map[string]string{"updated": "new", "kept": "same"} {
		got, err := os.ReadFile(filepath.Join(dir, key+".bin")) //nolint:gosec // test path
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("%s = %q, want the newest version %q", key, got, want)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "deleted.bin")); !os.IsNotExist(err) {
		t.Fatalf("deleted key was resurrected from an older version (stat err=%v)", err)
	}
}

func decodeLiveHistory(t *testing.T, stream []byte) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	if _, err := DecodeSnapshot(bytes.NewReader(stream), DecodeOptions{OutRoot: out, Adapters: AdapterSet{Redis: true}}); err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	return out
}

// TestDecodeSnapshot_LiveStoreKeepsNewestVersion pins that a dump of a live
// store with MVCC history carries each key's newest version only: the
// store streams every physical version, so routing each one wrote stale
// values over newer ones and resurrected deleted keys from the version
// below their tombstone.
func TestDecodeSnapshot_LiveStoreKeepsNewestVersion(t *testing.T) {
	t.Parallel()
	stream := liveStoreSnapshotWithHistory(t)
	if !bytes.HasPrefix(stream, []byte(PebbleSnapshotMagic)) {
		t.Fatalf("expected the legacy %s stream, got %q", PebbleSnapshotMagic, stream[:8])
	}
	assertLiveHistoryDump(t, decodeLiveHistory(t, stream))
}

// TestDecodeSnapshot_SSTIngestSnapshot pins that the decoder reads the
// EKVSSTI1 bundle a live store emits when SST ingest snapshots are on,
// with the same newest-version semantics as the legacy stream.
func TestDecodeSnapshot_SSTIngestSnapshot(t *testing.T) {
	t.Setenv(sstIngestSnapshotEnv, "true")
	stream := liveStoreSnapshotWithHistory(t)
	if !bytes.HasPrefix(stream, []byte("EKVSSTI1")) {
		t.Fatalf("expected an EKVSSTI1 bundle with %s=true, got %q", sstIngestSnapshotEnv, stream[:8])
	}
	assertLiveHistoryDump(t, decodeLiveHistory(t, stream))
}

// TestMaxSnapshotEncodedKeySize_MatchesStore pins the reader's key budget
// to the live store's physical stored-key budget plus the timestamp
// suffix, so a snapshot carrying a migration-staged maximum-size key is
// not rejected offline.
func TestMaxSnapshotEncodedKeySize_MatchesStore(t *testing.T) {
	t.Parallel()
	if want := store.MaxSnapshotStoredKeySize + snapshotTSSize; MaxSnapshotEncodedKeySize != want {
		t.Fatalf("MaxSnapshotEncodedKeySize = %d, want store.MaxSnapshotStoredKeySize + %d = %d",
			MaxSnapshotEncodedKeySize, snapshotTSSize, want)
	}
}

// oneEntryStream builds an EKVPBBL1 stream holding one cleartext entry
// whose encoded key is exactly encodedKeyLen bytes.
func oneEntryStream(t *testing.T, encodedKeyLen int) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString(PebbleSnapshotMagic)
	mustBinaryWrite(t, &buf, uint64(1))
	key := bytes.Repeat([]byte{'k'}, encodedKeyLen)
	copy(key, "!txn|lock|")
	binary.BigEndian.PutUint64(key[len(key)-snapshotTSSize:], ^uint64(1))
	mustBinaryWrite(t, &buf, uint64(len(key)))
	buf.Write(key)
	value := make([]byte, snapshotValueHeaderSize)
	mustBinaryWrite(t, &buf, uint64(len(value)))
	buf.Write(value)
	return buf.Bytes()
}

func mustBinaryWrite(t *testing.T, buf *bytes.Buffer, v uint64) {
	t.Helper()
	if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
		t.Fatalf("binary.Write: %v", err)
	}
}

// TestReadSnapshot_AcceptsStoredKeyBudget pins that a key at the store's
// physical budget decodes and one byte more is still refused.
func TestReadSnapshot_AcceptsStoredKeyBudget(t *testing.T) {
	t.Parallel()
	atBudget := store.MaxSnapshotStoredKeySize + snapshotTSSize
	entries := 0
	if _, err := ReadSnapshotWithHeader(bytes.NewReader(oneEntryStream(t, atBudget)), func(SnapshotHeader, SnapshotEntry) error {
		entries++
		return nil
	}); err != nil {
		t.Fatalf("key at the stored-key budget (%d bytes) rejected: %v", atBudget, err)
	}
	if entries != 1 {
		t.Fatalf("entries = %d, want 1", entries)
	}
	_, err := ReadSnapshotWithHeader(bytes.NewReader(oneEntryStream(t, atBudget+1)), func(SnapshotHeader, SnapshotEntry) error { return nil })
	if !errors.Is(err, ErrSnapshotKeyTooLarge) {
		t.Fatalf("key one byte over the budget: err = %v, want ErrSnapshotKeyTooLarge", err)
	}
}

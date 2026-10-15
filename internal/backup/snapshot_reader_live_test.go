package backup

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// liveStoreSnapshot writes one Redis string key through a real Pebble store
// with a Raft applied index, so the native snapshot stream carries the
// store's reserved metadata keys (`_meta_last_commit_ts`,
// `_meta_applied_index`) next to the MVCC version, exactly as a snapshot
// taken from a running node does.
func liveStoreSnapshot(t *testing.T, userKey, body string, commitTS, appliedIndex uint64) []byte {
	t.Helper()
	st, err := store.NewPebbleStore(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	muts := []*store.KVPairMutation{{Op: store.OpTypePut, Key: []byte(userKey), Value: []byte(body)}}
	if err := st.ApplyMutationsRaftAt(context.Background(), muts, nil, commitTS, commitTS, appliedIndex); err != nil {
		t.Fatalf("apply: %v", err)
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

// TestReadSnapshot_SkipsLiveStoreMetadataKeys pins that the reader decodes a
// snapshot taken from a live Pebble store: the store's reserved metadata
// keys are skipped instead of failing the read on their 8-byte values.
func TestReadSnapshot_SkipsLiveStoreMetadataKeys(t *testing.T) {
	t.Parallel()
	const (
		userKey  = RedisStringPrefix + "greeting"
		body     = "hello"
		commitTS = uint64(7)
	)
	stream := liveStoreSnapshot(t, userKey, body, commitTS, 3)
	if !bytes.Contains(stream, []byte("_meta_last_commit_ts")) {
		t.Fatalf("the live stream must carry the store's metadata keys for this test to mean anything")
	}

	var got []SnapshotEntry
	header, err := ReadSnapshotWithHeader(bytes.NewReader(stream), func(_ SnapshotHeader, e SnapshotEntry) error {
		got = append(got, SnapshotEntry{
			UserKey:   bytes.Clone(e.UserKey),
			UserValue: bytes.Clone(e.UserValue),
			CommitTS:  e.CommitTS,
			ExpireAt:  e.ExpireAt,
			Tombstone: e.Tombstone,
		})
		return nil
	})
	if err != nil {
		t.Fatalf("ReadSnapshotWithHeader: %v", err)
	}
	if header.LastCommitTS != commitTS {
		t.Fatalf("header.LastCommitTS = %d, want %d", header.LastCommitTS, commitTS)
	}
	if len(got) != 1 {
		t.Fatalf("entries = %d, want 1: %+v", len(got), got)
	}
	if string(got[0].UserKey) != userKey || string(got[0].UserValue) != body || got[0].CommitTS != commitTS || got[0].Tombstone {
		t.Fatalf("entry = %+v, want key %q value %q ts %d", got[0], userKey, body, commitTS)
	}
}

// WriteRaw appends one entry exactly as given, without the MVCC key suffix
// or value header: the shape of a store metadata key in a live snapshot.
func (b *snapBuilder) WriteRaw(encKey, encVal []byte) {
	_ = binary.Write(&b.buf, binary.LittleEndian, uint64(len(encKey)))
	b.buf.Write(encKey)
	_ = binary.Write(&b.buf, binary.LittleEndian, uint64(len(encVal)))
	b.buf.Write(encVal)
}

// TestReadSnapshot_MetadataKeyWithShortValueIsNotAnError pins the exact
// failure the skip closes: a reserved key whose value is 8 bytes (shorter
// than the value header) used to surface as ErrSnapshotShortValue.
func TestReadSnapshot_MetadataKeyWithShortValueIsNotAnError(t *testing.T) {
	t.Parallel()
	b := newSnapBuilder(1)
	b.WriteRaw([]byte("_meta_last_commit_ts"), []byte{1, 0, 0, 0, 0, 0, 0, 0})
	b.WriteEntry([]byte("k"), 1, []byte("v"), false, 0, 0)

	var keys []string
	err := ReadSnapshot(bytes.NewReader(b.Bytes()), func(_ SnapshotHeader, e SnapshotEntry) error {
		keys = append(keys, string(e.UserKey))
		return nil
	})
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if len(keys) != 1 || keys[0] != "k" {
		t.Fatalf("keys = %v, want [k]", keys)
	}

	// A short value on a user key is still corruption.
	c := newSnapBuilder(1)
	c.WriteRaw([]byte("user-key\x00\x00\x00\x00\x00\x00\x00\x00"), []byte{1, 0, 0, 0, 0, 0, 0, 0})
	err = ReadSnapshot(bytes.NewReader(c.Bytes()), func(SnapshotHeader, SnapshotEntry) error { return nil })
	if !errors.Is(err, ErrSnapshotShortValue) {
		t.Fatalf("err = %v, want ErrSnapshotShortValue", err)
	}
}

// TestIsPebbleOperationalKey_MatchesStore pins the offline mirror of the
// store's metadata-key classifier against the live one, key by key: every
// reserved key, a writer-registry row, and the near misses (an
// MVCC-encoded user key that starts with a reserved name, a registry row
// of the wrong length, the bare registry prefix).
func TestIsPebbleOperationalKey_MatchesStore(t *testing.T) {
	t.Parallel()
	registry := encryption.RegistryKey(7, 0x1234)
	cases := []struct {
		name string
		key  []byte
		want bool
	}{
		{name: "last commit ts", key: []byte("_meta_last_commit_ts"), want: true},
		{name: "min retained ts", key: []byte("_meta_min_retained_ts"), want: true},
		{name: "pending min retained ts", key: []byte("_meta_pending_min_retained_ts"), want: true},
		{name: "applied index", key: []byte("_meta_applied_index"), want: true},
		{name: "migration ack", key: []byte("_migack"), want: true},
		{name: "migration hlc floor", key: []byte("_mighlc"), want: true},
		{name: "migration promote", key: []byte("_migpromote"), want: true},
		{name: "writer registry row", key: registry, want: true},
		{name: "bare registry prefix", key: bytes.Clone(encryption.WriterRegistryPrefix), want: false},
		{name: "registry row one byte short", key: registry[:len(registry)-1], want: false},
		{name: "mvcc-encoded user key with a reserved name", key: append([]byte("_meta_last_commit_ts"), make([]byte, snapshotTSSize)...), want: false},
		{name: "user key", key: []byte(RedisStringPrefix + "greeting"), want: false},
		{name: "empty", key: nil, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isPebbleOperationalKey(tc.key); got != tc.want {
				t.Fatalf("isPebbleOperationalKey(%q) = %v, want %v", tc.key, got, tc.want)
			}
			if live := store.IsPebbleOperationalKey(tc.key); live != tc.want {
				t.Fatalf("store.IsPebbleOperationalKey(%q) = %v, want %v: the mirror in snapshot_reader.go is stale", tc.key, live, tc.want)
			}
		})
	}
}

// TestIsPebbleOperationalKey_AgreesWithStoreOnLiveStream walks every raw
// key of a snapshot a live Pebble store wrote and checks that the mirror
// and the live classifier agree on each one, so a metadata key the store
// starts writing later surfaces here rather than as a decode failure.
func TestIsPebbleOperationalKey_AgreesWithStoreOnLiveStream(t *testing.T) {
	t.Parallel()
	stream := liveStoreSnapshot(t, RedisStringPrefix+"walk", "value", 11, 4)
	keys := rawSnapshotKeys(t, stream)
	operational := 0
	for _, key := range keys {
		mirror, live := isPebbleOperationalKey(key), store.IsPebbleOperationalKey(key)
		if mirror != live {
			t.Fatalf("raw key %q: mirror says %v, store says %v", key, mirror, live)
		}
		if live {
			operational++
		}
	}
	if operational == 0 {
		t.Fatalf("the live stream carried no metadata key among %d raw keys; the test proves nothing", len(keys))
	}
	if operational == len(keys) {
		t.Fatalf("every one of %d raw keys classified as metadata; the user version is missing", len(keys))
	}
}

// rawSnapshotKeys splits a native snapshot stream into its raw encoded
// keys without decoding them: the header, then repeated
// (keyLen, key, valLen, value) frames, exactly the shape readOneEntry
// consumes.
func rawSnapshotKeys(t *testing.T, stream []byte) [][]byte {
	t.Helper()
	const lenPrefixSize = snapshotTSSize // both are 8-byte little-endian words
	rest := stream
	if len(rest) < PebbleSnapshotMagicLen+lenPrefixSize {
		t.Fatalf("stream of %d bytes is shorter than the header", len(rest))
	}
	rest = rest[PebbleSnapshotMagicLen+lenPrefixSize:]
	var keys [][]byte
	for len(rest) > 0 {
		var key []byte
		key, rest = rawSnapshotField(t, rest, "key")
		keys = append(keys, bytes.Clone(key))
		_, rest = rawSnapshotField(t, rest, "value")
	}
	return keys
}

// rawSnapshotField reads one length-prefixed field and returns it with
// the bytes that follow it.
func rawSnapshotField(t *testing.T, rest []byte, what string) (field, tail []byte) {
	t.Helper()
	const lenPrefixSize = snapshotTSSize
	if len(rest) < lenPrefixSize {
		t.Fatalf("truncated %s length prefix", what)
	}
	n := binary.LittleEndian.Uint64(rest[:lenPrefixSize])
	body := rest[lenPrefixSize:]
	if n > uint64(len(body)) {
		t.Fatalf("%s length %d exceeds the remaining %d bytes", what, n, len(body))
	}
	return body[:n], body[n:]
}

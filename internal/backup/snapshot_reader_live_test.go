package backup

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

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

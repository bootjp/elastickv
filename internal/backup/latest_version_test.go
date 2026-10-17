package backup

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/cockroachdb/errors"
	"pgregory.net/rapid"
)

// versionedKey is one physical version as the store writes it.
type versionedKey struct {
	userKey  []byte
	commitTS uint64
}

func (v versionedKey) encoded() []byte {
	return binary.BigEndian.AppendUint64(bytes.Clone(v.userKey), ^v.commitTS)
}

// sortedLikeStore orders versions the way a store snapshot streams them:
// bytewise over userKey || BE(^commitTS).
func sortedLikeStore(versions []versionedKey) []versionedKey {
	out := append([]versionedKey(nil), versions...)
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].encoded(), out[j].encoded()) < 0 })
	return out
}

// keptByFilter runs the filter over versions in store order and returns
// the commit timestamp it kept for each user key.
func keptByFilter(t *rapid.T, versions []versionedKey) map[string]uint64 {
	var f latestVersionFilter
	kept := map[string]uint64{}
	for _, v := range sortedLikeStore(versions) {
		newest, err := f.keep(SnapshotEntry{UserKey: v.userKey, CommitTS: v.commitTS})
		if err != nil {
			t.Fatalf("keep(%q@%d): %v", v.userKey, v.commitTS, err)
		}
		if !newest {
			continue
		}
		if prev, dup := kept[string(v.userKey)]; dup {
			t.Fatalf("kept two versions of %q: %d and %d", v.userKey, prev, v.commitTS)
		}
		kept[string(v.userKey)] = v.commitTS
	}
	return kept
}

// TestLatestVersionFilter_InterleavedVersions pins the case adjacency
// alone gets wrong: a user key that extends another one can sort between
// two versions of the shorter key, and the shorter key's older version
// must still be hidden.
func TestLatestVersionFilter_InterleavedVersions(t *testing.T) {
	t.Parallel()
	short := []byte("k")
	// ^20 and ^10 begin with seven 0xFF bytes; a key "k" + 0xFF*7 + 0xF0
	// sorts between "k"||^20 (…0xEB) and "k"||^10 (…0xF5).
	long := append([]byte("k"), 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF0)
	versions := sortedLikeStore([]versionedKey{
		{userKey: short, commitTS: 20},
		{userKey: short, commitTS: 10},
		{userKey: long, commitTS: 15},
	})
	if !bytes.Equal(versions[1].userKey, long) {
		t.Fatalf("fixture does not interleave: order %q, %q, %q", versions[0].userKey, versions[1].userKey, versions[2].userKey)
	}
	var f latestVersionFilter
	var got []bool
	for _, v := range versions {
		newest, err := f.keep(SnapshotEntry{UserKey: v.userKey, CommitTS: v.commitTS})
		if err != nil {
			t.Fatalf("keep: %v", err)
		}
		got = append(got, newest)
	}
	if want := []bool{true, true, false}; got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("kept = %v, want %v (k@20, the long key, then k@10 hidden)", got, want)
	}
}

// TestLatestVersionFilter_KeepsNewestPerKey checks, over random key sets
// with shared prefixes and 0xFF bytes, that exactly the newest version of
// every user key survives.
func TestLatestVersionFilter_KeepsNewestPerKey(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		alphabet := []byte{'a', 'b', 0x00, 0xEB, 0xF0, 0xFE, 0xFF}
		keyGen := rapid.SliceOfN(rapid.SampledFrom(alphabet), 1, 10)
		keys := rapid.SliceOfNDistinct(keyGen, 1, 12, func(k []byte) string { return string(k) }).Draw(t, "keys")
		var versions []versionedKey
		want := map[string]uint64{}
		for i, k := range keys {
			tss := rapid.SliceOfNDistinct(rapid.Uint64Range(1, 40), 1, 4, rapid.ID[uint64]).Draw(t, "ts"+string(rune('0'+i)))
			for _, ts := range tss {
				versions = append(versions, versionedKey{userKey: k, commitTS: ts})
				if ts > want[string(k)] {
					want[string(k)] = ts
				}
			}
		}
		got := keptByFilter(t, versions)
		if len(got) != len(want) {
			t.Fatalf("kept %d keys, want %d", len(got), len(want))
		}
		for k, ts := range want {
			if got[k] != ts {
				t.Fatalf("key %q: kept commit_ts %d, want the newest %d", k, got[k], ts)
			}
		}
	})
}

// TestLatestVersionFilter_RejectsUnsortedInput pins the fail-closed
// order check: out of order, the first version seen need not be the
// newest.
func TestLatestVersionFilter_RejectsUnsortedInput(t *testing.T) {
	t.Parallel()
	var f latestVersionFilter
	if _, err := f.keep(SnapshotEntry{UserKey: []byte("b"), CommitTS: 1}); err != nil {
		t.Fatalf("first entry: %v", err)
	}
	for _, e := range []SnapshotEntry{
		{UserKey: []byte("a"), CommitTS: 1}, // sorts before b
		{UserKey: []byte("b"), CommitTS: 1}, // the same physical key again
	} {
		if _, err := f.keep(e); !errors.Is(err, ErrSnapshotUnsorted) {
			t.Fatalf("keep(%q@%d): err = %v, want ErrSnapshotUnsorted", e.UserKey, e.CommitTS, err)
		}
	}
}

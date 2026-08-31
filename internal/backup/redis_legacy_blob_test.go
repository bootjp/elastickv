package backup

import (
	"os"
	"path/filepath"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"
)

func hashLegacyBlobKey(userKey string) []byte {
	return append([]byte(RedisHashLegacyBlobPrefix), userKey...)
}

func setLegacyBlobKey(userKey string) []byte {
	return append([]byte(RedisSetLegacyBlobPrefix), userKey...)
}

func encodeHashLegacyBlobValue(t *testing.T, entries map[string]string) []byte {
	t.Helper()
	body, err := gproto.Marshal(&pb.RedisHashValue{Entries: entries})
	require.NoError(t, err)
	return append(append([]byte{}, redisHashLegacyProtoPrefix...), body...)
}

func encodeSetLegacyBlobValue(t *testing.T, members []string) []byte {
	t.Helper()
	body, err := gproto.Marshal(&pb.RedisSetValue{Members: members})
	require.NoError(t, err)
	return append(append([]byte{}, redisSetLegacyProtoPrefix...), body...)
}

// A cluster upgraded from the consolidated layout still serves hashes and sets
// out of !redis|hash| / !redis|set| until a write migrates them, so a dump
// that does not decode those blobs silently omits live user data.
func TestRedisLegacyBlobsAreBackedUp(t *testing.T) {
	t.Parallel()

	for _, key := range [][]byte{
		hashLegacyBlobKey("user:42"),
		setLegacyBlobKey("colors"),
	} {
		scope, scoped, err := ScopeForKey(key)
		require.NoError(t, err)
		require.True(t, scoped, "key %q must be backed up", key)
		require.Equal(t, Scope{Adapter: "redis", Name: "db_0"}, scope)
	}
}

func TestRedisDB_HashLegacyBlobRoundTrip(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	value := encodeHashLegacyBlobValue(t, map[string]string{"name": "alice", "city": "kyoto"})
	require.NoError(t, db.HandleHashLegacyBlob(hashLegacyBlobKey("user:42"), value))
	require.NoError(t, db.Finalize())

	got := readHashJSON(t, filepath.Join(root, "redis", "db_0", "hashes", "user%3A42.json"))
	require.Equal(t, "alice", hashFieldByName(t, got, "name"))
	require.Equal(t, "kyoto", hashFieldByName(t, got, "city"))
}

func TestRedisDB_SetLegacyBlobRoundTrip(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	value := encodeSetLegacyBlobValue(t, []string{"red", "green", "blue"})
	require.NoError(t, db.HandleSetLegacyBlob(setLegacyBlobKey("colors"), value))
	require.NoError(t, db.Finalize())

	got := readSetJSON(t, filepath.Join(root, "redis", "db_0", "sets", "colors.json"))
	require.Equal(t, []any{"blue", "green", "red"}, setMembersArray(t, got))
}

// Wide-column rows are the live read path's source of truth, so a key that
// carries both layouts must dump the wide-column state alone -- merging the
// legacy blob on top would resurrect fields and members deleted after the
// migration. Both scan orders are exercised: "!hs|" sorts before
// "!redis|hash|", while "!redis|set|" sorts before "!st|".
func TestRedisDB_WideColumnRowsWinOverLegacyBlobs(t *testing.T) {
	t.Parallel()

	t.Run("hash wide first", func(t *testing.T) {
		t.Parallel()
		db, root := newRedisDB(t)
		require.NoError(t, db.HandleHashField(hashFieldKey("h", "kept"), []byte("new")))
		require.NoError(t, db.HandleHashLegacyBlob(hashLegacyBlobKey("h"),
			encodeHashLegacyBlobValue(t, map[string]string{"deleted": "stale"})))
		require.NoError(t, db.Finalize())

		got := readHashJSON(t, filepath.Join(root, "redis", "db_0", "hashes", "h.json"))
		require.Len(t, hashFieldArray(t, got), 1)
		require.Equal(t, "new", hashFieldByName(t, got, "kept"))
	})

	t.Run("set legacy first", func(t *testing.T) {
		t.Parallel()
		db, root := newRedisDB(t)
		require.NoError(t, db.HandleSetLegacyBlob(setLegacyBlobKey("s"),
			encodeSetLegacyBlobValue(t, []string{"deleted"})))
		require.NoError(t, db.HandleSetMember(setMemberKey("s", []byte("kept")), nil))
		require.NoError(t, db.Finalize())

		got := readSetJSON(t, filepath.Join(root, "redis", "db_0", "sets", "s.json"))
		require.Equal(t, []any{"kept"}, setMembersArray(t, got))
	})
}

// A blob without the live store's magic prefix is corruption, not an empty
// collection: decoding it as one would publish a dump that silently dropped
// the key's contents.
func TestRedisDB_LegacyBlobsRejectMissingMagic(t *testing.T) {
	t.Parallel()
	db, _ := newRedisDB(t)
	require.ErrorIs(t, db.HandleHashLegacyBlob(hashLegacyBlobKey("h"), []byte("garbage")),
		ErrRedisInvalidHashLegacyBlob)
	require.ErrorIs(t, db.HandleSetLegacyBlob(setLegacyBlobKey("s"), []byte("garbage")),
		ErrRedisInvalidSetLegacyBlob)
	// A key that is only the family prefix is the *empty* Redis key, which the
	// command paths accept, so it is the value that has to be rejected here --
	// not the key.
	require.ErrorIs(t, db.HandleHashLegacyBlob([]byte(RedisHashLegacyBlobPrefix), nil),
		ErrRedisInvalidHashLegacyBlob)
	require.ErrorIs(t, db.HandleSetLegacyBlob([]byte(RedisSetLegacyBlobPrefix), nil),
		ErrRedisInvalidSetLegacyBlob)
	// A key missing the prefix entirely is what "malformed key" means.
	require.ErrorIs(t, db.HandleHashLegacyBlob([]byte("!redis|other|k"),
		encodeHashLegacyBlobValue(t, map[string]string{"f": "v"})), ErrRedisInvalidHashLegacyBlob)
	require.ErrorIs(t, db.HandleSetLegacyBlob([]byte("!redis|other|k"),
		encodeSetLegacyBlobValue(t, []string{"m"})), ErrRedisInvalidSetLegacyBlob)
}

// The empty Redis key is legal, so its legacy blob is stored at exactly the
// family prefix. Treating that as malformed fails the dump on data the Redis
// API can create; the zset parser has always accepted it.
func TestRedisDB_LegacyBlobsAcceptTheEmptyKey(t *testing.T) {
	t.Parallel()
	db, root := newRedisDB(t)
	require.NoError(t, db.HandleHashLegacyBlob([]byte(RedisHashLegacyBlobPrefix),
		encodeHashLegacyBlobValue(t, map[string]string{"f": "v"})))
	require.NoError(t, db.HandleSetLegacyBlob([]byte(RedisSetLegacyBlobPrefix),
		encodeSetLegacyBlobValue(t, []string{"m"})))
	require.NoError(t, db.Finalize())

	entries, err := os.ReadDir(filepath.Join(root, "redis", "db_0", "hashes"))
	require.NoError(t, err)
	require.Len(t, entries, 1, "the empty-key hash must be written")
	got := readHashJSON(t, filepath.Join(root, "redis", "db_0", "hashes", entries[0].Name()))
	require.Equal(t, "v", hashFieldByName(t, got, "f"))
}

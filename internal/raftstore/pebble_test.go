package raftstore

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// Guards the pebble v1 → v2 migration: v2 refuses to open DBs below
// FormatFlushableIngest.
func TestPebbleStore_FormatRatchetedForV2(t *testing.T) {
	dir := t.TempDir()

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	require.GreaterOrEqual(t, s.db.FormatMajorVersion(), pebble.FormatFlushableIngest)
}

// Existing on-disk DBs from pre-migration binaries must be ratcheted up on
// reopen — this is the migration path for clusters that already have raft.db.
func TestPebbleStore_RatchetsExistingDB(t *testing.T) {
	dir := t.TempDir()

	// Simulate an existing on-disk DB created by a pre-migration binary,
	// which left FormatMajorVersion unset (== FormatMostCompatible after
	// EnsureDefaults in pebble v1).
	old, err := pebble.Open(dir, &pebble.Options{
		FormatMajorVersion: pebble.FormatMostCompatible,
	})
	require.NoError(t, err)
	require.Equal(t, pebble.FormatMostCompatible, old.FormatMajorVersion())
	require.NoError(t, old.Close())

	// Reopen through the production path.
	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	require.GreaterOrEqual(t, s.db.FormatMajorVersion(), pebble.FormatFlushableIngest)
}

// The format ratchet runs during DB open; committed Raft entries must survive it.
func TestPebbleStore_PreservesLogsAcrossRatchet(t *testing.T) {
	dir := t.TempDir()

	old, err := pebble.Open(dir, &pebble.Options{
		FormatMajorVersion: pebble.FormatMostCompatible,
	})
	require.NoError(t, err)
	pre := &PebbleStore{db: old}
	require.NoError(t, pre.StoreLog(&raft.Log{Index: 1, Term: 1, Data: []byte("hello")}))
	require.NoError(t, pre.StoreLog(&raft.Log{Index: 2, Term: 1, Data: []byte("world")}))
	require.NoError(t, old.Close())

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	var got raft.Log
	require.NoError(t, s.GetLog(1, &got))
	require.Equal(t, []byte("hello"), got.Data)
	require.NoError(t, s.GetLog(2, &got))
	require.Equal(t, []byte("world"), got.Data)

	last, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)
}

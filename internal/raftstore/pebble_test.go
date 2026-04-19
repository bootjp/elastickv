package raftstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// Pins the on-open format so unintended downgrades below pebble v2
// FormatMinSupported are caught early.
func TestPebbleStore_OpensAtPinnedFormat(t *testing.T) {
	dir := t.TempDir()

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.Equal(t, pebble.FormatVirtualSSTables, s.db.FormatMajorVersion())
}

// End-to-end check that StoreLog/GetLog round-trip across a close/reopen
// against pebble v2.
func TestPebbleStore_PersistsLogsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewPebbleStore(dir)
	require.NoError(t, err)
	require.NoError(t, s1.StoreLog(&raft.Log{Index: 1, Term: 1, Data: []byte("hello")}))
	require.NoError(t, s1.StoreLog(&raft.Log{Index: 2, Term: 1, Data: []byte("world")}))
	require.NoError(t, s1.Close())

	s2, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s2.Close()) })

	var got raft.Log
	require.NoError(t, s2.GetLog(1, &got))
	require.Equal(t, []byte("hello"), got.Data)
	require.NoError(t, s2.GetLog(2, &got))
	require.Equal(t, []byte("world"), got.Data)

	last, err := s2.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)
}

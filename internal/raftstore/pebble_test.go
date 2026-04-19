package raftstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// Pins the on-open format so unintended downgrades below pebble v2
// FormatMinSupported are caught early.
func TestPebbleStore_OpensAtPinnedFormat(t *testing.T) {
	dir := t.TempDir()

	s, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.GreaterOrEqual(t, s.db.FormatMajorVersion(), pebble.FormatFlushableIngest)
	require.Equal(t, pebble.FormatVirtualSSTables, s.db.FormatMajorVersion())
}

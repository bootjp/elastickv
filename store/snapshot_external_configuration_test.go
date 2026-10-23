package store

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExternalSnapshotRebasesOnlyRaftLineageMetadata(t *testing.T) {
	for _, format := range []string{"native", "sst"} {
		t.Run(format, func(t *testing.T) {
			st, err := NewPebbleStore(t.TempDir(), WithSSTIngestSnapshots(format == "sst"))
			require.NoError(t, err)
			source, ok := st.(*pebbleStore)
			require.True(t, ok)
			defer source.Close()
			key := MigrationConfigurationIndexKey()
			adjacent := append(bytes.Clone(key), []byte("/user")...)
			require.NoError(t, source.PutAt(t.Context(), key, binary.BigEndian.AppendUint64(nil, 9000), 10, 0))
			require.NoError(t, source.PutAt(t.Context(), adjacent, []byte("preserved"), 20, 0))
			require.NoError(t, source.SetDurableAppliedIndex(9001))
			raw := externalTestSnapshot(t, source)
			for _, external := range []bool{false, true} {
				t.Run(map[bool]string{false: "raft", true: "external"}[external], func(t *testing.T) {
					st, openErr := NewPebbleStore(t.TempDir())
					require.NoError(t, openErr)
					destination, ok := st.(*pebbleStore)
					require.True(t, ok)
					defer destination.Close()
					if external {
						require.NoError(t, destination.RestoreExternal(t.Context(), bytes.NewReader(raw)))
					} else {
						require.NoError(t, destination.Restore(bytes.NewReader(raw)))
					}
					applied, present, readErr := destination.LastAppliedIndex()
					require.NoError(t, readErr)
					value, readErr := destination.GetAt(t.Context(), key, 30)
					if external {
						require.ErrorIs(t, readErr, ErrKeyNotFound)
						require.False(t, present, "source Raft progress must not suppress destination applies")
						require.Zero(t, applied)
					} else {
						require.NoError(t, readErr)
						require.Equal(t, uint64(9000), binary.BigEndian.Uint64(value))
						require.True(t, present)
						require.Equal(t, uint64(9001), applied)
					}
					value, readErr = destination.GetAt(t.Context(), adjacent, 30)
					require.NoError(t, readErr)
					require.Equal(t, []byte("preserved"), value)
					require.Equal(t, uint64(20), destination.LastCommitTS())
				})
			}
		})
	}
}

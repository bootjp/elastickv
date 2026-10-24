package store

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationNamespaceSharedConfigurationCodec(t *testing.T) {
	for _, active := range []bool{false, true} {
		state := MigrationNamespaceConfiguration{Index: 5}
		if active {
			state.Version, state.ActivatedAt, state.Members, state.Joining = MigrationNamespaceVersion, 3, []uint64{1, 2}, []uint64{3}
		}
		encoded, err := EncodeMigrationNamespaceConfiguration(state)
		require.NoError(t, err)
		index, err := DecodeMigrationConfigurationIndex(encoded)
		require.NoError(t, err)
		require.Equal(t, state.Index, index)
		decoded, err := DecodeMigrationNamespaceConfiguration(encoded)
		require.NoError(t, err)
		require.Equal(t, state, decoded)
		if !active {
			require.Len(t, encoded, 8)
		}
	}
	for _, malformed := range [][]byte{nil, {1}, binary.BigEndian.AppendUint64(nil, 0), []byte("EKVNS1\x00{}"), []byte("EKVNS1\x00{\"Index\":5,\"Version\":2}"), []byte("EKVNS1\x00{\"Index\":5,\"Version\":1,\"ActivatedAt\":3,\"Members\":[1,1]}")} {
		_, err := DecodeMigrationConfigurationIndex(malformed)
		require.Error(t, err)
	}
}

package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func codecPropertyNamespaceState(t *rapid.T) MigrationNamespaceConfiguration {
	t.Helper()
	index := rapid.Uint64Range(1, ^uint64(0)-1).Draw(t, "configuration index")
	state := MigrationNamespaceConfiguration{
		Index: index, Version: MigrationNamespaceVersion,
		ActivatedAt: rapid.Uint64Range(1, index).Draw(t, "activation index"),
	}
	base := rapid.Uint64Range(1, ^uint64(0)-100).Draw(t, "member base")
	count := rapid.IntRange(2, 8).Draw(t, "member count")
	for i := range count {
		state.Members = append(state.Members, base+uint64(i))
	}
	joining := rapid.IntRange(0, 4).Draw(t, "joining count")
	for i := range joining {
		state.Joining = append(state.Joining, base+uint64(count+i))
	}
	return state
}

func TestMigrationCodecPropertyNamespaceRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		legacy := MigrationNamespaceConfiguration{Index: rapid.Uint64Range(1, ^uint64(0)).Draw(t, "legacy index")}
		active := codecPropertyNamespaceState(t)
		for _, state := range []MigrationNamespaceConfiguration{legacy, active} {
			encoded, err := EncodeMigrationNamespaceConfiguration(state)
			require.NoError(t, err)
			decoded, err := DecodeMigrationNamespaceConfiguration(encoded)
			require.NoError(t, err)
			require.Equal(t, state, decoded)
			index, err := DecodeMigrationConfigurationIndex(encoded)
			require.NoError(t, err)
			require.Equal(t, state.Index, index)
			reencoded, err := EncodeMigrationNamespaceConfiguration(decoded)
			require.NoError(t, err)
			require.Equal(t, encoded, reencoded)
			if state.Version == 0 {
				require.Equal(t, binary.BigEndian.AppendUint64(nil, state.Index), encoded)
			}
		}
		// This codec intentionally permits unknown JSON fields; unlike the
		// partition RPC codec, it has no strict/canonical-input contract.
		encoded, err := EncodeMigrationNamespaceConfiguration(active)
		require.NoError(t, err)
		extended := append(bytes.Clone(encoded[:len(encoded)-1]), []byte(`,"FutureField":{"Enabled":true}}`)...)
		decoded, err := DecodeMigrationNamespaceConfiguration(extended)
		require.NoError(t, err)
		require.Equal(t, active, decoded)
		reencoded, err := EncodeMigrationNamespaceConfiguration(decoded)
		require.NoError(t, err)
		require.Equal(t, encoded, reencoded, "known state is canonicalized without preserving extensions")
	})
}

func TestMigrationCodecPropertyNamespaceRejectsMalformed(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		state := codecPropertyNamespaceState(t)
		mutations := []struct {
			name   string
			mutate func(*MigrationNamespaceConfiguration)
		}{
			{"zero index", func(s *MigrationNamespaceConfiguration) { s.Index = 0 }},
			{"unsupported version", func(s *MigrationNamespaceConfiguration) { s.Version++ }},
			{"missing activation", func(s *MigrationNamespaceConfiguration) { s.ActivatedAt = 0 }},
			{"future activation", func(s *MigrationNamespaceConfiguration) { s.ActivatedAt = s.Index + 1 }},
			{"missing members", func(s *MigrationNamespaceConfiguration) { s.Members = nil }},
			{"zero member", func(s *MigrationNamespaceConfiguration) { s.Members[0] = 0 }},
			{"duplicate member", func(s *MigrationNamespaceConfiguration) { s.Members[1] = s.Members[0] }},
			{"unsorted members", func(s *MigrationNamespaceConfiguration) { slices.Reverse(s.Members) }},
			{"joining overlaps", func(s *MigrationNamespaceConfiguration) { s.Joining = []uint64{s.Members[0]} }},
			{"duplicate joining", func(s *MigrationNamespaceConfiguration) { s.Joining = []uint64{1, 1} }},
		}
		for _, mutation := range mutations {
			bad := state
			bad.Members, bad.Joining = slices.Clone(state.Members), slices.Clone(state.Joining)
			mutation.mutate(&bad)
			_, err := EncodeMigrationNamespaceConfiguration(bad)
			require.Error(t, err, mutation.name)
			payload, err := json.Marshal(bad)
			require.NoError(t, err)
			_, err = DecodeMigrationNamespaceConfiguration(append(bytes.Clone(migrationNamespaceConfigurationMagic), payload...))
			require.Error(t, err, mutation.name)
		}
		encoded, err := EncodeMigrationNamespaceConfiguration(state)
		require.NoError(t, err)
		for _, bad := range [][]byte{make([]byte, 8), encoded[:len(encoded)-1], append([]byte{0}, encoded[1:]...), append(bytes.Clone(encoded), '{')} {
			_, err = DecodeMigrationConfigurationIndex(bad)
			require.Error(t, err)
		}
		legacy := MigrationNamespaceConfiguration{Index: rapid.Uint64Range(1, ^uint64(0)).Draw(t, "legacy index")}
		encoded, err = EncodeMigrationNamespaceConfiguration(legacy)
		require.NoError(t, err)
		for _, bad := range [][]byte{encoded[:7], append(bytes.Clone(encoded), 0)} {
			_, err = DecodeMigrationConfigurationIndex(bad)
			require.Error(t, err, "legacy configuration requires exactly eight bytes")
		}
		legacy.ActivatedAt = 1
		_, err = EncodeMigrationNamespaceConfiguration(legacy)
		require.Error(t, err, "legacy configuration cannot carry activation state")
	})
}

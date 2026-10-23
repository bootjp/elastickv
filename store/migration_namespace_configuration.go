package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"slices"

	"github.com/cockroachdb/errors"
)

const MigrationNamespaceVersion uint32 = 1

// MigrationNamespaceConfiguration extends the existing exact peer-local MVCC
// configuration row only after explicit protocol activation. Keep this codec
// shared by FSM and partition storage; neither may assume an eight-byte row.
type MigrationNamespaceConfiguration struct {
	Index       uint64
	Version     uint32
	ActivatedAt uint64
	Members     []uint64
	Joining     []uint64
}

var migrationNamespaceConfigurationMagic = []byte("EKVNS1\x00")

func DecodeMigrationConfigurationIndex(value []byte) (uint64, error) {
	state, err := DecodeMigrationNamespaceConfiguration(value)
	return state.Index, err
}

func DecodeMigrationNamespaceConfiguration(value []byte) (MigrationNamespaceConfiguration, error) {
	if len(value) == 8 && binary.BigEndian.Uint64(value) != 0 {
		return MigrationNamespaceConfiguration{Index: binary.BigEndian.Uint64(value)}, nil
	}
	var state MigrationNamespaceConfiguration
	if !bytes.HasPrefix(value, migrationNamespaceConfigurationMagic) {
		return state, errors.New("invalid durable migration configuration index")
	}
	if err := json.Unmarshal(value[len(migrationNamespaceConfigurationMagic):], &state); err != nil {
		return state, errors.Wrap(err, "invalid durable migration configuration envelope")
	}
	if !validMigrationNamespaceConfiguration(state) {
		return state, errors.New("invalid durable migration namespace configuration")
	}
	return state, nil
}

func EncodeMigrationNamespaceConfiguration(state MigrationNamespaceConfiguration) ([]byte, error) {
	if state.Version == 0 {
		if state.Index == 0 || state.ActivatedAt != 0 || len(state.Joining) != 0 {
			return nil, errors.New("invalid legacy migration configuration")
		}
		return binary.BigEndian.AppendUint64(nil, state.Index), nil
	}
	if !validMigrationNamespaceConfiguration(state) {
		return nil, errors.New("invalid durable migration namespace configuration")
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return append(slices.Clone(migrationNamespaceConfigurationMagic), encoded...), nil
}

func validMigrationNamespaceConfiguration(state MigrationNamespaceConfiguration) bool {
	if state.Index == 0 || state.Version != MigrationNamespaceVersion || state.ActivatedAt == 0 || state.ActivatedAt > state.Index || !validMigrationNamespaceMemberIDs(state.Members) {
		return false
	}
	if len(state.Joining) != 0 && !validMigrationNamespaceMemberIDs(state.Joining) {
		return false
	}
	for _, id := range state.Joining {
		if slices.Contains(state.Members, id) {
			return false
		}
	}
	return true
}

func validMigrationNamespaceMemberIDs(ids []uint64) bool {
	return len(ids) > 0 && ids[0] != 0 && slices.IsSorted(ids) && len(slices.Compact(slices.Clone(ids))) == len(ids)
}

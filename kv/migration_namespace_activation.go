package kv

import (
	"context"
	"slices"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// CheckMigrationNamespacePrejoin runs BEFORE a new member receives snapshots
// or joins as a learner. Static installed support alone is insufficient after
// activation: MODE8 must also durably authorize this member at the current cfg.
// The ordered configuration hook repeats the inventory check against races.
func CheckMigrationNamespacePrejoin(ctx context.Context, st store.MVCCStore, configurationIndex, memberID uint64, installedVersion uint32) error {
	if st == nil {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	state, err := readNamespaceConfiguration(ctx, st)
	if err != nil {
		return err
	}
	if state.Version == 0 {
		return nil
	}
	if configurationIndex != state.Index || memberID == 0 || installedVersion < state.Version ||
		(!slices.Contains(state.Members, memberID) && !slices.Contains(state.Joining, memberID)) {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	return nil
}

// MigrationNamespaceInstalledVersion is deliberately independent of activation
// and of the Draft gate. Wrappers must forward the ordered configuration hooks.
func MigrationNamespaceInstalledVersion(st store.MVCCStore, fsm raftengine.StateMachine) uint32 {
	if st == nil || fsm == nil {
		return 0
	}
	if _, ok := fsm.(raftengine.NamespaceConfigurationAware); !ok {
		return 0
	}
	return MigrationNamespaceVersion
}

// MigrationNamespaceAdmissionVersion reports durable activation, not installed
// support. It is safe for public handlers to fail closed before proposal using
// this API; FSM user admission consults only its ordered durable counterpart.
func MigrationNamespaceAdmissionVersion(ctx context.Context, st store.MVCCStore) (uint32, error) {
	if st == nil {
		return 0, errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	state, err := readNamespaceConfiguration(ctx, st)
	return state.Version, err
}

// RequireMigrationNamespaceAdmission applies to every mutating migration RPC,
// including unversioned import/promote/retire/readiness/cleanup calls. Only the
// namespace activation control is exempt, avoiding a bootstrap dependency.
func RequireMigrationNamespaceAdmission(ctx context.Context, st store.MVCCStore) error {
	version, err := MigrationNamespaceAdmissionVersion(ctx, st)
	if err != nil {
		return err
	}
	if version != MigrationNamespaceVersion {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	return nil
}

// ActivateMigrationNamespace is an explicit internal control, never an
// automatic response to installed support or a watcher. The controller gathers
// Members from all current apply-members and Joining from prospective members
// before this call. The FSM rechecks the exact configuration and inventory.
func ActivateMigrationNamespace(ctx context.Context, st store.MVCCStore, proposer raftengine.Proposer, command NamespaceAdmissionCommand) error {
	if st == nil || proposer == nil {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	index, err := MigrationConfigurationIndex(ctx, st)
	if err != nil {
		return err
	}
	if index != command.ConfigurationIndex {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	data, err := MarshalNamespaceAdmissionCommand(command)
	if err != nil {
		return err
	}
	result, err := proposer.ProposeAdmin(ctx, data)
	if err != nil {
		return errors.WithStack(err)
	}
	if result == nil {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	if err, ok := result.Response.(error); ok {
		return errors.WithStack(err)
	}
	if err := RequireMigrationNamespaceAdmission(ctx, st); err != nil {
		return err
	}
	index, err = MigrationConfigurationIndex(ctx, st)
	if err != nil {
		return err
	}
	if index != command.ConfigurationIndex {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	return nil
}

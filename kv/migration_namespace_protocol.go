package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// MigrationNamespaceVersion is installed support, not permission to change
// committed-entry admission. Only the configuration-bound control activates it.
const MigrationNamespaceVersion = store.MigrationNamespaceVersion

var ErrMigrationNamespaceProtocol = errors.New("namespace admission protocol is not proven for this configuration")

// NamespaceMemberCapability is a trusted peer's acknowledgement of support for
// the protocol at ConfigurationIndex. Include learners, not just voters.
type NamespaceMemberCapability struct {
	MemberID           uint64
	Version            uint32
	ConfigurationIndex uint64
}

// NamespaceAdmissionCommand must only be proposed after probing every apply
// member. Joining members must acknowledge support BEFORE AddLearner/AddVoter,
// since catch-up already executes the activated log and installs its snapshots.
// Neither this control nor installed support opens the migration Draft gate.
type NamespaceAdmissionCommand struct {
	Version            uint32
	ConfigurationIndex uint64
	ApplyMembers       []uint64
	Members            []NamespaceMemberCapability
	Joining            []NamespaceMemberCapability
}

// Reuse the existing exact peer-local MVCC key. Its versions already survive
// native snapshots and are all stripped by external/rebased restore. Until
// activation it retains the legacy eight-byte value and snapshot compatibility.
type namespaceConfiguration = store.MigrationNamespaceConfiguration

func readNamespaceConfiguration(ctx context.Context, st store.MVCCStore) (namespaceConfiguration, error) {
	value, err := st.GetAt(ctx, migrationConfigurationIndexKey, ^uint64(0))
	if errors.Is(err, store.ErrKeyNotFound) {
		return namespaceConfiguration{}, nil
	}
	if err != nil {
		return namespaceConfiguration{}, errors.WithStack(err)
	}
	state, err := store.DecodeMigrationNamespaceConfiguration(value)
	return state, errors.WithStack(errors.Mark(err, ErrMigrationNamespaceProtocol))
}

func validNamespaceMembers(ids []uint64) bool {
	return len(ids) > 0 && ids[0] != 0 && slices.IsSorted(ids) && len(slices.Compact(slices.Clone(ids))) == len(ids)
}

func validNamespaceJoining(ids, members []uint64) bool {
	if len(ids) == 0 {
		return true
	}
	if !validNamespaceMembers(ids) {
		return false
	}
	for _, id := range ids {
		if slices.Contains(members, id) {
			return false
		}
	}
	return true
}

func (f *kvFSM) persistNamespaceConfiguration(ctx context.Context, state namespaceConfiguration, applyIndex uint64) error {
	value, err := store.EncodeMigrationNamespaceConfiguration(state)
	if err != nil {
		return errors.WithStack(err)
	}
	ts := max(f.store.LastCommitTS(), 1)
	if err := f.store.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{{Op: store.OpTypePut, Key: migrationConfigurationIndexKey, Value: value}}, nil, ts, ts, applyIndex); err != nil {
		return errors.WithStack(err)
	}
	f.migrationConfigurationIndex.Store(state.Index)
	f.migrationNamespaceVersion.Store(state.Version)
	return nil
}

// RecordAppliedNamespaceConfiguration is called on the serial Raft apply loop,
// never by a local watcher. All current/joint voters and learners are required.
func (f *kvFSM) RecordAppliedNamespaceConfiguration(index uint64, members []uint64) error {
	if f.backupFloorLoadErr != nil {
		return errors.WithStack(f.backupFloorLoadErr)
	}
	members = slices.Clone(members)
	slices.Sort(members)
	if index == 0 || !validNamespaceMembers(members) {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	state, err := readNamespaceConfiguration(context.Background(), f.store)
	if err != nil || index < state.Index {
		return err
	}
	if state.Version != 0 {
		for _, id := range members {
			if !slices.Contains(state.Members, id) && !slices.Contains(state.Joining, id) {
				return errors.WithStack(ErrMigrationNamespaceProtocol)
			}
		}
		if index == state.Index && !slices.Equal(state.Members, members) {
			return errors.WithStack(ErrMigrationNamespaceProtocol)
		}
		if index != state.Index {
			state.Joining = nil // Prejoin acknowledgements cannot cross a ConfChange.
		}
	}
	if index == state.Index {
		// Cold-start replays ConfChanges below the store's data frontier. Only
		// recover the ordered inventory; never rewrite a smaller applied index.
		f.migrationNamespaceMembers = members
		return nil
	}
	state.Index, state.Members = index, members
	if err := f.persistNamespaceConfiguration(context.Background(), state, index); err != nil {
		return err
	}
	f.migrationNamespaceMembers = members
	return nil
}

// RecoverNamespaceConfiguration binds an inactive replica to the actual
// snapshot ConfState. A store ahead of that snapshot must replay its ConfChange
// before activation; never label the snapshot's data index a configuration.
func (f *kvFSM) RecoverNamespaceConfiguration(snapshotIndex uint64, members []uint64) error {
	state, err := readNamespaceConfiguration(context.Background(), f.store)
	if err != nil || state.Index == 0 || state.Index > snapshotIndex {
		return err
	}
	members = slices.Clone(members)
	slices.Sort(members)
	if !validNamespaceMembers(members) || (state.Version != 0 && !slices.Equal(state.Members, members)) {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	f.migrationNamespaceMembers = members
	return nil
}

func MarshalNamespaceAdmissionCommand(command NamespaceAdmissionCommand) ([]byte, error) {
	request, err := NewNamespaceAdmissionRequest(1, command)
	if err != nil {
		return nil, err
	}
	return MarshalMigrationCleanupCommand(request)
}

func NewNamespaceAdmissionRequest(jobID uint64, command NamespaceAdmissionCommand) (*pb.CleanupMigrationRequest, error) {
	if err := validateNamespaceAdmissionCommand(command); err != nil {
		return nil, err
	}
	data, err := json.Marshal(command)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &pb.CleanupMigrationRequest{
		JobId:                      jobID,
		Mode:                       pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_NAMESPACE_ADMISSION,
		ExpectedConfigurationIndex: command.ConfigurationIndex, RequireConfigurationIndex: true,
		NamespaceAdmission: data,
	}, nil
}

func ValidateNamespaceAdmissionRequest(request *pb.CleanupMigrationRequest) error {
	if request == nil || request.GetMode() != pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_NAMESPACE_ADMISSION || !request.GetRequireConfigurationIndex() {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	var command NamespaceAdmissionCommand
	if err := json.Unmarshal(request.GetNamespaceAdmission(), &command); err != nil {
		return errors.WithStack(err)
	}
	if command.ConfigurationIndex != request.GetExpectedConfigurationIndex() {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	return validateNamespaceAdmissionCommand(command)
}

func validateNamespaceAdmissionCommand(command NamespaceAdmissionCommand) error {
	if command.Version != MigrationNamespaceVersion || command.ConfigurationIndex == 0 || !validNamespaceMembers(command.ApplyMembers) {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	proved, err := namespaceCapabilityIDs(command.Members, command.ConfigurationIndex)
	if err != nil || !slices.Equal(proved, command.ApplyMembers) {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	joining, err := namespaceCapabilityIDs(command.Joining, command.ConfigurationIndex)
	if err != nil || !validNamespaceJoining(joining, command.ApplyMembers) {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	return nil
}

func (f *kvFSM) applyNamespaceAdmission(ctx context.Context, data []byte) any {
	var command NamespaceAdmissionCommand
	if err := json.Unmarshal(data, &command); err != nil {
		return haltErr(errors.Wrap(err, "decode namespace admission control"))
	}
	state, err := readNamespaceConfiguration(ctx, f.store)
	if err != nil {
		return haltErr(err)
	}
	members := f.migrationNamespaceMembers
	if state.Version != 0 {
		members = state.Members
	}
	if validateNamespaceAdmissionCommand(command) != nil || command.ConfigurationIndex != state.Index || !slices.Equal(command.ApplyMembers, members) || !validNamespaceMembers(members) {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	proved, err := namespaceCapabilityIDs(command.Members, command.ConfigurationIndex)
	if err != nil || !slices.Equal(proved, members) {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	joining, err := namespaceCapabilityIDs(command.Joining, command.ConfigurationIndex)
	if err != nil || !validNamespaceJoining(joining, members) {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	if f.pendingApplyIdx <= state.Index {
		return errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	// Concurrent controllers may refresh activation while membership admission
	// is between its prejoin control and ConfChange. Never revoke an already
	// committed permission inside the same configuration.
	joining = append(slices.Clone(state.Joining), joining...)
	slices.Sort(joining)
	state.Version, state.Members, state.Joining = command.Version, members, slices.Compact(joining)
	if state.ActivatedAt == 0 {
		state.ActivatedAt = state.Index
	}
	if err := f.persistNamespaceConfiguration(ctx, state, f.pendingApplyIdx); err != nil {
		return haltErr(errors.Wrap(err, "persist namespace admission control"))
	}
	return nil
}

func namespaceCapabilityIDs(capabilities []NamespaceMemberCapability, index uint64) ([]uint64, error) {
	ids := make([]uint64, 0, len(capabilities))
	for _, capability := range capabilities {
		if capability.Version < MigrationNamespaceVersion || capability.ConfigurationIndex != index {
			return nil, errors.WithStack(ErrMigrationNamespaceProtocol)
		}
		ids = append(ids, capability.MemberID)
	}
	slices.Sort(ids)
	if len(ids) > 0 && !validNamespaceMembers(ids) {
		return nil, errors.WithStack(ErrMigrationNamespaceProtocol)
	}
	return ids, nil
}

func (f *kvFSM) isMigrationOnlyControlKeyForApply(key []byte) bool {
	if f.migrationNamespaceVersion.Load() != 0 {
		return distribution.IsMigrationOnlyControlKey(key)
	}
	// Exact 2d1a36ac classifier. In particular !migstage| was an ordinary
	// transaction key, even though raw writes already refused that namespace.
	return bytes.HasPrefix(key, []byte("!dist|migstage|")) || bytes.HasPrefix(key, []byte("!migwrite|")) || bytes.HasPrefix(key, []byte("!migfence|"))
}

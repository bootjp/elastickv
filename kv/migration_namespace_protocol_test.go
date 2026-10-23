package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func namespaceCommand(index uint64, ids ...uint64) NamespaceAdmissionCommand {
	command := NamespaceAdmissionCommand{Version: MigrationNamespaceVersion, ConfigurationIndex: index, ApplyMembers: ids}
	for _, id := range ids {
		command.Members = append(command.Members, NamespaceMemberCapability{MemberID: id, Version: MigrationNamespaceVersion, ConfigurationIndex: index})
	}
	return command
}

func applyNamespaceCommand(t *testing.T, f *kvFSM, index uint64, command NamespaceAdmissionCommand) any {
	t.Helper()
	// Bypass the preproposal verifier to exercise the actual FSM validation.
	data, err := json.Marshal(command)
	require.NoError(t, err)
	encoded, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
		Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_NAMESPACE_ADMISSION, RequireConfigurationIndex: true,
		ExpectedConfigurationIndex: command.ConfigurationIndex, NamespaceAdmission: data})
	require.NoError(t, err)
	f.SetApplyIndex(index)
	return f.Apply(encoded)
}

func TestMigrationNamespaceActivationRequiresEveryApplyMember(t *testing.T) {
	for _, kind := range []string{"missing-learner", "old-member", "wrong-member-config", "extra-member", "duplicate", "wrong-roster", "stale-config", "zero-version", "valid"} {
		t.Run(kind, func(t *testing.T) {
			st := store.NewMVCCStore()
			f := &kvFSM{store: st}
			require.NoError(t, f.RecordAppliedNamespaceConfiguration(5, []uint64{1, 2, 3}))
			before, err := st.GetAt(t.Context(), migrationConfigurationIndexKey, ^uint64(0))
			require.NoError(t, err)
			require.Len(t, before, 8, "inactive records keep the old snapshot layout")
			command := namespaceCommand(5, 1, 2, 3)
			switch kind {
			case "missing-learner":
				command.Members = command.Members[:2]
			case "old-member":
				command.Members[2].Version = 0
			case "wrong-member-config":
				command.Members[2].ConfigurationIndex = 4
			case "extra-member":
				command.Members = append(command.Members, NamespaceMemberCapability{4, 1, 5})
			case "duplicate":
				command.Members[2] = command.Members[1]
			case "wrong-roster":
				command = namespaceCommand(5, 1, 2)
			case "stale-config":
				require.NoError(t, f.RecordAppliedNamespaceConfiguration(6, []uint64{1, 2, 3}))
			case "zero-version":
				command.Version = 0
			}
			response := applyNamespaceCommand(t, f, 7, command)
			if kind == "valid" {
				require.Nil(t, response)
				require.NoError(t, RequireMigrationNamespaceAdmission(t.Context(), st))
				return
			}
			err, ok := response.(error)
			require.True(t, ok)
			require.ErrorIs(t, err, ErrMigrationNamespaceProtocol)
			require.Zero(t, f.migrationNamespaceVersion.Load())
			require.ErrorIs(t, RequireMigrationNamespaceAdmission(t.Context(), st), ErrMigrationNamespaceProtocol)
		})
	}
}

func TestMigrationNamespacePrejoinAndConfigurationMonotonicity(t *testing.T) {
	st := store.NewMVCCStore()
	f := &kvFSM{store: st}
	require.NoError(t, f.RecordAppliedNamespaceConfiguration(5, []uint64{1, 2}))
	command := namespaceCommand(5, 1, 2)
	command.Joining = []NamespaceMemberCapability{{3, MigrationNamespaceVersion, 5}}
	require.Nil(t, applyNamespaceCommand(t, f, 6, command))
	require.NoError(t, f.RecordAppliedNamespaceConfiguration(7, []uint64{1, 2, 3}))
	require.NoError(t, RequireMigrationNamespaceAdmission(t.Context(), st))
	require.ErrorIs(t, f.RecordAppliedNamespaceConfiguration(8, []uint64{1, 2, 3, 4}), ErrMigrationNamespaceProtocol)
	require.Equal(t, uint64(7), f.AppliedConfigurationIndex())
	require.ErrorIs(t, f.RecordAppliedConfiguration(8), ErrMigrationNamespaceProtocol)
	// Removing a member never disables admission, and invalidates old prejoins.
	require.NoError(t, f.RecordAppliedNamespaceConfiguration(8, []uint64{1, 2}))
	require.ErrorIs(t, f.RecordAppliedNamespaceConfiguration(9, []uint64{1, 2, 3}), ErrMigrationNamespaceProtocol)
	require.NoError(t, RequireMigrationNamespaceAdmission(t.Context(), st))
	require.NoError(t, f.RecordAppliedNamespaceConfiguration(5, []uint64{99}), "old WAL configuration cannot regress state")
	require.Equal(t, uint64(8), f.AppliedConfigurationIndex())
}

func TestMigrationNamespaceActivationSnapshotRestartAndExternalRebase(t *testing.T) {
	for _, backend := range []string{"mvcc", "pebble"} {
		t.Run(backend, func(t *testing.T) {
			var source store.MVCCStore = store.NewMVCCStore()
			if backend == "pebble" {
				var err error
				source, err = store.NewPebbleStore(t.TempDir())
				require.NoError(t, err)
			}
			defer source.Close()
			f := &kvFSM{store: source}
			activateMigrationNamespace(t, f)
			snapshot, err := source.Snapshot()
			require.NoError(t, err)
			defer snapshot.Close()
			var raw bytes.Buffer
			_, err = snapshot.WriteTo(&raw)
			require.NoError(t, err)
			for _, external := range []bool{false, true} {
				dir := t.TempDir()
				target, err := store.NewPebbleStore(dir)
				require.NoError(t, err)
				if external {
					require.NoError(t, target.(store.ExternalSnapshotRestorer).RestoreExternal(t.Context(), bytes.NewReader(raw.Bytes())))
				} else {
					require.NoError(t, target.Restore(bytes.NewReader(raw.Bytes())))
				}
				require.NoError(t, target.Close())
				target, err = store.NewPebbleStore(dir)
				require.NoError(t, err)
				restored := NewKvFSMWithHLC(target, nil).(*kvFSM)
				if external {
					require.Zero(t, restored.migrationNamespaceVersion.Load())
					// The destination may reuse exactly the source configuration index.
					require.NoError(t, restored.RecordAppliedNamespaceConfiguration(1, []uint64{77}))
					require.ErrorIs(t, RequireMigrationNamespaceAdmission(t.Context(), target), ErrMigrationNamespaceProtocol)
				} else {
					require.Equal(t, MigrationNamespaceVersion, restored.migrationNamespaceVersion.Load())
					require.NoError(t, RequireMigrationNamespaceAdmission(t.Context(), target))
					require.ErrorIs(t, restored.RecordAppliedNamespaceConfiguration(3, []uint64{77}), ErrMigrationNamespaceProtocol)
				}
				require.NoError(t, target.Close())
			}
			// MVCC external restore strips all versions as well, not just Pebble.
			if backend != "mvcc" {
				return
			}
			target := store.NewMVCCStore()
			defer target.Close()
			require.NoError(t, target.(store.ExternalSnapshotRestorer).RestoreExternal(t.Context(), bytes.NewReader(raw.Bytes())))
			require.ErrorIs(t, RequireMigrationNamespaceAdmission(t.Context(), target), ErrMigrationNamespaceProtocol)
		})
	}
}

type namespaceFailStore struct {
	store.MVCCStore
	fail bool
}

func (s *namespaceFailStore) ApplyMutationsRaftAt(ctx context.Context, muts []*store.KVPairMutation, reads [][]byte, start, commit, index uint64) error {
	if s.fail {
		return errors.New("namespace durability failure")
	}
	return s.MVCCStore.ApplyMutationsRaftAt(ctx, muts, reads, start, commit, index)
}

func TestMigrationNamespaceDurabilityFailureHaltsWithoutActivation(t *testing.T) {
	st := &namespaceFailStore{MVCCStore: store.NewMVCCStore()}
	f := &kvFSM{store: st}
	require.NoError(t, f.RecordAppliedNamespaceConfiguration(5, []uint64{1}))
	st.fail = true
	response := applyNamespaceCommand(t, f, 6, namespaceCommand(5, 1))
	halted, ok := response.(interface{ HaltApply() error })
	require.True(t, ok)
	require.ErrorContains(t, halted.HaltApply(), "namespace durability failure")
	require.Zero(t, f.migrationNamespaceVersion.Load())
	require.ErrorIs(t, RequireMigrationNamespaceAdmission(t.Context(), st), ErrMigrationNamespaceProtocol)
}

type namespaceProposer struct {
	f     *kvFSM
	calls int
}

func (p *namespaceProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.ProposeAdmin(ctx, data)
}
func (p *namespaceProposer) ProposeAdmin(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	p.calls++
	p.f.SetApplyIndex(6)
	return &raftengine.ProposalResult{CommitIndex: 6, Response: p.f.Apply(data)}, nil
}

func TestMigrationNamespaceActivationHelperRefusesIncompleteNegotiationBeforeProposal(t *testing.T) {
	st := store.NewMVCCStore()
	f := &kvFSM{store: st}
	require.NoError(t, f.RecordAppliedNamespaceConfiguration(5, []uint64{1, 2}))
	p := &namespaceProposer{f: f}
	command := namespaceCommand(5, 1, 2)
	command.Members = command.Members[:1]
	require.ErrorIs(t, ActivateMigrationNamespace(t.Context(), st, p, command), ErrMigrationNamespaceProtocol)
	require.Zero(t, p.calls)
	require.NoError(t, ActivateMigrationNamespace(t.Context(), st, p, namespaceCommand(5, 1, 2)))
	require.Equal(t, 1, p.calls)
	require.Equal(t, MigrationNamespaceVersion, MigrationNamespaceInstalledVersion(st, f))
}

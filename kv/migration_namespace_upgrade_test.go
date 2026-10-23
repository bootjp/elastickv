package kv

import (
	"bytes"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func activateMigrationNamespace(t *testing.T, f *kvFSM) {
	t.Helper()
	require.NoError(t, f.RecordAppliedNamespaceConfiguration(1, []uint64{1}))
	command, err := MarshalNamespaceAdmissionCommand(NamespaceAdmissionCommand{
		Version: MigrationNamespaceVersion, ConfigurationIndex: 1,
		ApplyMembers: []uint64{1},
		Members:      []NamespaceMemberCapability{{MemberID: 1, Version: MigrationNamespaceVersion, ConfigurationIndex: 1}},
	})
	require.NoError(t, err)
	f.SetApplyIndex(2)
	require.Nil(t, f.Apply(command))
}

func TestMigrationNamespaceOrderedBatchIgnoresDifferentWatchers(t *testing.T) {
	for _, rejectedPosition := range []int{0, 1, 2} {
		var snapshots [][]byte
		for _, watcher := range []RouteHistory{nil, forbiddenApplyWatcher{}} {
			st := store.NewMVCCStore()
			f := &kvFSM{store: st, routes: watcher, shardGroupID: 1}
			activateMigrationNamespace(t, f)
			deterministicMigrationOwner(t, st)
			requests := []*pb.Request{deterministicRawRequest(10, "n"), deterministicRawRequest(10, "p"), migrationNamespaceRequest(pb.Phase_NONE, []byte("!migstage|ready|customer"))}
			requests[rejectedPosition], requests[2] = requests[2], requests[rejectedPosition]
			encoded, err := proto.Marshal(&pb.RaftCommand{Requests: requests})
			require.NoError(t, err)
			f.SetApplyIndex(20)
			response, ok := f.Apply(append([]byte{raftEncodeBatch}, encoded...)).(*fsmApplyResponse)
			require.True(t, ok)
			for i, verdict := range response.results {
				if i == rejectedPosition {
					require.ErrorIs(t, verdict, ErrInvalidRequest)
				} else {
					require.NoError(t, verdict)
				}
			}
			snapshot, err := st.Snapshot()
			require.NoError(t, err)
			var raw bytes.Buffer
			_, err = snapshot.WriteTo(&raw)
			require.NoError(t, err)
			require.NoError(t, snapshot.Close())
			snapshots = append(snapshots, raw.Bytes())
			require.NoError(t, st.Close())
		}
		require.Equal(t, snapshots[0], snapshots[1], "watcher state cannot authorize namespace admission")
	}
}

func TestMigrationNamespaceSharedPrepareAndCommitValidatorsUseDurableVersion(t *testing.T) {
	for _, active := range []bool{false, true} {
		f := &kvFSM{store: store.NewMVCCStore()}
		if active {
			activateMigrationNamespace(t, f)
		}
		key := []byte("!migstage|ready|customer")
		_, err := f.prepareTxnMutation(t.Context(), &pb.Mutation{Key: key, Value: []byte("value")}, key, 10, 0, 20)
		lockErr := f.validatePreparedCommitLock(txnLock{StartTS: 10, CommitTS: 20, PrimaryKey: key}, key, key, 10, 20)
		_, resolutionErr := f.migrationResolutionMutations([]*pb.Mutation{{Key: key, Value: []byte("value")}})
		if active {
			require.ErrorIs(t, err, ErrInvalidRequest)
			require.ErrorIs(t, lockErr, ErrInvalidRequest)
			require.ErrorIs(t, resolutionErr, ErrInvalidRequest)
		} else {
			require.NoError(t, err)
			require.NoError(t, lockErr)
			require.NoError(t, resolutionErr)
		}
	}
}

// These entries were admitted by 2d1a36ac. A newly upgraded apply-member must
// produce the same writes, including resolution of pre-upgrade intents.
func TestMigrationNamespaceUpgradeLegacyHistory(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE, pb.Phase_COMMIT, pb.Phase_ABORT} {
		t.Run(phase.String(), func(t *testing.T) {
			st := store.NewMVCCStore()
			f := &kvFSM{store: st, shardGroupID: 1}
			key := []byte("!migstage|ready|customer")
			if phase == pb.Phase_COMMIT || phase == pb.Phase_ABORT {
				seedLegacyMigrationIntent(t, st, key)
			}
			request := migrationNamespaceRequest(phase, key)
			require.ErrorIs(t, CheckMigrationRequest(t.Context(), st, 1, request), ErrInvalidRequest)
			require.NoError(t, applyFSMRequest(t, f, request), "committed legacy entry must retain old apply semantics")
			if phase == pb.Phase_NONE || phase == pb.Phase_COMMIT {
				_, err := st.GetAt(t.Context(), key, 20)
				require.NoError(t, err)
			}
		})
	}
}

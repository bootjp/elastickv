package kv

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type secondOnlineReadFailure struct {
	store.MVCCStore
	store.MigrationTargetReadinessReader
	store.MigrationOwnershipStore
	store.RaftControlMetadataStore
	store.RaftEntryApplier
	reads   int
	failure error
}

func TestOnlineCaptureRequestLimitPreservesCommittedBatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   int
		outside bool
	}{
		{"at limit", store.MaxMigrationCaptureRequests, false},
		{"over limit", store.MaxMigrationCaptureRequests + 1, false},
		{"over limit with unrelated requests", store.MaxMigrationCaptureRequests + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := g17OpenStore(t, t.TempDir())
			f := g17FSM(t, st, WithRouteHistory(nil, 1))
			installSourceOwnershipForTest(t, f, 0)
			session := onlineRegressionSession()
			session.Identity.JobID, session.Identity.Index = 11, 12
			session.StartIndex, session.LastIndex = 12, 12
			applier, ok := st.(store.RaftEntryApplier)
			require.True(t, ok)
			require.NoError(t, applier.ApplyRaftEntry(t.Context(), 12, func(ctx context.Context) error {
				return store.StartOnlineMigrationSession(ctx, st, session)
			}))
			requests := make([]*pb.Request, tc.count)
			for i := range requests {
				prefix := "n"
				if tc.outside && i != 0 {
					prefix = "a"
				}
				requests[i] = &pb.Request{Ts: uint64(20 + i), ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(fmt.Sprintf("%s%03d", prefix, i)), Value: []byte("v")}}}
			}
			command, err := proto.Marshal(&pb.RaftCommand{Requests: requests})
			require.NoError(t, err)
			f.SetApplyIndex(13)
			response := f.Apply(command)
			require.NoError(t, haltApplyOf(response))
			require.Nil(t, response)
			index, _ := g17AppliedIndex(t, st)
			require.Equal(t, uint64(13), index)
			session, err = store.ReadOnlineMigrationSession(t.Context(), st, 11)
			require.NoError(t, err)
			if tc.count > store.MaxMigrationCaptureRequests {
				require.NotEmpty(t, session.Invalid)
				require.Zero(t, session.Sequence)
			} else {
				require.Empty(t, session.Invalid)
				require.Equal(t, uint64(1), session.Sequence)
			}
			for _, request := range requests {
				value, err := st.GetAt(t.Context(), request.Mutations[0].Key, 1000)
				require.NoError(t, err)
				require.Equal(t, []byte("v"), value)
			}
		})
	}
}

func (s *secondOnlineReadFailure) ReadRaftControlMetadata(ctx context.Context, key []byte) ([]byte, error) {
	if bytes.HasPrefix(key, []byte("R/online-split/v1/source/")) {
		s.reads++
		if s.reads == 2 {
			return nil, s.failure
		}
	}
	return s.RaftControlMetadataStore.ReadRaftControlMetadata(ctx, key)
}
func TestOnlineSecondReadFailureHaltsUnrelatedWrite(t *testing.T) {
	f, st := onlineRegressionFSM(t, true)
	readiness, ok := st.(store.MigrationTargetReadinessReader)
	require.True(t, ok)
	serving, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	failure := errors.New("second source manifest read failed")
	wrapper := &secondOnlineReadFailure{MVCCStore: st, MigrationTargetReadinessReader: readiness, MigrationOwnershipStore: serving, RaftControlMetadataStore: metadata, RaftEntryApplier: applier, failure: failure}
	f.store = wrapper
	request := &pb.Request{Ts: 20, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("a"), Value: []byte("v")}}}
	command, err := proto.Marshal(request)
	require.NoError(t, err)
	f.SetApplyIndex(12)
	response := f.Apply(command)
	require.ErrorIs(t, haltApplyOf(response), failure)
	index, _ := g17AppliedIndex(t, st)
	require.Equal(t, uint64(11), index)
}
func TestOnlineMixedRangeBatchPreservesCapture(t *testing.T) {
	st := g17OpenStore(t, t.TempDir())
	f := g17FSM(t, st, WithRouteHistory(nil, 1))
	installSourceOwnershipForTest(t, f, 0)
	sessionStart := onlineRegressionSession()
	sessionStart.Identity.JobID = 11
	sessionStart.Identity.Index = 12
	sessionStart.StartIndex = 12
	sessionStart.LastIndex = 12
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 12, func(ctx context.Context) error { return store.StartOnlineMigrationSession(ctx, st, sessionStart) }))
	hash := store.HashFieldKey([]byte("b"), []byte("field"))
	requests := []*pb.Request{
		{Ts: 20, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("point")}}},
		{Ts: 21, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: hash, Value: []byte("hash")}}},
	}
	command, err := proto.Marshal(&pb.RaftCommand{Requests: requests})
	require.NoError(t, err)
	f.SetApplyIndex(13)
	response := f.Apply(command)
	require.NoError(t, haltApplyOf(response))
	require.Nil(t, response)
	session, err := store.ReadOnlineMigrationSession(t.Context(), st, 11)
	require.NoError(t, err)
	require.Empty(t, session.Invalid)
	require.Equal(t, uint64(1), session.Sequence)
	for _, req := range requests {
		v, e := st.GetAt(t.Context(), req.Mutations[0].Key, 30)
		require.NoError(t, e)
		require.Equal(t, req.Mutations[0].Value, v)
	}
	frame, err := store.ReadOnlineMigrationFrame(t.Context(), st, session, 1)
	require.NoError(t, err)
	require.Len(t, frame.Effects, 1)
	require.Equal(t, []byte("n"), frame.Effects[0].Key)
}

package kv

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type homeBindingLeader struct {
	reads             int
	follower          bool
	lostRead          bool
	term              uint64
	advanceTermOnRead bool
}

func (l *homeBindingLeader) State() raftengine.State {
	if l.follower {
		return raftengine.StateFollower
	}
	return raftengine.StateLeader
}

func (l *homeBindingLeader) Leader() raftengine.LeaderInfo { return raftengine.LeaderInfo{ID: "self"} }
func (l *homeBindingLeader) Status() raftengine.Status {
	term := l.term
	if term == 0 {
		term = 1
	}
	return raftengine.Status{State: l.State(), Term: term}
}
func (l *homeBindingLeader) VerifyLeader(context.Context) error {
	return nil
}
func (l *homeBindingLeader) LinearizableRead(context.Context) (uint64, error) {
	l.reads++
	if l.advanceTermOnRead {
		l.term++
	}
	if l.lostRead {
		l.follower = true
	}
	return uint64(l.reads), nil
}

func TestPrepareHomeRawTimestampBindingReusesExactOperation(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	leader := &homeBindingLeader{}
	request := &pb.Request{TimestampBindingOperationId: append([]byte{1}, make([]byte, 15)...),
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("value")}}}
	issues := 0
	issue := func(context.Context) (uint64, error) {
		issues++
		require.Equal(t, 1, leader.reads, "absence must be checked after an applied read")
		return 42, nil
	}
	first, err := PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, request)
	require.NoError(t, err)
	require.Equal(t, uint64(42), first.Ts)
	require.Zero(t, request.Ts, "caller request must remain unmodified")
	binding, err := timestampBindingForCreationRequest(first)
	require.NoError(t, err)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
		_, err := StageTimestampBinding(ctx, metadata, binding)
		return err
	}))
	retry, err := PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, request)
	require.NoError(t, err)
	require.Equal(t, first, retry)
	leader.term = 2
	retry, err = PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, request)
	require.NoError(t, err)
	require.Equal(t, first.Ts, retry.Ts)
	require.Equal(t, uint64(2), retry.TimestampBindingAdmissionTerm)
	retryBinding, err := timestampBindingForCreationRequest(retry)
	require.NoError(t, err)
	require.Equal(t, binding, retryBinding, "admission term must not change the durable manifest")
	require.Equal(t, 1, issues, "exact retry must not ask group 0 for another timestamp")
	require.Equal(t, 3, leader.reads)
	changed := &pb.Request{TimestampBindingOperationId: request.TimestampBindingOperationId,
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("different")}}}
	_, err = PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, changed)
	require.ErrorIs(t, err, ErrTimestampBindingConflict)
	require.Equal(t, 1, issues)
}

func TestPrepareHomeRawTimestampBindingRejectsUnprovenCreation(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	leader := &homeBindingLeader{}
	request := &pb.Request{Ts: 42, TimestampBindingOperationId: append([]byte{1}, make([]byte, 15)...)}
	issues := 0
	issue := func(context.Context) (uint64, error) { issues++; return 43, nil }
	_, err := PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, request)
	require.ErrorIs(t, err, ErrTimestampBindingInvalid)
	require.Zero(t, leader.reads)
	require.Zero(t, issues)
	request.Ts = 0
	leader.lostRead = true
	_, err = PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, request)
	require.Error(t, err)
	require.Zero(t, issues, "a lost home leader cannot mint from stale absence")
}

func TestPrepareHomeRawTimestampBindingConcurrentRetryStaysPending(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	request := &pb.Request{TimestampBindingOperationId: append([]byte{7}, make([]byte, 15)...),
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("value")}}}
	clearHomeBindingAttemptAfterTest(t, request)
	entered := make(chan struct{})
	release := make(chan struct{})
	var issues atomic.Int32
	issue := func(context.Context) (uint64, error) {
		issues.Add(1)
		close(entered)
		<-release
		return 42, nil
	}
	firstResult := make(chan error, 1)
	go func() {
		_, err := PrepareHomeRawTimestampBinding(t.Context(), &homeBindingLeader{}, metadata, issue, request)
		firstResult <- err
	}()
	<-entered
	secondResult := make(chan error, 1)
	go func() {
		_, err := PrepareHomeRawTimestampBinding(t.Context(), &homeBindingLeader{}, metadata, issue, request)
		secondResult <- err
	}()
	close(release)
	require.NoError(t, <-firstResult)
	require.ErrorIs(t, <-secondResult, ErrTimestampBindingPending)
	require.EqualValues(t, 1, issues.Load())
}

func TestPrepareHomeRawTimestampBindingAmbiguousIssueFailsClosed(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	request := &pb.Request{TimestampBindingOperationId: append([]byte{8}, make([]byte, 15)...),
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("value")}}}
	clearHomeBindingAttemptAfterTest(t, request)
	var issues int
	ambiguous := errors.New("unknown issue result")
	issue := func(context.Context) (uint64, error) { issues++; return 0, ambiguous }
	_, err := PrepareHomeRawTimestampBinding(t.Context(), &homeBindingLeader{}, metadata, issue, request)
	require.ErrorIs(t, err, ambiguous)
	_, err = PrepareHomeRawTimestampBinding(t.Context(), &homeBindingLeader{}, metadata, issue, request)
	require.ErrorIs(t, err, ErrTimestampBindingPending)
	require.Equal(t, 1, issues)
}

func clearHomeBindingAttemptAfterTest(t *testing.T, request *pb.Request) {
	t.Helper()
	var operationID [16]byte
	copy(operationID[:], request.TimestampBindingOperationId)
	t.Cleanup(func() { homeBindingAttempts.Delete(operationID) })
}

func TestPrepareHomeRawTimestampBindingAmbiguousProposalResolvesOnlyFromAppliedBinding(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	request := &pb.Request{TimestampBindingOperationId: append([]byte{9}, make([]byte, 15)...),
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("value")}}}
	issues := 0
	issue := func(context.Context) (uint64, error) { issues++; return 42, nil }
	first, err := PrepareHomeRawTimestampBinding(t.Context(), &homeBindingLeader{}, metadata, issue, request)
	require.NoError(t, err)
	_, err = PrepareHomeRawTimestampBinding(t.Context(), &homeBindingLeader{}, metadata, issue, request)
	require.ErrorIs(t, err, ErrTimestampBindingPending)
	require.Equal(t, 1, issues)
	binding, err := timestampBindingForCreationRequest(first)
	require.NoError(t, err)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 1, func(ctx context.Context) error {
		_, err := StageTimestampBinding(ctx, metadata, binding)
		return err
	}))
	retry, err := PrepareHomeRawTimestampBinding(t.Context(), &homeBindingLeader{}, metadata, issue, request)
	require.NoError(t, err)
	require.Equal(t, first, retry)
	require.Equal(t, 1, issues)
}

func TestPrepareHomeRawTimestampBindingReissuesAfterNewTermBarrier(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	request := &pb.Request{TimestampBindingOperationId: append([]byte{10}, make([]byte, 15)...),
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("value")}}}
	clearHomeBindingAttemptAfterTest(t, request)
	leader := &homeBindingLeader{term: 1}
	issues := 0
	issue := func(context.Context) (uint64, error) {
		issues++
		return uint64(40 + issues), nil
	}
	first, err := PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, request)
	require.NoError(t, err)
	require.Equal(t, uint64(41), first.Ts)
	_, err = PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, request)
	require.ErrorIs(t, err, ErrTimestampBindingPending)
	require.Equal(t, 1, issues)

	leader.term = 2
	second, err := PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, request)
	require.NoError(t, err)
	require.Equal(t, uint64(42), second.Ts)
	require.Equal(t, uint64(1), first.TimestampBindingAdmissionTerm)
	require.Equal(t, uint64(2), second.TimestampBindingAdmissionTerm)
	require.Equal(t, 2, issues)
	_, err = PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, issue, request)
	require.ErrorIs(t, err, ErrTimestampBindingPending)
	require.Equal(t, 2, issues)
}

func TestPrepareHomeRawTimestampBindingRejectsTermChangeDuringRead(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	leader := &homeBindingLeader{term: 1, advanceTermOnRead: true}
	request := &pb.Request{TimestampBindingOperationId: append([]byte{11}, make([]byte, 15)...),
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("value")}}}
	clearHomeBindingAttemptAfterTest(t, request)
	issues := 0
	_, err := PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, func(context.Context) (uint64, error) {
		issues++
		return 42, nil
	}, request)
	require.ErrorIs(t, err, ErrLeaderNotFound)
	require.Zero(t, issues)
}

func TestPrepareHomeRawTimestampBindingRequiresTermStatus(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	request := &pb.Request{TimestampBindingOperationId: append([]byte{12}, make([]byte, 15)...),
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("value")}}}
	leader := struct{ raftengine.LeaderView }{LeaderView: &homeBindingLeader{term: 1}}
	issues := 0
	_, err := PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, func(context.Context) (uint64, error) {
		issues++
		return 42, nil
	}, request)
	require.ErrorIs(t, err, ErrTimestampBindingInvalid)
	require.Zero(t, issues)
}

func TestPrepareHomeRawTimestampBindingRejectsIssueAcrossTermChange(t *testing.T) {
	st := store.NewMVCCStore()
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	request := &pb.Request{TimestampBindingOperationId: append([]byte{13}, make([]byte, 15)...),
		Mutations: []*pb.Mutation{{Key: []byte("key"), Value: []byte("value")}}}
	clearHomeBindingAttemptAfterTest(t, request)
	leader := &homeBindingLeader{term: 1}
	_, err := PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, func(context.Context) (uint64, error) {
		leader.term = 2
		return 42, nil
	}, request)
	require.ErrorIs(t, err, ErrTimestampBindingInvalid)
	second, err := PrepareHomeRawTimestampBinding(t.Context(), leader, metadata, func(context.Context) (uint64, error) {
		return 43, nil
	}, request)
	require.NoError(t, err)
	require.Equal(t, uint64(43), second.Ts)
	require.Equal(t, uint64(2), second.TimestampBindingAdmissionTerm)
}

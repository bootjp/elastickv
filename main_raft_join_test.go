package main

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

type raftJoinReadyEngineStub struct {
	raftengine.Engine
	configuration raftengine.Configuration
	status        raftengine.Status
	err           error
}

func (e *raftJoinReadyEngineStub) Configuration(context.Context) (raftengine.Configuration, error) {
	return e.configuration, e.err
}

func (e *raftJoinReadyEngineStub) Status() raftengine.Status {
	return e.status
}

func TestRaftJoinRuntimesReady(t *testing.T) {
	readyStatus := raftengine.Status{
		Leader:       raftengine.LeaderInfo{ID: "n1"},
		CommitIndex:  42,
		AppliedIndex: 42,
	}
	readyConfig := raftengine.Configuration{Servers: []raftengine.Server{
		{ID: "n1", Suffrage: "voter"},
		{ID: "n2", Suffrage: "learner"},
	}}

	tests := []struct {
		name   string
		engine *raftJoinReadyEngineStub
		ready  bool
	}{
		{name: "member caught up", engine: &raftJoinReadyEngineStub{configuration: readyConfig, status: readyStatus}, ready: true},
		{name: "membership missing", engine: &raftJoinReadyEngineStub{status: readyStatus}},
		{name: "leader unknown", engine: &raftJoinReadyEngineStub{configuration: readyConfig}},
		{name: "apply lag", engine: &raftJoinReadyEngineStub{
			configuration: readyConfig,
			status: raftengine.Status{
				Leader:       raftengine.LeaderInfo{ID: "n1"},
				CommitIndex:  42,
				AppliedIndex: 41,
			},
		}},
		{name: "conf change pending", engine: &raftJoinReadyEngineStub{
			configuration: readyConfig,
			status: func() raftengine.Status {
				status := readyStatus
				status.PendingConfChange = true
				return status
			}(),
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready, err := raftJoinRuntimesReady(context.Background(), []*raftGroupRuntime{{
				spec:   groupSpec{id: 1},
				engine: tc.engine,
			}}, "n2")
			require.NoError(t, err)
			require.Equal(t, tc.ready, ready)
		})
	}
}

func TestRaftJoinRuntimesReadyReturnsConfigurationError(t *testing.T) {
	readErr := errors.New("configuration unavailable")
	ready, err := raftJoinRuntimesReady(context.Background(), []*raftGroupRuntime{{
		spec:   groupSpec{id: 7},
		engine: &raftJoinReadyEngineStub{err: readErr},
	}}, "n2")
	require.False(t, ready)
	require.ErrorIs(t, err, readErr)
	require.Contains(t, err.Error(), "raft group 7")
}

func TestWaitForRaftJoinReadyDisabled(t *testing.T) {
	require.NoError(t, waitForRaftJoinReady(context.Background(), nil, "n2", false))
}

func TestPreparePublicServicesAfterRaftJoinReadyDoesNotBindWhileJoinIsUnready(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prepared := false

	err := preparePublicServicesAfterRaftJoinReady(
		ctx,
		[]*raftGroupRuntime{{
			spec: groupSpec{id: 1},
			engine: &raftJoinReadyEngineStub{
				status: raftengine.Status{Leader: raftengine.LeaderInfo{ID: "n1"}},
			},
		}},
		"n2",
		true,
		func() error {
			prepared = true
			return nil
		},
	)

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, prepared)
}

func TestPreparePublicServicesAfterRaftJoinReadyBindsWhenJoinIsDisabled(t *testing.T) {
	prepared := false
	err := preparePublicServicesAfterRaftJoinReady(
		context.Background(),
		nil,
		"n2",
		false,
		func() error {
			prepared = true
			return nil
		},
	)
	require.NoError(t, err)
	require.True(t, prepared)
}

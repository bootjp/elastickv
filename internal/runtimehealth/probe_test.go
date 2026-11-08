package runtimehealth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

type testEngine struct {
	state         raftengine.ReadinessState
	status        raftengine.Status
	configuration raftengine.Configuration
	err           error
	observed      func() monoclock.Instant
	configure     func(context.Context) (raftengine.Configuration, error)
}

func (e *testEngine) ReadinessState() raftengine.ReadinessState { return e.state }
func (e *testEngine) Status() raftengine.Status                 { return e.status }
func (e *testEngine) ReadinessObservedAt() monoclock.Instant {
	if e.observed != nil {
		return e.observed()
	}
	return monoclock.Now()
}

func (e *testEngine) Configuration(ctx context.Context) (raftengine.Configuration, error) {
	if e.configure != nil {
		return e.configure(ctx)
	}
	return e.configuration, e.err
}

func fixture() (*Probe, *testEngine) {
	e := &testEngine{state: raftengine.ReadinessState{Started: true}, status: raftengine.Status{
		State: raftengine.StateFollower, Leader: raftengine.LeaderInfo{ID: "n2"}, CommitIndex: 10, AppliedIndex: 10, LastSnapshotIndex: 5, LastContact: time.Second},
		configuration: raftengine.Configuration{Servers: []raftengine.Server{{ID: "n1", Suffrage: "voter"}, {ID: "n2", Suffrage: "voter"}}}}
	p := New("n1")
	p.SetSources([]Source{{GroupID: 1, Engine: func() Engine { return e }}})
	p.MarkServing()
	return p, e
}

func TestReadinessRejectsUnusableMembers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, reason string
		change       func(*testEngine)
	}{
		{"startup", "starting", func(e *testEngine) { e.state.Started = false }},
		{"restore", "snapshot_restoring", func(e *testEngine) { e.state.Restoring = true }},
		{"restore failed", "engine_failed", func(e *testEngine) { e.state.Failed = true }},
		{"stopped", "engine_stopped", func(e *testEngine) { e.state.Stopped = true }},
		{"removed", "not_a_voter", func(e *testEngine) { e.configuration.Servers = e.configuration.Servers[1:] }},
		{"learner", "not_a_voter", func(e *testEngine) { e.configuration.Servers[0].Suffrage = "learner" }},
		{"unknown leader", "leader_unknown", func(e *testEngine) { e.status.Leader.ID = "unknown" }},
		{"no leader", "leader_unknown", func(e *testEngine) { e.status.Leader.ID = "" }},
		{"unknown contact", "leader_contact_stale", func(e *testEngine) { e.status.LastContact = -1 }},
		{"stale contact", "leader_contact_stale", func(e *testEngine) { e.status.LastContact = DefaultContactTimeout + time.Second }},
		{"election", "not_participating", func(e *testEngine) { e.status.State = raftengine.StateCandidate }},
		{"configuration change", "membership_changing", func(e *testEngine) { e.status.PendingConfChange = true }},
		{"unapplied snapshot", "catching_up", func(e *testEngine) { e.status.LastSnapshotIndex = 11 }},
		{"startup backlog", "catching_up", func(e *testEngine) { e.status.CommitIndex = 20 }},
		{"read failure", "configuration_unavailable", func(e *testEngine) { e.err = errors.New("private restore file and secret") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, e := fixture()
			tc.change(e)
			require.Equal(t, Result{Reason: tc.reason}, p.Evaluate(t.Context()))
		})
	}
}

func TestIdleMembersAndProgressingBacklogsRemainReady(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	now := time.Now()
	p.now = func() time.Time { return now }
	require.True(t, p.Evaluate(t.Context()).Ready)
	now = now.Add(time.Hour)
	require.True(t, p.Evaluate(t.Context()).Ready, "idle does not mean stalled")
	e.status.CommitIndex = 20
	e.status.FSMPending = 10
	require.True(t, p.Evaluate(t.Context()).Ready)
	now = now.Add(DefaultStallTimeout - time.Second)
	require.True(t, p.Evaluate(t.Context()).Ready)
	now = now.Add(time.Second)
	require.Equal(t, "apply_stalled", p.Evaluate(t.Context()).Reason)
	e.status.AppliedIndex = 15
	require.True(t, p.Evaluate(t.Context()).Ready, "making progress with a backlog is usable")
	e.status.AppliedIndex = 20
	e.status.FSMPending = 0
	now = now.Add(time.Hour)
	require.True(t, p.Evaluate(t.Context()).Ready)
}

func TestStartupMustCrossItsCommittedWatermark(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	e.status.CommitIndex = 20
	require.False(t, p.Evaluate(t.Context()).Ready)
	e.status.AppliedIndex = 20
	e.status.CommitIndex = 21
	require.True(t, p.Evaluate(t.Context()).Ready, "new commits cannot move the initial barrier forever")
}

func TestApplyRegressionRemainsNotReady(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	require.True(t, p.Evaluate(t.Context()).Ready)
	e.status.AppliedIndex = 9
	require.Equal(t, "apply_regressed", p.Evaluate(t.Context()).Reason)
	e.status.AppliedIndex = 10
	require.False(t, p.Evaluate(t.Context()).Ready)
}

func TestEveryGroupAndServingGateAreChecked(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	p.SetSources([]Source{{GroupID: 1, Engine: func() Engine { return e }}, {GroupID: 2, Engine: func() Engine { return nil }}})
	require.False(t, p.Evaluate(t.Context()).Ready)
	require.False(t, New("n1").Evaluate(t.Context()).Ready)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.False(t, p.Evaluate(ctx).Ready)
	e.status.State = raftengine.StateLeader
	e.status.Leader.ID = "n1"
	p.SetSources([]Source{{GroupID: 1, Engine: func() Engine { return e }}})
	require.True(t, p.Evaluate(t.Context()).Ready)
}

func TestHTTPSeparatesLivenessAndReadinessWithoutLeakingErrors(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	e.err = errors.New("secret restore filename")
	for _, tc := range []struct {
		method, path string
		code         int
	}{{"GET", "/livez", 200}, {"GET", "/readyz", 503}, {"HEAD", "/healthz", 503}, {"POST", "/readyz", 405}, {"GET", "/unknown", 404}} {
		r := httptest.NewRecorder()
		p.Handler().ServeHTTP(r, httptest.NewRequest(tc.method, tc.path, nil))
		require.Equal(t, tc.code, r.Code)
		require.Equal(t, "no-store", r.Header().Get("Cache-Control"))
		require.False(t, strings.Contains(r.Body.String(), "secret"))
		if tc.method == http.MethodHead {
			require.Empty(t, r.Body.String())
		}
	}
}

func TestFrozenIdleObservationIsNotReady(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	require.True(t, p.Evaluate(t.Context()).Ready)
	e.observed = func() monoclock.Instant { return monoclock.Now().Add(-DefaultStallTimeout - time.Second) }
	require.False(t, p.Evaluate(t.Context()).Ready, "cached idle status must not hide a blocked Raft loop")
}

func TestFollowerContactAgesSinceObservation(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	e.observed = func() monoclock.Instant { return monoclock.Now().Add(-DefaultContactTimeout - time.Second) }
	require.Equal(t, "leader_contact_stale", p.Evaluate(t.Context()).Reason)
}

func TestBacklogTimerBeginsWhenFirstObserved(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	now := time.Now()
	p.now = func() time.Time { return now }
	require.True(t, p.Evaluate(t.Context()).Ready)
	now = now.Add(time.Hour)
	e.status.CommitIndex = 20
	e.status.FSMPending = 10
	require.True(t, p.Evaluate(t.Context()).Ready, "a new backlog is not an hour-old stall")
	now = now.Add(DefaultStallTimeout)
	require.Equal(t, "apply_stalled", p.Evaluate(t.Context()).Reason)
}

func TestUnreadyGroupDoesNotHideOtherGroupsProgress(t *testing.T) {
	t.Parallel()
	p, a := fixture()
	_, b := fixture()
	a.state.Restoring = true
	p.SetSources([]Source{{GroupID: 1, Engine: func() Engine { return a }}, {GroupID: 2, Engine: func() Engine { return b }}})
	require.False(t, p.Evaluate(t.Context()).Ready)
	a.state.Restoring = false
	b.status.CommitIndex = 20
	b.status.AppliedIndex = 11
	require.True(t, p.Evaluate(t.Context()).Ready, "group B was already observed caught up before new commits")
}

func TestHTTPReadinessDoesNotWaitForBlockedObserver(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	entered, release := make(chan struct{}), make(chan struct{})
	e.configure = func(context.Context) (raftengine.Configuration, error) {
		close(entered)
		<-release
		return e.configuration, nil
	}
	observed := make(chan struct{})
	go func() { p.Evaluate(t.Context()); close(observed) }()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	finished := make(chan struct{})
	recorder := httptest.NewRecorder()
	go func() {
		p.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(ctx))
		close(finished)
	}()
	bounded := false
	select {
	case <-finished:
		bounded = true
	case <-time.After(time.Second):
	}
	close(release)
	<-observed
	<-finished
	require.True(t, bounded, "a blocked source must not queue readiness HTTP requests")
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

func TestCachedReadinessExpiresWithOneBlockedWorker(t *testing.T) {
	t.Parallel()
	p, e := fixture()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	e.configure = func(ctx context.Context) (raftengine.Configuration, error) {
		if calls.Add(1) > 1 {
			close(entered)
			<-release
		}
		return e.configuration, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	t.Cleanup(func() { cancel(); close(release); <-done })
	require.Eventually(t, func() bool { return p.Current(t.Context()).Ready }, time.Second, time.Millisecond)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("sampler did not enter blocked source")
	}
	// Age only the published result; the worker remains blocked in Configuration.
	old := p.latest.Load()
	p.latest.Store(&observation{result: old.result, at: time.Now().Add(-DefaultObservationMaxAge - time.Second)})
	for range 100 {
		recorder := httptest.NewRecorder()
		p.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
		require.Contains(t, recorder.Body.String(), "observation_stale")
	}
	live := httptest.NewRecorder()
	p.Handler().ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/livez", nil))
	require.Equal(t, http.StatusOK, live.Code)
	require.EqualValues(t, 2, calls.Load(), "HTTP requests must not spawn observation workers")
}

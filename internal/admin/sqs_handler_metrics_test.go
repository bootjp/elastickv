package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordedQueueOutcome struct {
	queue   string
	outcome string
}

type recordingQueueObserver struct {
	purges []recordedQueueOutcome
	peeks  []recordedQueueOutcome
}

func (o *recordingQueueObserver) ObserveAdminPurgeQueue(queue, outcome string) {
	o.purges = append(o.purges, recordedQueueOutcome{queue: queue, outcome: outcome})
}

func (o *recordingQueueObserver) ObserveAdminPeekQueue(queue, outcome string) {
	o.peeks = append(o.peeks, recordedQueueOutcome{queue: queue, outcome: outcome})
}

// queuesSourceWithObserver is a QueuesSource that also exposes counters, the
// shape production uses via the main_admin.go bridge.
type queuesSourceWithObserver struct {
	*stubQueuesSource
	observer AdminQueueObserver
}

func (s *queuesSourceWithObserver) AdminQueueObserver() AdminQueueObserver {
	return s.observer
}

// TestSqsHandlerCountsRejectionsItServesItself is the regression test for
// under-reported §3.6 outcomes.
//
// handlePurge returns at principalForWriteOnPurge and at the empty-name check
// BEFORE AdminPurgeQueue runs, and handlePeek returns at
// principalForReadSensitive and at parsePeekQueryParams before AdminPeekQueue
// runs. The adapter-side counters therefore never saw the most common
// forbidden and validation rejections, so the metric under-reported exactly
// the requests an operator goes looking for.
func TestSqsHandlerCountsRejectionsItServesItself(t *testing.T) {
	t.Parallel()

	t.Run("purge forbidden by the live role", func(t *testing.T) {
		t.Parallel()
		observer := &recordingQueueObserver{}
		src := &queuesSourceWithObserver{
			stubQueuesSource: &stubQueuesSource{queues: []string{"orders"}},
			observer:         observer,
		}
		h := NewSqsHandler(src).
			WithRoleStore(MapRoleStore{"AKIA_RO": RoleReadOnly}).
			WithAdminQueueObserver(adminQueueObserverFrom(src))

		req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders/messages", nil)
		req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
			AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleFull}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
		require.Equal(t,
			[]recordedQueueOutcome{{queue: "orders", outcome: adminQueueOutcomeForbidden}},
			observer.purges,
			"a rejection the handler serves must still be counted")
	})

	t.Run("peek forbidden by the live role", func(t *testing.T) {
		t.Parallel()
		observer := &recordingQueueObserver{}
		src := &queuesSourceWithObserver{
			stubQueuesSource: &stubQueuesSource{queues: []string{"orders"}},
			observer:         observer,
		}
		h := NewSqsHandler(src).
			WithRoleStore(MapRoleStore{"AKIA_NONE": Role("")}).
			WithAdminQueueObserver(adminQueueObserverFrom(src))

		req := httptest.NewRequest(http.MethodGet, pathPrefixSqsQueues+"orders/messages", nil)
		req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
			AuthPrincipal{AccessKey: "AKIA_NONE", Role: RoleFull}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
		require.Equal(t,
			[]recordedQueueOutcome{{queue: "orders", outcome: adminQueueOutcomeForbidden}},
			observer.peeks)
	})

	t.Run("peek rejected for a non-numeric limit", func(t *testing.T) {
		t.Parallel()
		observer := &recordingQueueObserver{}
		src := &queuesSourceWithObserver{
			stubQueuesSource: &stubQueuesSource{queues: []string{"orders"}},
			observer:         observer,
		}
		h := NewSqsHandler(src).
			WithRoleStore(MapRoleStore{"AKIA_FULL": RoleFull}).
			WithAdminQueueObserver(adminQueueObserverFrom(src))

		req := httptest.NewRequest(http.MethodGet,
			pathPrefixSqsQueues+"orders/messages?limit=abc", nil)
		req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
			AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
		require.Equal(t,
			[]recordedQueueOutcome{{queue: "orders", outcome: adminQueueOutcomeValidation}},
			observer.peeks)
	})
}

// A source that exposes no counters must leave the handler working, just
// uncounted — the pre-existing behaviour for fixtures without metrics.
func TestSqsHandlerWithoutAnObserverStillServes(t *testing.T) {
	t.Parallel()

	src := &stubQueuesSource{queues: []string{"orders"}}
	require.Nil(t, adminQueueObserverFrom(src),
		"a source that does not expose counters must yield none")

	h := NewSqsHandler(src).
		WithRoleStore(MapRoleStore{"AKIA_RO": RoleReadOnly}).
		WithAdminQueueObserver(adminQueueObserverFrom(src))

	req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders/messages", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleFull}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
}

// TestSqsHandlerCountsRouteParsingRejections covers the pre-dispatch parser
// exit.
//
// §3.6 counts invalid paths as validation failures, but parseSqsRouteSegments
// writes its 400 and ServeHTTP returns before any per-operation handler runs,
// so those cases were still missing from the counters after the first fix --
// my round-1 tests covered authorization and a bad query parameter and stopped
// there.
func TestSqsHandlerCountsRouteParsingRejections(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		method     string
		path       string
		wantPurges int
		wantPeeks  int
	}{
		{
			name: "interior empty segment on a purge", method: http.MethodDelete,
			path: pathPrefixSqsQueues + "orders//messages", wantPurges: 1,
		},
		{
			name: "percent-encoded slash on a purge", method: http.MethodDelete,
			path: pathPrefixSqsQueues + "orders%2Fmessages/messages", wantPurges: 1,
		},
		{
			name: "dot-segment on a peek", method: http.MethodGet,
			path: pathPrefixSqsQueues + "../messages", wantPeeks: 1,
		},
		{
			name: "interior empty segment on a peek", method: http.MethodGet,
			path: pathPrefixSqsQueues + "orders//messages", wantPeeks: 1,
		},
		{
			// Queue CRUD has no §3.6 counter, so a malformed queue path must
			// not be attributed to purge or peek.
			name: "malformed queue path is attributed to neither", method: http.MethodDelete,
			path: pathPrefixSqsQueues + "bad%2Fname",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observer := &recordingQueueObserver{}
			src := &queuesSourceWithObserver{
				stubQueuesSource: &stubQueuesSource{queues: []string{"orders"}},
				observer:         observer,
			}
			h := NewSqsHandler(src).
				WithRoleStore(MapRoleStore{"AKIA_FULL": RoleFull}).
				WithAdminQueueObserver(adminQueueObserverFrom(src))

			req := httptest.NewRequest(tc.method, tc.path, nil)
			req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
				AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
			require.Len(t, observer.purges, tc.wantPurges)
			require.Len(t, observer.peeks, tc.wantPeeks)
			for _, got := range append(observer.purges, observer.peeks...) {
				require.Equal(t, adminQueueOutcomeValidation, got.outcome)
				require.Empty(t, got.queue,
					"the queue name is unusable on a path the validator refused, so it "+
						"must not be guessed out of it")
			}
		})
	}
}

// TestSqsHandlerClassifiesAMissingPrincipalAsInternal pins the outcome for a
// wiring fault.
//
// A missing session principal writes 500 internal, but the refusal was counted
// as `forbidden`: that hid the fault from the internal_error series and
// reported an authorization rejection that never happened.
func TestSqsHandlerClassifiesAMissingPrincipalAsInternal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		method string
		pick   func(*recordingQueueObserver) []recordedQueueOutcome
	}{
		{
			name: "purge", method: http.MethodDelete,
			pick: func(o *recordingQueueObserver) []recordedQueueOutcome { return o.purges },
		},
		{
			name: "peek", method: http.MethodGet,
			pick: func(o *recordingQueueObserver) []recordedQueueOutcome { return o.peeks },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observer := &recordingQueueObserver{}
			src := &queuesSourceWithObserver{
				stubQueuesSource: &stubQueuesSource{queues: []string{"orders"}},
				observer:         observer,
			}
			h := NewSqsHandler(src).
				WithRoleStore(MapRoleStore{"AKIA_FULL": RoleFull}).
				WithAdminQueueObserver(adminQueueObserverFrom(src))

			// No principal in the context: SessionAuth did not run, which is a
			// mount/middleware regression rather than a denied request.
			req := httptest.NewRequest(tc.method, pathPrefixSqsQueues+"orders/messages", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			require.Equal(t, http.StatusInternalServerError, rec.Code, "body=%s", rec.Body.String())
			got := tc.pick(observer)
			require.Len(t, got, 1)
			require.Equal(t, adminQueueOutcomeInternalError, got[0].outcome,
				"a wiring fault must not be reported as an authorization rejection")
		})
	}
}

// TestSqsHandlerAuditsAPurgeRefusedBeforeTheAdapter closes the audit gap my
// round-1 fix left.
//
// The adapter owns admin.sqs.purge_queue for everything that reaches it, but a
// purge refused by the live RoleStore returns from the handler, so
// AdminPurgeQueue and recordAdminPurge never run. The attempt appeared in the
// counter and in the generic HTTP audit, and was absent from the
// operation-specific record an operator greps.
func TestSqsHandlerAuditsAPurgeRefusedBeforeTheAdapter(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	observer := &recordingQueueObserver{}
	src := &queuesSourceWithObserver{
		stubQueuesSource: &stubQueuesSource{queues: []string{"orders"}},
		observer:         observer,
	}
	h := NewSqsHandler(src).
		WithLogger(slog.New(slog.NewJSONHandler(&buf, nil))).
		WithRoleStore(MapRoleStore{"AKIA_RO": RoleReadOnly}).
		WithAdminQueueObserver(adminQueueObserverFrom(src))

	req := httptest.NewRequest(http.MethodDelete, pathPrefixSqsQueues+"orders/messages", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleFull}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)

	var record map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var candidate map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &candidate))
		if candidate["msg"] == "admin.sqs.purge_queue" {
			record = candidate
		}
	}
	require.NotNil(t, record,
		"a purge refused before the adapter must still produce the operation-specific audit line")
	require.Equal(t, adminQueueOutcomeForbidden, record["outcome"])
	require.Equal(t, "orders", record["queue"])
	require.Equal(t, "AKIA_RO", record["access_key"])
}

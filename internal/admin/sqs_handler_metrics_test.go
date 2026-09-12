package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
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

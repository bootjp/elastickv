package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestAdminQueueOutcomesClassifyLeaderChurn(t *testing.T) {
	t.Parallel()
	for _, err := range []error{ErrAdminNotLeader, ErrNotLeader, kv.ErrLeaderNotFound} {
		require.Equal(t, adminOutcomeNotLeader, adminPurgeOutcomeForError(err))
		require.Equal(t, adminOutcomeNotLeader, adminPeekOutcomeForError(err))
	}
}

type adminQueueOutcomeRecorder struct {
	purge string
	peek  string
}

func (o *adminQueueOutcomeRecorder) ObserveAdminPurgeQueue(_ string, outcome string) {
	o.purge = outcome
}

func (o *adminQueueOutcomeRecorder) ObserveAdminPeekQueue(_ string, outcome string) {
	o.peek = outcome
}

// Regression for PR #1228: malformed queue names are validation failures,
// not missing queues, in both the return value and the observed outcome.
func TestAdminQueueInvalidNamesRecordValidation(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	server := sqsLeaderNode(t, nodes).sqsServer
	records := captureAdminAudit(t, server)

	for _, name := range []string{"orders.bad", "orders/bad", " orders", "orders ", strings.Repeat("a", 81)} {
		t.Run(name, func(t *testing.T) {
			observer := &adminQueueOutcomeRecorder{}
			server.adminObserver = observer
			t.Run("purge", func(t *testing.T) {
				_, err := server.AdminPurgeQueue(t.Context(), fullAdminPrincipal, name)
				require.ErrorIs(t, err, ErrAdminSQSValidation)
				require.Equal(t, adminOutcomeValidation, observer.purge)
				got := records()
				require.NotEmpty(t, got)
				require.Equal(t, adminOutcomeValidation, got[len(got)-1]["outcome"])
			})
			t.Run("peek", func(t *testing.T) {
				_, _, err := server.AdminPeekQueue(t.Context(), fullAdminPrincipal, name, AdminPeekMessageOptions{})
				require.ErrorIs(t, err, ErrAdminSQSValidation)
				require.Equal(t, adminOutcomeValidation, observer.peek)
			})
		})
	}
}

// captureAdminAudit points the server's audit logger at a buffer and returns a
// reader for the admin.sqs.purge_queue records it emits.
func captureAdminAudit(t *testing.T, server *SQSServer) func() []map[string]any {
	t.Helper()

	var buf bytes.Buffer
	// A distinguishing attribute, so a record that went out through
	// slog.Default() instead of this logger is detectable rather than just
	// absent.
	server.adminAuditLogger = slog.New(slog.NewJSONHandler(&buf, nil)).
		With(slog.String("component", "admin"))

	return func() []map[string]any {
		var records []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var rec map[string]any
			require.NoError(t, json.Unmarshal([]byte(line), &rec))
			if rec["msg"] == "admin.sqs.purge_queue" {
				records = append(records, rec)
			}
		}
		return records
	}
}

// TestAdminPurgeQueueAuditsThroughTheConfiguredLogger pins that the §3.6 audit
// record goes to the configured audit destination.
//
// It used to call slog.InfoContext, i.e. the process-wide slog.Default(), so a
// server built with a dedicated audit sink or the production component="admin"
// child logger never saw these records and they lost that logger's attributes
// — unlike every other admin audit entry.
func TestAdminPurgeQueueAuditsThroughTheConfiguredLogger(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_ = createSQSQueueForTest(t, node, "audited")
	records := captureAdminAudit(t, node.sqsServer)

	_, err := node.sqsServer.AdminPurgeQueue(context.Background(), fullAdminPrincipal, "audited")
	require.NoError(t, err)

	got := records()
	require.Len(t, got, 1, "exactly one audit record for one purge")
	require.Equal(t, "admin", got[0]["component"],
		"the record must carry the configured logger's attributes")
	require.Equal(t, "audited", got[0]["queue"])
	require.Equal(t, adminOutcomeOK, got[0]["outcome"])
	require.Contains(t, got[0], "generation_before")
	require.Contains(t, got[0], "generation_after")
}

// TestAdminPurgeQueueAuditsAPurgeInProgressRefusal is the load-bearing case.
//
// A second purge inside the 60-second window returned before the only audit
// call, so the documented signal for repeated rate-limited attempts produced
// nothing. The generic HTTP audit middleware records status and path, which
// cannot say WHY the request was refused — the one thing this record exists to
// answer.
func TestAdminPurgeQueueAuditsAPurgeInProgressRefusal(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_ = createSQSQueueForTest(t, node, "repeat-purged")
	records := captureAdminAudit(t, node.sqsServer)
	ctx := context.Background()

	_, err := node.sqsServer.AdminPurgeQueue(ctx, fullAdminPrincipal, "repeat-purged")
	require.NoError(t, err)
	_, err = node.sqsServer.AdminPurgeQueue(ctx, fullAdminPrincipal, "repeat-purged")
	require.ErrorIs(t, err, ErrAdminSQSPurgeInProgress)

	got := records()
	require.Len(t, got, 2, "the refusal must be audited, not only the success")
	require.Equal(t, adminOutcomeOK, got[0]["outcome"])
	require.Equal(t, adminOutcomePurgeInProgress, got[1]["outcome"])
	require.Equal(t, "repeat-purged", got[1]["queue"])

	// No invented generation pair: the refusal committed nothing, and
	// recording a state that never existed would corrupt the audit trail.
	require.NotContains(t, got[1], "generation_before")
	require.NotContains(t, got[1], "generation_after")
}

// A forbidden principal is refused before any storage work, and must still be
// audited with its own outcome.
func TestAdminPurgeQueueAuditsAForbiddenRefusal(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_ = createSQSQueueForTest(t, node, "guarded")
	records := captureAdminAudit(t, node.sqsServer)

	_, err := node.sqsServer.AdminPurgeQueue(context.Background(), readOnlyAdminPrincipal, "guarded")
	require.ErrorIs(t, err, ErrAdminForbidden)

	got := records()
	require.Len(t, got, 1)
	require.Equal(t, adminOutcomeForbidden, got[0]["outcome"])
	require.NotContains(t, got[0], "generation_before")
}

// A server built without the option must still audit, through the default.
func TestAdminPurgeQueueFallsBackToTheDefaultLogger(t *testing.T) {
	t.Parallel()

	var server *SQSServer
	require.NotNil(t, server.adminLogger(), "a nil server must not panic")
	require.NotNil(t, (&SQSServer{}).adminLogger(),
		"a server with no configured audit logger must still have somewhere to audit")
}

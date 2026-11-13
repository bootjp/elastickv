package adminbridge

import (
	"errors"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	crdberrors "github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTranslateAdminTablesError_LeaderChurn verifies that the
// bridge maps transient leader-churn errors from the kv coordinator
// (which AdminCreateTable/AdminDeleteTable can return after their
// initial isVerifiedDynamoLeader check) to admin.ErrTablesNotLeader
// rather than the generic 500 fallthrough. P2 review on PR #634.
func TestTranslateAdminTablesError_LeaderChurn(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   error
	}{
		{"kv.ErrLeaderNotFound", kv.ErrLeaderNotFound},
		{"kv.ErrLeaderProxyCircuitOpen", kv.ErrLeaderProxyCircuitOpen},
		{"wrapped circuit open", crdberrors.WithStack(kv.ErrLeaderProxyCircuitOpen)},
		{"adapter.ErrNotLeader", adapter.ErrNotLeader},
		{"adapter.ErrLeaderNotFound", adapter.ErrLeaderNotFound},
		{"marked raft not leader", crdberrors.Mark(errors.New("rejected"), raftengine.ErrNotLeader)},
		{"typed transfer in progress", crdberrors.WithStack(raftengine.ErrLeadershipTransferInProgress)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := translateAdminTablesError(tc.in)
			require.ErrorIs(t, out, admin.ErrTablesNotLeader,
				"input %q must map to ErrTablesNotLeader", tc.in)
		})
	}
}

// A leader phrase inside a validation error is not a typed rejection.
func TestTranslateAdminTablesError_LeaderPhraseInMiddleOfMessage(t *testing.T) {
	t.Parallel()
	cases := []string{
		"not leader: actually a downstream error",
		"leader not found, but recovered automatically",
		"leadership lost mid-snapshot, retried successfully",
	}
	for _, msg := range cases {
		t.Run(msg, func(t *testing.T) {
			out := translateAdminTablesError(errors.New(msg))
			require.NotErrorIs(t, out, admin.ErrTablesNotLeader,
				"mid-message leader phrase %q must not classify as leader-churn", msg)
		})
	}
}

// TestTranslateAdminTablesError_UnrelatedErrorPassesThrough confirms
// the leader-churn detector does not swallow unrelated errors that
// happen to mention the word "leader" outside the canonical phrases.
func TestTranslateAdminTablesError_UnrelatedErrorPassesThrough(t *testing.T) {
	t.Parallel()
	in := errors.New("team leader misconfigured")
	out := translateAdminTablesError(in)
	// Falls through to default — same error returned, NOT
	// ErrTablesNotLeader.
	require.NotErrorIs(t, out, admin.ErrTablesNotLeader)
	require.Equal(t, in, out)
}

// TestTranslateAdminObjectsError pins the object-tier sentinel mapping. An
// admin upload refused by the S3 PUT admission limiter comes back from the
// adapter marked ErrAdminUploadThrottled and must become the retryable 503
// sentinel rather than a 500.
func TestTranslateAdminObjectsError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   error
		want error
	}{
		{
			name: "upload throttled",
			in: crdberrors.WithStack(crdberrors.Mark(
				errors.New("s3 put admission budget exhausted"), adapter.ErrAdminUploadThrottled)),
			want: admin.ErrObjectsUploadThrottled,
		},
		{name: "upload too large", in: adapter.ErrAdminUploadTooLarge, want: admin.ErrObjectsUploadTooLarge},
		{name: "bucket not found", in: adapter.ErrAdminBucketNotFound, want: admin.ErrObjectsBucketNotFound},
		{
			name: "invalid continuation token",
			in:   crdberrors.Wrap(adapter.ErrAdminInvalidContinuationToken, "continuation token does not match request"),
			want: admin.ErrObjectsValidation,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, translateAdminObjectsError(tc.in), tc.want)
		})
	}
}

// TestTranslateAdminItemsError_LeaderChurn is the items-tier
// counterpart of TestTranslateAdminTablesError_LeaderChurn — P2
// review on PR #813 flagged that translateAdminItemsError did not
// classify transient leader-churn errors as 503/retryable, so an
// election mid-AdminPutItem / AdminGetItem would 500 instead of
// surfacing through the SPA's retry path.
func TestTranslateAdminItemsError_LeaderChurn(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   error
	}{
		{"kv.ErrLeaderNotFound", kv.ErrLeaderNotFound},
		{"kv.ErrLeaderProxyCircuitOpen", kv.ErrLeaderProxyCircuitOpen},
		{"adapter.ErrNotLeader", adapter.ErrNotLeader},
		{"adapter.ErrLeaderNotFound", adapter.ErrLeaderNotFound},
		{"marked raft not leader", crdberrors.Mark(errors.New("rejected"), raftengine.ErrNotLeader)},
		{"typed transfer in progress", crdberrors.WithStack(raftengine.ErrLeadershipTransferInProgress)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := translateAdminItemsError(tc.in)
			require.ErrorIs(t, out, admin.ErrItemsNotLeader,
				"input %q must map to ErrItemsNotLeader", tc.in)
		})
	}
}

// TestTranslateAdminItemsError_UnrelatedErrorPassesThrough is the
// items-tier counterpart of the table-tier passthrough test — keep
// the leader-churn detector narrow enough that an unrelated error
// mentioning "leader" still surfaces as a 500.
func TestTranslateAdminItemsError_UnrelatedErrorPassesThrough(t *testing.T) {
	t.Parallel()
	in := errors.New("team leader misconfigured")
	out := translateAdminItemsError(in)
	require.NotErrorIs(t, out, admin.ErrItemsNotLeader)
	require.Equal(t, in, out)
}

// TestTranslateAdminItemsError_ValidationPreservesAdapterMessage pins
// the Claude review on PR #813 r3 finding: a wrapped
// adapter.ErrAdminDynamoValidation must reach the HTTP 400 body
// with its operator-facing reason intact, not the bare sentinel
// "admin dynamo items: invalid request". The migration-required
// hint in particular (e.g. legacy-key migration on a freshly-loaded
// table) is the most operator-useful 400 message in the adapter
// vocabulary; dropping it at the bridge made the SPA unhelpful.
func TestTranslateAdminItemsError_ValidationPreservesAdapterMessage(t *testing.T) {
	t.Parallel()
	in := fmt.Errorf("%w: table requires a one-time legacy-key migration before admin read endpoints are available",
		adapter.ErrAdminDynamoValidation)
	out := translateAdminItemsError(in)
	require.ErrorIs(t, out, admin.ErrItemsValidation,
		"wrapped adapter.ErrAdminDynamoValidation must still match admin.ErrItemsValidation")
	require.Contains(t, out.Error(), "legacy-key migration",
		"adapter validation message must propagate so the SPA's 400 body carries the operator hint")
}

// TestTranslateAdminQueuesError_LeaderChurn is the SQS counterpart of
// TestTranslateAdminTablesError_LeaderChurn. AdminDeleteQueue clears
// the upfront isVerifiedSQSLeader check but the kv coordinator can
// still drop leadership inside deleteQueueWithRetry's Dispatch; the
// resulting typed ErrLeaderNotFound / ErrNotLeader must
// classify as 503 leader_unavailable, not the generic 500 fallthrough.
func TestTranslateAdminQueuesError_LeaderChurn(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   error
	}{
		{"kv.ErrLeaderNotFound", kv.ErrLeaderNotFound},
		{"kv.ErrLeaderProxyCircuitOpen", kv.ErrLeaderProxyCircuitOpen},
		{"adapter.ErrNotLeader", adapter.ErrNotLeader},
		{"adapter.ErrLeaderNotFound", adapter.ErrLeaderNotFound},
		{"marked raft not leader", crdberrors.Mark(errors.New("rejected"), raftengine.ErrNotLeader)},
		{"typed transfer in progress", crdberrors.WithStack(raftengine.ErrLeadershipTransferInProgress)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := translateAdminQueuesError(tc.in)
			require.ErrorIs(t, out, admin.ErrQueuesNotLeader,
				"input %q must map to ErrQueuesNotLeader", tc.in)
		})
	}
}

// A leader phrase inside a queue error is not a typed rejection.
func TestTranslateAdminQueuesError_LeaderPhraseInMiddleOfMessage(t *testing.T) {
	t.Parallel()
	cases := []string{
		"not leader: actually a downstream error",
		"leader not found, but recovered automatically",
		"leadership lost mid-snapshot, retried successfully",
	}
	for _, msg := range cases {
		t.Run(msg, func(t *testing.T) {
			out := translateAdminQueuesError(errors.New(msg))
			require.NotErrorIs(t, out, admin.ErrQueuesNotLeader,
				"mid-message leader phrase %q must not classify as leader-churn", msg)
		})
	}
}

func TestAdminBridgeRejectsUnprovenLeaderTextAndUnknownOutcome(t *testing.T) {
	t.Parallel()
	translators := []struct {
		name string
		fn   func(error) error
	}{
		{"tables", translateAdminTablesError},
		{"items", translateAdminItemsError},
		{"buckets", translateAdminBucketsError},
		{"objects", translateAdminObjectsError},
		{"queues", translateAdminQueuesError},
	}
	for _, translator := range translators {
		t.Run(translator.name, func(t *testing.T) {
			t.Parallel()
			for _, message := range []string{
				"not leader", "leader not found", "raft engine: leadership lost",
				"rpc error: code = Unavailable desc = leader proxy circuit open",
				adapter.ErrAdminNotLeader.Error(),
			} {
				unproven := errors.New(message)
				require.Equal(t, unproven, translator.fn(unproven), message)
			}
			unknown := errors.Join(kv.ErrTransactionOutcomeUnknown, adapter.ErrAdminNotLeader)
			require.Equal(t, unknown, translator.fn(unknown))
			unknownLeader := errors.Join(kv.ErrTransactionOutcomeUnknown, raftengine.ErrNotLeader)
			require.Equal(t, unknownLeader, translator.fn(unknownLeader))
			require.Equal(t, raftengine.ErrLeadershipLost, translator.fn(raftengine.ErrLeadershipLost))
		})
	}
}

// TestTranslateAdminQueuesError_UnrelatedErrorPassesThrough confirms
// the leader-churn detector does not swallow unrelated errors that
// happen to mention the word "leader" outside the canonical phrases.
func TestTranslateAdminQueuesError_UnrelatedErrorPassesThrough(t *testing.T) {
	t.Parallel()
	in := errors.New("team leader misconfigured")
	out := translateAdminQueuesError(in)
	require.NotErrorIs(t, out, admin.ErrQueuesNotLeader)
	require.Equal(t, in, out)
}

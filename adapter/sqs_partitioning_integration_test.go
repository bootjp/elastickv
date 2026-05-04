package adapter

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSQSServer_HTFIFO_CapabilityGate_AcceptsOnSingleNode pins the
// §11 PR 5b-3 gate-lift at the wire layer on a single-node cluster:
// CreateQueue with PartitionCount > 1 now succeeds because the local
// binary advertises htfifo and there are no peers to poll. This test
// replaces the prior PR 2 dormancy-reject test (which expected a 400
// with "not yet enabled" — the dormancy gate has been removed). The
// peer-rejection class is exercised in
// sqs_capability_gate_test.go where a fake peer can be wired in
// without a multi-node cluster.
func TestSQSServer_HTFIFO_CapabilityGate_AcceptsOnSingleNode(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	for _, n := range []string{"2", "4", "8", "32"} {
		status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
			"QueueName": "htfifo-gate-" + n + ".fifo",
			"Attributes": map[string]string{
				"FifoQueue":      "true",
				"PartitionCount": n,
			},
		})
		if status != http.StatusOK {
			t.Fatalf("PartitionCount=%s on single-node cluster: status %d (expected 200 after gate-lift); body=%v", n, status, out)
		}
	}
}

// TestSQSServer_HTFIFO_CapabilityGate_IsIdempotentOnExistingQueue
// pins the Codex P1 review fix on PR #734: a CreateQueue retry on
// an already-existing partitioned queue with identical attributes
// MUST return 200 (idempotent) even when the cluster-wide
// capability poll would now fail. Before the fix, the gate ran
// before the existence check, so a transient peer outage during
// a CreateQueue retry would flip a 200-OK idempotent response
// into a 400. Now the gate runs INSIDE tryCreateQueueOnce after
// the existence check; an existing queue with matching attrs
// short-circuits to 200 and never touches the network.
//
// The test creates a partitioned queue on a single-node cluster
// (gate passes vacuously), then poisons the SQSServer's peer
// map with an unreachable address so any subsequent gate
// invocation would reject. The second CreateQueue with identical
// attrs must still succeed; only an actually-new partitioned
// queue would now fail the gate.
func TestSQSServer_HTFIFO_CapabilityGate_IsIdempotentOnExistingQueue(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	attrs := map[string]string{
		"FifoQueue":      "true",
		"PartitionCount": "4",
	}

	// First create — succeeds because the leaderSQS map is empty
	// and the local binary advertises htfifo (vacuous gate).
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "htfifo-idempotent.fifo",
		"Attributes": attrs,
	})
	require.Equal(t, http.StatusOK, status,
		"first create on single-node cluster must succeed: body=%v", out)

	// Poison the peer map with an unreachable address. Any gate
	// invocation from this point would call PollSQSHTFIFOCapability
	// against this dead peer and fail closed → 400. The
	// post-fix code path skips the gate on an existing-queue
	// match, so the next create must STILL succeed.
	require.NotNil(t, node.sqsServer)
	node.sqsServer.leaderSQS = map[string]string{
		"raft-fake": "127.0.0.1:1", // unreachable: connection refused
	}

	status, out = callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "htfifo-idempotent.fifo",
		"Attributes": attrs,
	})
	require.Equal(t, http.StatusOK, status,
		"second create with identical attrs must be idempotent (gate must NOT run on existing-queue match) — Codex P1 PR #734; body=%v", out)

	// Sanity: the gate IS still in effect for genuinely new
	// partitioned queues — a different name with the poisoned
	// peer map must fail the gate.
	status, out = callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "htfifo-newqueue.fifo",
		"Attributes": attrs,
	})
	require.Equal(t, http.StatusBadRequest, status,
		"new partitioned queue must hit the gate when a peer is unreachable: body=%v", out)
	if got, _ := out["__type"].(string); got != sqsErrInvalidAttributeValue {
		t.Fatalf("new queue: __type=%q (expected InvalidAttributeValue)", got)
	}
}

// TestSQSServer_HTFIFO_CapabilityGate_AllowsPartitionCountOne pins
// the no-op-partition-count path: PartitionCount=1 is the legacy
// single-partition layout and bypasses the capability gate
// entirely (validateHTFIFOCapability short-circuits on
// PartitionCount <= 1).
func TestSQSServer_HTFIFO_CapabilityGate_AllowsPartitionCountOne(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "htfifo-singlepart.fifo",
		"Attributes": map[string]string{
			"FifoQueue":      "true",
			"PartitionCount": "1",
		},
	})
	if status != http.StatusOK {
		t.Fatalf("PartitionCount=1 must be accepted: status %d body %v", status, out)
	}
}

// TestSQSServer_HTFIFO_RejectsNonPowerOfTwoPartitionCount pins the
// validator's power-of-two rule. The schema validator runs before
// the capability gate so an invalid count (3) reports the
// validator's reason, not the gate's.
func TestSQSServer_HTFIFO_RejectsNonPowerOfTwoPartitionCount(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "htfifo-bad-count.fifo",
		"Attributes": map[string]string{
			"FifoQueue":      "true",
			"PartitionCount": "3",
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("PartitionCount=3 must reject: status %d", status)
	}
	if msg, _ := out["message"].(string); msg == "" || !strings.Contains(msg, "power of two") {
		t.Fatalf("expected 'power of two' in message, got %q", msg)
	}
}

// TestSQSServer_HTFIFO_RejectsHTFIFOAttrsOnStandardQueue pins the
// FIFO-only rule: setting FifoThroughputLimit or DeduplicationScope
// on a Standard queue rejects with InvalidAttributeValue. Without
// this, the queue would silently land with no-op attributes that
// SDK clients might mistake for actually configured.
func TestSQSServer_HTFIFO_RejectsHTFIFOAttrsOnStandardQueue(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	status, _ := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "standard-with-htfifo-attr",
		"Attributes": map[string]string{
			"FifoThroughputLimit": htfifoThroughputPerMessageGroupID,
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("FifoThroughputLimit on Standard queue: status %d (expected 400)", status)
	}
}

// TestSQSServer_HTFIFO_RejectsQueueScopedDedupOnPartitioned pins
// the §3.2 cross-attribute control-plane gate at the wire layer.
// {PartitionCount > 1, DeduplicationScope = "queue"} is rejected by
// validatePartitionConfig (the schema validator) which runs inside
// parseAttributesIntoMeta — that is, BEFORE the capability gate
// (validateHTFIFOCapability) ever runs. The schema rejection is the
// sole rejection path after the PR 5b-3 dormancy gate-lift.
//
// The test only checks the 400 status to stay agnostic about which
// validator fires first — a future reordering of the createQueue
// control flow does not need to break this test.
func TestSQSServer_HTFIFO_RejectsQueueScopedDedupOnPartitioned(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "htfifo-bad-dedup.fifo",
		"Attributes": map[string]string{
			"FifoQueue":          "true",
			"PartitionCount":     "2",
			"DeduplicationScope": htfifoDedupeScopeQueue,
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("PartitionCount=2 + DeduplicationScope=queue must reject: status %d body %v", status, out)
	}
}

// TestSQSServer_HTFIFO_ImmutabilitySetQueueAttributesRejects pins
// the §3.2 immutability rule at the wire layer: SetQueueAttributes
// attempts to change PartitionCount / FifoThroughputLimit /
// DeduplicationScope reject with InvalidAttributeValue. Test creates
// a single-partition FIFO queue (which bypasses the capability
// gate entirely) with FifoThroughputLimit set, then tries to
// change it.
func TestSQSServer_HTFIFO_ImmutabilitySetQueueAttributesRejects(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateFIFOWithThroughputLimit(t, node, "htfifo-immutable.fifo", htfifoThroughputPerQueue)

	// Try to flip FifoThroughputLimit. Must reject — both the
	// immutability rule and the rule that perMessageGroupId requires
	// PartitionCount > 1 fire on this attempt; either one rejecting
	// is the correct outcome from the wire.
	status, out := callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
		"Attributes": map[string]string{
			"FifoThroughputLimit": htfifoThroughputPerMessageGroupID,
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("FifoThroughputLimit change: status %d body %v (expected 400 immutable)", status, out)
	}
	// Same-value no-op succeeds.
	status, _ = callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
		"Attributes": map[string]string{
			"FifoThroughputLimit": htfifoThroughputPerQueue,
		},
	})
	if status != http.StatusOK {
		t.Fatalf("same-value no-op SetQueueAttributes: status %d (expected 200)", status)
	}
}

// TestSQSServer_HTFIFO_ImmutabilityAllOrNothing pins the §3.2 all-
// or-nothing rule: a SetQueueAttributes that touches a *mutable*
// attribute alongside an attempted *immutable* change rejects the
// whole request, leaving the mutable attribute unchanged on the
// meta record.
func TestSQSServer_HTFIFO_ImmutabilityAllOrNothing(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateFIFOWithThroughputLimit(t, node, "htfifo-allornothing.fifo", htfifoThroughputPerQueue)

	// Combined: mutable VisibilityTimeout + immutable FifoThroughputLimit
	// change. Must reject as a whole, mutable change must not commit.
	status, _ := callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
		"Attributes": map[string]string{
			"VisibilityTimeout":   "60",
			"FifoThroughputLimit": htfifoThroughputPerMessageGroupID,
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("mutable+immutable combined: status %d (expected 400)", status)
	}
	// Confirm VisibilityTimeout did NOT commit by reading it back.
	status, out := callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"VisibilityTimeout"},
	})
	if status != http.StatusOK {
		t.Fatalf("get attrs: %d", status)
	}
	attrs, _ := out["Attributes"].(map[string]any)
	if got, _ := attrs["VisibilityTimeout"].(string); got == "60" {
		t.Fatalf("all-or-nothing violated: VisibilityTimeout committed even though immutable change rejected (got %q)", got)
	}
}

// TestSQSServer_HTFIFO_GetQueueAttributesRoundTrip pins the wire
// surface for the configured HT-FIFO attributes: SetQueueAttributes
// (or CreateQueue with the attribute) followed by GetQueueAttributes
// returns the same value.
func TestSQSServer_HTFIFO_GetQueueAttributesRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateFIFOWithThroughputLimit(t, node, "htfifo-roundtrip.fifo", htfifoThroughputPerQueue)
	status, out := callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"All"},
	})
	if status != http.StatusOK {
		t.Fatalf("GetQueueAttributes: status %d", status)
	}
	attrs, _ := out["Attributes"].(map[string]any)
	if got, _ := attrs["FifoThroughputLimit"].(string); got != htfifoThroughputPerQueue {
		t.Fatalf("FifoThroughputLimit round-trip: got %q want %q", got, htfifoThroughputPerQueue)
	}
	if _, present := attrs["DeduplicationScope"]; present {
		t.Fatalf("DeduplicationScope must be omitted when not set; attrs=%v", attrs)
	}
	if _, present := attrs["PartitionCount"]; present {
		t.Fatalf("PartitionCount must be omitted when not set / left at zero; attrs=%v", attrs)
	}
}

// --- helpers ---

// mustCreateFIFOWithThroughputLimit creates a single-partition FIFO
// queue (PartitionCount=1 bypasses the §11 PR 5b-3 capability
// gate) with the requested FifoThroughputLimit set. Used by the
// immutability tests so they have a non-empty FifoThroughputLimit
// to attempt to change.
func mustCreateFIFOWithThroughputLimit(t *testing.T, node Node, name, limit string) string {
	t.Helper()
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": name,
		"Attributes": map[string]string{
			"FifoQueue":           "true",
			"FifoThroughputLimit": limit,
		},
	})
	if status != http.StatusOK {
		t.Fatalf("createQueue %q: status %d body %v", name, status, out)
	}
	url, _ := out["QueueUrl"].(string)
	return url
}

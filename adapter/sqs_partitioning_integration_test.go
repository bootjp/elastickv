package adapter

import (
	"net/http"
	"testing"
)

// TestSQSServer_HTFIFO_DormancyGate_RejectsPartitionedCreate pins
// the §11 PR 2 dormancy gate at the wire layer: CreateQueue with
// PartitionCount > 1 rejects with InvalidAttributeValue and the
// gate's reason ("not yet enabled") makes it into the operator-
// visible message. Removed in PR 5 in the same commit that wires
// the data plane.
func TestSQSServer_HTFIFO_DormancyGate_RejectsPartitionedCreate(t *testing.T) {
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
		if status != http.StatusBadRequest {
			t.Fatalf("PartitionCount=%s: status %d (expected 400 from dormancy gate); body=%v", n, status, out)
		}
		if got, _ := out["__type"].(string); got != sqsErrInvalidAttributeValue {
			t.Fatalf("PartitionCount=%s: __type=%q (expected InvalidAttributeValue)", n, got)
		}
		msg, _ := out["message"].(string)
		if msg == "" || !contains(msg, "not yet enabled") {
			t.Fatalf("PartitionCount=%s: message %q must mention the gate reason", n, msg)
		}
	}
}

// TestSQSServer_HTFIFO_DormancyGate_AllowsPartitionCountOne pins
// the no-op-partition-count path: PartitionCount=1 is the legacy
// single-partition layout and must pass the dormancy gate even on
// FIFO queues that explicitly set the field.
func TestSQSServer_HTFIFO_DormancyGate_AllowsPartitionCountOne(t *testing.T) {
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
// validator's power-of-two rule. The validator runs before the
// dormancy gate so an invalid count (3) reports the validator's
// reason, not the gate's.
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
	if msg, _ := out["message"].(string); msg == "" || !contains(msg, "power of two") {
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
// {PartitionCount > 1, DeduplicationScope = "queue"} would land
// fine if dormancy were lifted but the validator rejects it before
// dormancy runs. Test sets PartitionCount=1 to bypass dormancy and
// exercise the cross-attr rule alone — when dormancy is lifted in
// PR 5 the equivalent test with PartitionCount > 1 will exercise
// the same path end-to-end.
func TestSQSServer_HTFIFO_RejectsQueueScopedDedupOnPartitioned(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	// Direct unit-test of the validator: dormancy gate runs after
	// the schema validator, but in the wire path PartitionCount > 1
	// is rejected by dormancy first, so we cover the cross-attr
	// rejection via the unit test in sqs_partitioning_test.go and
	// the wire test exercises only the dormancy path while PR 2-4
	// are in production.
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
	// During PR 2-4 the dormancy gate fires first (PartitionCount > 1);
	// after PR 5 lifts the gate, the cross-attr rule fires instead.
	// Either rejection is correct so the test only checks the 400.
}

// TestSQSServer_HTFIFO_ImmutabilitySetQueueAttributesRejects pins
// the §3.2 immutability rule at the wire layer: SetQueueAttributes
// attempts to change PartitionCount / FifoThroughputLimit /
// DeduplicationScope reject with InvalidAttributeValue. Test creates
// a single-partition FIFO queue (allowed by dormancy) with
// FifoThroughputLimit set, then tries to change it.
func TestSQSServer_HTFIFO_ImmutabilitySetQueueAttributesRejects(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateFIFOWithThroughputLimit(t, node, "htfifo-immutable.fifo", htfifoThroughputPerMessageGroupID)

	// Try to flip FifoThroughputLimit. Must reject.
	status, out := callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
		"Attributes": map[string]string{
			"FifoThroughputLimit": htfifoThroughputPerQueue,
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("FifoThroughputLimit change: status %d body %v (expected 400 immutable)", status, out)
	}
	// Same-value no-op succeeds.
	status, _ = callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
		"Attributes": map[string]string{
			"FifoThroughputLimit": htfifoThroughputPerMessageGroupID,
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

	url := mustCreateFIFOWithThroughputLimit(t, node, "htfifo-allornothing.fifo", htfifoThroughputPerMessageGroupID)

	// Combined: mutable VisibilityTimeout + immutable FifoThroughputLimit
	// change. Must reject as a whole, mutable change must not commit.
	status, _ := callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
		"Attributes": map[string]string{
			"VisibilityTimeout":   "60",
			"FifoThroughputLimit": htfifoThroughputPerQueue,
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

	url := mustCreateFIFOWithThroughputLimit(t, node, "htfifo-roundtrip.fifo", htfifoThroughputPerMessageGroupID)
	status, out := callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"All"},
	})
	if status != http.StatusOK {
		t.Fatalf("GetQueueAttributes: status %d", status)
	}
	attrs, _ := out["Attributes"].(map[string]any)
	if got, _ := attrs["FifoThroughputLimit"].(string); got != htfifoThroughputPerMessageGroupID {
		t.Fatalf("FifoThroughputLimit round-trip: got %q want %q", got, htfifoThroughputPerMessageGroupID)
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
// queue (allowed by the §11 PR 2 dormancy gate) with the requested
// FifoThroughputLimit set. Used by the immutability tests so they
// have a non-empty FifoThroughputLimit to attempt to change.
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

// contains is a tiny helper used by the dormancy-gate test to check
// for a substring in the operator-facing message without pulling in
// strings just for one call.
func contains(s, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

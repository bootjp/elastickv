package adapter

import (
	"net/http"
	"strconv"
)

// HT-FIFO (Phase 3.D split-queue FIFO) configuration vocabulary and
// the routing primitive partitionFor. See the design doc at
// docs/design/2026_04_26_proposed_sqs_split_queue_fifo.md.
//
// PR 2 of the §11 rollout introduces the schema fields plus the
// validation surface — including the temporary dormancy gate that
// rejects PartitionCount > 1 at CreateQueue. PR 5 lifts the gate
// atomically with the data-plane fanout so a half-deployed cluster
// can never accept a partitioned queue without the data plane to
// serve it. Until then the field exists in the meta type and the
// router function compiles, but no partitioned queue can land.

const (
	// htfifoMaxPartitions caps the per-queue partition count. 32 is
	// enough for ~30,000 RPS per queue at the per-shard ~1,000 RPS
	// limit. Higher would require larger per-queue meta records and
	// more reaper cycles; bumping the cap is a follow-up if operators
	// demand it. See §10 of the design.
	htfifoMaxPartitions uint32 = 32

	// htfifoThroughputPerMessageGroupID is the default
	// FifoThroughputLimit value for HT-FIFO queues — every group ID
	// hashes to a partition independently, giving the throughput
	// scaling HT-FIFO is designed for.
	htfifoThroughputPerMessageGroupID = "perMessageGroupId"
	// htfifoThroughputPerQueue activates the §3.3 short-circuit: every
	// group ID routes to partition 0, collapsing throughput back to
	// what a single-partition queue gets. Useful for clients that want
	// the AWS attribute set without the extra capacity.
	htfifoThroughputPerQueue = "perQueue"

	// htfifoDedupeScopeMessageGroup is the default DeduplicationScope
	// value for HT-FIFO queues — the dedup window is per (queue,
	// partition, MessageGroupId, dedupId).
	htfifoDedupeScopeMessageGroup = "messageGroup"
	// htfifoDedupeScopeQueue is the legacy single-window scope. Per
	// §3.2 this is incompatible with PartitionCount > 1 (the dedup
	// key cannot be globally unique across partitions without a
	// cross-partition OCC transaction); the validator rejects the
	// combination at CreateQueue time.
	htfifoDedupeScopeQueue = "queue"
)

// htfifoTemporaryGateMessage is the operator-facing reason the
// CreateQueue gate uses while PR 2-4 are in production. Removed in
// PR 5 in the same commit that wires the data-plane fanout.
const htfifoTemporaryGateMessage = "PartitionCount > 1 requires HT-FIFO data plane — not yet enabled"

// partitionFor maps a (queue meta, MessageGroupId) pair to a
// partition index in [0, PartitionCount). Edge cases:
//
//   - PartitionCount == 0 or 1 → always 0 (legacy single-partition).
//   - FifoThroughputLimit == "perQueue" → always 0 (the §3.3
//     short-circuit; collapses every group to one partition).
//   - Empty MessageGroupId → 0 (defensive; FIFO send validation
//     should already have rejected this).
//
// Hashing uses FNV-1a per §3.3 of the design: fast, no SIMD setup
// cost, deterministic across Go versions and architectures, no key.
// Operators do not need this to be cryptographically strong —
// well-distributed and deterministic is what matters.
func partitionFor(meta *sqsQueueMeta, messageGroupID string) uint32 {
	if meta == nil {
		return 0
	}
	if meta.PartitionCount <= 1 {
		return 0
	}
	if meta.FifoThroughputLimit == htfifoThroughputPerQueue {
		return 0
	}
	if messageGroupID == "" {
		return 0
	}
	// Inlined FNV-1a over the string to avoid the []byte allocation
	// hash/fnv.New64a + h.Write would force (Gemini medium on PR
	// #681). MessageGroupId is capped at 128 chars by validation, so
	// this loop bounds at 128 iterations of integer arithmetic per
	// SendMessage — measurably faster than the hash.Hash interface
	// path on the routing hot path.
	const (
		fnv64Offset uint64 = 14695981039346656037
		fnv64Prime  uint64 = 1099511628211
	)
	hash := fnv64Offset
	for i := 0; i < len(messageGroupID); i++ {
		hash ^= uint64(messageGroupID[i])
		hash *= fnv64Prime
	}
	// PartitionCount is a power of two (validator-enforced); mod is
	// equivalent to mask-AND. The mask is meta.PartitionCount - 1.
	// Computing the mask in uint64 first then narrowing to uint32 is
	// safe because htfifoMaxPartitions == 32 fits in uint32 trivially.
	mask := uint64(meta.PartitionCount - 1)
	return uint32(hash & mask) //nolint:gosec // masked by (PartitionCount - 1) ≤ htfifoMaxPartitions − 1, fits in uint32.
}

// isPowerOfTwo returns true when n is a positive power of two.
// PartitionCount must satisfy this so partitionFor's bitwise mask
// (h & (n-1)) is equivalent to (h % n) — without the constraint the
// distribution would be biased toward the lower indices.
func isPowerOfTwo(n uint32) bool {
	return n > 0 && (n&(n-1)) == 0
}

// validatePartitionConfig enforces the §3.2 cross-attribute rules on
// the post-applier meta. Per-field constraints (parse, range) live
// inside the per-attribute appliers. Cross-field rules:
//
//   - PartitionCount must be a power of two in [1, htfifoMaxPartitions]
//     when set. PartitionCount == 0 is canonical "unset" and is
//     equivalent to 1 for routing purposes.
//   - FifoThroughputLimit / DeduplicationScope are FIFO-only —
//     setting either on a Standard queue rejects with
//     InvalidAttributeValue.
//   - {PartitionCount > 1, DeduplicationScope = "queue"} rejects
//     with InvalidParameterValue: queue-scoped dedup is incompatible
//     with multi-partition FIFO because the dedup key cannot be
//     globally unique across partitions without a cross-partition
//     OCC transaction.
//   - The §11 PR 2 dormancy gate (PartitionCount > 1 rejected at
//     CreateQueue) lives in validatePartitionDormancyGate so the
//     dormancy check can be turned off in unit tests that want to
//     exercise the full schema path. Production CreateQueue calls
//     both validators.
func validatePartitionConfig(meta *sqsQueueMeta) error {
	if meta.PartitionCount > 0 {
		if !isPowerOfTwo(meta.PartitionCount) {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"PartitionCount must be a power of two")
		}
		if meta.PartitionCount > htfifoMaxPartitions {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"PartitionCount exceeds the per-queue cap of "+strconv.FormatUint(uint64(htfifoMaxPartitions), 10))
		}
	}
	if !meta.IsFIFO {
		// PartitionCount > 1 only makes sense on FIFO queues (HT-FIFO
		// is by definition a FIFO feature). Without this guard a
		// Standard queue with PartitionCount=2 would slip past the
		// validator once PR 5 lifts the dormancy gate (Claude review
		// on PR #681 round 2 caught this). PartitionCount=0 and 1
		// are accepted because both mean "single-partition layout"
		// which is valid on Standard queues.
		if meta.PartitionCount > 1 {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"PartitionCount > 1 is only valid on FIFO queues")
		}
		if meta.FifoThroughputLimit != "" {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"FifoThroughputLimit is only valid on FIFO queues")
		}
		if meta.DeduplicationScope != "" {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"DeduplicationScope is only valid on FIFO queues")
		}
	}
	if meta.PartitionCount > 1 && meta.DeduplicationScope == htfifoDedupeScopeQueue {
		// sqsErrValidation is "InvalidParameterValue" (Gemini medium
		// on PR #681 — uses the existing constant rather than a
		// duplicate-value alias).
		return newSQSAPIError(http.StatusBadRequest, sqsErrValidation,
			"queue-scoped deduplication is incompatible with multi-partition FIFO because the dedup key cannot be globally unique across partitions without a cross-partition OCC transaction")
	}
	return nil
}

// validatePartitionDormancyGate is the temporary §11 PR 2 gate. As
// long as the data-plane fanout (PR 5) has not landed, accepting a
// partitioned-queue CreateQueue would let SendMessage write under
// the legacy single-partition prefix — the PR 5 reader would never
// find those messages and the reaper would not enumerate them. This
// gate makes the wrong-layout-data class of bug impossible.
//
// Removed in PR 5 in the same commit that wires the data plane so
// the gate-and-lift land atomically.
func validatePartitionDormancyGate(meta *sqsQueueMeta) error {
	if meta.PartitionCount > 1 {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			htfifoTemporaryGateMessage)
	}
	return nil
}

// validatePartitionImmutability enforces the §3.2 rule that
// PartitionCount, FifoThroughputLimit, and DeduplicationScope are
// all immutable from CreateQueue onward. Called from
// trySetQueueAttributesOnce after the meta is loaded; rejects the
// whole SetQueueAttributes call (all-or-nothing — even mutable
// attributes in the same request do not commit when an immutable
// one is invalid) per §3.2.
//
// requested is the post-apply meta; current is the on-disk meta.
// If any of the three immutable fields differs, the validator
// returns InvalidAttributeValue naming the attribute so the
// operator sees the cause directly. A same-value "no-op" succeeds.
//
// PartitionCount uses normalisePartitionCount so a SetQueueAttributes
// request that passes the canonical-equivalent value (e.g. 1 on a
// queue stored with 0, or 0 on a queue stored with 1) is treated as
// the no-op it semantically is — strict equality would reject with
// "PartitionCount is immutable" even though the partition layout
// hasn't changed (Claude Low on PR #679 round 6.2).
func validatePartitionImmutability(current, requested *sqsQueueMeta) error {
	if current == nil || requested == nil {
		return nil
	}
	if normalisePartitionCount(current.PartitionCount) != normalisePartitionCount(requested.PartitionCount) {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"PartitionCount is immutable; SetQueueAttributes cannot change it (DeleteQueue + CreateQueue to reconfigure)")
	}
	if current.FifoThroughputLimit != requested.FifoThroughputLimit {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"FifoThroughputLimit is immutable; SetQueueAttributes cannot change it (DeleteQueue + CreateQueue to reconfigure)")
	}
	if current.DeduplicationScope != requested.DeduplicationScope {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"DeduplicationScope is immutable; SetQueueAttributes cannot change it (DeleteQueue + CreateQueue to reconfigure)")
	}
	return nil
}

// htfifoAttributeKeys lists the wire-side attribute names that this
// PR introduces. Used by the immutability check (and future
// admin-surface code) to know which keys a SetQueueAttributes
// request might attempt to change.
var htfifoAttributeKeys = []string{
	"PartitionCount",
	"FifoThroughputLimit",
	"DeduplicationScope",
}

// htfifoAttributesPresent reports whether any HT-FIFO attribute key
// appears in attrs. Cheap helper used by the validator to short-
// circuit the immutability check for SetQueueAttributes requests
// that touch only mutable attributes.
func htfifoAttributesPresent(attrs map[string]string) bool {
	for _, k := range htfifoAttributeKeys {
		if _, ok := attrs[k]; ok {
			return true
		}
	}
	return false
}

// addHTFIFOAttributes renders the configured HT-FIFO attributes into
// out. Mirrors the Throttle* renderer in addThrottleAttributes; same
// omission rule (only present when set), same wire-side names. Kept
// in this file so the HT-FIFO surface lives in one place.
func addHTFIFOAttributes(out map[string]string, meta *sqsQueueMeta) {
	if meta == nil {
		return
	}
	if meta.PartitionCount > 0 {
		out["PartitionCount"] = strconv.FormatUint(uint64(meta.PartitionCount), 10)
	}
	if meta.FifoThroughputLimit != "" {
		out["FifoThroughputLimit"] = meta.FifoThroughputLimit
	}
	if meta.DeduplicationScope != "" {
		out["DeduplicationScope"] = meta.DeduplicationScope
	}
}

// snapshotImmutableHTFIFO captures the three immutable HT-FIFO field
// values from a meta record. Returned struct is shallow-equal-comparable
// — validatePartitionImmutability uses the snapshot to check for any
// differing value after applyAttributes runs.
func snapshotImmutableHTFIFO(meta *sqsQueueMeta) *sqsQueueMeta {
	if meta == nil {
		return nil
	}
	return &sqsQueueMeta{
		PartitionCount:      meta.PartitionCount,
		FifoThroughputLimit: meta.FifoThroughputLimit,
		DeduplicationScope:  meta.DeduplicationScope,
	}
}

package adapter

import (
	"bytes"

	"github.com/cockroachdb/errors"
)

// Per-key dispatch helpers that route to the legacy single-partition
// constructor or the partitioned-FIFO constructor based on
// meta.PartitionCount. Phase 3.D PR 5b's central abstraction:
// every send/receive/delete code path that constructs a message-
// keyspace key goes through one of these wrappers, so the
// PartitionCount > 1 → partitioned-prefix dispatch lives in one
// place instead of being scattered across 14 call sites.
//
// Contract
//
//   - meta.PartitionCount <= 1: legacy single-partition layout.
//     The partition argument is ignored. Existing data on disk
//     stays byte-identical with pre-PR-5 deployments.
//   - meta.PartitionCount > 1: partitioned layout, partition is
//     the index in [0, PartitionCount) the caller resolved via
//     partitionFor (for SendMessage) or extracted from a v2
//     receipt handle (for Delete/ChangeMessageVisibility).
//
// Caller responsibility
//
// The partition value MUST be valid for the queue's PartitionCount
// when meta.PartitionCount > 1. Out-of-range values produce a key
// the cluster's --sqsFifoPartitionMap doesn't have a route for —
// the partition resolver returns (0, false) and the request fails
// closed at the routing layer. parseSQSFifoPartitionMap +
// validatePartitionedFIFO + the v2 codec each enforce their
// piece, so the dispatch helpers don't re-validate.

// sqsMsgDataKeyDispatch builds the data-record key for either the
// legacy or partitioned keyspace, depending on meta.PartitionCount.
func sqsMsgDataKeyDispatch(meta *sqsQueueMeta, queueName string, partition uint32, gen uint64, messageID string) []byte {
	if meta != nil && meta.PartitionCount > 1 {
		return sqsPartitionedMsgDataKey(queueName, partition, gen, messageID)
	}
	return sqsMsgDataKey(queueName, gen, messageID)
}

// sqsMsgVisKeyDispatch builds the visibility-index key for either
// keyspace.
func sqsMsgVisKeyDispatch(meta *sqsQueueMeta, queueName string, partition uint32, gen uint64, visibleAtMillis int64, messageID string) []byte {
	if meta != nil && meta.PartitionCount > 1 {
		return sqsPartitionedMsgVisKey(queueName, partition, gen, visibleAtMillis, messageID)
	}
	return sqsMsgVisKey(queueName, gen, visibleAtMillis, messageID)
}

// sqsMsgDedupKeyDispatch builds the FIFO dedup key for either
// keyspace. Dedup scope is per-partition on partitioned queues
// (DeduplicationScope = messageGroup is enforced by the validator
// on PartitionCount > 1).
func sqsMsgDedupKeyDispatch(meta *sqsQueueMeta, queueName string, partition uint32, gen uint64, dedupID string) []byte {
	if meta != nil && meta.PartitionCount > 1 {
		return sqsPartitionedMsgDedupKey(queueName, partition, gen, dedupID)
	}
	return sqsMsgDedupKey(queueName, gen, dedupID)
}

// sqsMsgGroupKeyDispatch builds the FIFO group-lock key for either
// keyspace. partitionFor maps a MessageGroupId to one partition,
// so a group lock for any given group lives on exactly one
// partition — there is no cross-partition group-lock invariant
// to maintain.
func sqsMsgGroupKeyDispatch(meta *sqsQueueMeta, queueName string, partition uint32, gen uint64, groupID string) []byte {
	if meta != nil && meta.PartitionCount > 1 {
		return sqsPartitionedMsgGroupKey(queueName, partition, gen, groupID)
	}
	return sqsMsgGroupKey(queueName, gen, groupID)
}

// sqsMsgByAgeKeyDispatch builds the send-age index key for either
// keyspace. The reaper's enumeration helper
// (sqsMsgByAgePrefixesForQueue) already returns BOTH legacy and
// partitioned prefixes per queue, so a queue that was created
// legacy and later — hypothetically — gains partitions does not
// strand its old data.
func sqsMsgByAgeKeyDispatch(meta *sqsQueueMeta, queueName string, partition uint32, gen uint64, sendTimestampMs int64, messageID string) []byte {
	if meta != nil && meta.PartitionCount > 1 {
		return sqsPartitionedMsgByAgeKey(queueName, partition, gen, sendTimestampMs, messageID)
	}
	return sqsMsgByAgeKey(queueName, gen, sendTimestampMs, messageID)
}

// sqsMsgVisPrefixForQueueDispatch returns the vis-prefix used by
// ReceiveMessage's per-partition scan. Legacy queues have one
// per-(queue, gen) prefix; partitioned queues have one prefix per
// (queue, partition, gen) — the fanout reader iterates these.
func sqsMsgVisPrefixForQueueDispatch(meta *sqsQueueMeta, queueName string, partition uint32, gen uint64) []byte {
	if meta != nil && meta.PartitionCount > 1 {
		return sqsPartitionedMsgVisPrefixForQueue(queueName, partition, gen)
	}
	return sqsMsgVisPrefixForQueue(queueName, gen)
}

// effectivePartitionCount returns the number of partitions the
// fanout reader iterates. Treats meta.PartitionCount values 0 and
// 1 as the legacy single-partition layout (one iteration on
// partition 0).
//
// Honors the §3.3 perQueue short-circuit: when
// meta.FifoThroughputLimit == "perQueue", partitionFor forces
// every MessageGroupId to partition 0 regardless of
// PartitionCount, so the only non-empty partition the fanout
// reader will ever find is 0. Returning the literal
// PartitionCount in that mode would have ReceiveMessage scan up
// to 31 guaranteed-empty partitions on every poll, multiplying
// read / CPU work for no correctness benefit (codex P2 round 1
// on PR #731). Mirror the routing decision: collapse to 1.
func effectivePartitionCount(meta *sqsQueueMeta) uint32 {
	if meta == nil || meta.PartitionCount <= 1 {
		return 1
	}
	if meta.FifoThroughputLimit == htfifoThroughputPerQueue {
		return 1
	}
	return meta.PartitionCount
}

// sqsMsgVisScanBoundsDispatch returns the start/end byte ranges that
// ReceiveMessage's per-partition visibility-index scan iterates.
// Mirrors sqsMsgVisScanBounds (legacy keyspace) but parametrises the
// prefix on partition when the queue is partitioned. The bounds are
// always [prefix||u64(0), prefix||u64(maxVisibleAtMillis+1)) so
// messages with visible_at == maxVisibleAtMillis are included.
func sqsMsgVisScanBoundsDispatch(meta *sqsQueueMeta, queueName string, partition uint32, gen uint64, maxVisibleAtMillis int64) (start, end []byte) {
	prefix := sqsMsgVisPrefixForQueueDispatch(meta, queueName, partition, gen)
	start = append(bytes.Clone(prefix), zeroU64()...)
	upper := uint64MaxZero(maxVisibleAtMillis)
	if upper < ^uint64(0) {
		upper++
	}
	end = append(bytes.Clone(prefix), encodedU64(upper)...)
	return start, end
}

// encodeReceiptHandleDispatch picks the receipt-handle wire format
// based on meta.PartitionCount: v1 on legacy / non-partitioned
// queues, v2 on partitioned ones. The partition argument is only
// consulted on the v2 branch — callers may pass 0 on the legacy
// branch.
//
// This is the single point where a fresh receipt handle commits to
// a wire version. Pairing the choice with the same meta.PartitionCount
// the dispatch helpers used to build keys keeps the handle's
// recorded partition consistent with the partition the message was
// stored under, so a later DeleteMessage / ChangeMessageVisibility
// routes to the right keyspace.
func encodeReceiptHandleDispatch(meta *sqsQueueMeta, partition uint32, queueGen uint64, messageIDHex string, receiptToken []byte) (string, error) {
	if meta != nil && meta.PartitionCount > 1 {
		return encodeReceiptHandleV2(partition, queueGen, messageIDHex, receiptToken)
	}
	return encodeReceiptHandle(queueGen, messageIDHex, receiptToken)
}

// validateReceiptHandleVersion enforces the queue-aware version
// rule that replaced the dormancy gate from PR 5a:
//
//   - meta.PartitionCount <= 1 (legacy / non-partitioned queue):
//     handle MUST be v1. A v2 handle on a non-partitioned queue
//     is structurally impossible (SendMessage would never have
//     produced one) and accepting it would let a malicious caller
//     re-encode a v1 handle as v2 to probe / corrupt the v2 layout
//     before any partitioned queue exists.
//   - meta.PartitionCount > 1 (partitioned queue): handle MUST be
//     v2. A v1 handle on a partitioned queue carries no partition
//     index, so dispatch would default to partition 0 and the
//     delete / change-visibility would silently miss messages on
//     other partitions.
//
// Mismatches surface as ReceiptHandleIsInvalid (the same AWS error
// shape used for malformed handles), so a misrouted client cannot
// distinguish "wrong version" from "garbled bytes" — preserving the
// PR 5a / PR 724 round 3 dormancy guarantee that the v2 wire format
// is not probeable from the public API.
func validateReceiptHandleVersion(meta *sqsQueueMeta, handle *decodedReceiptHandle) error {
	if handle == nil {
		return errors.New("receipt handle is nil")
	}
	if meta != nil && meta.PartitionCount > 1 {
		if handle.Version != sqsReceiptHandleVersion2 {
			return errors.New("receipt handle version mismatch for partitioned queue")
		}
		return nil
	}
	if handle.Version != sqsReceiptHandleVersion1 {
		return errors.New("receipt handle version mismatch for non-partitioned queue")
	}
	return nil
}

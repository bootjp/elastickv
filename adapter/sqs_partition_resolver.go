package adapter

import (
	"bytes"
	"encoding/binary"
)

// SQSPartitionResolver maps a partitioned-SQS key to the operator-
// chosen Raft group for the (queue, partition) tuple. Implements
// kv.PartitionResolver via duck typing — see the integration in
// main.go where the resolver is installed on ShardedCoordinator.
//
// The byte-range engine cannot route partitioned queues because
// adding per-partition routes would break its non-overlapping-cover
// invariant (a partition route for partition K of one queue would
// leave a gap for legacy keys that fall lexicographically between
// partitions K and K+1). The resolver-first dispatch path avoids
// this — it answers only for keys that match a partitioned family
// prefix and otherwise lets the engine handle dispatch.
type SQSPartitionResolver struct {
	routes map[string][]uint64
}

// NewSQSPartitionResolver builds a resolver from the operator-
// supplied partition map. routes[queue][k] is the Raft group ID
// that owns partition k of queue, with len(routes[queue]) equal to
// the queue's PartitionCount.
//
// Returns nil when routes is empty so callers can keep the resolver
// out of the request path entirely on a non-partitioned cluster
// (kv.ShardRouter.WithPartitionResolver(nil) is a documented no-op).
//
// The constructor takes a defensive copy so a later caller mutation
// to the input map does not leak into the resolver's view at
// runtime.
func NewSQSPartitionResolver(routes map[string][]uint64) *SQSPartitionResolver {
	if len(routes) == 0 {
		return nil
	}
	cp := make(map[string][]uint64, len(routes))
	for queue, groups := range routes {
		ids := make([]uint64, len(groups))
		copy(ids, groups)
		cp[queue] = ids
	}
	return &SQSPartitionResolver{routes: cp}
}

// sqsResolverFamilyPrefixes is the set of partitioned-SQS family
// prefixes ResolveGroup recognises. Pre-converted to []byte so the
// hot-path bytes.HasPrefix call avoids an allocation per check
// (gemini medium on PR #715). Kept package-internal so any future
// renamed prefix touches both this list and the constant
// declaration in sqs_keys.go — TestSQSPartitionResolver_PrefixesAlign
// pins the alignment.
var sqsResolverFamilyPrefixes = [][]byte{
	[]byte(SqsPartitionedMsgDataPrefix),
	[]byte(SqsPartitionedMsgVisPrefix),
	[]byte(SqsPartitionedMsgDedupPrefix),
	[]byte(SqsPartitionedMsgGroupPrefix),
	[]byte(SqsPartitionedMsgByAgePrefix),
}

// ResolveGroup decodes the (queue, partition) embedded in a
// partitioned-SQS key and returns the operator-chosen Raft group.
//
// Returns (0, false) for any key that does not match a partitioned
// family prefix (legacy SQS, KV, S3, DynamoDB, queue-meta records,
// …) so kv.ShardRouter falls through to its byte-range engine for
// default routing.
func (r *SQSPartitionResolver) ResolveGroup(key []byte) (uint64, bool) {
	if r == nil || len(key) == 0 {
		return 0, false
	}
	queue, partition, ok := parsePartitionedSQSKey(key)
	if !ok {
		return 0, false
	}
	groups, found := r.routes[queue]
	if !found {
		return 0, false
	}
	// Defensive: a partition value outside the slice is a config /
	// upstream-bug signal, not a routable key. Returning false
	// surfaces it as "no route" at the router boundary, which is
	// the correct fail-closed behaviour.
	if uint64(partition) >= uint64(len(groups)) {
		return 0, false
	}
	return groups[partition], true
}

// parsePartitionedSQSKey extracts the (queue, partition) pair from
// a partitioned-SQS key. Returns ok=false for any key that does not
// match a partitioned family prefix or that has a malformed queue /
// partition segment. Exposed at package-internal scope so the
// adapter's reaper / fanout reader can share the same parser
// (Phase 3.D PR 5).
func parsePartitionedSQSKey(key []byte) (string, uint32, bool) {
	rest, matched := stripPartitionedFamilyPrefix(key)
	if !matched {
		return "", 0, false
	}
	// After the family prefix, the variable-length encoded queue
	// segment is terminated by '|' (sqsPartitionedQueueTerminator).
	// base64.RawURLEncoding never emits '|', so the first '|' in
	// rest is unambiguously the queue terminator.
	pipeIdx := bytes.IndexByte(rest, sqsPartitionedQueueTerminator)
	if pipeIdx <= 0 {
		return "", 0, false
	}
	encQueue := rest[:pipeIdx]
	rest = rest[pipeIdx+1:]
	const partitionLen = 4
	if len(rest) < partitionLen {
		return "", 0, false
	}
	partition := binary.BigEndian.Uint32(rest[:partitionLen])
	queue, err := decodeSQSSegment(string(encQueue))
	if err != nil {
		return "", 0, false
	}
	return queue, partition, true
}

// stripPartitionedFamilyPrefix returns the bytes after the matched
// family prefix. matched=false if key has none of the known
// partitioned family prefixes.
func stripPartitionedFamilyPrefix(key []byte) ([]byte, bool) {
	for _, prefix := range sqsResolverFamilyPrefixes {
		if bytes.HasPrefix(key, prefix) {
			return key[len(prefix):], true
		}
	}
	return nil, false
}

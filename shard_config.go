package main

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

type groupSpec struct {
	id      uint64
	address string
}

type rangeSpec struct {
	start   []byte
	end     []byte
	groupID uint64
}

const splitParts = 2

var (
	ErrAddressRequired         = errors.New("address is required")
	ErrNoRaftGroupsConfigured  = errors.New("no raft groups configured")
	ErrNoShardRangesConfigured = errors.New("no shard ranges configured")

	ErrInvalidRaftGroupsEntry           = errors.New("invalid raftGroups entry")
	ErrInvalidShardRangesEntry          = errors.New("invalid shardRanges entry")
	ErrInvalidRaftRedisMapEntry         = errors.New("invalid raftRedisMap entry")
	ErrInvalidRaftS3MapEntry            = errors.New("invalid raftS3Map entry")
	ErrInvalidRaftDynamoMapEntry        = errors.New("invalid raftDynamoMap entry")
	ErrInvalidRaftSQSMapEntry           = errors.New("invalid raftSqsMap entry")
	ErrInvalidSQSFifoPartitionMapEntry  = errors.New("invalid sqsFifoPartitionMap entry")
	ErrInvalidRaftBootstrapMembersEntry = errors.New("invalid raftBootstrapMembers entry")
)

// sqsFifoPartitionMaxPartitions caps the per-queue partition count so
// the partitionFor mask + bucket-store sizing arguments in
// docs/design/2026_04_26_proposed_sqs_split_queue_fifo.md §3.1 stay
// honest: 32 partitions × ~1k RPS per shard ≈ 30k aggregate RPS per
// queue, which matches the design's stated ceiling. Operators who
// need more should split the workload across queues rather than
// raising this value.
const sqsFifoPartitionMaxPartitions = 32

// sqsFifoQueueRouting captures the operator-supplied partition-to-
// group assignment for a single FIFO queue. Groups are listed in
// partition-index order — Groups[k] owns partition k. The validator
// (validateSQSFifoPartitionMap) checks that PartitionCount matches
// len(Groups), is a power of two, and is within the per-queue cap.
type sqsFifoQueueRouting struct {
	PartitionCount uint32
	Groups         []string
}

func parseRaftGroups(raw, defaultAddr string) ([]groupSpec, error) {
	if raw == "" {
		if defaultAddr == "" {
			return nil, errors.WithStack(ErrAddressRequired)
		}
		return []groupSpec{{id: 1, address: defaultAddr}}, nil
	}
	parts := strings.Split(raw, ",")
	groups := make([]groupSpec, 0, len(parts))
	seen := map[uint64]struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", splitParts)
		if len(kv) != splitParts {
			return nil, errors.Wrapf(ErrInvalidRaftGroupsEntry, "%q", part)
		}
		idRaw := strings.TrimSpace(kv[0])
		id, err := strconv.ParseUint(idRaw, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(ErrInvalidRaftGroupsEntry, "invalid group id %q: %v", idRaw, err)
		}
		addr := strings.TrimSpace(kv[1])
		if addr == "" {
			return nil, errors.Wrapf(ErrInvalidRaftGroupsEntry, "empty address for group %d", id)
		}
		if _, ok := seen[id]; ok {
			return nil, errors.Wrapf(ErrInvalidRaftGroupsEntry, "duplicate group id %d", id)
		}
		seen[id] = struct{}{}
		groups = append(groups, groupSpec{id: id, address: addr})
	}
	if len(groups) == 0 {
		return nil, errors.WithStack(ErrNoRaftGroupsConfigured)
	}
	return groups, nil
}

func parseShardRanges(raw string, defaultGroup uint64) ([]rangeSpec, error) {
	if raw == "" {
		return []rangeSpec{{start: []byte(""), end: nil, groupID: defaultGroup}}, nil
	}
	parts := strings.Split(raw, ",")
	ranges := make([]rangeSpec, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", splitParts)
		if len(kv) != splitParts {
			return nil, errors.Wrapf(ErrInvalidShardRangesEntry, "%q", part)
		}
		groupID, err := strconv.ParseUint(strings.TrimSpace(kv[1]), 10, 64)
		if err != nil {
			return nil, errors.Wrapf(ErrInvalidShardRangesEntry, "invalid group id in %q: %v", part, err)
		}
		rangePart := strings.TrimSpace(kv[0])
		bounds := strings.SplitN(rangePart, ":", splitParts)
		if len(bounds) != splitParts {
			return nil, errors.Wrapf(ErrInvalidShardRangesEntry, "invalid range %q (expected start:end)", rangePart)
		}
		// An empty start key represents the minimum key boundary.
		start := []byte(strings.TrimSpace(bounds[0]))
		var end []byte
		if endStr := strings.TrimSpace(bounds[1]); endStr != "" {
			end = []byte(endStr)
			if bytes.Compare(start, end) >= 0 {
				return nil, errors.Wrapf(ErrInvalidShardRangesEntry, "invalid range %q (start must be < end)", rangePart)
			}
		}
		ranges = append(ranges, rangeSpec{start: start, end: end, groupID: groupID})
	}
	if len(ranges) == 0 {
		return nil, errors.WithStack(ErrNoShardRangesConfigured)
	}
	return ranges, nil
}

func parseRaftRedisMap(raw string) (map[string]string, error) {
	return parseRaftAddressMap(raw, ErrInvalidRaftRedisMapEntry)
}

func parseRaftS3Map(raw string) (map[string]string, error) {
	return parseRaftAddressMap(raw, ErrInvalidRaftS3MapEntry)
}

func parseRaftDynamoMap(raw string) (map[string]string, error) {
	return parseRaftAddressMap(raw, ErrInvalidRaftDynamoMapEntry)
}

func parseRaftSQSMap(raw string) (map[string]string, error) {
	return parseRaftAddressMap(raw, ErrInvalidRaftSQSMapEntry)
}

// parseSQSFifoPartitionMap reads the `--sqsFifoPartitionMap` operator
// flag. The grammar is:
//
//	queue1.fifo:N=group_0,group_1,...,group_{N-1}
//	;queue2.fifo:M=group_0,...,group_{M-1}
//
// Multiple queue entries are separated by ';' (commas are reserved for
// the per-queue group list). Each queue's PartitionCount must equal
// len(Groups) — a mismatch is rejected at parse time so a config error
// cannot silently produce a wrong-shaped routing map at runtime.
//
// This function does not validate that referenced Raft groups exist
// or that the queue exists in the catalog; that's
// validateSQSFifoPartitionMap's job and runs against the parsed
// runtime config after both --raftGroups and --sqsFifoPartitionMap
// have been parsed.
func parseSQSFifoPartitionMap(raw string) (map[string]sqsFifoQueueRouting, error) {
	out := make(map[string]sqsFifoQueueRouting)
	if raw == "" {
		return out, nil
	}
	entries := strings.SplitSeq(raw, ";")
	for entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		queue, routing, err := parseSQSFifoPartitionMapEntry(entry)
		if err != nil {
			return nil, err
		}
		if _, dup := out[queue]; dup {
			return nil, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
				"duplicate queue %q", queue)
		}
		out[queue] = routing
	}
	return out, nil
}

func parseSQSFifoPartitionMapEntry(entry string) (string, sqsFifoQueueRouting, error) {
	// Shape: queue.fifo:N=g0,g1,...,g{N-1}
	colonIdx := strings.Index(entry, ":")
	eqIdx := strings.Index(entry, "=")
	if colonIdx <= 0 || eqIdx <= colonIdx+1 {
		return "", sqsFifoQueueRouting{}, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
			"%q: expected queue.fifo:N=group_0,...,group_{N-1}", entry)
	}
	queue := strings.TrimSpace(entry[:colonIdx])
	if queue == "" {
		return "", sqsFifoQueueRouting{}, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
			"%q: empty queue name", entry)
	}
	count, err := parseSQSFifoPartitionCount(entry, entry[colonIdx+1:eqIdx])
	if err != nil {
		return "", sqsFifoQueueRouting{}, err
	}
	groups, err := parseSQSFifoGroupList(entry, entry[eqIdx+1:], count)
	if err != nil {
		return "", sqsFifoQueueRouting{}, err
	}
	return queue, sqsFifoQueueRouting{PartitionCount: count, Groups: groups}, nil
}

// parseSQSFifoPartitionCount validates the N in `queue.fifo:N=...`.
// Extracted from parseSQSFifoPartitionMapEntry to keep that function
// under the cyclop ceiling once the validation surface grew to four
// distinct rejections (parse error, zero, overflow cap, non-power-of-2).
func parseSQSFifoPartitionCount(entry, countStr string) (uint32, error) {
	countStr = strings.TrimSpace(countStr)
	count64, err := strconv.ParseUint(countStr, 10, 32)
	if err != nil {
		return 0, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
			"%q: PartitionCount %q is not a non-negative integer", entry, countStr)
	}
	if count64 == 0 {
		return 0, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
			"%q: PartitionCount must be > 0", entry)
	}
	if count64 > sqsFifoPartitionMaxPartitions {
		return 0, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
			"%q: PartitionCount %d exceeds the per-queue cap of %d",
			entry, count64, sqsFifoPartitionMaxPartitions)
	}
	if count64&(count64-1) != 0 {
		return 0, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
			"%q: PartitionCount %d must be a power of two", entry, count64)
	}
	// count64 is bounded by sqsFifoPartitionMaxPartitions (32) — well
	// inside uint32 range — so the narrowing is safe by construction.
	return uint32(count64), nil
}

// parseSQSFifoGroupList validates the comma-separated group list and
// asserts its length matches the parsed PartitionCount. Extracted for
// the same cyclop reason as parseSQSFifoPartitionCount.
func parseSQSFifoGroupList(entry, groupRaw string, count uint32) ([]string, error) {
	groupRaw = strings.TrimSpace(groupRaw)
	if groupRaw == "" {
		return nil, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
			"%q: empty group list", entry)
	}
	groups := make([]string, 0, count)
	for g := range strings.SplitSeq(groupRaw, ",") {
		g = strings.TrimSpace(g)
		if g == "" {
			return nil, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
				"%q: empty group name in list", entry)
		}
		groups = append(groups, g)
	}
	// Compare lengths in int rather than narrowing len(groups) to
	// uint32 — the narrowing would trip gosec G115 even though the
	// per-queue cap (32) keeps the value well in range. count is at
	// most sqsFifoPartitionMaxPartitions (32) so the int(count)
	// widening is safe and exact.
	if len(groups) != int(count) {
		return nil, errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
			"%q: PartitionCount=%d but %d groups listed; both must agree",
			entry, count, len(groups))
	}
	return groups, nil
}

// validateSQSFifoPartitionMap checks the parsed map against the
// configured Raft groups. Every group named in any queue's routing
// must appear in the --raftGroups list with a matching ID — otherwise
// the operator has typed a group ID that does not exist and the
// runtime would route partition traffic to a non-existent shard.
//
// raftGroupsByName is the {ID-as-string -> address} map produced by
// parseRaftGroups; the partition-map flag uses the same string IDs
// the operator supplied to --raftGroups so this lookup is direct.
func validateSQSFifoPartitionMap(m map[string]sqsFifoQueueRouting, raftGroupsByName map[string]string) error {
	for queue, routing := range m {
		for partition, group := range routing.Groups {
			if _, ok := raftGroupsByName[group]; !ok {
				return errors.Wrapf(ErrInvalidSQSFifoPartitionMapEntry,
					"queue %q partition %d: group %q is not in --raftGroups",
					queue, partition, group)
			}
		}
	}
	return nil
}

func parseRaftAddressMap(raw string, invalidEntry error) (map[string]string, error) {
	out := make(map[string]string)
	if raw == "" {
		return out, nil
	}
	parts := strings.SplitSeq(raw, ",")
	for part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", splitParts)
		if len(kv) != splitParts {
			return nil, errors.Wrapf(invalidEntry, "%q", part)
		}
		k := strings.TrimSpace(kv[0])
		v := strings.TrimSpace(kv[1])
		if k == "" || v == "" {
			return nil, errors.Wrapf(invalidEntry, "%q", part)
		}
		out[k] = v
	}
	return out, nil
}

func parseRaftBootstrapMembers(raw string) ([]raftengine.Server, error) {
	servers := make([]raftengine.Server, 0)
	if raw == "" {
		return servers, nil
	}
	seen := make(map[string]struct{})
	parts := strings.SplitSeq(raw, ",")
	for part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", splitParts)
		if len(kv) != splitParts {
			return nil, errors.Wrapf(ErrInvalidRaftBootstrapMembersEntry, "%q", part)
		}
		id := strings.TrimSpace(kv[0])
		addr := strings.TrimSpace(kv[1])
		if id == "" || addr == "" {
			return nil, errors.Wrapf(ErrInvalidRaftBootstrapMembersEntry, "%q", part)
		}
		if _, exists := seen[id]; exists {
			return nil, errors.Wrapf(ErrInvalidRaftBootstrapMembersEntry, "duplicate id %q", id)
		}
		seen[id] = struct{}{}
		servers = append(servers, raftengine.Server{
			Suffrage: "voter",
			ID:       id,
			Address:  addr,
		})
	}
	return servers, nil
}

func defaultGroupID(groups []groupSpec) uint64 {
	min := uint64(0)
	for _, g := range groups {
		if min == 0 || g.id < min {
			min = g.id
		}
	}
	if min == 0 {
		return 1
	}
	return min
}

func validateShardRanges(ranges []rangeSpec, groups []groupSpec) error {
	ids := map[uint64]struct{}{}
	for _, g := range groups {
		ids[g.id] = struct{}{}
	}
	for _, r := range ranges {
		if _, ok := ids[r.groupID]; !ok {
			return errors.WithStack(errors.Newf("shard range references unknown group %d", r.groupID))
		}
	}
	return nil
}

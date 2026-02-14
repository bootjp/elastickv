package main

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
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

	ErrInvalidRaftGroupsEntry   = errors.New("invalid raftGroups entry")
	ErrInvalidShardRangesEntry  = errors.New("invalid shardRanges entry")
	ErrInvalidRaftRedisMapEntry = errors.New("invalid raftRedisMap entry")
)

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

func parseRaftRedisMap(raw string) (map[raft.ServerAddress]string, error) {
	out := make(map[raft.ServerAddress]string)
	if raw == "" {
		return out, nil
	}
	parts := strings.Split(raw, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", splitParts)
		if len(kv) != splitParts {
			return nil, errors.Wrapf(ErrInvalidRaftRedisMapEntry, "%q", part)
		}
		k := strings.TrimSpace(kv[0])
		v := strings.TrimSpace(kv[1])
		if k == "" || v == "" {
			return nil, errors.Wrapf(ErrInvalidRaftRedisMapEntry, "%q", part)
		}
		out[raft.ServerAddress(k)] = v
	}
	return out, nil
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

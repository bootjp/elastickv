package main

import (
	"fmt"
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
)

func parseRaftGroups(raw, defaultAddr string) ([]groupSpec, error) {
	if raw == "" {
		if defaultAddr == "" {
			return nil, ErrAddressRequired
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
			return nil, errors.WithStack(errors.Newf("invalid raftGroups entry: %q", part))
		}
		id, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid group id %q", kv[0])
		}
		addr := strings.TrimSpace(kv[1])
		if addr == "" {
			return nil, errors.WithStack(errors.Newf("empty address for group %d", id))
		}
		if _, ok := seen[id]; ok {
			return nil, errors.WithStack(errors.Newf("duplicate group id %d", id))
		}
		seen[id] = struct{}{}
		groups = append(groups, groupSpec{id: id, address: addr})
	}
	if len(groups) == 0 {
		return nil, ErrNoRaftGroupsConfigured
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
			return nil, errors.WithStack(errors.Newf("invalid shardRanges entry: %q", part))
		}
		groupID, err := strconv.ParseUint(kv[1], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid group id in %q", part)
		}
		rangePart := kv[0]
		bounds := strings.SplitN(rangePart, ":", splitParts)
		if len(bounds) != splitParts {
			return nil, errors.WithStack(errors.Newf("invalid range %q (expected start:end)", rangePart))
		}
		start := []byte(bounds[0])
		var end []byte
		if bounds[1] != "" {
			end = []byte(bounds[1])
		}
		ranges = append(ranges, rangeSpec{start: start, end: end, groupID: groupID})
	}
	if len(ranges) == 0 {
		return nil, ErrNoShardRangesConfigured
	}
	return ranges, nil
}

func parseRaftRedisMap(raw string) map[raft.ServerAddress]string {
	out := make(map[raft.ServerAddress]string)
	if raw == "" {
		return out
	}
	parts := strings.Split(raw, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", splitParts)
		if len(kv) != splitParts {
			continue
		}
		out[raft.ServerAddress(kv[0])] = kv[1]
	}
	return out
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
			return fmt.Errorf("shard range references unknown group %d", r.groupID)
		}
	}
	return nil
}

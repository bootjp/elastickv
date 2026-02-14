package main

import (
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestParseRaftGroups(t *testing.T) {
	t.Run("default address required", func(t *testing.T) {
		_, err := parseRaftGroups("", "")
		require.ErrorIs(t, err, ErrAddressRequired)
	})

	t.Run("default group from default address", func(t *testing.T) {
		groups, err := parseRaftGroups("", "127.0.0.1:50051")
		require.NoError(t, err)
		require.Equal(t, []groupSpec{{id: 1, address: "127.0.0.1:50051"}}, groups)
	})

	t.Run("multiple groups", func(t *testing.T) {
		groups, err := parseRaftGroups("1=127.0.0.1:50051, 2=127.0.0.1:50052", "")
		require.NoError(t, err)
		require.Equal(t, []groupSpec{
			{id: 1, address: "127.0.0.1:50051"},
			{id: 2, address: "127.0.0.1:50052"},
		}, groups)
	})

	t.Run("invalid entry", func(t *testing.T) {
		_, err := parseRaftGroups("nope", "127.0.0.1:50051")
		require.Error(t, err)
	})

	t.Run("duplicate ids", func(t *testing.T) {
		_, err := parseRaftGroups("1=a,1=b", "")
		require.Error(t, err)
	})

	t.Run("empty after trimming", func(t *testing.T) {
		_, err := parseRaftGroups(" , , ", "127.0.0.1:50051")
		require.ErrorIs(t, err, ErrNoRaftGroupsConfigured)
	})
}

func TestParseShardRanges(t *testing.T) {
	t.Run("default range", func(t *testing.T) {
		ranges, err := parseShardRanges("", 7)
		require.NoError(t, err)
		require.Len(t, ranges, 1)
		require.Equal(t, []byte(""), ranges[0].start)
		require.Nil(t, ranges[0].end)
		require.Equal(t, uint64(7), ranges[0].groupID)
	})

	t.Run("multiple ranges", func(t *testing.T) {
		ranges, err := parseShardRanges("a:m=1, m:=2", 1)
		require.NoError(t, err)
		require.Equal(t, []rangeSpec{
			{start: []byte("a"), end: []byte("m"), groupID: 1},
			{start: []byte("m"), end: nil, groupID: 2},
		}, ranges)
	})

	t.Run("invalid entry", func(t *testing.T) {
		_, err := parseShardRanges("a=1", 1)
		require.Error(t, err)
	})

	t.Run("empty after trimming", func(t *testing.T) {
		_, err := parseShardRanges(" , , ", 1)
		require.ErrorIs(t, err, ErrNoShardRangesConfigured)
	})
}

func TestParseRaftRedisMap(t *testing.T) {
	m := parseRaftRedisMap("a=b, c=d")
	require.Equal(t, map[raft.ServerAddress]string{
		"a": "b",
		"c": "d",
	}, m)
}

func TestDefaultGroupID(t *testing.T) {
	require.Equal(t, uint64(1), defaultGroupID(nil))
	require.Equal(t, uint64(2), defaultGroupID([]groupSpec{{id: 3}, {id: 2}}))
}

func TestValidateShardRanges(t *testing.T) {
	groups := []groupSpec{{id: 1}, {id: 2}}

	t.Run("valid", func(t *testing.T) {
		err := validateShardRanges([]rangeSpec{{groupID: 1}}, groups)
		require.NoError(t, err)
	})

	t.Run("unknown group", func(t *testing.T) {
		err := validateShardRanges([]rangeSpec{{groupID: 3}}, groups)
		require.Error(t, err)
	})
}

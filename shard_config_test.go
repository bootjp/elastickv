package main

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
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

	t.Run("trims whitespace around id", func(t *testing.T) {
		groups, err := parseRaftGroups("1 = 127.0.0.1:50051, 2=127.0.0.1:50052", "")
		require.NoError(t, err)
		require.Equal(t, []groupSpec{
			{id: 1, address: "127.0.0.1:50051"},
			{id: 2, address: "127.0.0.1:50052"},
		}, groups)
	})

	t.Run("invalid entry", func(t *testing.T) {
		_, err := parseRaftGroups("nope", "127.0.0.1:50051")
		require.ErrorIs(t, err, ErrInvalidRaftGroupsEntry)
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

	t.Run("trims whitespace", func(t *testing.T) {
		ranges, err := parseShardRanges(" a : m = 1 , m :  = 2 ", 1)
		require.NoError(t, err)
		require.Equal(t, []rangeSpec{
			{start: []byte("a"), end: []byte("m"), groupID: 1},
			{start: []byte("m"), end: nil, groupID: 2},
		}, ranges)
	})

	t.Run("invalid entry", func(t *testing.T) {
		_, err := parseShardRanges("a=1", 1)
		require.ErrorIs(t, err, ErrInvalidShardRangesEntry)
	})

	t.Run("empty after trimming", func(t *testing.T) {
		_, err := parseShardRanges(" , , ", 1)
		require.ErrorIs(t, err, ErrNoShardRangesConfigured)
	})
}

func TestParseRaftRedisMap(t *testing.T) {
	m, err := parseRaftRedisMap("a=b, c=d")
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"a": "b",
		"c": "d",
	}, m)

	t.Run("trims whitespace", func(t *testing.T) {
		m, err := parseRaftRedisMap(" a = b , c = d ")
		require.NoError(t, err)
		require.Equal(t, map[string]string{
			"a": "b",
			"c": "d",
		}, m)
	})

	t.Run("invalid entry errors", func(t *testing.T) {
		_, err := parseRaftRedisMap("a=b, nope")
		require.ErrorIs(t, err, ErrInvalidRaftRedisMapEntry)
	})
}

func TestParseRaftS3Map(t *testing.T) {
	m, err := parseRaftS3Map("a=b, c=d")
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"a": "b",
		"c": "d",
	}, m)

	t.Run("trims whitespace", func(t *testing.T) {
		m, err := parseRaftS3Map(" a = b , c = d ")
		require.NoError(t, err)
		require.Equal(t, map[string]string{
			"a": "b",
			"c": "d",
		}, m)
	})

	t.Run("invalid entry errors", func(t *testing.T) {
		_, err := parseRaftS3Map("a=b, nope")
		require.ErrorIs(t, err, ErrInvalidRaftS3MapEntry)
	})
}

func TestParseSQSFifoPartitionMap(t *testing.T) {
	t.Parallel()
	t.Run("empty input yields empty map", func(t *testing.T) {
		t.Parallel()
		m, err := parseSQSFifoPartitionMap("")
		require.NoError(t, err)
		require.Empty(t, m)
	})

	t.Run("single queue", func(t *testing.T) {
		t.Parallel()
		m, err := parseSQSFifoPartitionMap("orders.fifo:8=g0,g1,g2,g3,g4,g5,g6,g7")
		require.NoError(t, err)
		require.Len(t, m, 1)
		require.Equal(t, sqsFifoQueueRouting{
			PartitionCount: 8,
			Groups:         []string{"g0", "g1", "g2", "g3", "g4", "g5", "g6", "g7"},
		}, m["orders.fifo"])
	})

	t.Run("multiple queues separated by ;", func(t *testing.T) {
		t.Parallel()
		m, err := parseSQSFifoPartitionMap("orders.fifo:2=g0,g1;events.fifo:4=h0,h1,h2,h3")
		require.NoError(t, err)
		require.Len(t, m, 2)
		require.Equal(t, uint32(2), m["orders.fifo"].PartitionCount)
		require.Equal(t, []string{"h0", "h1", "h2", "h3"}, m["events.fifo"].Groups)
	})

	t.Run("trims whitespace", func(t *testing.T) {
		t.Parallel()
		m, err := parseSQSFifoPartitionMap(" orders.fifo : 2 = g0 , g1 ")
		require.NoError(t, err)
		require.Equal(t, []string{"g0", "g1"}, m["orders.fifo"].Groups)
	})

	t.Run("PartitionCount must be > 0", func(t *testing.T) {
		t.Parallel()
		_, err := parseSQSFifoPartitionMap("q.fifo:0=")
		require.ErrorIs(t, err, ErrInvalidSQSFifoPartitionMapEntry)
	})

	t.Run("PartitionCount must be a power of two", func(t *testing.T) {
		t.Parallel()
		// 3 partitions is a power-of-two violation; the partitionFor
		// mask-AND optimisation in §3.1 only works for powers of two,
		// and the validator rejects it at config time so a typo
		// cannot land a half-shaped queue.
		_, err := parseSQSFifoPartitionMap("q.fifo:3=g0,g1,g2")
		require.ErrorIs(t, err, ErrInvalidSQSFifoPartitionMapEntry)
		require.Contains(t, err.Error(), "power of two")
	})

	t.Run("PartitionCount within cap", func(t *testing.T) {
		t.Parallel()
		// 64 exceeds the per-queue cap of 32 from §3.1.
		_, err := parseSQSFifoPartitionMap("q.fifo:64=" +
			"g0,g1,g2,g3,g4,g5,g6,g7,g8,g9,g10,g11,g12,g13,g14,g15," +
			"g16,g17,g18,g19,g20,g21,g22,g23,g24,g25,g26,g27,g28,g29,g30,g31," +
			"g32,g33,g34,g35,g36,g37,g38,g39,g40,g41,g42,g43,g44,g45,g46,g47," +
			"g48,g49,g50,g51,g52,g53,g54,g55,g56,g57,g58,g59,g60,g61,g62,g63")
		require.ErrorIs(t, err, ErrInvalidSQSFifoPartitionMapEntry)
		require.Contains(t, err.Error(), "exceeds the per-queue cap")
	})

	t.Run("count and group-list length must agree", func(t *testing.T) {
		t.Parallel()
		// PartitionCount says 4 but only 2 groups listed — the parser
		// rejects rather than silently routing partitions 2-3 to a
		// nil/wrap-around group.
		_, err := parseSQSFifoPartitionMap("q.fifo:4=g0,g1")
		require.ErrorIs(t, err, ErrInvalidSQSFifoPartitionMapEntry)
		require.Contains(t, err.Error(), "groups listed")
	})

	t.Run("malformed entry rejects", func(t *testing.T) {
		t.Parallel()
		// Missing '=' — the operator typed a queue spec without the
		// group-list separator.
		_, err := parseSQSFifoPartitionMap("orders.fifo:2 g0,g1")
		require.ErrorIs(t, err, ErrInvalidSQSFifoPartitionMapEntry)
	})

	t.Run("empty queue name rejects", func(t *testing.T) {
		t.Parallel()
		_, err := parseSQSFifoPartitionMap(":2=g0,g1")
		require.ErrorIs(t, err, ErrInvalidSQSFifoPartitionMapEntry)
	})

	t.Run("empty group entry rejects", func(t *testing.T) {
		t.Parallel()
		// Trailing comma without a group name — easy typo to make in
		// a long group list, and would otherwise produce a
		// silently-shorter list that mismatches PartitionCount.
		_, err := parseSQSFifoPartitionMap("q.fifo:2=g0,")
		require.ErrorIs(t, err, ErrInvalidSQSFifoPartitionMapEntry)
	})

	t.Run("duplicate queue rejects", func(t *testing.T) {
		t.Parallel()
		// Two entries for the same queue — the second would
		// silently overwrite the first under a naive map insertion,
		// hiding the operator's mistake. Reject explicitly.
		_, err := parseSQSFifoPartitionMap("q.fifo:2=g0,g1;q.fifo:4=h0,h1,h2,h3")
		require.ErrorIs(t, err, ErrInvalidSQSFifoPartitionMapEntry)
		require.Contains(t, err.Error(), "duplicate queue")
	})
}

func TestValidateSQSFifoPartitionMap(t *testing.T) {
	t.Parallel()
	t.Run("all groups present passes", func(t *testing.T) {
		t.Parallel()
		m := map[string]sqsFifoQueueRouting{
			"q.fifo": {PartitionCount: 2, Groups: []string{"g0", "g1"}},
		}
		groups := map[string]string{"g0": "127.0.0.1:1", "g1": "127.0.0.1:2"}
		require.NoError(t, validateSQSFifoPartitionMap(m, groups))
	})

	t.Run("missing group fails with partition pointer", func(t *testing.T) {
		t.Parallel()
		// Operator typed "g99" but only "g0" through "g3" exist in
		// --raftGroups. The validator must surface the queue and
		// partition index so the operator can fix the typo without
		// re-counting.
		m := map[string]sqsFifoQueueRouting{
			"orders.fifo": {PartitionCount: 4, Groups: []string{"g0", "g99", "g2", "g3"}},
		}
		groups := map[string]string{
			"g0": "a", "g1": "b", "g2": "c", "g3": "d",
		}
		err := validateSQSFifoPartitionMap(m, groups)
		require.ErrorIs(t, err, ErrInvalidSQSFifoPartitionMapEntry)
		require.Contains(t, err.Error(), "orders.fifo")
		require.Contains(t, err.Error(), "partition 1")
		require.Contains(t, err.Error(), "g99")
	})

	t.Run("empty map passes", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, validateSQSFifoPartitionMap(nil, nil))
	})
}

func TestParseRaftBootstrapMembers(t *testing.T) {
	t.Run("parses members", func(t *testing.T) {
		members, err := parseRaftBootstrapMembers("n1=10.0.0.11:50051, n2=10.0.0.12:50051")
		require.NoError(t, err)
		require.Equal(t, []raftengine.Server{
			{Suffrage: "voter", ID: "n1", Address: "10.0.0.11:50051"},
			{Suffrage: "voter", ID: "n2", Address: "10.0.0.12:50051"},
		}, members)
	})

	t.Run("trims whitespace", func(t *testing.T) {
		members, err := parseRaftBootstrapMembers(" n1 = 10.0.0.11:50051 , n2=10.0.0.12:50051 ")
		require.NoError(t, err)
		require.Len(t, members, 2)
	})

	t.Run("duplicate id errors", func(t *testing.T) {
		_, err := parseRaftBootstrapMembers("n1=a,n1=b")
		require.ErrorIs(t, err, ErrInvalidRaftBootstrapMembersEntry)
	})

	t.Run("invalid entry errors", func(t *testing.T) {
		_, err := parseRaftBootstrapMembers("n1=a,nope")
		require.ErrorIs(t, err, ErrInvalidRaftBootstrapMembersEntry)
	})
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

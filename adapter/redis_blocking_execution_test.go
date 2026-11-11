package adapter

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func redisBlockingExecutionUsage(s *RedisServer) (used, peak int) {
	s.blockingExecutionBudget.mu.Lock()
	defer s.blockingExecutionBudget.mu.Unlock()
	return s.blockingExecutionBudget.used, s.blockingExecutionBudget.peak
}

func redisStreamWaiterCount(s *RedisServer, key string) int {
	s.streamWaiters.mu.Lock()
	defer s.streamWaiters.mu.Unlock()
	return len(s.streamWaiters.waiters[key])
}

func TestRedisBlockingExecutionXREADDoesNotStarveXADD(t *testing.T) {
	t.Parallel()
	runRedisBlockingProducerProgress(t, []string{cmdXAdd, "events", "1-0", "f", "v"})
}

func TestRedisBlockingExecutionXREADDoesNotStarveEvalXADD(t *testing.T) {
	t.Parallel()
	runRedisBlockingProducerProgress(t, []string{cmdEval,
		`return redis.call('XADD', KEYS[1], ARGV[1], ARGV[2], ARGV[3])`,
		"1", "events", "1-0", "f", "v"})
}

func runRedisBlockingProducerProgress(t *testing.T, producerCommand []string) {
	t.Helper()
	const readers = 4
	st := newRedisWireStore(t)
	g := newRedisWireTestGate(t)
	s := startRedisWireServer(t, st, newLocalAdapterCoordinator(st), g,
		WithRedisHeavyCommandSlots(readers+1), func(s *RedisServer) {
			s.executionBudget.limit = readers
			s.blockingExecutionBudget.limit = readers
		})
	require.GreaterOrEqual(t, cap(s.heavyCommandLimiter.slots), readers)
	clients := make([]*redisWireClient, 0, readers)
	owners := make([]*redisWireTestOwner, 0, readers)
	for range readers {
		c := dialRedisWire(t, s)
		c.send(t, []string{cmdXRead, "BLOCK", "60000", "STREAMS", "events", "0-0"})
		clients = append(clients, c)
		owners = append(owners, g.next(t))
	}
	require.Eventually(t, func() bool { return redisStreamWaiterCount(s, "events") == readers },
		3*time.Second, time.Millisecond, "all four consumers must be waiting before the producer starts")
	used, peak := redisBlockingExecutionUsage(s)
	require.Equal(t, readers, used)
	require.Equal(t, readers, peak)
	normal, _ := redisReadExecutionUsage(s)
	require.Zero(t, normal, "sleeping consumers cannot occupy producer capacity")
	// The independent consumer lane remains bounded, including mixed case.
	excess := dialRedisWire(t, s)
	excess.send(t, []string{"xReAd", "BLOCK", "60000", "STREAMS", "events", "0-0"})
	excess.want(t, "-"+errRedisExecutionLimit.Error()+"\r\n")
	_, err := excess.r.ReadByte()
	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, g.owners, "exhausted consumer lane refuses before minting another H")
	producer := dialRedisWire(t, s)
	producer.send(t, producerCommand)
	producer.want(t, "$3\r\n1-0\r\n")
	requireRedisWireJoined(t, g.next(t))
	want := "*1\r\n*2\r\n$6\r\nevents\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
	for i, c := range clients {
		c.want(t, want)
		requireRedisWireJoined(t, owners[i])
	}
	require.Eventually(t, func() bool { used, _ := redisBlockingExecutionUsage(s); return used == 0 }, 3*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { used, _ := redisReadExecutionUsage(s); return used == 0 }, 3*time.Second, time.Millisecond)
	_, producerPeak := redisReadExecutionUsage(s)
	require.Equal(t, 1, producerPeak, "producer uses bounded ordinary admission, not an unbounded exception")
	require.Zero(t, redisStreamWaiterCount(s, "events"))
	require.Empty(t, g.owners, "wake-up retains the original four owners")
	requireRedisReplyReleased(t, s)
}

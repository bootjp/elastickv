package adapter

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func redisReplyUsage(b *redisReplyBudget) (used, peak int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.peak
}

func requireRedisReplyReleased(t *testing.T, s *RedisServer) {
	t.Helper()
	require.Eventually(t, func() bool { used, _ := redisReplyUsage(&s.replyBudget); return used == 0 }, 3*time.Second, time.Millisecond)
}

func TestRedisReplyBudgetCapacityGrowthAndPreallocation(t *testing.T) {
	t.Parallel()
	old, next := redisReplyCapacityCharge(128), redisReplyCapacityCharge(256)
	for _, fits := range []bool{false, true} {
		t.Run(map[bool]string{false: "old plus new does not fit", true: "transient growth charged"}[fits], func(t *testing.T) {
			budget := &redisReplyBudget{limit: old + next}
			if !fits {
				budget.limit--
			}
			b := &redisReplyBuffer{budget: budget}
			defer b.release()
			b.WriteRaw(make([]byte, 128))
			require.Equal(t, 128, cap(b.data))
			b.WriteRaw([]byte{1})
			used, peak := redisReplyUsage(budget)
			if fits {
				require.NoError(t, b.err)
				require.Equal(t, 256, cap(b.data))
				require.Equal(t, next, used)
				require.Equal(t, old+next, peak)
			} else {
				require.ErrorIs(t, b.err, errRedisReplyBudget)
				require.Empty(t, b.Buffer())
				require.Equal(t, 128, cap(b.data), "rejected growth allocates no replacement")
				require.Equal(t, old, used, "old allocation stays charged until cleanup")
			}
			require.LessOrEqual(t, peak, budget.limit)
			b.release()
			used, _ = redisReplyUsage(budget)
			require.Zero(t, used)
		})
	}
	budget := &redisReplyBudget{limit: 1024}
	b := &redisReplyBuffer{budget: budget}
	require.False(t, b.reserve(redisMaxReplyBytes-1))
	require.ErrorIs(t, b.err, errRedisReplyBudget)
	require.Zero(t, cap(b.data), "near-64 MiB reservation fails before allocating")
	used, peak := redisReplyUsage(budget)
	require.Zero(t, used)
	require.Zero(t, peak)
	require.Panics(t, func() { budget.release(1) })
}

func TestRedisReplyBudgetConcurrentReceiverReservations(t *testing.T) {
	t.Parallel()
	const clients, capacity, admitted = 32, 4096, 4
	budget := &redisReplyBudget{limit: admitted * redisReplyCapacityCharge(capacity)}
	start, release := make(chan struct{}), make(chan struct{})
	ready := make(chan bool, clients)
	var joined sync.WaitGroup
	for range clients {
		joined.Add(1)
		go func() {
			defer joined.Done()
			<-start
			b := &redisReplyBuffer{budget: budget}
			defer b.release()
			ready <- b.reserve(capacity)
			<-release
		}()
	}
	close(start)
	count := 0
	for range clients {
		if <-ready {
			count++
		}
	}
	used, peak := redisReplyUsage(budget)
	close(release)
	joined.Wait()
	require.Equal(t, admitted, count)
	require.Equal(t, budget.limit, used)
	require.LessOrEqual(t, peak, budget.limit)
	used, _ = redisReplyUsage(budget)
	require.Zero(t, used)
}

type redisReplyReader func([]byte) (int, error)

func (f redisReplyReader) Read(p []byte) (int, error) { return f(p) }

func TestRedisReplyBudgetBulkFromHasNoUnchargedScratch(t *testing.T) {
	t.Parallel()
	budget := &redisReplyBudget{limit: redisReplyCapacityCharge(2048)}
	b := &redisReplyBuffer{budget: budget}
	b.WriteBulkFrom(1024, redisReplyReader(func(p []byte) (int, error) {
		used, _ := redisReplyUsage(budget)
		require.Equal(t, budget.limit, used)
		require.Equal(t, 2048, cap(b.data))
		return copy(p, strings.Repeat("x", len(p))), nil
	}))
	require.NoError(t, b.err)
	require.Equal(t, "$1024\r\n"+strings.Repeat("x", 1024)+"\r\n", string(b.Buffer()))
	b.release()
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "read error", true: "reader panic"}[panics], func(t *testing.T) {
			b := &redisReplyBuffer{budget: budget}
			action := func() {
				defer b.release()
				b.WriteBulkFrom(1024, redisReplyReader(func([]byte) (int, error) {
					if panics {
						panic("bulk reader panic")
					}
					return 0, io.ErrUnexpectedEOF
				}))
				require.ErrorIs(t, b.err, io.ErrUnexpectedEOF)
			}
			if panics {
				require.Panics(t, action)
			} else {
				action()
			}
			used, peak := redisReplyUsage(budget)
			require.Zero(t, used)
			require.LessOrEqual(t, peak, budget.limit)
		})
	}
}

type redisReplyJoinOwner struct {
	RequestOwnership
	join func()
}

func (o redisReplyJoinOwner) CancelAndJoin() { o.join(); o.RequestOwnership.CancelAndJoin() }

func TestRedisReplyBudgetRetainedThroughOwnerCleanup(t *testing.T) {
	t.Parallel()
	for _, exit := range []string{"success", "revoked", "panic", "overflow"} {
		t.Run(exit, func(t *testing.T) {
			owner := &redisTestOwnership{ctx: t.Context()}
			s := redisOwnedTestServer(owner)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			owner.onFinish = func() {
				used, _ := redisReplyUsage(&s.replyBudget)
				require.Positive(t, used, "terminal validation still owns reply capacity")
			}
			s.requestAdmission = func(context.Context) (RequestOwnership, error) {
				return redisReplyJoinOwner{RequestOwnership: owner, join: func() {
					used, _ := redisReplyUsage(&s.replyBudget)
					require.Positive(t, used, "capacity cannot be reused before child joins")
				}}, nil
			}
			c := &redisOwnedTestConn{}
			action := func() {
				s.dispatchCommand(c, cmdGet, func(conn redcon.Conn, _ redcon.Command) {
					conn.WriteString("private")
					switch exit {
					case "revoked":
						owner.revoked.Store(true)
					case "panic":
						panic("handler panic")
					case "overflow":
						conn.WriteBulkFrom(redisMaxReplyBytes, strings.NewReader("not read"))
					}
				}, redcon.Command{}, time.Now())
			}
			if exit == "panic" {
				require.Panics(t, action)
			} else {
				action()
			}
			require.Equal(t, 1, owner.finishes)
			require.Equal(t, 1, owner.joins)
			requireRedisReplyReleased(t, s)
		})
	}
}

func TestRedisReplyBudgetLineEncodingHasNoSanitationCopy(t *testing.T) {
	t.Parallel()
	for _, prefix := range []byte{'+', '-'} {
		budget := &redisReplyBudget{limit: redisReplyCapacityCharge(4096)}
		b := &redisReplyBuffer{budget: budget}
		b.writeLine(prefix, strings.Repeat("a\r\nb", 512))
		require.NoError(t, b.err)
		require.Equal(t, string(prefix)+strings.Repeat("a  b", 512)+"\r\n", string(b.Buffer()))
		used, peak := redisReplyUsage(budget)
		require.Equal(t, budget.limit, used)
		require.Equal(t, used, peak)
		b.release()
	}
}

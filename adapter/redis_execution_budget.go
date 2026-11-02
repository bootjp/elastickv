package adapter

import (
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

const (
	redisDefaultProtectedExecutions = 4
	redisAdmissionCommandNameLimit  = 64
)

var errRedisExecutionLimit = errors.New("BUSY protected command execution capacity exhausted")

// Admission precedes backend/proxy materialization, not merely RESP encoding.
// The serving command retains its slot through child joins and network output.
type redisProtectedExecutionBudget struct {
	mu                sync.Mutex
	limit, used, peak int
}

func (b *redisProtectedExecutionBudget) acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	limit := b.limit
	if limit <= 0 {
		limit = redisDefaultProtectedExecutions
	}
	if b.used >= limit {
		return false
	}
	b.used++
	if b.used > b.peak {
		b.peak = b.used
	}
	return true
}

func (b *redisProtectedExecutionBudget) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used <= 0 {
		panic("redis execution budget accounting underflow")
	}
	b.used--
}

func releaseRedisExecution(b *redisProtectedExecutionBudget) {
	if b != nil {
		b.release()
	}
}

func (r *RedisServer) acquireProtectedExecution(conn redcon.Conn, cmd redcon.Command) (*redisProtectedExecutionBudget, bool) {
	if r.requestAdmission == nil {
		return nil, true
	}
	budget := &r.executionBudget
	if redisBlockingExecution(cmd) {
		// Sleeping consumers must not monopolize their producers' lane.
		budget = &r.blockingExecutionBudget
	}
	if budget.acquire() {
		return budget, true
	}
	// Reject before ownership or backend work. Closing also discards a retained
	// MULTI and prevents buffered commands from being reinterpreted as new work.
	conn.WriteError(errRedisExecutionLimit.Error())
	conn.ReadPipeline()
	_ = conn.Close()
	return nil, false
}

func redisBlockingExecution(cmd redcon.Command) bool {
	if len(cmd.Args) == 0 || len(cmd.Args[0]) > redisAdmissionCommandNameLimit {
		return false
	}
	name := strings.ToUpper(string(cmd.Args[0]))
	return isRedisIdleWaitCommand(name)
}

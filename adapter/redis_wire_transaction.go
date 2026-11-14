package adapter

import (
	"context"
	"sync"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

const (
	redisMaxQueuedCommands     = 65536
	redisQueuedCommandOverhead = 64
	redisQueuedArgOverhead     = 24
)

var errRedisExecAbort = errors.New("EXECABORT Transaction discarded because of previous errors.")
var errRedisTxnQueueLimit = errors.New("ERR transaction queue limit exceeded")

// The holder owns MULTI's original H, not a new admission for each queued argv.
// Its lock serializes command publication with cancellation and child joins.
// Only the serving goroutine touches redcon's writer; idle cancellation closes
// the raw socket instead, waking redcon's reader without racing its buffers.
type redisWireTxn struct {
	readRequest *kv.ReadRequest
	mu          sync.Mutex
	server      *RedisServer
	conn        redcon.Conn
	state       *connState
	owner       RequestOwnership
	ctx         context.Context
	cancel      context.CancelFunc
	stopServer  func() bool
	stopWatch   func() bool
	watchDone   chan struct{}
	watchOnce   sync.Once
	idle        *time.Timer
	closed      bool
	done        chan struct{}
	queueBytes  int
	queueErr    error
	reserved    int
	poisoned    bool
	reply       *redisReplyBuffer
}

func (r *RedisServer) dispatchProtectedTransaction(conn redcon.Conn, name string, cmd redcon.Command, start time.Time) bool {
	if r.requestAdmission == nil {
		return false
	}
	state := getConnState(conn)
	if state.transactionPoisoned {
		conn.WriteError(errRedisTxnQueueLimit.Error())
		return true
	}
	state.mu.Lock()
	txn := state.wireTxn
	state.mu.Unlock()
	if txn == nil && (state.transactionRefused || state.inTxn) {
		// A queue without a carrier must never be admitted under a fresh H.
		state.inTxn, state.queue = false, nil
		state.transactionRefused = name != cmdExec && name != cmdDiscard
		conn.WriteError(errRedisAdmission.Error())
		return true
	}
	if txn == nil {
		if name != cmdMulti {
			return false
		}
		txn = r.beginWireTransaction(conn, state, cmd)
		if txn == nil {
			return true
		}
	}
	txn.dispatch(name, cmd, start)
	return true
}

func (r *RedisServer) beginWireTransaction(conn redcon.Conn, state *connState, cmd redcon.Command) *redisWireTxn {
	if err := r.validateCmd(cmd); err != nil {
		writeRedisError(conn, err)
		return nil
	}
	txn, err := r.newWireTransaction(conn, state)
	if err == nil {
		return txn
	}
	if errors.Is(err, errRedisTxnQueueLimit) {
		// redcon can still dispatch buffered frames after Close. Keep a
		// permanent tombstone so none becomes a fresh standalone request.
		state.transactionPoisoned = true
		conn.WriteError(err.Error())
		_ = conn.Close()
		return nil
	}
	state.transactionRefused = true
	conn.WriteError(errRedisAdmission.Error())
	return nil
}

func (r *RedisServer) newWireTransaction(conn redcon.Conn, state *connState) (*redisWireTxn, error) {
	if !r.reserveWireTxnBytes(redisWireTxnBaseBytes) {
		return nil, errRedisTxnQueueLimit
	}
	transferred := false
	defer func() {
		if !transferred {
			r.releaseWireTxnBytes(redisWireTxnBaseBytes)
		}
	}()
	owner, err := r.requestAdmission(r.baseRequestContext())
	if err != nil || owner == nil || owner.Context() == nil {
		if owner != nil {
			owner.CancelAndJoin()
			_ = owner.Finish(errors.Join(err, errRedisAdmission))
		}
		return nil, errRedisAdmission
	}
	ctx, cancel := context.WithCancel(context.WithValue(owner.Context(), redisOwnedRequestKey{}, true))
	readRequest := kv.NewReadRequest(ctx, r.coordinator, r.store, r.readTracker)
	ctx = readRequest.Context()
	t := &redisWireTxn{readRequest: readRequest, server: r, conn: redisLifetimeConn(conn), state: state, owner: owner,
		ctx: ctx, cancel: cancel, done: make(chan struct{}), watchDone: make(chan struct{}), reserved: redisWireTxnBaseBytes, reply: r.newReplyBuffer()}
	t.mu.Lock()
	t.stopServer = context.AfterFunc(r.baseRequestContext(), cancel)
	r.wireTxnMu.Lock()
	if r.baseRequestContext().Err() != nil {
		r.wireTxnMu.Unlock()
		t.stopServer()
		cancel()
		readRequest.Close()
		owner.CancelAndJoin()
		_ = owner.Finish(errRedisAdmission)
		t.mu.Unlock()
		return nil, errRedisAdmission
	}
	if r.wireTxns == nil {
		r.wireTxns = make(map[*redisWireTxn]struct{})
	}
	r.wireTxns[t] = struct{}{}
	r.wireTxnMu.Unlock()
	state.mu.Lock()
	state.wireTxn = t
	state.mu.Unlock()
	t.stopWatch = context.AfterFunc(ctx, func() {
		defer t.forget()
		defer t.watchOnce.Do(func() { close(t.watchDone) })
		t.abort()
	})
	t.mu.Unlock()
	transferred = true
	return t, nil
}

func (t *redisWireTxn) dispatch(name string, cmd redcon.Command, start time.Time) {
	t.mu.Lock()
	defer func() {
		closed := t.closed
		t.mu.Unlock()
		if closed {
			<-t.watchDone
			t.forget()
		}
	}()
	if t.closed {
		t.conn.WriteError(errRedisAdmission.Error())
		return
	}
	if t.idle != nil {
		t.idle.Stop()
	}
	completed := false
	defer func() {
		if !completed {
			t.cleanupLocked(false, errRedisAdmission)
		}
	}()
	t.reply.reset()
	c := &redisOwnedConn{Conn: t.conn, owner: t.owner, ctx: t.ctx,
		serverCtx: t.server.baseRequestContext(), writer: t.reply}
	var terminal bool
	if err := errors.Join(c.cancellationError(), t.owner.Validate()); err != nil {
		c.resultErr, terminal = err, true
	} else {
		terminal = t.execute(c, name, cmd, start)
	}
	if terminal {
		t.publishTerminal(c)
	} else {
		t.publishQueued(c)
	}
	completed = true
}

func (t *redisWireTxn) publishTerminal(c *redisOwnedConn) {
	// Finish may cancel H itself. Stop the idle watcher before publication;
	// any already-started watcher will observe closed under this same lock.
	t.stopWatcher()
	t.poisonReplyBudget(c)
	live := c.publishResult(true)
	t.cleanupLocked(true, nil)
	if !live || t.poisoned {
		// Synchronous Close flushes the terminal error before disconnecting.
		_ = t.conn.Close()
		return
	}
	t.state.mu.Lock()
	t.state.wireTxn = nil
	t.state.mu.Unlock()
}

func (t *redisWireTxn) publishQueued(c *redisOwnedConn) {
	t.poisonReplyBudget(c)
	c.publishResult(false)
	if err := errors.Join(t.ctx.Err(), t.owner.Validate()); err != nil || t.poisoned {
		t.cleanupLocked(false, err)
		_ = t.conn.Close()
		return
	}
	// Idle argv-only transactions cannot retain an admission owner forever.
	timeout := t.server.wireTxnIdleTimeout
	if timeout <= 0 {
		timeout = redisDispatchTimeout
	}
	t.idle = time.AfterFunc(timeout, t.cancel)
}

func (t *redisWireTxn) poisonReplyBudget(c *redisOwnedConn) {
	if errors.Is(c.writer.err, errRedisReplyBudget) {
		t.poisoned = true
		t.conn.ReadPipeline()
	}
}

func (t *redisWireTxn) execute(c *redisOwnedConn, name string, cmd redcon.Command, start time.Time) bool {
	handler, ok := t.server.route[name]
	if !ok {
		t.queueErr = errRedisExecAbort
		c.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
		t.server.observeRedisUnsupported(string(cmd.Args[0]), time.Since(start))
		return false
	}
	if err := t.server.validateCmd(cmd); err != nil {
		t.queueErr = errRedisExecAbort
		writeRedisError(c, err)
		t.server.observeRedisError(name, time.Since(start))
		return false
	}
	if redisQueueOnlyCommand(t.state, name) && name != cmdQuit {
		return t.enqueue(c, name, cmd, start)
	}
	if name == cmdExec && t.queueErr != nil {
		writeRedisError(c, t.queueErr)
		return true
	}
	c.mayWrite = name == cmdExec
	t.server.dispatchCommandUnowned(c, name, func(conn redcon.Conn, command redcon.Command) {
		if err := errors.Join(t.ctx.Err(), t.owner.Validate()); err != nil {
			c.resultErr = err
			return
		}
		c.workStarted = c.mayWrite
		handler(conn, command)
	}, cmd, start)
	return name == cmdExec || name == cmdDiscard || name == cmdQuit
}

func (t *redisWireTxn) enqueue(c *redisOwnedConn, name string, cmd redcon.Command, start time.Time) bool {
	size := queuedRedisCommandSize(cmd)
	capacity, growth := redisQueueCapacityGrowth(t.state.queue)
	charge := size + growth
	if len(t.state.queue) >= redisMaxQueuedCommands || charge > redisMaxReplyBytes-t.queueBytes || !t.server.reserveWireTxnBytes(charge) {
		t.poisoned = true
		c.WriteError(errRedisTxnQueueLimit.Error())
		return true
	}
	// Own the reservation before either the slice growth or payload clone.
	t.reserved += charge
	t.queueBytes += charge
	if capacity != cap(t.state.queue) {
		queue := make([]redcon.Command, len(t.state.queue), capacity)
		copy(queue, t.state.queue)
		t.state.queue = queue
	}
	t.state.queue = append(t.state.queue, cloneCommand(cmd))
	c.WriteString("QUEUED")
	t.server.traceCommandDone(t.conn, name, cmd.Args[1:], 0, true)
	t.server.observeRedisSuccess(name, time.Since(start))
	return false
}

func (t *redisWireTxn) abort() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		if raw := t.conn.NetConn(); raw != nil {
			_ = raw.Close()
		}
		t.cleanupLocked(false, errRedisAdmission)
	}
}

func (t *redisWireTxn) cleanupLocked(finished bool, err error) {
	if t.closed {
		return
	}
	t.closed = true
	if t.idle != nil {
		t.idle.Stop()
	}
	t.stopWatcher()
	t.stopServer()
	t.cancel()
	t.owner.CancelAndJoin()
	if t.readRequest != nil {
		t.readRequest.Close()
	}
	if !finished {
		_ = t.owner.Finish(err)
	}
	t.reply.release()
	t.state.inTxn, t.state.queue = false, nil
	// EXEC may have detached queue into its handler's local variable. Only
	// here, after that handler returned and all children joined, is its charge
	// reusable. Keep the lightweight closed holder for poisoned pipelines.
	t.queueBytes = 0
	t.server.releaseWireTxnBytes(t.reserved)
	t.reserved = 0
	close(t.done)
}

func (t *redisWireTxn) forget() {
	t.server.wireTxnMu.Lock()
	delete(t.server.wireTxns, t)
	t.server.wireTxnMu.Unlock()
}

func (t *redisWireTxn) stopWatcher() {
	if t.stopWatch() {
		t.watchOnce.Do(func() { close(t.watchDone) })
	}
}

func (r *RedisServer) closeWireTransaction(conn redcon.Conn) {
	if state, ok := conn.Context().(*connState); ok {
		state.mu.Lock()
		txn := state.wireTxn
		state.mu.Unlock()
		if txn != nil {
			txn.cancel()
			txn.abort()
			<-txn.watchDone
			txn.forget()
		}
	}
}

func (r *RedisServer) closeWireTransactions() {
	r.wireTxnMu.Lock()
	txns := make([]*redisWireTxn, 0, len(r.wireTxns))
	for txn := range r.wireTxns {
		txns = append(txns, txn)
	}
	r.wireTxnMu.Unlock()
	for _, txn := range txns {
		txn.cancel()
		txn.abort()
		<-txn.done
		<-txn.watchDone
		txn.forget()
	}
}

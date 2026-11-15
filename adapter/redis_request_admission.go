package adapter

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

// WithRedisRequestAdmission is startup-only. Supported commands retain their
// receiver-local owner through work, child joins and terminal validation.
// Detached subscriptions retain the original owner until cleanup. A nil hook
// preserves legacy behavior.
// Wire transactions retain the MULTI owner through EXEC or DISCARD.
func WithRedisRequestAdmission(hook RequestAdmission) RedisServerOption {
	return func(r *RedisServer) { r.requestAdmission = hook }
}

var errRedisAdmission = errors.New("ERR request admission unavailable or expired")
var errRedisOwnershipUnsupported = errors.New("ERR command request ownership is not implemented")

func redisOwnedCommandSupported(name string) bool {
	switch name {
	case cmdPing, cmdGet, cmdSet, cmdSetEx, cmdSetNX, cmdGetDel, cmdIncr, cmdDel, cmdExists,
		cmdExpire, cmdPExpire, cmdTTL, cmdPTTL, cmdType, cmdRename,
		cmdHSet, cmdHMSet, cmdHGet, cmdHMGet, cmdHGetAll, cmdHDel, cmdHExists, cmdHIncrBy, cmdHLen,
		cmdLPush, cmdRPush, cmdLPop, cmdRPop, cmdLRange, cmdLIndex, cmdLLen, cmdLPos, cmdLRem, cmdLSet, cmdLTrim, cmdRPopLPush,
		cmdSAdd, cmdSRem, cmdSIsMember, cmdSMembers, cmdSCard, cmdPFAdd, cmdPFCount,
		cmdZAdd, cmdZIncrBy, cmdZRem, cmdElasticKVZRemFast, cmdZCard, cmdZScore, cmdZCount,
		cmdZRange, cmdZRevRange, cmdZRangeByScore, cmdZRevRangeByScore, cmdZRemRangeByScore, cmdZRemRangeByRank, cmdZPopMin,
		cmdXAdd, cmdXTrim, cmdXLen, cmdXRange, cmdXRevRange, cmdXRead, cmdBZPopMin,
		cmdEval, cmdEvalSHA, cmdSubscribe, cmdPublish, cmdPubSub,
		cmdHello, cmdClient, cmdCommand, cmdInfo, cmdQuit, cmdSelect, cmdKeys, cmdScan, cmdDBSize,
		cmdFlushDB, cmdFlushAll, cmdFlushLegacy, cmdMulti, cmdExec, cmdDiscard:
		return true
	default:
		return false
	}
}

func (r *RedisServer) dispatchOwnedCommand(conn redcon.Conn, name string, handler func(redcon.Conn, redcon.Command), cmd redcon.Command, start time.Time) {
	owner, err := r.requestAdmission(r.baseRequestContext())
	if err != nil || owner == nil {
		if owner != nil {
			defer owner.CancelAndJoin()
			_ = owner.Finish(err)
		}
		conn.WriteError(errRedisAdmission.Error())
		return
	}
	if owner.Context() == nil {
		defer owner.CancelAndJoin()
		_ = owner.Finish(errRedisAdmission)
		conn.WriteError(errRedisAdmission.Error())
		return
	}
	ctx := context.WithValue(owner.Context(), redisOwnedRequestKey{}, true)
	ctx, cancel := context.WithCancel(ctx)
	readRequest := kv.NewReadRequest(ctx, r.coordinator, r.store, r.readTracker)
	ctx = readRequest.Context()
	stopServer := context.AfterFunc(r.baseRequestContext(), cancel)
	carrier := &redisOwnedConn{readRequest: readRequest, Conn: conn, owner: owner, ctx: ctx, serverCtx: r.baseRequestContext(), writer: r.newReplyBuffer(), initialDone: make(chan struct{}), cancel: cancel, stopServer: stopServer, mayWrite: redisCommandMayWrite(name)}
	completed := false
	defer func() {
		if carrier.detached && !completed {
			carrier.cancel()
		}
		close(carrier.initialDone)
		if !carrier.detached {
			carrier.release()
		}
	}()
	defer carrier.finishOnExit()
	if !redisOwnedCommandSupported(name) {
		carrier.WriteError(errRedisOwnershipUnsupported.Error())
	} else {
		r.dispatchCommandUnowned(carrier, name, func(c redcon.Conn, command redcon.Command) {
			// Validate the captured owner at execution, never mint a replacement.
			if err := errors.Join(carrier.cancellationError(), owner.Validate()); err != nil {
				carrier.resultErr = errors.Join(carrier.resultErr, err)
				return
			}
			carrier.workStarted = true
			handler(c, command)
		}, cmd, start)
	}
	carrier.publishIfAttached()
	completed = true
}

func (c *redisOwnedConn) publishIfAttached() {
	if !c.detached {
		c.publish()
	}
}

// This carrier belongs to one command, not the connection or a copied server.
// RESP stays buffered until its terminal result is checked; no partial success
// is published if a context-insensitive local read returns after revocation.
type redisOwnedConn struct {
	readRequest *kv.ReadRequest
	redcon.Conn
	owner          RequestOwnership
	ctx            context.Context
	serverCtx      context.Context
	writer         *redisReplyBuffer
	resultErr      error
	errorReply     string
	finished       bool
	detached       bool
	initialDone    chan struct{}
	cancel         context.CancelFunc
	stopServer     func() bool
	releaseOnce    sync.Once
	mayWrite       bool
	workStarted    bool
	closeRequested bool
}

// A command's Close is an intent: its private RESP must pass terminal
// validation before redcon flushes and closes the socket. redcon dispatches
// already parsed pipeline commands even after Close, so discard them now.
func (c *redisOwnedConn) Close() error {
	c.closeRequested = true
	c.ReadPipeline()
	return nil
}

func (c *redisOwnedConn) closeIfRequested() {
	if c.closeRequested {
		c.closeRequested = false
		_ = c.Conn.Close()
	}
}

func (c *redisOwnedConn) cancellationError() error {
	if c.serverCtx != nil {
		return errors.Join(c.ctx.Err(), c.serverCtx.Err())
	}
	return errors.WithStack(c.ctx.Err())
}

func redisCommandMayWrite(name string) bool {
	if name == cmdExec || name == cmdPublish {
		return true
	}
	for _, spec := range redisCommandSpecs {
		if spec.Constant == name {
			for _, flag := range spec.Flags {
				if flag == redisCmdFlagWrite {
					return true
				}
			}
		}
	}
	return false
}

func (c *redisOwnedConn) release() {
	c.releaseOnce.Do(func() {
		c.stopServer()
		c.cancel()
		c.owner.CancelAndJoin()
		if c.readRequest != nil {
			c.readRequest.Close()
		}
		c.writer.release()
	})
}

type redisOwnedRequestKey struct{}

func (r *RedisServer) baseRequestContext() context.Context {
	if r != nil && r.baseCtx != nil {
		return r.baseCtx
	}
	return context.Background()
}

func (r *RedisServer) redisContext(contexts ...context.Context) context.Context {
	if len(contexts) != 0 && contexts[0] != nil && (r == nil || r.requestAdmission == nil || contexts[0].Value(redisOwnedRequestKey{}) != nil) {
		return contexts[0]
	}
	if r != nil && r.requestAdmission != nil {
		ctx, cancel := context.WithCancelCause(r.baseRequestContext())
		cancel(errRedisAdmission)
		return ctx
	}
	return r.baseRequestContext()
}

// The legacy urgent queue carries no owner. Protected reads compact inline
// under their captured H instead of enqueueing work that could mint a fresh H.
// The original ErrDeltaScanTruncated is retained; clients may retry the read.
func (r *RedisServer) triggerUrgentCompactionContext(ctx context.Context, kind string, key []byte) {
	if ctx.Value(redisOwnedRequestKey{}) != nil {
		if r.compactor != nil && ctx.Err() == nil {
			r.compactor.compactUrgentKey(ctx, urgentCompactionRequest{typeName: kind, userKey: key})
		}
		return
	}
	r.triggerUrgentCompaction(kind, key)
}

func (c *redisOwnedConn) requestContext() context.Context { return c.ctx }

func (c *redisOwnedConn) recordRequestError(err error) {
	if len(c.writer.Buffer()) == 0 {
		c.resultErr = errors.Join(c.resultErr, err)
	}
}
func (c *redisMetricsConn) recordRequestError(err error) {
	if recorder, ok := c.Conn.(interface{ recordRequestError(error) }); ok {
		recorder.recordRequestError(err)
	}
}

func (r *RedisServer) requireRequestContext(conn redcon.Conn) bool {
	if r.requestAdmission == nil {
		return true
	}
	ctx := r.handlerContext(conn)
	if ctx.Err() != nil || ctx.Value(redisOwnedRequestKey{}) == nil {
		conn.WriteError(errRedisAdmission.Error())
		return false
	}
	return true
}

func (c *redisMetricsConn) requestContext() context.Context {
	if carrier, ok := c.Conn.(interface{ requestContext() context.Context }); ok {
		return carrier.requestContext()
	}
	return nil
}

// Metrics wrappers are borrowed only for synchronous dispatch. A detached
// lifetime must retain the original carrier, never a pooled/resettable wrapper.
func redisLifetimeConn(conn redcon.Conn) redcon.Conn {
	for {
		metrics, ok := conn.(*redisMetricsConn)
		if !ok {
			return conn
		}
		conn = metrics.Conn
	}
}

func (c *redisOwnedConn) finishOnExit() {
	defer c.closeIfRequested()
	if !c.finished {
		if c.detached {
			return
		}
		// Panic paths still release ownership, but publish no success response.
		c.finished = true
		_ = c.owner.Finish(errRedisAdmission)
	}
}

func (c *redisOwnedConn) publish() {
	c.publishResult(true)
}

func (c *redisOwnedConn) publishResult(terminal bool) bool {
	if terminal {
		defer c.closeIfRequested()
	}
	validationErr := errors.Join(c.cancellationError(), c.owner.Validate())
	c.prepareTerminalError(validationErr)
	terminalErr := errors.Join(c.resultErr, validationErr)
	finishedErr := terminalErr
	if terminal {
		c.finished = true
		finishedErr = c.owner.Finish(terminalErr)
	}
	if validationErr != nil || (finishedErr != nil && !errors.Is(finishedErr, c.resultErr)) {
		c.writeTerminalFailure()
		return false
	}
	if c.resultErr != nil {
		c.writeTerminalFailure()
		return true
	}
	c.Conn.WriteRaw(c.writer.Buffer())
	return true
}

func (c *redisOwnedConn) prepareTerminalError(validationErr error) {
	if c.resultErr == nil && c.workStarted && c.mayWrite && (validationErr != nil || c.writer.err != nil) {
		c.resultErr = kv.ErrTransactionOutcomeUnknown
	}
	c.resultErr = errors.Join(c.resultErr, c.writer.err)
}

func (c *redisOwnedConn) writeTerminalFailure() {
	if c.writer.err != nil {
		message := errRedisReplyTooLarge.Error()
		if errors.Is(c.writer.err, errRedisReplyBudget) {
			message = errRedisReplyBudget.Error()
		}
		if c.workStarted && c.mayWrite {
			c.Conn.WriteError(kv.ErrTransactionOutcomeUnknown.Error() + "; " + message)
			return
		}
		c.Conn.WriteError(message)
		return
	}
	if c.resultErr != nil {
		message := c.errorReply
		if message == "" {
			message = c.resultErr.Error()
		}
		if len(message) > redisMaxReplyBytes-redisRESPLineOverhead {
			message = errRedisReplyTooLarge.Error()
		}
		c.Conn.WriteError(message)
		return
	}
	if c.workStarted && c.mayWrite {
		c.Conn.WriteError(kv.ErrTransactionOutcomeUnknown.Error())
		return
	}
	c.Conn.WriteError(errRedisAdmission.Error())
}

func (c *redisOwnedConn) WriteError(message string) {
	top := len(c.writer.Buffer()) == 0
	c.writer.WriteError(message)
	if top && c.writer.err == nil {
		c.errorReply = message
		if c.resultErr == nil {
			c.resultErr = errors.New(message)
		}
	}
}
func (c *redisOwnedConn) WriteString(value string)     { c.writer.WriteString(value) }
func (c *redisOwnedConn) WriteBulk(value []byte)       { c.writer.WriteBulk(value) }
func (c *redisOwnedConn) WriteBulkString(value string) { c.writer.WriteBulkString(value) }
func (c *redisOwnedConn) WriteInt(value int)           { c.writer.WriteInt(value) }
func (c *redisOwnedConn) WriteInt64(value int64)       { c.writer.WriteInt64(value) }
func (c *redisOwnedConn) WriteUint64(value uint64)     { c.writer.WriteUint64(value) }
func (c *redisOwnedConn) WriteArray(value int)         { c.writer.WriteArray(value) }
func (c *redisOwnedConn) WriteNull()                   { c.writer.WriteNull() }
func (c *redisOwnedConn) WriteRaw(value []byte)        { c.writer.WriteRaw(value) }
func (c *redisOwnedConn) WriteAny(value any)           { c.writer.WriteAny(value) }
func (c *redisOwnedConn) WriteBulkFrom(size int64, reader io.Reader) {
	c.writer.WriteBulkFrom(size, reader)
}

// Detachment transfers (not remints) the root to the pubsub reader. Cleanup
// joins the cancellation callback and initial handler before finishing it.
func (c *redisOwnedConn) Detach() redcon.DetachedConn {
	if c.detached {
		panic("Redis request already detached")
	}
	c.detached = true
	raw := c.Conn.Detach()
	d := &redisOwnedDetachedConn{DetachedConn: raw, root: c, writer: &redisReplyBuffer{budget: c.writer.budget}, cancelDone: make(chan struct{})}
	d.stopCancel = context.AfterFunc(c.ctx, func() { defer close(d.cancelDone); _ = raw.NetConn().Close() })
	return d
}

type redisOwnedDetachedConn struct {
	redcon.DetachedConn
	root       *redisOwnedConn
	writer     *redisReplyBuffer
	stopCancel func() bool
	cancelDone chan struct{}
}

func (c *redisOwnedDetachedConn) ReadCommand() (redcon.Command, error) {
	if err := errors.Join(c.root.cancellationError(), c.root.owner.Validate()); err != nil {
		return redcon.Command{}, errors.WithStack(err)
	}
	cmd, err := c.DetachedConn.ReadCommand()
	if err != nil {
		return cmd, errors.WithStack(err)
	}
	if err = errors.Join(c.root.cancellationError(), c.root.owner.Validate()); err != nil {
		return redcon.Command{}, errors.WithStack(err)
	}
	return cmd, nil
}
func (c *redisOwnedDetachedConn) Flush() error {
	if err := errors.Join(c.writer.err, c.root.cancellationError(), c.root.owner.Validate()); err != nil {
		_ = c.DetachedConn.NetConn().Close()
		return errors.WithStack(err)
	}
	if sink, ok := c.root.Conn.(*redisReplySink); ok {
		if err := sink.writeReply(c.writer.Buffer()); err != nil {
			_ = c.DetachedConn.NetConn().Close()
			return err
		}
	} else {
		// Non-network test carriers retain their original sink contract.
		c.DetachedConn.WriteRaw(c.writer.Buffer())
		if err := c.DetachedConn.Flush(); err != nil {
			return errors.WithStack(err)
		}
	}
	c.writer.release()
	c.writer.reset()
	return nil
}
func (c *redisOwnedDetachedConn) finish() {
	defer c.writer.release()
	<-c.root.initialDone
	_ = c.DetachedConn.NetConn().Close()
	if !c.stopCancel() {
		<-c.cancelDone
	}
	defer c.root.release()
	c.root.finished = true
	_ = c.root.owner.Finish(errors.Join(c.root.cancellationError(), c.writer.err))
}
func (c *redisOwnedDetachedConn) WriteError(v string)                { c.writer.WriteError(v) }
func (c *redisOwnedDetachedConn) WriteString(v string)               { c.writer.WriteString(v) }
func (c *redisOwnedDetachedConn) WriteBulk(v []byte)                 { c.writer.WriteBulk(v) }
func (c *redisOwnedDetachedConn) WriteBulkString(v string)           { c.writer.WriteBulkString(v) }
func (c *redisOwnedDetachedConn) WriteInt(v int)                     { c.writer.WriteInt(v) }
func (c *redisOwnedDetachedConn) WriteInt64(v int64)                 { c.writer.WriteInt64(v) }
func (c *redisOwnedDetachedConn) WriteUint64(v uint64)               { c.writer.WriteUint64(v) }
func (c *redisOwnedDetachedConn) WriteArray(v int)                   { c.writer.WriteArray(v) }
func (c *redisOwnedDetachedConn) WriteNull()                         { c.writer.WriteNull() }
func (c *redisOwnedDetachedConn) WriteRaw(v []byte)                  { c.writer.WriteRaw(v) }
func (c *redisOwnedDetachedConn) WriteAny(v any)                     { c.writer.WriteAny(v) }
func (c *redisOwnedDetachedConn) WriteBulkFrom(n int64, r io.Reader) { c.writer.WriteBulkFrom(n, r) }

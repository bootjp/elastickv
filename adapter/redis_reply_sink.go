package adapter

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

// Protected handlers never hand RESP to redcon's copying writer. Each complete
// validated publication is written synchronously, in serving/transaction order;
// pubsub uses its existing per-subscriber serialization. No transport copy or
// idle connection backing capacity survives the write. Legacy never uses this.
type redisReplySink struct {
	redcon.Conn
	server *RedisServer
}

func (r *RedisServer) replyConn(conn redcon.Conn) redcon.Conn {
	if r.requestAdmission == nil || conn.NetConn() == nil {
		return conn
	}
	if _, ok := conn.(*redisReplySink); ok {
		return conn
	}
	return &redisReplySink{Conn: conn, server: r}
}

func (s *redisReplySink) writeReply(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	ctx := s.server.baseRequestContext()
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = s.NetConn().Close(); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
	}()
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	// This server owns protected write deadlines. net.Conn cannot expose an
	// earlier deadline; do not change its independent read deadline.
	timeout := s.server.replyWriteTimeout
	if timeout <= 0 {
		timeout = redisDispatchTimeout
	}
	if err := s.NetConn().SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = s.NetConn().SetWriteDeadline(time.Time{}) }()
	for len(p) != 0 {
		n, err := s.NetConn().Write(p)
		if err != nil {
			return errors.WithStack(err)
		}
		if n <= 0 || n > len(p) {
			return errors.WithStack(io.ErrShortWrite)
		}
		p = p[n:]
	}
	return nil
}

func (s *redisReplySink) WriteRaw(p []byte) {
	if err := s.writeReply(p); err != nil {
		s.abortReply()
	}
}

func (s *redisReplySink) abortReply() {
	s.ReadPipeline()
	_ = s.Close()
}

// Pre-admission errors and other direct control replies are charged too. A
// full budget closes the connection rather than allocating an uncharged error.
func (s *redisReplySink) emit(write func(*redisReplyBuffer)) {
	b := s.server.newReplyBuffer()
	defer b.release()
	write(b)
	if b.err != nil {
		s.abortReply()
		return
	}
	s.WriteRaw(b.Buffer())
}

func (s *redisReplySink) WriteError(v string) { s.emit(func(b *redisReplyBuffer) { b.WriteError(v) }) }
func (s *redisReplySink) WriteString(v string) {
	s.emit(func(b *redisReplyBuffer) { b.WriteString(v) })
}
func (s *redisReplySink) WriteBulk(v []byte) { s.emit(func(b *redisReplyBuffer) { b.WriteBulk(v) }) }
func (s *redisReplySink) WriteBulkString(v string) {
	s.emit(func(b *redisReplyBuffer) { b.WriteBulkString(v) })
}
func (s *redisReplySink) WriteInt(v int)     { s.emit(func(b *redisReplyBuffer) { b.WriteInt(v) }) }
func (s *redisReplySink) WriteInt64(v int64) { s.emit(func(b *redisReplyBuffer) { b.WriteInt64(v) }) }
func (s *redisReplySink) WriteUint64(v uint64) {
	s.emit(func(b *redisReplyBuffer) { b.WriteUint64(v) })
}
func (s *redisReplySink) WriteArray(v int) { s.emit(func(b *redisReplyBuffer) { b.WriteArray(v) }) }
func (s *redisReplySink) WriteNull()       { s.emit(func(b *redisReplyBuffer) { b.WriteNull() }) }
func (s *redisReplySink) WriteAny(v any)   { s.emit(func(b *redisReplyBuffer) { b.WriteAny(v) }) }
func (s *redisReplySink) WriteBulkFrom(n int64, r io.Reader) {
	s.emit(func(b *redisReplyBuffer) { b.WriteBulkFrom(n, r) })
}

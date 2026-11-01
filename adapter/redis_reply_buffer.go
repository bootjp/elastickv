package adapter

import (
	"io"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

const (
	redisMaxReplyBytes    = 64 << 20
	redisRESPBulkOverhead = 5
	redisRESPLineOverhead = 3
)

var errRedisReplyTooLarge = errors.New("ERR reply exceeds 64 MiB; command effects may have committed")

// Each command/message owns its buffer; reserve runs before RESP allocation.
type redisReplyBuffer struct {
	data []byte
	err  error
}

func (b *redisReplyBuffer) reserve(n int) bool {
	if b.err != nil {
		return false
	}
	if n < 0 || n > redisMaxReplyBytes-len(b.data) {
		b.err = errRedisReplyTooLarge
		b.data = nil
		return false
	}
	return true
}
func (b *redisReplyBuffer) Buffer() []byte { return b.data }
func (b *redisReplyBuffer) WriteRaw(p []byte) {
	if b.reserve(len(p)) {
		b.data = append(b.data, p...)
	}
}
func (b *redisReplyBuffer) WriteBulk(p []byte) {
	if b.reserve(len(p) + len(strconv.Itoa(len(p))) + redisRESPBulkOverhead) {
		b.data = redcon.AppendBulk(b.data, p)
	}
}
func (b *redisReplyBuffer) WriteBulkString(p string) {
	if b.reserve(len(p) + len(strconv.Itoa(len(p))) + redisRESPBulkOverhead) {
		b.data = redcon.AppendBulkString(b.data, p)
	}
}
func (b *redisReplyBuffer) WriteString(p string) {
	if b.reserve(len(p) + redisRESPLineOverhead) {
		b.data = redcon.AppendString(b.data, p)
	}
}
func (b *redisReplyBuffer) WriteError(p string) {
	if b.reserve(len(p) + redisRESPLineOverhead) {
		b.data = redcon.AppendError(b.data, p)
	}
}
func (b *redisReplyBuffer) WriteInt(p int) { b.WriteInt64(int64(p)) }
func (b *redisReplyBuffer) WriteInt64(p int64) {
	if b.reserve(len(strconv.FormatInt(p, 10)) + redisRESPLineOverhead) {
		b.data = redcon.AppendInt(b.data, p)
	}
}
func (b *redisReplyBuffer) WriteUint64(p uint64) {
	if b.reserve(len(strconv.FormatUint(p, 10)) + redisRESPLineOverhead) {
		b.data = redcon.AppendUint(b.data, p)
	}
}
func (b *redisReplyBuffer) WriteArray(p int) {
	if b.reserve(len(strconv.Itoa(p)) + redisRESPLineOverhead) {
		b.data = redcon.AppendArray(b.data, p)
	}
}
func (b *redisReplyBuffer) WriteNull() {
	if b.reserve(redisRESPBulkOverhead) {
		b.data = redcon.AppendNull(b.data)
	}
}
func (b *redisReplyBuffer) WriteAny(p any) {
	switch v := p.(type) {
	case nil:
		b.WriteNull()
	case string:
		b.WriteBulkString(v)
	case []byte:
		b.WriteBulk(v)
	case int:
		b.WriteInt(v)
	case int64:
		b.WriteInt64(v)
	case uint64:
		b.WriteUint64(v)
	case error:
		b.WriteError(v.Error())
	default:
		b.err = errRedisOwnershipUnsupported
		b.data = nil
	}
}
func (b *redisReplyBuffer) WriteBulkFrom(size int64, reader io.Reader) {
	if size < 0 || size > int64(redisMaxReplyBytes-len(b.data)) {
		b.err = errRedisReplyTooLarge
		b.data = nil
		return
	}
	n := int(size)
	if !b.reserve(n + len(strconv.Itoa(n)) + redisRESPBulkOverhead) {
		return
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(reader, p); err != nil {
		b.err = errors.WithStack(err)
		b.data = nil
		return
	}
	b.WriteBulk(p)
}

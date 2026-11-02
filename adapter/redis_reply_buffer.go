package adapter

import (
	"io"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

const (
	redisMaxReplyBytes        = 64 << 20
	redisRESPBulkOverhead     = 5
	redisRESPLineOverhead     = 3
	redisReplyMinimumCapacity = 64
	redisReplyCapacityGrowth  = 2
	redisReplyDecimalBase     = 10
)

var errRedisReplyTooLarge = errors.New("ERR reply exceeds 64 MiB; command effects may have committed")
var errRedisReplyBudget = errors.New("ERR server reply capacity exhausted")

// Each command/message owns its buffer; reserve runs before RESP allocation.
type redisReplyBuffer struct {
	data    []byte
	err     error
	budget  *redisReplyBudget
	charged int
}

func (b *redisReplyBuffer) reserve(n int) bool {
	if b.err != nil {
		return false
	}
	if n < 0 || n > redisMaxReplyBytes-len(b.data) {
		b.fail(errRedisReplyTooLarge)
		return false
	}
	needed := len(b.data) + n
	if needed <= cap(b.data) {
		return true
	}
	capacity := max(redisReplyMinimumCapacity, cap(b.data))
	for capacity < needed {
		capacity = min(redisMaxReplyBytes, capacity*redisReplyCapacityGrowth)
	}
	charge := redisReplyCapacityCharge(capacity)
	if !b.budget.acquire(charge) {
		b.fail(errRedisReplyBudget)
		return false
	}
	next := make([]byte, len(b.data), capacity)
	copy(next, b.data)
	b.data = next
	b.budget.release(b.charged)
	b.charged = charge
	return true
}

func (b *redisReplyBuffer) fail(err error) {
	if b.err == nil {
		b.err = err
	}
	b.data = b.data[:0]
}
func (b *redisReplyBuffer) reset() { b.err, b.data = nil, b.data[:0] }
func (b *redisReplyBuffer) release() {
	b.data = nil
	b.budget.release(b.charged)
	b.charged = 0
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
	b.writeLine('+', p)
}
func (b *redisReplyBuffer) WriteError(p string) {
	b.writeLine('-', p)
}
func (b *redisReplyBuffer) writeLine(prefix byte, p string) {
	if !b.reserve(len(p) + redisRESPLineOverhead) {
		return
	}
	b.data = append(b.data, prefix)
	start := len(b.data)
	b.data = append(b.data, p...)
	// Match redcon's newline sanitation without its full-size temporary string.
	for i := start; i < len(b.data); i++ {
		if b.data[i] == '\r' || b.data[i] == '\n' {
			b.data[i] = ' '
		}
	}
	b.data = append(b.data, '\r', '\n')
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
		b.fail(errRedisOwnershipUnsupported)
	}
}
func (b *redisReplyBuffer) WriteBulkFrom(size int64, reader io.Reader) {
	if size < 0 || size > int64(redisMaxReplyBytes-len(b.data)) {
		b.fail(errRedisReplyTooLarge)
		return
	}
	n := int(size)
	if !b.reserve(n + len(strconv.Itoa(n)) + redisRESPBulkOverhead) {
		return
	}
	b.data = append(b.data, '$')
	b.data = strconv.AppendInt(b.data, size, redisReplyDecimalBase)
	b.data = append(b.data, '\r', '\n')
	start := len(b.data)
	b.data = b.data[:start+n]
	if _, err := io.ReadFull(reader, b.data[start:]); err != nil {
		b.fail(errors.WithStack(err))
		return
	}
	b.data = append(b.data, '\r', '\n')
}

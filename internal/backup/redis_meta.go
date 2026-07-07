package backup

import (
	"encoding/binary"
	"math"

	cockroachdberr "github.com/cockroachdb/errors"
)

const redisCountMetaInlineBytes = redisUint64Bytes * 2

func decodeRedisCountMeta(value []byte, invalidMeta error) (declaredLen int64, expireAtMs uint64, hasTTL bool, err error) {
	if len(value) != redisUint64Bytes && len(value) != redisCountMetaInlineBytes {
		return 0, 0, false, cockroachdberr.Wrapf(invalidMeta,
			"length %d not in {%d,%d}", len(value), redisUint64Bytes, redisCountMetaInlineBytes)
	}
	rawLen := binary.BigEndian.Uint64(value[0:redisUint64Bytes])
	if rawLen > math.MaxInt64 {
		return 0, 0, false, cockroachdberr.Wrapf(invalidMeta,
			"declared len %d overflows int64", rawLen)
	}
	if len(value) == redisCountMetaInlineBytes {
		expireAtMs = binary.BigEndian.Uint64(value[redisUint64Bytes:redisCountMetaInlineBytes])
		hasTTL = expireAtMs != 0
	}
	return int64(rawLen), expireAtMs, hasTTL, nil //nolint:gosec // rawLen is bounded above.
}

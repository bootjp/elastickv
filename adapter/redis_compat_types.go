package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	redisHashPrefix   = "!redis|hash|"
	redisSetPrefix    = "!redis|set|"
	redisZSetPrefix   = "!redis|zset|"
	redisHLLPrefix    = "!redis|hll|"
	redisStreamPrefix = "!redis|stream|"
	redisTTLPrefix    = "!redis|ttl|"
)

var redisInternalPrefixes = []string{
	redisHashPrefix,
	redisSetPrefix,
	redisHLLPrefix,
	redisZSetPrefix,
	redisStreamPrefix,
}

const (
	redisUint64Bytes = 8
)

type redisValueType string

const (
	redisTypeNone   redisValueType = "none"
	redisTypeString redisValueType = "string"
	redisTypeList   redisValueType = "list"
	redisTypeHash   redisValueType = "hash"
	redisTypeSet    redisValueType = "set"
	redisTypeZSet   redisValueType = "zset"
	redisTypeStream redisValueType = "stream"
)

type redisHashValue map[string]string

type redisSetValue struct {
	Members []string `json:"members,omitempty"`
}

type redisZSetEntry struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

type redisZSetValue struct {
	Entries []redisZSetEntry `json:"entries,omitempty"`
}

type redisStreamEntry struct {
	ID     string   `json:"id"`
	Fields []string `json:"fields,omitempty"`

	parsedID      redisStreamID
	parsedIDValid bool
}

type redisStreamValue struct {
	Entries []redisStreamEntry `json:"entries,omitempty"`
}

type redisStreamID struct {
	ms  uint64
	seq uint64
}

func redisHashKey(userKey []byte) []byte {
	return append([]byte(redisHashPrefix), userKey...)
}

func redisSetKey(userKey []byte) []byte {
	return append([]byte(redisSetPrefix), userKey...)
}

func redisHLLKey(userKey []byte) []byte {
	return append([]byte(redisHLLPrefix), userKey...)
}

func redisZSetKey(userKey []byte) []byte {
	return append([]byte(redisZSetPrefix), userKey...)
}

func redisStreamKey(userKey []byte) []byte {
	return append([]byte(redisStreamPrefix), userKey...)
}

func redisTTLKey(userKey []byte) []byte {
	return append([]byte(redisTTLPrefix), userKey...)
}

func redisExactSetStorageKey(kind string, userKey []byte) []byte {
	switch kind {
	case "set":
		return redisSetKey(userKey)
	case "hll":
		return redisHLLKey(userKey)
	default:
		return nil
	}
}

func isRedisTTLKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(redisTTLPrefix))
}

func extractRedisInternalUserKey(key []byte) []byte {
	switch {
	case bytes.HasPrefix(key, []byte(redisHashPrefix)):
		return bytes.TrimPrefix(key, []byte(redisHashPrefix))
	case bytes.HasPrefix(key, []byte(redisSetPrefix)):
		return bytes.TrimPrefix(key, []byte(redisSetPrefix))
	case bytes.HasPrefix(key, []byte(redisZSetPrefix)):
		return bytes.TrimPrefix(key, []byte(redisZSetPrefix))
	case bytes.HasPrefix(key, []byte(redisHLLPrefix)):
		return bytes.TrimPrefix(key, []byte(redisHLLPrefix))
	case bytes.HasPrefix(key, []byte(redisStreamPrefix)):
		return bytes.TrimPrefix(key, []byte(redisStreamPrefix))
	case bytes.HasPrefix(key, []byte(redisTTLPrefix)):
		return nil
	default:
		return nil
	}
}

func newRedisStreamEntry(id string, fields []string) redisStreamEntry {
	entry := redisStreamEntry{ID: id, Fields: fields}
	entry.cacheParsedID()
	return entry
}

func (e *redisStreamEntry) cacheParsedID() {
	e.parsedID, e.parsedIDValid = tryParseRedisStreamID(e.ID)
}

func (e redisStreamEntry) compareID(raw string, parsed redisStreamID, parsedValid bool) int {
	if !e.parsedIDValid {
		return compareRedisStreamID(e.ID, raw)
	}
	return compareParsedRedisStreamID(e.ID, e.parsedID, true, raw, parsed, parsedValid)
}

func (v *redisStreamValue) cacheParsedIDs() {
	for i := range v.Entries {
		v.Entries[i].cacheParsedID()
	}
}

func encodeRedisTTL(expireAt time.Time) []byte {
	ms := max(expireAt.UnixMilli(), 0)
	buf := make([]byte, redisUint64Bytes)
	binary.BigEndian.PutUint64(buf, uint64(ms)) // #nosec G115 -- ms is clamped to non-negative int64 range.
	return buf
}

func decodeRedisTTL(raw []byte) (time.Time, error) {
	if len(raw) != redisUint64Bytes {
		return time.Time{}, errors.WithStack(errors.Newf("invalid ttl length %d", len(raw)))
	}
	ms := min(binary.BigEndian.Uint64(raw), math.MaxInt64)
	return time.UnixMilli(int64(ms)), nil // #nosec G115 -- ms <= MaxInt64 is guaranteed by the check above.
}

func (r *RedisServer) ttlAt(ctx context.Context, userKey []byte, readTS uint64) (*time.Time, error) {
	raw, err := r.store.GetAt(ctx, redisTTLKey(userKey), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, errors.WithStack(err)
	}
	ttl, err := decodeRedisTTL(raw)
	if err != nil {
		return nil, err
	}
	return &ttl, nil
}

func (r *RedisServer) hasExpiredTTLAt(ctx context.Context, userKey []byte, readTS uint64) (bool, error) {
	ttl, err := r.ttlAt(ctx, userKey, readTS)
	if err != nil {
		return false, err
	}
	if ttl == nil {
		return false, nil
	}
	return !ttl.After(time.Now()), nil
}

func ttlMilliseconds(ttl *time.Time) int64 {
	if ttl == nil {
		return -1
	}
	remaining := time.Until(*ttl).Milliseconds()
	if remaining < 0 {
		return -2
	}
	return remaining
}

func sortZSetEntries(entries []redisZSetEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score < entries[j].Score
		}
		return entries[i].Member < entries[j].Member
	})
}

func zsetEntriesToMap(entries []redisZSetEntry) map[string]float64 {
	out := make(map[string]float64, len(entries))
	for _, entry := range entries {
		out[entry.Member] = entry.Score
	}
	return out
}

func zsetMapToEntries(in map[string]float64) []redisZSetEntry {
	out := make([]redisZSetEntry, 0, len(in))
	for member, score := range in {
		out = append(out, redisZSetEntry{Member: member, Score: score})
	}
	sortZSetEntries(out)
	return out
}

func parseRedisStreamID(raw string) (redisStreamID, error) {
	sep := strings.IndexByte(raw, '-')
	if sep <= 0 || sep >= len(raw)-1 || strings.IndexByte(raw[sep+1:], '-') >= 0 {
		return redisStreamID{}, errors.WithStack(errors.Newf("invalid stream id %q", raw))
	}
	ms, err := strconv.ParseUint(raw[:sep], 10, 64)
	if err != nil {
		return redisStreamID{}, errors.WithStack(err)
	}
	seq, err := strconv.ParseUint(raw[sep+1:], 10, 64)
	if err != nil {
		return redisStreamID{}, errors.WithStack(err)
	}
	return redisStreamID{ms: ms, seq: seq}, nil
}

func compareRedisStreamID(left, right string) int {
	lid, lok := tryParseRedisStreamID(left)
	rid, rok := tryParseRedisStreamID(right)
	return compareParsedRedisStreamID(left, lid, lok, right, rid, rok)
}

func tryParseRedisStreamID(raw string) (redisStreamID, bool) {
	id, err := parseRedisStreamID(raw)
	return id, err == nil
}

func compareParsedRedisStreamID(leftRaw string, left redisStreamID, leftValid bool, rightRaw string, right redisStreamID, rightValid bool) int {
	switch {
	case !leftValid && !rightValid:
		return strings.Compare(leftRaw, rightRaw)
	case !leftValid:
		return -1
	case !rightValid:
		return 1
	case left.ms < right.ms:
		return -1
	case left.ms > right.ms:
		return 1
	case left.seq < right.seq:
		return -1
	case left.seq > right.seq:
		return 1
	default:
		return 0
	}
}

func formatRedisFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

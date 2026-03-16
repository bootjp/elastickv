package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
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
	redisUint64Bytes   = 8
	redisStreamIDParts = 2
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

func marshalHashValue(v redisHashValue) ([]byte, error) {
	if v == nil {
		v = redisHashValue{}
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return payload, nil
}

func unmarshalHashValue(raw []byte) (redisHashValue, error) {
	if len(raw) == 0 {
		return redisHashValue{}, nil
	}
	out := redisHashValue{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, errors.WithStack(err)
	}
	return out, nil
}

func marshalSetValue(v redisSetValue) ([]byte, error) {
	sort.Strings(v.Members)
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return payload, nil
}

func unmarshalSetValue(raw []byte) (redisSetValue, error) {
	if len(raw) == 0 {
		return redisSetValue{}, nil
	}
	var out redisSetValue
	if err := json.Unmarshal(raw, &out); err != nil {
		return redisSetValue{}, errors.WithStack(err)
	}
	sort.Strings(out.Members)
	return out, nil
}

func marshalZSetValue(v redisZSetValue) ([]byte, error) {
	sortZSetEntries(v.Entries)
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return payload, nil
}

func unmarshalZSetValue(raw []byte) (redisZSetValue, error) {
	if len(raw) == 0 {
		return redisZSetValue{}, nil
	}
	var out redisZSetValue
	if err := json.Unmarshal(raw, &out); err != nil {
		return redisZSetValue{}, errors.WithStack(err)
	}
	sortZSetEntries(out.Entries)
	return out, nil
}

func marshalStreamValue(v redisStreamValue) ([]byte, error) {
	sort.SliceStable(v.Entries, func(i, j int) bool {
		return compareRedisStreamID(v.Entries[i].ID, v.Entries[j].ID) < 0
	})
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return payload, nil
}

func unmarshalStreamValue(raw []byte) (redisStreamValue, error) {
	if len(raw) == 0 {
		return redisStreamValue{}, nil
	}
	var out redisStreamValue
	if err := json.Unmarshal(raw, &out); err != nil {
		return redisStreamValue{}, errors.WithStack(err)
	}
	sort.SliceStable(out.Entries, func(i, j int) bool {
		return compareRedisStreamID(out.Entries[i].ID, out.Entries[j].ID) < 0
	})
	return out, nil
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
	parts := strings.Split(raw, "-")
	if len(parts) != redisStreamIDParts {
		return redisStreamID{}, errors.WithStack(errors.Newf("invalid stream id %q", raw))
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return redisStreamID{}, errors.WithStack(err)
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return redisStreamID{}, errors.WithStack(err)
	}
	return redisStreamID{ms: ms, seq: seq}, nil
}

func compareRedisStreamID(left, right string) int {
	lid, lerr := parseRedisStreamID(left)
	rid, rerr := parseRedisStreamID(right)
	switch {
	case lerr != nil && rerr != nil:
		return strings.Compare(left, right)
	case lerr != nil:
		return -1
	case rerr != nil:
		return 1
	}

	switch {
	case lid.ms < rid.ms:
		return -1
	case lid.ms > rid.ms:
		return 1
	case lid.seq < rid.seq:
		return -1
	case lid.seq > rid.seq:
		return 1
	default:
		return 0
	}
}

func formatRedisFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

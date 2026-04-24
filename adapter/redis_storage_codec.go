package adapter

import (
	"bytes"
	"sort"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	gproto "google.golang.org/protobuf/proto"
)

var (
	storedRedisHashProtoPrefix        = []byte{0x00, 'R', 'H', 0x01}
	storedRedisSetProtoPrefix         = []byte{0x00, 'R', 'S', 0x01}
	storedRedisZSetProtoPrefix        = []byte{0x00, 'R', 'Z', 0x01}
	storedRedisStreamProtoPrefix      = []byte{0x00, 'R', 'X', 0x01}
	storedRedisStreamEntryProtoPrefix = []byte{0x00, 'R', 'X', 'E', 0x01}
	storedRedisMarshalOptions         = gproto.MarshalOptions{Deterministic: true}

	errStoredRedisMessageTooLarge    = errors.New("stored redis message too large")
	errUnrecognizedStoredRedisFormat = errors.New("unrecognized stored redis format")
)

func marshalHashValue(v redisHashValue) ([]byte, error) {
	if v == nil {
		v = redisHashValue{}
	}
	return marshalStoredRedisMessage(storedRedisHashProtoPrefix, redisHashValueToProto(v))
}

func unmarshalHashValue(raw []byte) (redisHashValue, error) {
	if len(raw) == 0 {
		return redisHashValue{}, nil
	}
	if !hasStoredRedisPrefix(raw, storedRedisHashProtoPrefix) {
		return nil, errUnrecognizedStoredRedisFormat
	}
	msg := &pb.RedisHashValue{}
	if err := gproto.Unmarshal(raw[len(storedRedisHashProtoPrefix):], msg); err != nil {
		return nil, errors.WithStack(err)
	}
	return redisHashValueFromProto(msg), nil
}

func marshalSetValue(v redisSetValue) ([]byte, error) {
	sortStrings(v.Members)
	return marshalStoredRedisMessage(storedRedisSetProtoPrefix, redisSetValueToProto(v))
}

func unmarshalSetValue(raw []byte) (redisSetValue, error) {
	if len(raw) == 0 {
		return redisSetValue{}, nil
	}
	if !hasStoredRedisPrefix(raw, storedRedisSetProtoPrefix) {
		return redisSetValue{}, errUnrecognizedStoredRedisFormat
	}
	msg := &pb.RedisSetValue{}
	if err := gproto.Unmarshal(raw[len(storedRedisSetProtoPrefix):], msg); err != nil {
		return redisSetValue{}, errors.WithStack(err)
	}
	out := redisSetValueFromProto(msg)
	sortStrings(out.Members)
	return out, nil
}

func marshalZSetValue(v redisZSetValue) ([]byte, error) {
	sortZSetEntries(v.Entries)
	return marshalStoredRedisMessage(storedRedisZSetProtoPrefix, redisZSetValueToProto(v))
}

func unmarshalZSetValue(raw []byte) (redisZSetValue, error) {
	if len(raw) == 0 {
		return redisZSetValue{}, nil
	}
	if !hasStoredRedisPrefix(raw, storedRedisZSetProtoPrefix) {
		return redisZSetValue{}, errUnrecognizedStoredRedisFormat
	}
	msg := &pb.RedisZSetValue{}
	if err := gproto.Unmarshal(raw[len(storedRedisZSetProtoPrefix):], msg); err != nil {
		return redisZSetValue{}, errors.WithStack(err)
	}
	out := redisZSetValueFromProto(msg)
	sortZSetEntries(out.Entries)
	return out, nil
}

func marshalStreamValue(v redisStreamValue) ([]byte, error) {
	return marshalStoredRedisMessage(storedRedisStreamProtoPrefix, redisStreamValueToProto(v))
}

func unmarshalStreamValue(raw []byte) (redisStreamValue, error) {
	if len(raw) == 0 {
		return redisStreamValue{}, nil
	}
	if !hasStoredRedisPrefix(raw, storedRedisStreamProtoPrefix) {
		return redisStreamValue{}, errUnrecognizedStoredRedisFormat
	}
	msg := &pb.RedisStreamValue{}
	if err := gproto.Unmarshal(raw[len(storedRedisStreamProtoPrefix):], msg); err != nil {
		return redisStreamValue{}, errors.WithStack(err)
	}
	return redisStreamValueFromProto(msg), nil
}

// marshalStreamEntry encodes a single stream entry for the entry-per-key
// layout. The per-entry ID is authoritatively encoded in the storage key;
// we also serialize it into the value so unmarshalStreamEntry can return
// a fully-formed entry without having to parse the key back. Fields are
// serialized into the value as well. The ID duplication costs ~16 bytes
// per entry and is worth the absence of key-parsing plumbing at every
// caller (XREAD, XRANGE, XREVRANGE, Lua streamState).
func marshalStreamEntry(entry redisStreamEntry) ([]byte, error) {
	return marshalStoredRedisMessage(storedRedisStreamEntryProtoPrefix, &pb.RedisStreamEntry{
		Id:     entry.ID,
		Fields: cloneStringSlice(entry.Fields),
	})
}

// unmarshalStreamEntry is the inverse of marshalStreamEntry. The caller
// supplies the raw value bytes loaded from an entry key.
func unmarshalStreamEntry(raw []byte) (redisStreamEntry, error) {
	if len(raw) == 0 {
		return redisStreamEntry{}, nil
	}
	if !hasStoredRedisPrefix(raw, storedRedisStreamEntryProtoPrefix) {
		return redisStreamEntry{}, errUnrecognizedStoredRedisFormat
	}
	msg := &pb.RedisStreamEntry{}
	if err := gproto.Unmarshal(raw[len(storedRedisStreamEntryProtoPrefix):], msg); err != nil {
		return redisStreamEntry{}, errors.WithStack(err)
	}
	return newRedisStreamEntry(msg.GetId(), cloneStringSlice(msg.GetFields())), nil
}

func marshalStoredRedisMessage(prefix []byte, msg gproto.Message) ([]byte, error) {
	body, err := storedRedisMarshalOptions.Marshal(msg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	prefixLen := uint64(len(prefix))
	bodyLen := uint64(len(body))
	maxInt := uint64(int(^uint(0) >> 1))
	if prefixLen > maxInt || bodyLen > maxInt-prefixLen {
		return nil, errStoredRedisMessageTooLarge
	}

	totalLen := int(prefixLen + bodyLen) //nolint:gosec // overflow checked above
	buf := make([]byte, totalLen)
	copy(buf, prefix)
	copy(buf[len(prefix):], body)
	return buf, nil
}

func hasStoredRedisPrefix(b []byte, prefix []byte) bool {
	return len(b) >= len(prefix) && bytes.Equal(b[:len(prefix)], prefix)
}

func redisHashValueToProto(v redisHashValue) *pb.RedisHashValue {
	return &pb.RedisHashValue{Entries: cloneStringMap(v)}
}

func redisHashValueFromProto(msg *pb.RedisHashValue) redisHashValue {
	if msg == nil {
		return redisHashValue{}
	}
	return redisHashValue(cloneStringMap(msg.GetEntries()))
}

func redisSetValueToProto(v redisSetValue) *pb.RedisSetValue {
	return &pb.RedisSetValue{Members: cloneStringSlice(v.Members)}
}

func redisSetValueFromProto(msg *pb.RedisSetValue) redisSetValue {
	if msg == nil {
		return redisSetValue{}
	}
	return redisSetValue{Members: cloneStringSlice(msg.GetMembers())}
}

func redisZSetValueToProto(v redisZSetValue) *pb.RedisZSetValue {
	entries := make([]*pb.RedisZSetEntry, 0, len(v.Entries))
	for _, entry := range v.Entries {
		entries = append(entries, &pb.RedisZSetEntry{
			Member: entry.Member,
			Score:  entry.Score,
		})
	}
	return &pb.RedisZSetValue{Entries: entries}
}

func redisZSetValueFromProto(msg *pb.RedisZSetValue) redisZSetValue {
	if msg == nil {
		return redisZSetValue{}
	}
	entries := make([]redisZSetEntry, 0, len(msg.GetEntries()))
	for _, entry := range msg.GetEntries() {
		entries = append(entries, redisZSetEntry{
			Member: entry.GetMember(),
			Score:  entry.GetScore(),
		})
	}
	return redisZSetValue{Entries: entries}
}

func redisStreamValueToProto(v redisStreamValue) *pb.RedisStreamValue {
	entries := make([]*pb.RedisStreamEntry, 0, len(v.Entries))
	for _, entry := range v.Entries {
		entries = append(entries, &pb.RedisStreamEntry{
			Id:     entry.ID,
			Fields: cloneStringSlice(entry.Fields),
		})
	}
	return &pb.RedisStreamValue{Entries: entries}
}

func redisStreamValueFromProto(msg *pb.RedisStreamValue) redisStreamValue {
	if msg == nil {
		return redisStreamValue{}
	}
	entries := make([]redisStreamEntry, 0, len(msg.GetEntries()))
	for _, entry := range msg.GetEntries() {
		entries = append(entries, newRedisStreamEntry(entry.GetId(), cloneStringSlice(entry.GetFields())))
	}
	return redisStreamValue{Entries: entries}
}

func sortStrings(values []string) {
	sort.Strings(values)
}

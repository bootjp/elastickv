package store

import (
	"encoding/binary"
	"testing"
)

func TestWideColumnExtractorsRejectOverflowingUserKeyLength(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		prefix    string
		suffixLen int
		extract   func([]byte) []byte
	}{
		{name: "list claim", prefix: ListClaimPrefix, suffixLen: sortableInt64Bytes, extract: ExtractListUserKeyFromClaim},
		{name: "hash meta", prefix: HashMetaPrefix, extract: ExtractHashUserKeyFromMeta},
		{name: "hash field", prefix: HashFieldPrefix, extract: ExtractHashUserKeyFromField},
		{name: "hash delta", prefix: HashMetaDeltaPrefix, suffixLen: deltaKeyTSSize + deltaKeySeqSize, extract: ExtractHashUserKeyFromDelta},
		{name: "set meta", prefix: SetMetaPrefix, extract: ExtractSetUserKeyFromMeta},
		{name: "set member", prefix: SetMemberPrefix, extract: ExtractSetUserKeyFromMember},
		{name: "set delta", prefix: SetMetaDeltaPrefix, suffixLen: deltaKeyTSSize + deltaKeySeqSize, extract: ExtractSetUserKeyFromDelta},
		{name: "zset meta", prefix: ZSetMetaPrefix, extract: ExtractZSetUserKeyFromMeta},
		{name: "zset member", prefix: ZSetMemberPrefix, extract: ExtractZSetUserKeyFromMember},
		{name: "zset score", prefix: ZSetScorePrefix, suffixLen: zsetMetaSizeBytes, extract: ExtractZSetUserKeyFromScore},
		{name: "zset delta", prefix: ZSetMetaDeltaPrefix, suffixLen: deltaKeyTSSize + deltaKeySeqSize, extract: ExtractZSetUserKeyFromDelta},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.extract(malformedWideColumnStorageKey(tc.prefix, tc.suffixLen)); got != nil {
				t.Fatalf("overflowing user-key length: want nil, got %q", got)
			}
		})
	}
}

func TestWideColumnExtractorsRoundTripEmptyUserKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		key     []byte
		extract func([]byte) []byte
	}{
		{name: "list claim", key: ListClaimKey(nil, 1), extract: ExtractListUserKeyFromClaim},
		{name: "hash meta", key: HashMetaKey(nil), extract: ExtractHashUserKeyFromMeta},
		{name: "hash field", key: HashFieldKey(nil, []byte("field")), extract: ExtractHashUserKeyFromField},
		{name: "hash delta", key: HashMetaDeltaKey(nil, 2, 3), extract: ExtractHashUserKeyFromDelta},
		{name: "set meta", key: SetMetaKey(nil), extract: ExtractSetUserKeyFromMeta},
		{name: "set member", key: SetMemberKey(nil, []byte("member")), extract: ExtractSetUserKeyFromMember},
		{name: "set delta", key: SetMetaDeltaKey(nil, 2, 3), extract: ExtractSetUserKeyFromDelta},
		{name: "zset meta", key: ZSetMetaKey(nil), extract: ExtractZSetUserKeyFromMeta},
		{name: "zset member", key: ZSetMemberKey(nil, []byte("member")), extract: ExtractZSetUserKeyFromMember},
		{name: "zset score", key: ZSetScoreKey(nil, 1.5, []byte("member")), extract: ExtractZSetUserKeyFromScore},
		{name: "zset delta", key: ZSetMetaDeltaKey(nil, 2, 3), extract: ExtractZSetUserKeyFromDelta},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.extract(tc.key)
			if got == nil {
				t.Fatal("empty user-key extraction returned nil")
			}
			if len(got) != 0 {
				t.Fatalf("empty user-key extraction: want empty, got %q", got)
			}
		})
	}
}

func malformedWideColumnStorageKey(prefix string, suffixLen int) []byte {
	key := make([]byte, 0, len(prefix)+wideColKeyLenSize+suffixLen)
	key = append(key, prefix...)
	var lenPrefix [wideColKeyLenSize]byte
	binary.BigEndian.PutUint32(lenPrefix[:], ^uint32(0))
	key = append(key, lenPrefix[:]...)
	key = append(key, make([]byte, suffixLen)...)
	return key
}

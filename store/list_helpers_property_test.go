package store

import (
	"bytes"
	"math"
	"testing"
	"testing/quick"
)

func TestListMetaRoundTripProperty(t *testing.T) {
	property := func(head, tail, length uint64) bool {
		if head > math.MaxInt64 || tail > math.MaxInt64 || length > math.MaxInt64 {
			return true
		}
		meta := ListMeta{Head: int64(head), Tail: int64(tail), Len: int64(length)}
		encoded, err := MarshalListMeta(meta)
		if err != nil {
			return false
		}
		decoded, err := UnmarshalListMeta(encoded)
		if err != nil {
			return false
		}
		return decoded == meta
	}

	if err := quick.Check(property, nil); err != nil {
		t.Fatal(err)
	}
}

func TestListKeyExtractionProperty(t *testing.T) {
	metaProperty := func(userKey []byte) bool {
		extracted := ExtractListUserKey(ListMetaKey(userKey))
		return bytes.Equal(extracted, userKey)
	}

	itemProperty := func(userKey []byte, seq int64) bool {
		extracted := ExtractListUserKey(ListItemKey(userKey, seq))
		return bytes.Equal(extracted, userKey)
	}

	if err := quick.Check(metaProperty, nil); err != nil {
		t.Fatal(err)
	}
	if err := quick.Check(itemProperty, nil); err != nil {
		t.Fatal(err)
	}
}

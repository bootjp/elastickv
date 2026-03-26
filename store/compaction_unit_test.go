package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompactKeepIndex_EmptyVersions(t *testing.T) {
	assert.Equal(t, -1, compactKeepIndex(nil, 100))
	assert.Equal(t, -1, compactKeepIndex([]VersionedValue{}, 100))
}

func TestCompactKeepIndex_AllNewerThanMinTS(t *testing.T) {
	versions := []VersionedValue{
		{TS: 200, Value: []byte("v200")},
		{TS: 100, Value: []byte("v100")},
	}
	assert.Equal(t, -1, compactKeepIndex(versions, 50))
}

func TestCompactKeepIndex_SingleVersionEqualMinTS(t *testing.T) {
	versions := []VersionedValue{
		{TS: 100, Value: []byte("v100")},
	}
	// keepIdx=0, but 0 <= 0 → returns -1 (nothing to compact)
	assert.Equal(t, -1, compactKeepIndex(versions, 100))
}

func TestCompactKeepIndex_SingleVersionBelowMinTS(t *testing.T) {
	versions := []VersionedValue{
		{TS: 50, Value: []byte("v50")},
	}
	// keepIdx=0 → returns -1 (can't remove anything before idx 0)
	assert.Equal(t, -1, compactKeepIndex(versions, 100))
}

func TestCompactKeepIndex_TwoVersionsOldestKept(t *testing.T) {
	versions := []VersionedValue{
		{TS: 200, Value: []byte("v200")},
		{TS: 100, Value: []byte("v100")},
	}
	// keepIdx=1, which is > 0 → return 1 (keep from idx 1)
	assert.Equal(t, 1, compactKeepIndex(versions, 150))
}

func TestCompactKeepIndex_MultipleVersions(t *testing.T) {
	// Versions stored in ascending order (oldest first) as in mvccStore.
	versions := []VersionedValue{
		{TS: 100, Value: []byte("v100")},
		{TS: 200, Value: []byte("v200")},
		{TS: 300, Value: []byte("v300")},
		{TS: 400, Value: []byte("v400")},
	}
	// minTS=250: iterates from end: i=3(400>250), i=2(300>250), i=1(200<=250)
	// keepIdx=1, > 0 → compact versions before idx 1
	assert.Equal(t, 1, compactKeepIndex(versions, 250))
}

func TestCompactKeepIndex_AllBelowMinTS(t *testing.T) {
	// Versions stored in ascending order (oldest first).
	versions := []VersionedValue{
		{TS: 10, Value: []byte("v10")},
		{TS: 20, Value: []byte("v20")},
		{TS: 30, Value: []byte("v30")},
	}
	// minTS=100: iterates from end: i=2(30<=100) → keepIdx=2, > 0 → return 2
	// compact versions before idx 2 (i.e. TS=10 and TS=20)
	assert.Equal(t, 2, compactKeepIndex(versions, 100))
}

func TestKeyUpperBound_SimpleKey(t *testing.T) {
	upper := keyUpperBound([]byte("abc"))
	assert.Equal(t, []byte("abd"), upper)
}

func TestKeyUpperBound_TrailingFF(t *testing.T) {
	upper := keyUpperBound([]byte("ab\xff"))
	assert.Equal(t, []byte("ac"), upper)
}

func TestKeyUpperBound_AllFF(t *testing.T) {
	upper := keyUpperBound([]byte{0xff, 0xff, 0xff})
	assert.Nil(t, upper, "all 0xFF key has no finite upper bound")
}

func TestKeyUpperBound_SingleByte(t *testing.T) {
	upper := keyUpperBound([]byte{0x00})
	assert.Equal(t, []byte{0x01}, upper)
}

func TestKeyUpperBound_EmptyKey(t *testing.T) {
	upper := keyUpperBound([]byte{})
	// Empty key: loop doesn't execute → returns nil
	assert.Nil(t, upper)
}

func TestPastScanEnd_NilEnd(t *testing.T) {
	assert.False(t, pastScanEnd([]byte("abc"), nil))
}

func TestPastScanEnd_KeyBeforeEnd(t *testing.T) {
	assert.False(t, pastScanEnd([]byte("abc"), []byte("def")))
}

func TestPastScanEnd_KeyEqualsEnd(t *testing.T) {
	// Exclusive: key == end should be past the end
	assert.True(t, pastScanEnd([]byte("abc"), []byte("abc")))
}

func TestPastScanEnd_KeyAfterEnd(t *testing.T) {
	assert.True(t, pastScanEnd([]byte("def"), []byte("abc")))
}

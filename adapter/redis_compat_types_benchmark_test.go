package adapter

import (
	"strconv"
	"testing"
)

func benchmarkRedisStreamValue(entryCount int) redisStreamValue {
	entries := make([]redisStreamEntry, 0, entryCount)
	for i := range entryCount {
		id := strconv.Itoa(1000+i) + "-0"
		entries = append(entries, redisStreamEntry{
			ID:     id,
			Fields: []string{"field", "value", "field2", strconv.Itoa(i)},
		})
	}
	return redisStreamValue{Entries: entries}
}

func BenchmarkUnmarshalStreamValue(b *testing.B) {
	payload, err := marshalStreamValue(benchmarkRedisStreamValue(2048))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, err := unmarshalStreamValue(payload)
		if err != nil {
			b.Fatal(err)
		}
		if len(value.Entries) != 2048 {
			b.Fatalf("unexpected entry count: %d", len(value.Entries))
		}
	}
}

func BenchmarkSelectXReadEntries(b *testing.B) {
	value := benchmarkRedisStreamValue(32768)
	afterID := "25000-0"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selected := selectXReadEntries(value.Entries, afterID, 128)
		if len(selected) != 128 {
			b.Fatalf("unexpected selected entry count: %d", len(selected))
		}
	}
}

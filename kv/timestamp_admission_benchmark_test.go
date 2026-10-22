package kv

import (
	"context"
	"encoding/binary"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
)

var timestampAdmissionBenchmarkOwned []*pb.Request

func BenchmarkTimestampAdmissionOwnership(b *testing.B) {
	for _, size := range []struct {
		name  string
		bytes int
	}{{"32B", 32}, {"1MiB", 1 << 20}} {
		b.Run(size.name, func(b *testing.B) {
			requests := []*pb.Request{{Ts: 110, Mutations: []*pb.Mutation{{
				Key: []byte("key"), Value: make([]byte, size.bytes),
			}}}}
			legacy := &transactionTimestampAdmission{store: store.NewMVCCStore()}
			allocator := &phaseDTestAllocator{}
			released := &transactionTimestampAdmission{store: store.NewMVCCStore(),
				allocator: func() TimestampAllocator { return allocator }}
			value := binary.BigEndian.AppendUint64([]byte{prepareCreationReleased}, 100)
			if err := released.store.PutAt(context.Background(), prepareEvidenceGateKey, value, 1, 0); err != nil {
				b.Fatal(err)
			}
			for _, variant := range []struct {
				name      string
				admission *transactionTimestampAdmission
			}{{"before_unguarded", nil}, {"after_legacy", legacy}, {"after_released", released}} {
				b.Run(variant.name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(size.bytes))
					for range b.N {
						owned, err := variant.admission.prepare(context.Background(), requests)
						if err != nil {
							b.Fatal(err)
						}
						timestampAdmissionBenchmarkOwned = owned
					}
				})
			}
		})
	}
}

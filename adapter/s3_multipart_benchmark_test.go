package adapter

import (
	"context"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	json "github.com/goccy/go-json"
)

// This measures descriptor reads and manifest assembly, not payload I/O or Raft.
func BenchmarkS3LoadMultipartDescriptors(b *testing.B) {
	for _, count := range []int{1, 100, s3MaxPartsPerUpload} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			ctx := context.Background()
			st := store.NewMVCCStore()
			server := NewS3Server(nil, "", st, nil, nil)
			b.Cleanup(server.Stop)
			meta, err := encodeS3BucketMeta(&s3BucketMeta{BucketName: "bench", Generation: 1})
			if err != nil {
				b.Fatal(err)
			}
			elems := []*store.KVPairMutation{
				{Op: store.OpTypePut, Key: s3keys.BucketMetaKey("bench"), Value: meta},
				{Op: store.OpTypePut, Key: s3keys.UploadMetaKey("bench", 1, "object", "upload"), Value: []byte(`{}`)},
			}
			request := s3CompleteMultipartUploadRequest{}
			for i := 1; i <= count; i++ {
				partNo, err := uint64FromInt(i)
				if err != nil {
					b.Fatal(err)
				}
				etag := md5Hex("part")
				raw, err := json.Marshal(s3PartDescriptor{PartNo: partNo, ETag: etag, SizeBytes: s3MinPartSize, PartVersion: 1})
				if err != nil {
					b.Fatal(err)
				}
				elems = append(elems, &store.KVPairMutation{Op: store.OpTypePut, Key: s3keys.UploadPartKey("bench", 1, "object", "upload", partNo), Value: raw})
				request.Parts = append(request.Parts, s3CompleteMultipartUploadPart{PartNumber: i, ETag: etag})
			}
			if err := st.ApplyMutations(ctx, elems, nil, 0, 1); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := server.loadS3MultipartCompletion(ctx, 1, "bench", "object", "upload", request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

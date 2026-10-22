package adapter

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type sealBenchmarkCoordinator struct {
	kv.Coordinator
	seals     atomic.Uint64
	conflicts atomic.Uint64
}

func (c *sealBenchmarkCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if isDynamoSnapshotSealRequest(req) {
		c.seals.Add(1)
	}
	resp, err := c.Coordinator.Dispatch(ctx, req)
	if errors.Is(err, store.ErrWriteConflict) {
		c.conflicts.Add(1)
	}
	return resp, err
}

// A single durable Raft voter isolates the extra seal/OCC cost. This is not a
// networked quorum or a production throughput claim. Separate frontends avoid
// serializing the workload with a shared server's item-lock stripes.
func BenchmarkDynamoDBSnapshotSealRealRaft(b *testing.B) {
	for _, mode := range []string{"independent", "hot_item", "collision"} {
		b.Run(mode, func(b *testing.B) {
			ctx := context.Background()
			st := store.NewMVCCStore()
			defer st.Close()
			server, _, stop := sealRaftRestartableServer(b, b.TempDir(), st)
			defer stop()
			schema := dedupItemTable()
			require.NoError(b, server.createTableWithRetry(ctx, schema.TableName, schema))
			keys := sealBenchmarkKeys(b, schema, mode)
			for _, key := range keys {
				_, err := server.putItemWithRetry(ctx, putItemInput{
					TableName: schema.TableName, Item: map[string]attributeValue{"pk": newStringAttributeValue(key)},
				})
				require.NoError(b, err)
			}
			coord := &sealBenchmarkCoordinator{Coordinator: server.coordinator}
			var worker atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				local := NewDynamoDBServer(nil, st, coord)
				key := keys[(worker.Add(1)-1)%uint64(len(keys))]
				in := putItemInput{TableName: schema.TableName, Item: map[string]attributeValue{
					"pk": newStringAttributeValue(key), "value": newStringAttributeValue("constant-size replacement"),
				}}
				for pb.Next() {
					if _, err := local.putItemWithRetry(ctx, in); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(coord.seals.Load())/float64(b.N), "seals/op")
			b.ReportMetric(float64(coord.conflicts.Load())/float64(b.N), "conflicts/op")
		})
	}
}

func sealBenchmarkKeys(b *testing.B, schema *dynamoTableSchema, mode string) []string {
	b.Helper()
	if mode == "hot_item" {
		return []string{"hot"}
	}
	const count = 4
	buckets := make(map[string][]string)
	independent := make([]string, 0, count)
	for i := range 1 << 20 {
		key := fmt.Sprintf("seal-bench-%d", i)
		item, err := schema.itemKeyFromAttributes(map[string]attributeValue{"pk": newStringAttributeValue(key)})
		require.NoError(b, err)
		seal := string(dynamoItemSnapshotSealKey(schema.TableName, schema.Generation, item))
		if mode == "independent" && len(buckets[seal]) == 0 {
			independent = append(independent, key)
			if len(independent) == count {
				return independent
			}
		}
		buckets[seal] = append(buckets[seal], key)
		if mode == "collision" && len(buckets[seal]) == count {
			return buckets[seal]
		}
	}
	b.Fatal("could not construct seal bucket fixture")
	return nil
}

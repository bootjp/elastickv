package adapter

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSQSFIFODelayUpgradesExistingUnindexedRecords(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		t.Run(fmt.Sprint(partitions), func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := newDelayTestServer(t, st)
			createDelayTestQueue(t, srv, partitions, 900)
			ctx := context.Background()
			stamp, err := srv.beginTxnReadTimestamp(ctx, "legacy record fixture")
			require.NoError(t, err)
			meta, _, err := srv.loadQueueMetaAt(ctx, fifoDelayTestQueue, stamp.Timestamp())
			require.NoError(t, err)
			rawMeta, err := st.GetAt(ctx, sqsQueueMetaKey(meta.Name), stamp.Timestamp())
			require.NoError(t, err)
			require.NotContains(t, string(rawMeta), "fifo_order_indexed")
			require.NotContains(t, string(rawMeta), "fifo_delay_reindex")
			const id = "00000000000000000000000000000001"
			sent := time.Now().UnixMilli()
			visible := sent + 900_000
			// Literal pre-index SM/1 format, deliberately not encoded from the
			// new message struct and carrying no membership or migration fields.
			body := fmt.Sprintf(`{"message_id":%q,"body":"QQ==","md5_of_body":"7fc56270e7a70fa81a5935b72eacbe29","send_timestamp_millis":%d,"available_at_millis":%d,"visible_at_millis":%d,"receive_count":0,"current_receipt_token":null,"queue_generation":%d,"message_group_id":"g","message_deduplication_id":"old-A","sequence_number":1}`, id, sent, visible, visible, meta.Generation)
			raw := append([]byte{0, 'S', 'M', 1}, body...)
			partition := partitionFor(meta, "g")
			req := &kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: stamp.Timestamp(), ReadKeys: [][]byte{sqsQueueMetaKey(meta.Name)}, Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Put, Key: sqsMsgDataKeyDispatch(meta, meta.Name, partition, meta.Generation, id), Value: raw},
				{Op: kv.Put, Key: sqsMsgVisKeyDispatch(meta, meta.Name, partition, meta.Generation, visible, id), Value: []byte(id)},
				{Op: kv.Put, Key: sqsMsgByAgeKeyDispatch(meta, meta.Name, partition, meta.Generation, sent, id), Value: []byte(id)},
				{Op: kv.Put, Key: sqsQueueSeqKey(meta.Name), Value: []byte("1")},
			}}
			_, err = kv.DispatchWithReadTimestamp(stamp.WithDispatchVoucher(ctx), srv.coordinator, req)
			require.NoError(t, err)
			prefix := sqsFifoOrderPrefix(meta, meta.Name, partition, meta.Generation, "g")
			page, err := st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, st.LastCommitTS())
			require.NoError(t, err)
			require.Empty(t, page, "the old queued record must have no pre-created index")
			require.NoError(t, srv.setQueueAttributesCore(ctx, meta.Name, map[string]string{"DelaySeconds": "0"}))
			sendDelayTestMessage(t, srv, "B", "g")
			page, err = st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, st.LastCommitTS())
			require.NoError(t, err)
			require.Len(t, page, 2)
			require.Equal(t, id, string(page[0].Value))
			messages, err := srv.scanAndDeliverOnce(ctx, meta.Name, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.Equal(t, id, messages[0]["MessageId"])
		})
	}
}

func TestSQSFIFODelayEncryptedReopen(t *testing.T) {
	keys := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	_, err := rand.Read(dek)
	require.NoError(t, err)
	require.NoError(t, keys.Set(1, dek))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	dir := t.TempDir()
	open := func(epoch uint16) store.MVCCStore {
		t.Helper()
		st, err := store.NewPebbleStore(dir, store.WithEncryption(cipher, encryption.NewDeterministicNonceFactory(1, epoch), func() (uint32, bool) { return 1, true }))
		require.NoError(t, err)
		return st
	}
	st := open(1)
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 4, 0)
	ids := make([]string, sqsFifoDelayPageSize+2)
	for i := range ids {
		ids[i] = sendDelayTestMessage(t, srv, fmt.Sprint(i), "g")
	}
	beginDelayTestUpdate(t, srv, 900)
	done, err := srv.advanceFifoDelayReindex(context.Background(), fifoDelayTestQueue)
	require.NoError(t, err)
	require.False(t, done)
	require.NoError(t, st.Close())
	st = open(2)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv = newDelayTestServer(t, st)
	require.NoError(t, srv.completeFifoDelayReindex(context.Background(), fifoDelayTestQueue))
	for _, id := range ids {
		rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
		require.Equal(t, rec.SendTimestampMillis+900_000, rec.VisibleAtMillis)
	}
	require.NoError(t, srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "0"}))
	messages, err := srv.scanAndDeliverOnce(context.Background(), fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, ids[0], messages[0]["MessageId"])
}

type fifoDelayRoutedStore struct {
	store.MVCCStore
	resolver *SQSPartitionResolver
	scans    int
	gets     int
}

func (s *fifoDelayRoutedStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if s.resolver.RecognisesPartitionedKey(key) {
		return nil, errors.New("partitioned read used byte-range fallback")
	}
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func (s *fifoDelayRoutedStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if s.resolver.RecognisesPartitionedKey(start) {
		return nil, errors.New("partitioned scan used byte-range fallback")
	}
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func (s *fifoDelayRoutedStore) GetGroupAt(ctx context.Context, group uint64, key []byte, ts uint64) ([]byte, error) {
	want, ok := s.resolver.ResolveGroup(key)
	if !ok || group != want {
		return nil, errors.New("partitioned data read used the wrong group")
	}
	s.gets++
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func (s *fifoDelayRoutedStore) ScanGroupAt(ctx context.Context, group uint64, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	want, ok := s.resolver.ResolveGroup(start)
	if !ok || group != want {
		return nil, errors.New("partitioned scan used the wrong group")
	}
	s.scans++
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func TestSQSFIFODelayUsesPartitionGroupForBackfillAndHead(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	resolver := NewSQSPartitionResolver(map[string][]uint64{fifoDelayTestQueue: {10, 11, 12, 13}})
	routed := &fifoDelayRoutedStore{MVCCStore: st, resolver: resolver}
	srv := newDelayTestServer(t, routed)
	srv.partitionResolver = resolver
	createDelayTestQueue(t, srv, 4, 900)
	meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
	require.NoError(t, err)
	for partition := uint32(0); partition < 4; partition++ {
		group := delayGroupOnPartition(meta, partition)
		sendDelayTestMessage(t, srv, group, group)
	}
	require.NoError(t, srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "0"}))
	messages, err := srv.scanAndDeliverOnce(context.Background(), fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 4)
	require.GreaterOrEqual(t, routed.scans, 12, "backfill, visibility and group-order lookups must all use group-aware scans")
	require.GreaterOrEqual(t, routed.gets, 8)
	for _, message := range messages {
		id, ok := message["MessageId"].(string)
		require.True(t, ok)
		require.NotEmpty(t, id)
	}
}

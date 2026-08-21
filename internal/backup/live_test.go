package backup

import (
	"encoding/base64"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestScopeForKey(t *testing.T) {
	t.Parallel()
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	cases := []struct {
		name   string
		key    []byte
		want   Scope
		scoped bool
	}{
		{name: "dynamodb meta", key: []byte(DDBTableMetaPrefix + enc("orders")), want: Scope{Adapter: "dynamodb", Name: "orders"}, scoped: true},
		{name: "dynamodb generation", key: []byte(DDBTableGenPrefix + enc("orders")), scoped: false},
		{name: "s3 bucket", key: s3keys.BucketMetaKey("photos"), want: Scope{Adapter: "s3", Name: "photos"}, scoped: true},
		{name: "sqs metadata", key: []byte(SQSQueueMetaPrefix + enc("jobs")), want: Scope{Adapter: "sqs", Name: "jobs"}, scoped: true},
		{name: "sqs sequence", key: []byte(SQSQueueSeqPrefix + enc("jobs")), scoped: false},
		{name: "sqs visibility index", key: []byte(SQSMsgVisPrefix + enc("jobs") + "|message"), scoped: false},
		{name: "s3 generation", key: []byte(S3BucketGenPrefix + enc("photos")), scoped: false},
		{name: "s3 incomplete upload", key: []byte(S3UploadMetaPrefix + "ignored"), scoped: false},
		{name: "redis", key: []byte(RedisStringPrefix + "key"), want: Scope{Adapter: "redis", Name: "db_0"}, scoped: true},
		{name: "ddb derived gsi", key: []byte(DDBGSIPrefix + "ignored"), scoped: false},
		{name: "dynamodb generation counter", key: []byte(DDBTableGenPrefix + enc("deleted")), scoped: false},
		{name: "s3 generation counter", key: []byte(S3BucketGenPrefix + "deleted"), scoped: false},
		{name: "transaction", key: []byte("!txn|lock|ignored"), scoped: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, scoped, err := ScopeForKey(tc.key)
			require.NoError(t, err)
			require.Equal(t, tc.scoped, scoped)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestScopeForKeyRejectsMalformedRecognizedKey(t *testing.T) {
	t.Parallel()
	_, _, err := ScopeForKey([]byte(DDBTableMetaPrefix + "%%%"))
	require.True(t, errors.Is(err, ErrScopeKeyMalformed), "err = %v", err)
}

func TestLiveDecoderReusesPhase0Encoders(t *testing.T) {
	t.Parallel()
	out := t.TempDir()
	decoder, err := NewLiveDecoder(DecodeOptions{OutRoot: out, Adapters: AdapterSet{Redis: true}})
	require.NoError(t, err)
	require.NoError(t, decoder.Add([]byte(RedisStringPrefix+"greeting"), []byte("hello")))
	counters, err := decoder.Finalize()
	require.NoError(t, err)
	require.Equal(t, uint64(1), counters.Total)
	require.Equal(t, uint64(1), counters.Redis)
	require.FileExists(t, out+"/redis/db_0/strings/greeting.bin")
}

func TestLiveDecoderAcceptsSQSQueueSequenceSideRecord(t *testing.T) {
	t.Parallel()
	out := t.TempDir()
	encodedQueue := base64.RawURLEncoding.EncodeToString([]byte("orders.fifo"))
	decoder, err := NewLiveDecoder(DecodeOptions{
		OutRoot: out, Adapters: AdapterSet{SQS: true}, IncludeSQSSideRecords: true,
	})
	require.NoError(t, err)
	require.NoError(t, decoder.Add([]byte(SQSQueueSeqPrefix+encodedQueue), []byte("42")))
	counters, err := decoder.Finalize()
	require.NoError(t, err)
	require.Equal(t, uint64(1), counters.SQS)
	require.FileExists(t, out+"/sqs/"+encodedQueue+".orphan/_internals/side_records.jsonl")
}

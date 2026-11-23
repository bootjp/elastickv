package adapter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type luaProofCase struct {
	Ops []struct {
		Cmd   string
		Key   int
		Value *int
	}
	Initial, Reads, Final []*int
}

func luaProofText(n *int) string {
	if n == nil {
		return "missing"
	}
	return strconv.Itoa(*n)
}

func (c luaProofCase) check(t *testing.T, rdb *redis.Client) {
	t.Helper()
	keys := []string{"proof:lua:a", "proof:lua:b", "proof:lua:c"}
	for i, v := range c.Initial {
		if v == nil {
			require.NoError(t, rdb.Del(t.Context(), keys[i]).Err())
		} else {
			require.NoError(t, rdb.Set(t.Context(), keys[i], *v, 0).Err())
		}
	}
	var args []any
	for _, op := range c.Ops {
		args = append(args, op.Cmd, op.Key+1, luaProofText(op.Value))
	}
	const script = `local reads={}
for i=1,#ARGV,3 do
  local k=KEYS[tonumber(ARGV[i+1])]
  if ARGV[i]=='get' then
    reads[#reads+1]=redis.call('GET',k) or 'missing'
  elseif ARGV[i+2]=='missing' then
    redis.call('DEL',k)
  else
    redis.call('SET',k,ARGV[i+2])
  end
end
return reads`
	reply, err := rdb.Eval(t.Context(), script, keys, args...).StringSlice()
	require.NoError(t, err)
	require.Len(t, reply, len(c.Reads))
	for i, v := range c.Reads {
		require.Equal(t, luaProofText(v), reply[i])
	}
	for i, v := range c.Final {
		got, err := rdb.Get(t.Context(), keys[i]).Result()
		if v == nil {
			require.ErrorIs(t, err, redis.Nil)
		} else {
			require.NoError(t, err)
			require.Equal(t, luaProofText(v), got)
		}
	}
}

type multipartProofCase struct {
	IDs       []int
	Wrong     bool
	Published []int
}

func (c multipartProofCase) check(t *testing.T) {
	t.Helper()
	h := newS3OCCReproHarness(t)
	u := h.startUpload("proof-complete")
	revision := seedS3ReadSetParts(t, h, u, 3)
	xml := s3ReadSetCompletionBody(c.IDs, func(i int) string {
		if c.Wrong {
			return md5Hex("changed")
		}
		return md5Hex(strconv.Itoa(i))
	})
	response := h.do(http.MethodPost, u.objectPath()+"?uploadId="+u.uploadID, xml)
	key := s3keys.ObjectManifestKey(u.bucket, u.generation, s3OCCReproObject)
	manifest, published, err := h.server.loadObjectManifestAt(t.Context(), key, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, c.Published != nil, published)
	_, openErr := h.st.GetAt(t.Context(), u.uploadMetaKey, ^uint64(0))
	if !published {
		require.Equal(t, http.StatusBadRequest, response.Code)
		require.NoError(t, openErr)
		return
	}
	require.Equal(t, http.StatusOK, response.Code)
	require.ErrorIs(t, openErr, store.ErrKeyNotFound)
	require.Len(t, manifest.Parts, len(c.Published))
	for i, part := range manifest.Parts {
		require.EqualValues(t, c.Published[i], part.PartNo)
		require.Equal(t, revision, part.PartVersion)
	}
}

type ttlProofCase struct {
	Deadline      *int64
	Now, Expected int64
}

func (c ttlProofCase) check(t *testing.T) {
	t.Helper()
	var deadline *time.Time
	if c.Deadline != nil {
		decoded, err := decodeRedisTTL(encodeRedisTTL(time.UnixMilli(*c.Deadline)))
		require.NoError(t, err)
		require.Equal(t, *c.Deadline, decoded.UnixMilli())
		deadline = &decoded
	}
	require.Equal(t, c.Expected, ttlMilliseconds(deadline, time.UnixMilli(c.Now)))
}

type receiptProofCase struct {
	First, Last, Deadline, Now, Seconds int64
	Next                                *int64
}

func (c receiptProofCase) check(t *testing.T) {
	t.Helper()
	// Round-trip the real record codec, including the absent field in legacy rows.
	raw, err := encodeSQSMessageRecord(&sqsMessageRecord{
		FirstReceiveMillis: c.First, ReceiptStartMillis: c.Last, VisibleAtMillis: c.Deadline,
	})
	require.NoError(t, err)
	rec, err := decodeSQSMessageRecord(raw)
	require.NoError(t, err)
	next, err := rec.changedVisibilityDeadline(c.Now, c.Seconds)
	if c.Next == nil {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		require.Equal(t, *c.Next, next)
	}
}

func TestAdapterProtocolTransitions(t *testing.T) {
	path := os.Getenv("ADAPTER_PROTOCOL_CASES")
	if path == "" {
		t.Skip("make lean-check generates the protocol transitions")
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var cases struct {
		TTL      []ttlProofCase
		Lua      []luaProofCase
		Parts    []multipartProofCase
		Receipts []receiptProofCase
	}
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.EqualValues(t, 43200, sqsChangeVisibilityMaxSeconds)
	require.EqualValues(t, 1000, sqsMillisPerSecond)
	require.NotEmpty(t, cases.TTL)
	for i, c := range cases.TTL {
		t.Run(fmt.Sprintf("ttl/%d", i), c.check)
	}
	require.NotEmpty(t, cases.Lua)
	require.NotEmpty(t, cases.Parts)
	require.NotEmpty(t, cases.Receipts)
	t.Run("lua", func(t *testing.T) {
		nodes, _, _ := createNode(t, 1)
		defer shutdown(nodes)
		rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
		defer func() { _ = rdb.Close() }()
		for i, c := range cases.Lua {
			t.Run(strconv.Itoa(i), func(t *testing.T) { c.check(t, rdb) })
		}
	})
	for i, c := range cases.Parts {
		t.Run(fmt.Sprintf("multipart/%d", i), c.check)
	}
	for i, c := range cases.Receipts {
		t.Run(fmt.Sprintf("receipt/%d", i), c.check)
	}
}

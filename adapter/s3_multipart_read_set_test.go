package adapter

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These contracts inspect actual HTTP transactions forwarded to the production
// coordinator. Recording does not replace dispatch, timestamp admission or OCC.
func (c *s3OCCReproCoordinator) lastTxnWriting(t *testing.T, key []byte) *kv.OperationGroup[kv.OP] {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.txns) - 1; i >= 0; i-- {
		for _, elem := range c.txns[i].Elems {
			if bytes.Equal(elem.Key, key) {
				return c.txns[i]
			}
		}
	}
	t.Fatalf("no transaction wrote %q", key)
	return nil
}

func requireS3ReadAtStartTS(t *testing.T, reads []s3OCCReproRead, key []byte, startTS uint64) {
	t.Helper()
	for i := len(reads) - 1; i >= 0; i-- {
		if bytes.Equal(reads[i].key, key) {
			assert.Equal(t, startTS, reads[i].ts, "commit-state read of %q must use exactly StartTS", key)
			return
		}
	}
	t.Errorf("no commit-state GetAt for %q at StartTS %d", key, startTS)
}

func (h *s3OCCReproHarness) readsForTxn(req *kv.OperationGroup[kv.OP]) []s3OCCReproRead {
	h.t.Helper()
	h.coord.mu.Lock()
	id := h.coord.requestIDs[req]
	h.coord.mu.Unlock()
	require.NotZero(h.t, id, "transaction must belong to a traced HTTP request")
	var reads []s3OCCReproRead
	for _, read := range h.reads.readLog() {
		if read.requestID == id {
			reads = append(reads, read)
		}
	}
	return reads
}

func TestS3CommitUploadPartReadSet(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"first part", "previous part", "previous part between preparation and StartTS"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			h := newS3OCCReproHarness(t)
			u := h.startUpload("readset-upload")
			if scenario != "first part" {
				rec := h.uploadPart1(u, "previous")
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				h.waitCleanups()
			}
			var rec *httptest.ResponseRecorder
			if scenario == "previous part between preparation and StartTS" {
				rec = h.uploadPart1Racing(u, "latest", u.uploadMetaKey, func() {
					concurrent := h.uploadPart1(u, "window")
					require.Equal(t, http.StatusOK, concurrent.Code, concurrent.Body.String())
				})
			} else {
				rec = h.uploadPart1(u, "latest")
			}
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			req := h.coord.lastTxnWriting(t, u.partKey)
			assert.ElementsMatch(t, [][]byte{u.uploadMetaKey, s3keys.BucketGenerationKey(u.bucket), u.partKey}, req.ReadKeys)
			assert.NotContains(t, req.ReadKeys, s3keys.BucketMetaKey(u.bucket), "unrelated bucket writers must not serialize part uploads")
			reads := h.readsForTxn(req)
			for _, key := range [][]byte{s3keys.BucketMetaKey(u.bucket), u.uploadMetaKey, u.partKey} {
				requireS3ReadAtStartTS(t, reads, key, req.StartTS)
			}
		})
	}
}

func TestS3UploadPart_ReadSetUsesStartTSAfterConcurrentSamePartWrite(t *testing.T) {
	t.Parallel()
	for _, offloaded := range []bool{false, true} {
		t.Run(fmt.Sprintf("offloaded=%t", offloaded), func(t *testing.T) {
			t.Parallel()
			h := newS3OCCReproModeHarness(t, false, offloaded)
			u := h.startUpload("readset-after-start")
			rec := h.uploadPart1(u, "original")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var concurrent *s3PartDescriptor
			loser := h.uploadPart1Racing(u, "loser", u.partKey, func() {
				rec = h.uploadPart1(u, "winner")
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				var found bool
				concurrent, found = h.part1Descriptor(u)
				require.True(t, found)
			})
			assert.Equal(t, http.StatusServiceUnavailable, loser.Code, loser.Body.String())
			assert.Contains(t, loser.Body.String(), "<Code>SlowDown</Code>")
			req := h.coord.lastTxnWriting(t, u.partKey)
			assert.Less(t, req.StartTS, concurrent.PartVersion, "the winner must be newer than the loser's snapshot")
			for _, key := range [][]byte{s3keys.BucketMetaKey(u.bucket), u.uploadMetaKey, u.partKey} {
				requireS3ReadAtStartTS(t, h.readsForTxn(req), key, req.StartTS)
			}
			current, found := h.part1Descriptor(u)
			require.True(t, found)
			assert.Equal(t, concurrent, current, "the real FSM must reject the stale descriptor")
			_, err := h.st.GetAt(context.Background(), u.chunkKey(0, s3ObjectPart(*concurrent)), ^uint64(0))
			require.NoError(t, err, "conflict cleanup must not delete the winning version")
		})
	}
}

// Descriptor-only fixtures avoid allocating 50 GiB for the 10,000-part case.
// Fixture writes and the completion both use real Raft; payload integrity is
// checked separately by the re-upload reproductions in the companion file.
func seedS3ReadSetParts(t *testing.T, h *s3OCCReproHarness, u s3OCCReproUpload, count int) uint64 {
	t.Helper()
	ts := h.coord.Clock().Next()
	elems := make([]*kv.Elem[kv.OP], 0, count)
	for i := 1; i <= count; i++ {
		partNo, err := uint64FromInt(i)
		require.NoError(t, err)
		raw, err := json.Marshal(s3PartDescriptor{
			PartNo: partNo, ETag: md5Hex(fmt.Sprint(i)), SizeBytes: int64(s3MinPartSize), PartVersion: ts,
		})
		require.NoError(t, err)
		elems = append(elems, &kv.Elem[kv.OP]{
			Op: kv.Put, Key: s3keys.UploadPartKey(u.bucket, u.generation, s3OCCReproObject, u.uploadID, partNo), Value: raw,
		})
	}
	_, err := h.coord.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{CommitTS: ts, Elems: elems})
	require.NoError(t, err)
	return ts
}

func s3ReadSetCompletionBody(parts []int, etag func(int) string) string {
	var body strings.Builder
	body.WriteString("<CompleteMultipartUpload>")
	for _, part := range parts {
		fmt.Fprintf(&body, `<Part><PartNumber>%d</PartNumber><ETag>"%s"</ETag></Part>`, part, etag(part))
	}
	body.WriteString("</CompleteMultipartUpload>")
	return body.String()
}

func TestS3CommitMultipartCompletionReadSet(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		parts    []int
		etag     func(int) string
		wantCode string
	}{
		{name: "single part", parts: []int{1}},
		{name: "every consumed part", parts: []int{1, 2, 3}},
		{name: "unconsumed uploaded part excluded", parts: []int{1, 3}},
		{name: "missing part", parts: []int{1, 4}, wantCode: "InvalidPart"},
		{name: "ETag mismatch", parts: []int{1}, etag: func(int) string { return md5Hex("changed") }, wantCode: "InvalidPart"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newS3OCCReproHarness(t)
			u := h.startUpload("readset-completion")
			version := seedS3ReadSetParts(t, h, u, 3)
			etag := tc.etag
			if etag == nil {
				etag = func(part int) string { return md5Hex(fmt.Sprint(part)) }
			}
			rec := h.do(http.MethodPost, u.objectPath()+"?uploadId="+u.uploadID, s3ReadSetCompletionBody(tc.parts, etag))
			headKey := s3keys.ObjectManifestKey(u.bucket, u.generation, s3OCCReproObject)
			if tc.wantCode != "" {
				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				require.Contains(t, rec.Body.String(), "<Code>"+tc.wantCode+"</Code>")
				_, err := h.st.GetAt(context.Background(), headKey, ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
				return
			}
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			req := h.coord.lastTxnWriting(t, headKey)
			want := make([][]byte, 0, len(tc.parts))
			for _, part := range tc.parts {
				partNo, err := uint64FromInt(part)
				require.NoError(t, err)
				key := s3keys.UploadPartKey(u.bucket, u.generation, s3OCCReproObject, u.uploadID, partNo)
				want = append(want, key)
				requireS3ReadAtStartTS(t, h.readsForTxn(req), key, req.StartTS)
				for _, elem := range req.Elems {
					assert.NotEqual(t, key, elem.Key, "a part descriptor is a read-only dependency")
				}
			}
			assert.ElementsMatch(t, want, req.ReadKeys)
			manifest, found, err := h.server.loadObjectManifestAt(context.Background(), headKey, ^uint64(0))
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, manifest.Parts, len(tc.parts))
			for i, part := range manifest.Parts {
				assert.EqualValues(t, tc.parts[i], part.PartNo)
				assert.Equal(t, version, part.PartVersion)
			}
		})
	}
}

func TestS3DeleteBucketWritesUploadPartGenerationFence(t *testing.T) {
	t.Parallel()
	for _, admin := range []bool{false, true} {
		for _, state := range []struct {
			name   string
			stored uint64
			want   uint64
		}{
			{name: "current", stored: 4, want: 4},
			{name: "legacy missing generation key", want: 4},
			{name: "ahead of bucket generation", stored: 7, want: 7},
		} {
			t.Run(fmt.Sprintf("admin=%t/%s", admin, state.name), func(t *testing.T) {
				t.Parallel()
				h := newS3OCCReproHarness(t)
				const bucket = "generation-fence"
				metaKey := s3keys.BucketMetaKey(bucket)
				generationKey := s3keys.BucketGenerationKey(bucket)
				raw, err := encodeS3BucketMeta(&s3BucketMeta{BucketName: bucket, Generation: 4, CreatedAtHLC: 1, Owner: "AKIA_FULL", Acl: s3AclPrivate})
				require.NoError(t, err)
				elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: metaKey, Value: raw}}
				if state.stored != 0 {
					elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: generationKey, Value: encodeS3Generation(state.stored)})
				}
				_, err = h.coord.Dispatch(context.Background(), &kv.OperationGroup[kv.OP]{Elems: elems})
				require.NoError(t, err)
				if admin {
					require.NoError(t, h.server.AdminDeleteBucket(context.Background(), fullAdminBucketsPrincipal(), bucket))
				} else {
					rec := h.do(http.MethodDelete, "/"+bucket, "")
					require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
				}
				h.waitCleanups()
				req := h.coord.lastTxnWriting(t, metaKey)
				assert.Len(t, req.Elems, 2)
				foundGenerationFence := false
				for _, elem := range req.Elems {
					if bytes.Equal(elem.Key, metaKey) {
						assert.Equal(t, kv.Del, elem.Op)
					}
					if bytes.Equal(elem.Key, generationKey) {
						foundGenerationFence = true
						assert.Equal(t, kv.Put, elem.Op)
						assert.Equal(t, encodeS3Generation(state.want), elem.Value)
					}
				}
				assert.True(t, foundGenerationFence, "Phase 1 must atomically invalidate in-flight UploadPart transactions")
				_, err = h.st.GetAt(context.Background(), metaKey, ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
				next, err := h.server.nextBucketGenerationAt(context.Background(), bucket, h.st.LastCommitTS())
				require.NoError(t, err)
				assert.Equal(t, state.want+1, next, "recreation must never reuse the deleted generation")
			})
		}
	}
}

func TestS3CompleteMultipartUploadMaxPartsFitsReadKeyCap(t *testing.T) {
	t.Parallel()
	for _, staged := range []bool{false, true} {
		t.Run(fmt.Sprintf("staged=%t", staged), func(t *testing.T) {
			t.Parallel()
			h := newS3OCCReproVisibilityHarness(t, staged)
			u := h.startUpload("readset-max-parts")
			seedS3ReadSetParts(t, h, u, s3MaxPartsPerUpload)
			parts := make([]int, s3MaxPartsPerUpload)
			for i := range parts {
				parts[i] = i + 1
			}
			body := s3ReadSetCompletionBody(parts, func(part int) string { return md5Hex(fmt.Sprint(part)) })
			rec := h.do(http.MethodPost, u.objectPath()+"?uploadId="+u.uploadID, body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			headKey := s3keys.ObjectManifestKey(u.bucket, u.generation, s3OCCReproObject)
			req := h.coord.lastTxnWriting(t, headKey)
			require.Len(t, req.ReadKeys, s3MaxPartsPerUpload, "one raw read dependency per consumed part")
			for i, part := range parts {
				partNo, err := uint64FromInt(part)
				require.NoError(t, err)
				want := s3keys.UploadPartKey(u.bucket, u.generation, s3OCCReproObject, u.uploadID, partNo)
				require.Equal(t, want, req.ReadKeys[i])
			}
			manifest, found, err := h.server.loadObjectManifestAt(context.Background(), headKey, ^uint64(0))
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, manifest.Parts, s3MaxPartsPerUpload)
		})
	}
}

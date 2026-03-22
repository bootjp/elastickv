package adapter

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 ETag compatibility requires MD5.
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
	"github.com/hashicorp/raft"
)

const (
	s3HealthPath             = "/healthz"
	s3ChunkSize              = 1 << 20
	s3ChunkBatchOps          = 16
	s3XMLNamespace           = "http://s3.amazonaws.com/doc/2006-03-01/"
	s3DefaultRegion          = "us-east-1"
	s3MaxKeys                = 1000
	s3ListPageSize           = 256
	s3ManifestCleanupTimeout = 2 * time.Minute
	s3MaxObjectSizeBytes     = 5 * 1024 * 1024 * 1024 // 5 GiB, matching AWS S3 single PUT limit.

	s3TxnRetryInitialBackoff = 2 * time.Millisecond
	s3TxnRetryMaxBackoff     = 32 * time.Millisecond
	s3TxnRetryBackoffFactor  = 2
	s3TxnRetryMaxAttempts    = 8

	s3PathSplitParts   = 2
	s3GenerationBytes  = 8
	s3HLCPhysicalShift = 16
)

type S3Server struct {
	listen      net.Listener
	s3Addr      string
	region      string
	store       store.MVCCStore
	coordinator kv.Coordinator
	leaderS3    map[raft.ServerAddress]string
	staticCreds map[string]string
	readTracker *kv.ActiveTimestampTracker
	httpServer  *http.Server
}

type s3BucketMeta struct {
	BucketName   string `json:"bucket_name"`
	Generation   uint64 `json:"generation"`
	CreatedAtHLC uint64 `json:"created_at_hlc"`
	Owner        string `json:"owner,omitempty"`
	Region       string `json:"region,omitempty"`
}

type s3ObjectManifest struct {
	UploadID           string            `json:"upload_id"`
	ETag               string            `json:"etag"`
	SizeBytes          int64             `json:"size_bytes"`
	LastModifiedHLC    uint64            `json:"last_modified_hlc"`
	ContentType        string            `json:"content_type,omitempty"`
	ContentEncoding    string            `json:"content_encoding,omitempty"`
	CacheControl       string            `json:"cache_control,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	UserMetadata       map[string]string `json:"user_metadata,omitempty"`
	Parts              []s3ObjectPart    `json:"parts,omitempty"`
}

type s3ObjectPart struct {
	PartNo     uint64   `json:"part_no"`
	ETag       string   `json:"etag"`
	SizeBytes  int64    `json:"size_bytes"`
	ChunkCount uint64   `json:"chunk_count"`
	ChunkSizes []uint64 `json:"chunk_sizes,omitempty"`
}

type s3ContinuationToken struct {
	Bucket           string `json:"bucket"`
	Generation       uint64 `json:"generation"`
	Prefix           string `json:"prefix,omitempty"`
	Delimiter        string `json:"delimiter,omitempty"`
	LastKey          string `json:"last_key,omitempty"`
	LastCommonPrefix string `json:"last_common_prefix,omitempty"`
	ReadTS           uint64 `json:"read_ts,omitempty"`
}

type s3ResponseError struct {
	Status  int
	Code    string
	Message string
	Bucket  string
	Key     string
}

func (e *s3ResponseError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

type s3ErrorResponse struct {
	XMLName xml.Name `xml:"Error"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
	Bucket  string   `xml:"BucketName,omitempty"`
	Key     string   `xml:"Key,omitempty"`
}

type s3ListBucketsResult struct {
	XMLName xml.Name            `xml:"ListAllMyBucketsResult"`
	XMLNS   string              `xml:"xmlns,attr,omitempty"`
	Buckets s3BucketsCollection `xml:"Buckets"`
}

type s3BucketsCollection struct {
	Buckets []s3BucketEntry `xml:"Bucket"`
}

type s3BucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type s3ListBucketResult struct {
	XMLName               xml.Name              `xml:"ListBucketResult"`
	XMLNS                 string                `xml:"xmlns,attr,omitempty"`
	Name                  string                `xml:"Name"`
	Prefix                string                `xml:"Prefix,omitempty"`
	Delimiter             string                `xml:"Delimiter,omitempty"`
	MaxKeys               int                   `xml:"MaxKeys"`
	KeyCount              int                   `xml:"KeyCount"`
	IsTruncated           bool                  `xml:"IsTruncated"`
	ContinuationToken     string                `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string                `xml:"NextContinuationToken,omitempty"`
	Contents              []s3ListObjectContent `xml:"Contents"`
	CommonPrefixes        []s3CommonPrefix      `xml:"CommonPrefixes"`
}

type s3ListObjectContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type s3CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func NewS3Server(listen net.Listener, s3Addr string, st store.MVCCStore, coordinate kv.Coordinator, leaderS3 map[raft.ServerAddress]string, opts ...S3ServerOption) *S3Server {
	s := &S3Server{
		listen:      listen,
		s3Addr:      s3Addr,
		region:      s3DefaultRegion,
		store:       st,
		coordinator: coordinate,
		leaderS3:    cloneLeaderAddrMap(leaderS3),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.s3Addr != "" {
		if s.leaderS3 == nil {
			s.leaderS3 = map[raft.ServerAddress]string{}
		}
		if leader := s.coordinatorLeaderAddress(); leader != "" {
			s.leaderS3[leader] = s.s3Addr
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	s.httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	return s
}

func (s *S3Server) Run() error {
	if s.httpServer == nil {
		return errors.New("s3 server httpServer is nil")
	}
	if s.listen == nil {
		return errors.New("s3 server listener is nil")
	}
	if err := s.httpServer.Serve(s.listen); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.WithStack(err)
	}
	return nil
}

func (s *S3Server) Stop() {
	if s != nil && s.httpServer != nil {
		_ = s.httpServer.Shutdown(context.Background())
	}
}

func (s *S3Server) handle(w http.ResponseWriter, r *http.Request) {
	if serveS3Healthz(w, r) {
		return
	}
	if s.maybeProxyToLeader(w, r) {
		return
	}
	if authErr := s.authorizeRequest(r); authErr != nil {
		writeS3Error(w, authErr.Status, authErr.Code, authErr.Message, "", "")
		return
	}

	bucket, objectKey, hasObject, err := parseS3Path(r)
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidURI", err.Error(), bucket, objectKey)
		return
	}

	switch {
	case bucket == "":
		s.handleRoot(w, r)
	case !hasObject:
		s.handleBucket(w, r, bucket)
	default:
		s.handleObject(w, r, bucket, objectKey)
	}
}

func (s *S3Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method", "", "")
		return
	}
	s.listBuckets(w, r)
}

func (s *S3Server) handleBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodPut:
		s.createBucket(w, r, bucket)
	case http.MethodHead:
		s.headBucket(w, r, bucket)
	case http.MethodDelete:
		s.deleteBucket(w, r, bucket)
	case http.MethodGet:
		if r.URL.Query().Get("list-type") == "2" {
			s.listObjectsV2(w, r, bucket)
			return
		}
		writeS3Error(w, http.StatusNotImplemented, "NotImplemented", "only ListObjectsV2 is supported", bucket, "")
	default:
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method", bucket, "")
	}
}

func (s *S3Server) handleObject(w http.ResponseWriter, r *http.Request, bucket string, objectKey string) {
	switch r.Method {
	case http.MethodPut:
		s.putObject(w, r, bucket, objectKey)
	case http.MethodGet:
		s.getObject(w, r, bucket, objectKey, false)
	case http.MethodHead:
		s.getObject(w, r, bucket, objectKey, true)
	case http.MethodDelete:
		s.deleteObject(w, r, bucket, objectKey)
	default:
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method", bucket, objectKey)
	}
}

func (s *S3Server) listBuckets(w http.ResponseWriter, r *http.Request) {
	readTS := s.readTS()
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()
	kvs, err := s.store.ScanAt(r.Context(), []byte(s3keys.BucketMetaPrefix), prefixScanEnd([]byte(s3keys.BucketMetaPrefix)), s3MaxKeys, readTS)
	if err != nil {
		writeS3InternalError(w, err)
		return
	}

	out := s3ListBucketsResult{XMLNS: s3XMLNamespace}
	for _, kvp := range kvs {
		bucket, ok := s3keys.ParseBucketMetaKey(kvp.Key)
		if !ok {
			continue
		}
		meta, err := decodeS3BucketMeta(kvp.Value)
		if err != nil {
			writeS3InternalError(w, err)
			return
		}
		if meta == nil {
			continue
		}
		out.Buckets.Buckets = append(out.Buckets.Buckets, s3BucketEntry{
			Name:         bucket,
			CreationDate: formatS3ISOTime(meta.CreatedAtHLC),
		})
	}
	writeS3XML(w, http.StatusOK, out)
}

func (s *S3Server) createBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := validateS3BucketName(bucket); err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidBucketName", err.Error(), bucket, "")
		return
	}
	err := s.retryS3Mutation(r.Context(), func() error {
		readTS := s.readTS()
		startTS := s.txnStartTS(readTS)
		readPin := s.pinReadTS(readTS)
		defer readPin.Release()

		existing, exists, err := s.loadBucketMetaAt(r.Context(), bucket, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if exists && existing != nil {
			return &s3ResponseError{
				Status:  http.StatusConflict,
				Code:    "BucketAlreadyOwnedByYou",
				Message: "bucket already exists",
				Bucket:  bucket,
			}
		}

		nextGeneration, err := s.nextBucketGenerationAt(r.Context(), bucket, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		commitTS, err := s.nextTxnCommitTS(startTS)
		if err != nil {
			return errors.WithStack(err)
		}
		meta := &s3BucketMeta{
			BucketName:   bucket,
			Generation:   nextGeneration,
			CreatedAtHLC: commitTS,
			Region:       s.effectiveRegion(),
		}
		body, err := encodeS3BucketMeta(meta)
		if err != nil {
			return errors.WithStack(err)
		}
		req := &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  startTS,
			CommitTS: commitTS,
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Put, Key: s3keys.BucketMetaKey(bucket), Value: body},
				{Op: kv.Put, Key: s3keys.BucketGenerationKey(bucket), Value: encodeS3Generation(nextGeneration)},
			},
		}
		_, err = s.coordinator.Dispatch(r.Context(), req)
		return errors.WithStack(err)
	})
	if err != nil {
		writeS3MutationError(w, err, bucket, "")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *S3Server) headBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	readTS := s.readTS()
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()

	_, exists, err := s.loadBucketMetaAt(r.Context(), bucket, readTS)
	if err != nil {
		writeS3InternalError(w, err)
		return
	}
	if !exists {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", bucket, "")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *S3Server) deleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	err := s.retryS3Mutation(r.Context(), func() error {
		readTS := s.readTS()
		startTS := s.txnStartTS(readTS)
		readPin := s.pinReadTS(readTS)
		defer readPin.Release()

		meta, exists, err := s.loadBucketMetaAt(r.Context(), bucket, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if !exists || meta == nil {
			return &s3ResponseError{
				Status:  http.StatusNotFound,
				Code:    "NoSuchBucket",
				Message: "bucket not found",
				Bucket:  bucket,
			}
		}

		start := s3keys.ObjectManifestPrefixForBucket(bucket, meta.Generation)
		kvs, err := s.store.ScanAt(r.Context(), start, prefixScanEnd(start), 1, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(kvs) > 0 {
			return &s3ResponseError{
				Status:  http.StatusConflict,
				Code:    "BucketNotEmpty",
				Message: "bucket is not empty",
				Bucket:  bucket,
			}
		}

		_, err = s.coordinator.Dispatch(r.Context(), &kv.OperationGroup[kv.OP]{
			IsTxn:   true,
			StartTS: startTS,
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Del, Key: s3keys.BucketMetaKey(bucket)},
			},
		})
		return errors.WithStack(err)
	})
	if err != nil {
		writeS3MutationError(w, err, bucket, "")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

//nolint:cyclop,gocognit,nestif // The S3 PUT flow is intentionally linear and maps directly to protocol steps.
func (s *S3Server) putObject(w http.ResponseWriter, r *http.Request, bucket string, objectKey string) {
	readTS := s.readTS()
	startTS := s.txnStartTS(readTS)
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()

	meta, exists, err := s.loadBucketMetaAt(r.Context(), bucket, readTS)
	if err != nil {
		writeS3InternalError(w, err)
		return
	}
	if !exists || meta == nil {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", bucket, objectKey)
		return
	}

	headKey := s3keys.ObjectManifestKey(bucket, meta.Generation, objectKey)
	previous, _, err := s.loadObjectManifestAt(r.Context(), headKey, readTS)
	if err != nil {
		writeS3InternalError(w, err)
		return
	}
	if err := validateS3PutPreconditions(r, previous); err != nil {
		writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed", err.Error(), bucket, objectKey)
		return
	}

	uploadID := newS3UploadID(s.clock())
	hasher := md5.New() //nolint:gosec // S3 ETag compatibility requires MD5.
	sha256Hasher := sha256.New()
	expectedPayloadSHA := normalizeS3PayloadHash(r.Header.Get("X-Amz-Content-Sha256"))
	validatePayloadSHA := expectedPayloadSHA != "" && !strings.EqualFold(expectedPayloadSHA, s3UnsignedPayload)
	r.Body = http.MaxBytesReader(w, r.Body, s3MaxObjectSizeBytes)
	part := s3ObjectPart{PartNo: 1}
	sizeBytes := int64(0)
	chunkNo := uint64(0)
	buf := make([]byte, s3ChunkSize)
	pendingBatch := make([]*kv.Elem[kv.OP], 0, s3ChunkBatchOps)
	pendingChunkSizes := make([]uint64, 0, s3ChunkBatchOps)
	uploadedManifest := func() *s3ObjectManifest {
		if len(part.ChunkSizes) == 0 {
			return &s3ObjectManifest{UploadID: uploadID}
		}
		clonedPart := part
		clonedPart.ChunkSizes = append([]uint64(nil), part.ChunkSizes...)
		clonedPart.ChunkCount = uint64(len(clonedPart.ChunkSizes))
		return &s3ObjectManifest{
			UploadID: uploadID,
			Parts:    []s3ObjectPart{clonedPart},
		}
	}
	flushBatch := func() error {
		if len(pendingBatch) == 0 {
			return nil
		}
		_, err := s.coordinator.Dispatch(r.Context(), &kv.OperationGroup[kv.OP]{Elems: pendingBatch})
		if err != nil {
			return errors.WithStack(err)
		}
		part.ChunkSizes = append(part.ChunkSizes, pendingChunkSizes...)
		part.ChunkCount = uint64(len(part.ChunkSizes))
		pendingBatch = pendingBatch[:0]
		pendingChunkSizes = pendingChunkSizes[:0]
		return nil
	}
	for {
		n, readErr := r.Body.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			if _, err := hasher.Write(chunk); err != nil {
				writeS3InternalError(w, err)
				return
			}
			if validatePayloadSHA {
				if _, err := sha256Hasher.Write(chunk); err != nil {
					writeS3InternalError(w, err)
					return
				}
			}
			chunkKey := s3keys.BlobKey(bucket, meta.Generation, objectKey, uploadID, part.PartNo, chunkNo)
			pendingBatch = append(pendingBatch, &kv.Elem[kv.OP]{Op: kv.Put, Key: chunkKey, Value: chunk})
			chunkSize, err := uint64FromInt(n)
			if err != nil {
				s.cleanupManifestBlobs(r.Context(), bucket, meta.Generation, objectKey, uploadedManifest())
				writeS3InternalError(w, err)
				return
			}
			pendingChunkSizes = append(pendingChunkSizes, chunkSize)
			if len(pendingBatch) >= s3ChunkBatchOps {
				if err := flushBatch(); err != nil {
					s.cleanupManifestBlobs(r.Context(), bucket, meta.Generation, objectKey, uploadedManifest())
					writeS3InternalError(w, err)
					return
				}
			}
			sizeBytes += int64(n)
			chunkNo++
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			s.cleanupManifestBlobs(r.Context(), bucket, meta.Generation, objectKey, uploadedManifest())
			var maxBytesErr *http.MaxBytesError
			if errors.As(readErr, &maxBytesErr) {
				writeS3Error(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "object exceeds maximum allowed size", bucket, objectKey)
				return
			}
			writeS3InternalError(w, readErr)
			return
		}
	}
	if err := flushBatch(); err != nil {
		s.cleanupManifestBlobs(r.Context(), bucket, meta.Generation, objectKey, uploadedManifest())
		writeS3InternalError(w, err)
		return
	}
	if validatePayloadSHA {
		actualPayloadSHA := hex.EncodeToString(sha256Hasher.Sum(nil))
		if !strings.EqualFold(actualPayloadSHA, expectedPayloadSHA) {
			s.cleanupManifestBlobs(r.Context(), bucket, meta.Generation, objectKey, uploadedManifest())
			writeS3Error(w, http.StatusBadRequest, "XAmzContentSHA256Mismatch", "payload SHA256 does not match x-amz-content-sha256", bucket, objectKey)
			return
		}
	}

	etag := hex.EncodeToString(hasher.Sum(nil))
	part.ETag = etag
	part.SizeBytes = sizeBytes
	part.ChunkCount = uint64(len(part.ChunkSizes))
	commitTS, err := s.nextTxnCommitTS(startTS)
	if err != nil {
		s.cleanupManifestBlobs(r.Context(), bucket, meta.Generation, objectKey, uploadedManifest())
		writeS3InternalError(w, err)
		return
	}
	manifest := &s3ObjectManifest{
		UploadID:           uploadID,
		ETag:               etag,
		SizeBytes:          sizeBytes,
		LastModifiedHLC:    commitTS,
		ContentType:        headerOrDefault(r.Header.Get("Content-Type"), "application/octet-stream"),
		ContentEncoding:    r.Header.Get("Content-Encoding"),
		CacheControl:       r.Header.Get("Cache-Control"),
		ContentDisposition: r.Header.Get("Content-Disposition"),
		UserMetadata:       collectS3UserMetadata(r.Header),
	}
	if sizeBytes > 0 {
		manifest.Parts = []s3ObjectPart{part}
	}
	body, err := encodeS3ObjectManifest(manifest)
	if err != nil {
		s.cleanupManifestBlobs(r.Context(), bucket, meta.Generation, objectKey, uploadedManifest())
		writeS3InternalError(w, err)
		return
	}
	bucketFence, err := encodeS3BucketMeta(meta)
	if err != nil {
		s.cleanupManifestBlobs(r.Context(), bucket, meta.Generation, objectKey, uploadedManifest())
		writeS3InternalError(w, err)
		return
	}
	if _, err := s.coordinator.Dispatch(r.Context(), &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: s3keys.BucketMetaKey(bucket), Value: bucketFence},
			{Op: kv.Put, Key: headKey, Value: body},
		},
	}); err != nil {
		s.cleanupManifestBlobs(r.Context(), bucket, meta.Generation, objectKey, uploadedManifest())
		writeS3MutationError(w, err, bucket, objectKey)
		return
	}

	if previous != nil {
		s.cleanupManifestBlobsAsync(bucket, meta.Generation, objectKey, previous)
	}
	w.Header().Set("ETag", quoteS3ETag(etag))
	w.WriteHeader(http.StatusOK)
}

//nolint:cyclop // The read handler branches on bucket/object existence and HEAD vs GET response flow.
func (s *S3Server) getObject(w http.ResponseWriter, r *http.Request, bucket string, objectKey string, headOnly bool) {
	if !headOnly && strings.TrimSpace(r.Header.Get("Range")) != "" {
		writeS3Error(w, http.StatusNotImplemented, "NotImplemented", "range reads are not implemented yet", bucket, objectKey)
		return
	}
	readTS := s.readTS()
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()
	meta, exists, err := s.loadBucketMetaAt(r.Context(), bucket, readTS)
	if err != nil {
		writeS3InternalError(w, err)
		return
	}
	if !exists || meta == nil {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", bucket, objectKey)
		return
	}

	headKey := s3keys.ObjectManifestKey(bucket, meta.Generation, objectKey)
	manifest, found, err := s.loadObjectManifestAt(r.Context(), headKey, readTS)
	if err != nil {
		writeS3InternalError(w, err)
		return
	}
	if !found || manifest == nil {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "object not found", bucket, objectKey)
		return
	}

	writeS3ObjectHeaders(w.Header(), manifest)
	w.WriteHeader(http.StatusOK)
	if headOnly {
		return
	}
	for _, part := range manifest.Parts {
		for chunkNo := range part.ChunkSizes {
			chunkIndex, err := uint64FromInt(chunkNo)
			if err != nil {
				writeS3InternalError(w, err)
				return
			}
			chunkKey := s3keys.BlobKey(bucket, meta.Generation, objectKey, manifest.UploadID, part.PartNo, chunkIndex)
			chunk, err := s.store.GetAt(r.Context(), chunkKey, readTS)
			if err != nil {
				writeS3InternalError(w, err)
				return
			}
			//nolint:gosec // G705: S3 serves stored object bytes verbatim by design.
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}
}

func (s *S3Server) deleteObject(w http.ResponseWriter, r *http.Request, bucket string, objectKey string) {
	var cleanupManifest *s3ObjectManifest
	var generation uint64
	err := s.retryS3Mutation(r.Context(), func() error {
		readTS := s.readTS()
		startTS := s.txnStartTS(readTS)
		readPin := s.pinReadTS(readTS)
		defer readPin.Release()

		meta, exists, err := s.loadBucketMetaAt(r.Context(), bucket, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if !exists || meta == nil {
			return &s3ResponseError{
				Status:  http.StatusNotFound,
				Code:    "NoSuchBucket",
				Message: "bucket not found",
				Bucket:  bucket,
				Key:     objectKey,
			}
		}

		headKey := s3keys.ObjectManifestKey(bucket, meta.Generation, objectKey)
		manifest, found, err := s.loadObjectManifestAt(r.Context(), headKey, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if !found {
			cleanupManifest = nil
			return nil
		}
		_, err = s.coordinator.Dispatch(r.Context(), &kv.OperationGroup[kv.OP]{
			IsTxn:   true,
			StartTS: startTS,
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Del, Key: headKey},
			},
		})
		if err != nil {
			return errors.WithStack(err)
		}
		cleanupManifest = manifest
		generation = meta.Generation
		return nil
	})
	if err != nil {
		writeS3MutationError(w, err, bucket, objectKey)
		return
	}
	if cleanupManifest != nil {
		s.cleanupManifestBlobsAsync(bucket, generation, objectKey, cleanupManifest)
	}
	w.WriteHeader(http.StatusNoContent)
}

//nolint:cyclop,gocognit,gocyclo,nestif // ListObjectsV2 combines token validation, shard-stable snapshotting, and delimiter pagination rules.
func (s *S3Server) listObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	readTS := s.readTS()
	meta, exists, err := s.loadBucketMetaAt(r.Context(), bucket, readTS)
	if err != nil {
		writeS3InternalError(w, err)
		return
	}
	if !exists || meta == nil {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "bucket not found", bucket, "")
		return
	}

	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	maxKeys := parseS3MaxKeys(query.Get("max-keys"))
	token, err := decodeS3ContinuationToken(query.Get("continuation-token"))
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "invalid continuation token", bucket, "")
		return
	}
	if token != nil {
		if token.Bucket != bucket || token.Generation != meta.Generation || token.Prefix != prefix || token.Delimiter != delimiter {
			writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "continuation token does not match request", bucket, "")
			return
		}
		if token.ReadTS != 0 {
			readTS = token.ReadTS
		}
	}
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()

	start := s3keys.ObjectManifestScanStart(bucket, meta.Generation, prefix)
	if token != nil {
		start = nextScanCursor(s3keys.ObjectManifestKey(bucket, token.Generation, token.LastKey))
	}
	if token == nil {
		startAfter := query.Get("start-after")
		if startAfter != "" {
			start = nextScanCursor(s3keys.ObjectManifestKey(bucket, meta.Generation, startAfter))
		}
	}
	end := prefixScanEnd(s3keys.ObjectManifestScanStart(bucket, meta.Generation, prefix))

	result := s3ListBucketResult{
		XMLNS:             s3XMLNamespace,
		Name:              bucket,
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		ContinuationToken: query.Get("continuation-token"),
	}
	commonPrefixSeen := map[string]struct{}{}
	if token != nil && token.LastCommonPrefix != "" {
		commonPrefixSeen[token.LastCommonPrefix] = struct{}{}
	}
	cursor := start
	truncated := false
	nextToken := ""

	for result.KeyCount < maxKeys {
		pageLimit := s3ListPageSize
		if remaining := maxKeys - result.KeyCount; remaining < pageLimit {
			pageLimit = remaining
		}
		page, err := s.store.ScanAt(r.Context(), cursor, end, pageLimit, readTS)
		if err != nil {
			if token != nil && errors.Is(err, store.ErrReadTSCompacted) {
				writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "continuation token has expired", bucket, "")
				return
			}
			writeS3InternalError(w, err)
			return
		}
		if len(page) == 0 {
			break
		}
		for _, kvp := range page {
			_, generation, objectKey, ok := s3keys.ParseObjectManifestKey(kvp.Key)
			if !ok || generation != meta.Generation || !strings.HasPrefix(objectKey, prefix) {
				continue
			}
			if delimiter != "" {
				suffix := strings.TrimPrefix(objectKey, prefix)
				if idx := strings.Index(suffix, delimiter); idx >= 0 {
					commonPrefix := prefix + suffix[:idx+len(delimiter)]
					if _, exists := commonPrefixSeen[commonPrefix]; !exists {
						commonPrefixSeen[commonPrefix] = struct{}{}
						result.CommonPrefixes = append(result.CommonPrefixes, s3CommonPrefix{Prefix: commonPrefix})
						result.KeyCount++
					}
					if result.KeyCount >= maxKeys {
						truncated = true
						nextToken = encodeS3ContinuationToken(bucket, meta.Generation, prefix, delimiter, objectKey, commonPrefix, readTS)
						break
					}
					continue
				}
			}
			manifest, err := decodeS3ObjectManifest(kvp.Value)
			if err != nil {
				writeS3InternalError(w, err)
				return
			}
			result.Contents = append(result.Contents, s3ListObjectContent{
				Key:          objectKey,
				LastModified: formatS3ISOTime(manifest.LastModifiedHLC),
				ETag:         quoteS3ETag(manifest.ETag),
				Size:         manifest.SizeBytes,
				StorageClass: "STANDARD",
			})
			result.KeyCount++
			if result.KeyCount >= maxKeys {
				truncated = true
				nextToken = encodeS3ContinuationToken(bucket, meta.Generation, prefix, delimiter, objectKey, "", readTS)
				break
			}
		}
		if truncated {
			break
		}
		cursor = nextScanCursor(page[len(page)-1].Key)
		if len(page) < pageLimit {
			break
		}
	}

	result.IsTruncated = truncated
	result.NextContinuationToken = nextToken
	writeS3XML(w, http.StatusOK, result)
}

func (s *S3Server) loadBucketMetaAt(ctx context.Context, bucket string, readTS uint64) (*s3BucketMeta, bool, error) {
	raw, err := s.store.GetAt(ctx, s3keys.BucketMetaKey(bucket), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	meta, err := decodeS3BucketMeta(raw)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func (s *S3Server) nextBucketGenerationAt(ctx context.Context, bucket string, readTS uint64) (uint64, error) {
	raw, err := s.store.GetAt(ctx, s3keys.BucketGenerationKey(bucket), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 1, nil
		}
		return 0, errors.WithStack(err)
	}
	v, err := decodeS3Generation(raw)
	if err != nil {
		return 0, err
	}
	return v + 1, nil
}

func (s *S3Server) loadObjectManifestAt(ctx context.Context, key []byte, readTS uint64) (*s3ObjectManifest, bool, error) {
	raw, err := s.store.GetAt(ctx, key, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	manifest, err := decodeS3ObjectManifest(raw)
	if err != nil {
		return nil, false, err
	}
	return manifest, true, nil
}

func (s *S3Server) cleanupManifestBlobsAsync(bucket string, generation uint64, objectKey string, manifest *s3ObjectManifest) {
	if manifest == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), s3ManifestCleanupTimeout)
		defer cancel()
		s.cleanupManifestBlobs(ctx, bucket, generation, objectKey, manifest)
	}()
}

func (s *S3Server) cleanupManifestBlobs(ctx context.Context, bucket string, generation uint64, objectKey string, manifest *s3ObjectManifest) {
	if s == nil || manifest == nil || manifest.UploadID == "" || s.coordinator == nil {
		return
	}
	pending := make([]*kv.Elem[kv.OP], 0, s3ChunkBatchOps)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		_, _ = s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: pending})
		pending = pending[:0]
	}
	for _, part := range manifest.Parts {
		for chunkNo := range part.ChunkSizes {
			chunkIndex, err := uint64FromInt(chunkNo)
			if err != nil {
				return
			}
			pending = append(pending, &kv.Elem[kv.OP]{
				Op:  kv.Del,
				Key: s3keys.BlobKey(bucket, generation, objectKey, manifest.UploadID, part.PartNo, chunkIndex),
			})
			if len(pending) >= s3ChunkBatchOps {
				flush()
			}
		}
	}
	flush()
}

//nolint:cyclop // Proxying depends on root, bucket, and object-level leadership decisions.
func (s *S3Server) maybeProxyToLeader(w http.ResponseWriter, r *http.Request) bool {
	if s == nil || s.coordinator == nil {
		return false
	}
	key, err := s.proxyRouteKey(r)
	if err != nil {
		return false
	}
	var leader raft.ServerAddress
	if len(key) > 0 {
		if s.coordinator.IsLeaderForKey(key) && s.coordinator.VerifyLeaderForKey(key) == nil {
			return false
		}
		leader = s.coordinator.RaftLeaderForKey(key)
	} else {
		if s.coordinator.IsLeader() && s.coordinator.VerifyLeader() == nil {
			return false
		}
		leader = s.coordinator.RaftLeader()
	}
	if leader == "" {
		writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "leader not found", "", "")
		return true
	}
	targetHost, ok := s.leaderS3[leader]
	if !ok || strings.TrimSpace(targetHost) == "" {
		writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "leader endpoint not found", "", "")
		return true
	}
	target := &url.URL{Scheme: "http", Host: targetHost}
	originalHost := r.Host
	proxy := httputil.NewSingleHostReverseProxy(target)
	origDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		origDirector(req)
		req.Host = originalHost
	}
	proxy.ErrorHandler = func(rw http.ResponseWriter, _ *http.Request, err error) {
		writeS3InternalError(rw, err)
	}
	proxy.ServeHTTP(w, r)
	return true
}

func (s *S3Server) proxyRouteKey(r *http.Request) ([]byte, error) {
	bucket, objectKey, hasObject, err := parseS3Path(r)
	if err != nil || bucket == "" {
		return nil, errors.WithStack(err)
	}
	if !hasObject {
		return s3keys.BucketMetaKey(bucket), nil
	}

	meta, exists, err := s.loadBucketMetaAt(r.Context(), bucket, s.readTS())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists || meta == nil {
		return s3keys.BucketMetaKey(bucket), nil
	}
	return s3keys.RouteKey(bucket, meta.Generation, objectKey), nil
}

func (s *S3Server) clock() *kv.HLC {
	if s == nil || s.coordinator == nil {
		return nil
	}
	return s.coordinator.Clock()
}

func (s *S3Server) coordinatorLeaderAddress() raft.ServerAddress {
	if s == nil || s.coordinator == nil {
		return ""
	}
	return s.coordinator.RaftLeader()
}

func parseS3Path(r *http.Request) (string, string, bool, error) {
	escaped := "/"
	if r != nil && r.URL != nil {
		escaped = r.URL.EscapedPath()
	}
	trimmed := strings.TrimPrefix(escaped, "/")
	if trimmed == "" {
		return "", "", false, nil
	}
	parts := strings.SplitN(trimmed, "/", s3PathSplitParts)
	bucket, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", "", false, errors.WithStack(err)
	}
	if len(parts) == 1 {
		return bucket, "", false, nil
	}
	objectKey, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", "", false, errors.WithStack(err)
	}
	return bucket, objectKey, true, nil
}

func serveS3Healthz(w http.ResponseWriter, r *http.Request) bool {
	if r == nil || r.URL == nil || r.URL.Path != s3HealthPath {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return true
	}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = io.WriteString(w, "ok")
	}
	return true
}

func encodeS3BucketMeta(meta *s3BucketMeta) ([]byte, error) {
	if meta == nil {
		return nil, errors.New("s3 bucket metadata is required")
	}
	body, err := json.Marshal(meta)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return body, nil
}

func decodeS3BucketMeta(raw []byte) (*s3BucketMeta, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	meta := &s3BucketMeta{}
	if err := json.Unmarshal(raw, meta); err != nil {
		return nil, errors.WithStack(err)
	}
	return meta, nil
}

func encodeS3ObjectManifest(manifest *s3ObjectManifest) ([]byte, error) {
	if manifest == nil {
		return nil, errors.New("s3 manifest is required")
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return body, nil
}

func decodeS3ObjectManifest(raw []byte) (*s3ObjectManifest, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	manifest := &s3ObjectManifest{}
	if err := json.Unmarshal(raw, manifest); err != nil {
		return nil, errors.WithStack(err)
	}
	return manifest, nil
}

func encodeS3Generation(generation uint64) []byte {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], generation)
	return raw[:]
}

func decodeS3Generation(raw []byte) (uint64, error) {
	if len(raw) == s3GenerationBytes {
		return binary.BigEndian.Uint64(raw), nil
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return v, nil
}

func writeS3XML(w http.ResponseWriter, status int, body any) {
	payload, err := xml.Marshal(body)
	if err != nil {
		writeS3InternalError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(payload)
}

func writeS3Error(w http.ResponseWriter, status int, code string, message string, bucket string, key string) {
	writeS3XML(w, status, s3ErrorResponse{
		XMLNS:   s3XMLNamespace,
		Code:    code,
		Message: message,
		Bucket:  bucket,
		Key:     key,
	})
}

func writeS3InternalError(w http.ResponseWriter, err error) {
	if err != nil {
		slog.Error("s3 internal error", "error", err)
	}
	writeS3Error(w, http.StatusInternalServerError, "InternalError", "internal error", "", "")
}

func writeS3ObjectHeaders(h http.Header, manifest *s3ObjectManifest) {
	if h == nil || manifest == nil {
		return
	}
	h.Set("ETag", quoteS3ETag(manifest.ETag))
	h.Set("Content-Length", strconv.FormatInt(manifest.SizeBytes, 10))
	h.Set("Last-Modified", formatS3HTTPTime(manifest.LastModifiedHLC))
	h.Set("Content-Type", headerOrDefault(manifest.ContentType, "application/octet-stream"))
	if manifest.ContentEncoding != "" {
		h.Set("Content-Encoding", manifest.ContentEncoding)
	}
	if manifest.CacheControl != "" {
		h.Set("Cache-Control", manifest.CacheControl)
	}
	if manifest.ContentDisposition != "" {
		h.Set("Content-Disposition", manifest.ContentDisposition)
	}
	for key, value := range manifest.UserMetadata {
		h.Set("x-amz-meta-"+key, value)
	}
}

func collectS3UserMetadata(h http.Header) map[string]string {
	if h == nil {
		return nil
	}
	out := map[string]string{}
	for key, values := range h {
		lower := strings.ToLower(key)
		if !strings.HasPrefix(lower, "x-amz-meta-") || len(values) == 0 {
			continue
		}
		out[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func decodeS3ContinuationToken(raw string) (*s3ContinuationToken, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	token := &s3ContinuationToken{}
	if err := json.Unmarshal(data, token); err != nil {
		return nil, errors.WithStack(err)
	}
	return token, nil
}

func encodeS3ContinuationToken(bucket string, generation uint64, prefix string, delimiter string, lastKey string, lastCommonPrefix string, readTS uint64) string {
	data, err := json.Marshal(&s3ContinuationToken{
		Bucket:           bucket,
		Generation:       generation,
		Prefix:           prefix,
		Delimiter:        delimiter,
		LastKey:          lastKey,
		LastCommonPrefix: lastCommonPrefix,
		ReadTS:           readTS,
	})
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

func parseS3MaxKeys(raw string) int {
	if strings.TrimSpace(raw) == "" {
		return s3MaxKeys
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return s3MaxKeys
	}
	if v > s3MaxKeys {
		return s3MaxKeys
	}
	return v
}

func quoteS3ETag(etag string) string {
	if etag == "" {
		return `""`
	}
	return fmt.Sprintf("%q", etag)
}

func formatS3HTTPTime(ts uint64) string {
	return hlcToTime(ts).UTC().Format(http.TimeFormat)
}

func formatS3ISOTime(ts uint64) string {
	return hlcToTime(ts).UTC().Format("2006-01-02T15:04:05.000Z")
}

func hlcToTime(ts uint64) time.Time {
	if ts == 0 || ts == ^uint64(0) {
		return time.Unix(0, 0).UTC()
	}
	millis := ts >> s3HLCPhysicalShift
	if millis > math.MaxInt64 {
		millis = math.MaxInt64
	}
	return time.UnixMilli(int64(millis)).UTC() //nolint:gosec // G115: millis is clamped to MaxInt64 above.
}

func (s *S3Server) readTS() uint64 {
	return snapshotTS(s.clock(), s.store)
}

func (s *S3Server) pinReadTS(ts uint64) *kv.ActiveTimestampToken {
	if s == nil || s.readTracker == nil {
		return nil
	}
	return s.readTracker.Pin(ts)
}

func (s *S3Server) txnStartTS(readTS uint64) uint64 {
	if readTS == ^uint64(0) {
		if clock := s.clock(); clock != nil {
			return clock.Next()
		}
		return 1
	}
	return readTS
}

func newS3UploadID(clock *kv.HLC) string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	if clock != nil {
		return fmt.Sprintf("%016x", clock.Next())
	}
	return strconv.FormatInt(time.Now().UnixNano(), 16)
}

func (s *S3Server) effectiveRegion() string {
	if s == nil || strings.TrimSpace(s.region) == "" {
		return s3DefaultRegion
	}
	return s.region
}

func (s *S3Server) nextTxnCommitTS(startTS uint64) (uint64, error) {
	clock := s.clock()
	if clock == nil {
		if startTS == ^uint64(0) {
			return 0, errors.WithStack(kv.ErrTxnCommitTSRequired)
		}
		return startTS + 1, nil
	}
	clock.Observe(startTS)
	commitTS := clock.Next()
	if commitTS <= startTS {
		return 0, errors.WithStack(kv.ErrTxnCommitTSRequired)
	}
	return commitTS, nil
}

func isRetryableS3MutationErr(err error) bool {
	return errors.Is(err, store.ErrWriteConflict) || errors.Is(err, kv.ErrTxnLocked)
}

func waitS3RetryBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextS3RetryBackoff(current time.Duration) time.Duration {
	next := current * s3TxnRetryBackoffFactor
	if next > s3TxnRetryMaxBackoff {
		return s3TxnRetryMaxBackoff
	}
	return next
}

func (s *S3Server) retryS3Mutation(ctx context.Context, fn func() error) error {
	backoff := s3TxnRetryInitialBackoff
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !isRetryableS3MutationErr(err) {
			return errors.WithStack(err)
		}
		if attempt >= s3TxnRetryMaxAttempts {
			return errors.WithStack(err)
		}
		if !waitS3RetryBackoff(ctx, backoff) {
			return errors.WithStack(err)
		}
		backoff = nextS3RetryBackoff(backoff)
	}
}

func writeS3MutationError(w http.ResponseWriter, err error, bucket string, key string) {
	var responseErr *s3ResponseError
	if errors.As(err, &responseErr) {
		writeS3Error(w, responseErr.Status, responseErr.Code, responseErr.Message, responseErr.Bucket, responseErr.Key)
		return
	}
	if isRetryableS3MutationErr(err) {
		writeS3Error(w, http.StatusConflict, "OperationAborted", "conflicting conditional operation in progress", bucket, key)
		return
	}
	writeS3InternalError(w, err)
}

func validateS3BucketName(bucket string) error {
	if len(bucket) < 3 || len(bucket) > 63 {
		return errors.New("bucket name must be 3-63 characters")
	}
	for _, r := range bucket {
		if !isValidS3BucketRune(r) {
			return errors.New("bucket name contains unsupported characters")
		}
	}
	return nil
}

func isValidS3BucketRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z':
		return true
	case r >= '0' && r <= '9':
		return true
	case r == '.' || r == '-':
		return true
	default:
		return false
	}
}

func uint64FromInt(v int) (uint64, error) {
	if v < 0 {
		return 0, errors.New("negative integer cannot be converted to uint64")
	}
	return uint64(v), nil
}

func validateS3PutPreconditions(r *http.Request, previous *s3ObjectManifest) error {
	if r == nil {
		return nil
	}
	if ifNoneMatch := strings.TrimSpace(r.Header.Get("If-None-Match")); ifNoneMatch == "*" && previous != nil {
		return errors.New("object already exists")
	}
	if ifMatch := strings.TrimSpace(r.Header.Get("If-Match")); ifMatch != "" {
		if previous == nil || strings.Trim(ifMatch, `"`) != previous.ETag {
			return errors.New("etag precondition failed")
		}
	}
	return nil
}

func cloneLeaderAddrMap(src map[raft.ServerAddress]string) map[raft.ServerAddress]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[raft.ServerAddress]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func headerOrDefault(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

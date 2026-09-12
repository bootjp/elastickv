package snapshotoffload

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/cockroachdb/errors"
)

const (
	s3MetadataSHA256          = "elastickv-sha256"
	s3LoadConfigOptionCapHint = 4
	s3ConditionalWriteRetries = 3
	s3MaxSinglePutBytes       = int64(5 * 1024 * 1024 * 1024)
	s3DefaultMultipartPart    = int64(64 * 1024 * 1024)
	s3MaxMultipartPart        = int64(5 * 1024 * 1024 * 1024)
	s3MaxMultipartParts       = int64(10_000)
	s3MaxObjectBytes          = int64(5 * 1024 * 1024 * 1024 * 1024)
	s3MultipartAbortTimeout   = 30 * time.Second
)

type S3ObjectClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type S3StoreConfig struct {
	Client                 S3ObjectClient
	Bucket                 string
	Region                 string
	Endpoint               string
	Profile                string
	ForcePathStyle         bool
	AccessKeyID            string
	SecretAccessKey        string
	SessionToken           string
	ServerSideEncryption   string
	SSEKMSKeyID            string
	DisableChecksumHeaders bool
}

type S3Store struct {
	client                 S3ObjectClient
	bucket                 string
	serverSideEncryption   string
	sseKMSKeyID            string
	disableChecksumHeaders bool
	multipartThreshold     int64
	multipartPartSize      int64
}

func NewS3Store(ctx context.Context, cfg S3StoreConfig) (*S3Store, error) {
	if stringsTrim(cfg.Bucket) == "" {
		return nil, errors.Wrap(ErrInvalidOptions, "s3 bucket is required")
	}
	if err := validateS3EncryptionConfig(cfg.ServerSideEncryption, cfg.SSEKMSKeyID); err != nil {
		return nil, err
	}
	client := cfg.Client
	if client == nil {
		awsCfg, err := loadS3AWSConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}
		client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.UsePathStyle = cfg.ForcePathStyle
			if stringsTrim(cfg.Endpoint) != "" {
				o.BaseEndpoint = aws.String(stringsTrim(cfg.Endpoint))
			}
		})
	}
	return &S3Store{
		client:                 client,
		bucket:                 stringsTrim(cfg.Bucket),
		serverSideEncryption:   stringsTrim(cfg.ServerSideEncryption),
		sseKMSKeyID:            stringsTrim(cfg.SSEKMSKeyID),
		disableChecksumHeaders: cfg.DisableChecksumHeaders,
		multipartThreshold:     s3MaxSinglePutBytes,
		multipartPartSize:      s3DefaultMultipartPart,
	}, nil
}

func loadS3AWSConfig(ctx context.Context, cfg S3StoreConfig) (aws.Config, error) {
	optFns := make([]func(*config.LoadOptions) error, 0, s3LoadConfigOptionCapHint)
	if stringsTrim(cfg.Region) != "" {
		optFns = append(optFns, config.WithRegion(stringsTrim(cfg.Region)))
	}
	if stringsTrim(cfg.Profile) != "" {
		optFns = append(optFns, config.WithSharedConfigProfile(stringsTrim(cfg.Profile)))
	}
	if stringsTrim(cfg.AccessKeyID) != "" || stringsTrim(cfg.SecretAccessKey) != "" || stringsTrim(cfg.SessionToken) != "" {
		if stringsTrim(cfg.AccessKeyID) == "" || stringsTrim(cfg.SecretAccessKey) == "" {
			return aws.Config{}, errors.Wrap(ErrInvalidOptions, "both s3 access key id and secret access key are required")
		}
		optFns = append(optFns, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			stringsTrim(cfg.AccessKeyID),
			stringsTrim(cfg.SecretAccessKey),
			stringsTrim(cfg.SessionToken),
		)))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return aws.Config{}, errors.Wrap(err, "load s3 config")
	}
	return awsCfg, nil
}

func (s *S3Store) PutObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error) {
	if err := validatePutOptions(opts); err != nil {
		return ObjectInfo{}, err
	}
	normalized, err := validateStoreObjectKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := s.putObject(ctx, normalized, body, opts, true); err != nil {
		if errors.Is(err, ErrObjectConflict) {
			return ObjectInfo{}, err
		}
		if !isS3PreconditionFailed(err) {
			return ObjectInfo{}, err
		}
		return s.verifyS3ExistingObject(ctx, normalized, opts)
	}
	return s.verifyS3PutObject(ctx, normalized, opts)
}

func (s *S3Store) RefreshObject(ctx context.Context, key string, body io.Reader, opts PutOptions) (ObjectInfo, error) {
	if err := validatePutOptions(opts); err != nil {
		return ObjectInfo{}, err
	}
	normalized, err := validateStoreObjectKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := s.putObject(ctx, normalized, body, opts, false); err != nil {
		return ObjectInfo{}, err
	}
	return s.verifyS3PutObject(ctx, normalized, opts)
}

func (s *S3Store) putObject(ctx context.Context, key string, body io.Reader, opts PutOptions, ifAbsent bool) error {
	put := func() error {
		if opts.Size > s.multipartThreshold {
			return s.putMultipart(ctx, key, body, opts, ifAbsent)
		}
		input, err := s.putObjectInput(key, body, opts)
		if err != nil {
			return err
		}
		if !ifAbsent {
			input.IfNoneMatch = nil
		}
		if _, err = s.client.PutObject(ctx, input); err != nil {
			return errors.Wrap(err, "put s3 object")
		}
		return nil
	}
	if !ifAbsent {
		return put()
	}
	return s.putObjectWithRetry(key, body, put)
}

func (s *S3Store) putObjectWithRetry(
	key string,
	body io.Reader,
	put func() error,
) error {
	start, seeker := readerPosition(body)
	var err error
	for attempt := 0; attempt < s3ConditionalWriteRetries; attempt++ {
		if attempt > 0 {
			if seeker == nil {
				return errors.Wrap(ErrObjectConflict, "retrying s3 conditional conflict requires a seekable body")
			}
			if _, seekErr := seeker.Seek(start, io.SeekStart); seekErr != nil {
				return errors.Wrap(seekErr, "rewind s3 conditional write body")
			}
		}
		if err = put(); err == nil || !isS3ConditionalConflict(err) {
			return err
		}
	}
	return errors.Wrapf(ErrObjectConflict, "s3 conditional write for %s conflicted after %d attempts",
		key, s3ConditionalWriteRetries)
}

func (s *S3Store) putMultipart(
	ctx context.Context,
	key string,
	body io.Reader,
	opts PutOptions,
	ifAbsent bool,
) (retErr error) {
	partSize, err := multipartPartSize(opts.Size, s.multipartPartSize)
	if err != nil {
		return err
	}
	uploadID, err := s.createMultipartUpload(ctx, key, opts)
	if err != nil {
		return err
	}
	completed := false
	defer func() {
		if !completed {
			retErr = s.abortMultipartUpload(ctx, key, uploadID, retErr)
		}
	}()
	parts, err := s.uploadParts(ctx, key, uploadID, body, opts, partSize)
	if err != nil {
		return err
	}
	if err := s.completeMultipartUpload(ctx, key, uploadID, opts.Size, parts, ifAbsent); err != nil {
		return err
	}
	completed = true
	return nil
}

func (s *S3Store) createMultipartUpload(ctx context.Context, key string, opts PutOptions) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Metadata: map[string]string{
			s3MetadataSHA256: opts.SHA256,
		},
		ServerSideEncryption: types.ServerSideEncryption(s.serverSideEncryption),
	}
	if !s.disableChecksumHeaders {
		input.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
	}
	if stringsTrim(opts.ContentType) != "" {
		input.ContentType = aws.String(stringsTrim(opts.ContentType))
	}
	if s.sseKMSKeyID != "" {
		input.SSEKMSKeyId = aws.String(s.sseKMSKeyID)
	}
	out, err := s.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", errors.Wrap(err, "create s3 multipart upload")
	}
	uploadID := stringsTrim(aws.ToString(out.UploadId))
	if uploadID == "" {
		return "", errors.Wrap(ErrIntegrity, "s3 multipart upload returned empty upload id")
	}
	return uploadID, nil
}

func (s *S3Store) abortMultipartUpload(ctx context.Context, key, uploadID string, prior error) error {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s3MultipartAbortTimeout)
	defer cancel()
	_, abortErr := s.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if abortErr == nil {
		return prior
	}
	if prior == nil {
		return errors.Wrap(abortErr, "abort s3 multipart upload")
	}
	return errors.WithStack(errors.CombineErrors(prior, errors.Wrap(abortErr, "abort s3 multipart upload")))
}

func (s *S3Store) uploadParts(
	ctx context.Context,
	key string,
	uploadID string,
	body io.Reader,
	opts PutOptions,
	partSize int64,
) ([]types.CompletedPart, error) {
	totalBytes := opts.Size
	parts := make([]types.CompletedPart, 0, (totalBytes+partSize-1)/partSize)
	fullSum := sha256.New()
	source, sourceStart, err := multipartSectionSource(body, !s.disableChecksumHeaders)
	if err != nil {
		return nil, err
	}
	remaining := totalBytes
	var uploaded int64
	for partNumber := int32(1); remaining > 0; partNumber++ {
		partBytes := min(remaining, partSize)
		partReader, checksum, err := s.multipartPartReader(body, source, sourceStart+uploaded, partBytes, fullSum)
		if err != nil {
			return nil, err
		}
		completedPart, err := s.uploadMultipartPart(ctx, key, uploadID, partNumber, partBytes, partReader, checksum)
		if err != nil {
			return nil, err
		}
		parts = append(parts, completedPart)
		remaining -= partBytes
		uploaded += partBytes
	}
	if source != nil {
		if _, err := source.Seek(sourceStart+totalBytes, io.SeekStart); err != nil {
			return nil, errors.Wrap(err, "advance s3 multipart source")
		}
	}
	if err := requireNoTrailingBytes(body); err != nil {
		return nil, errors.Wrap(err, "s3 multipart source length differs from declared length")
	}
	if gotSHA := hex.EncodeToString(fullSum.Sum(nil)); gotSHA != opts.SHA256 {
		return nil, errors.Wrapf(ErrIntegrity, "s3 multipart source sha256 %s, expected %s", gotSHA, opts.SHA256)
	}
	return parts, nil
}

func (s *S3Store) uploadMultipartPart(
	ctx context.Context,
	key string,
	uploadID string,
	partNumber int32,
	partBytes int64,
	partReader io.Reader,
	checksum string,
) (types.CompletedPart, error) {
	counted := &countingReader{reader: partReader}
	input := &s3.UploadPartInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(partNumber),
		Body:          counted,
		ContentLength: aws.Int64(partBytes),
	}
	if checksum != "" {
		input.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
		input.ChecksumSHA256 = aws.String(checksum)
	}
	out, err := s.client.UploadPart(ctx, input)
	if err != nil {
		return types.CompletedPart{}, errors.Wrap(err, "upload s3 multipart part")
	}
	if counted.n != partBytes {
		return types.CompletedPart{}, errors.Wrapf(ErrIntegrity, "s3 multipart part %d read %d bytes, expected %d",
			partNumber, counted.n, partBytes)
	}
	if stringsTrim(aws.ToString(out.ETag)) == "" {
		return types.CompletedPart{}, errors.Wrapf(ErrIntegrity, "s3 multipart part %d returned no etag", partNumber)
	}
	completedPart := types.CompletedPart{
		ETag:       out.ETag,
		PartNumber: aws.Int32(partNumber),
	}
	if checksum != "" {
		completedPart.ChecksumSHA256 = aws.String(checksum)
	}
	return completedPart, nil
}

type readAtSeeker interface {
	io.ReaderAt
	io.Seeker
}

type countingReader struct {
	reader io.Reader
	n      int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.n += int64(n)
	if err == nil {
		return n, nil
	}
	if errors.Is(err, io.EOF) {
		return n, io.EOF
	}
	return n, errors.WithStack(err)
}

func multipartSectionSource(body io.Reader, requireSeekable bool) (readAtSeeker, int64, error) {
	source, ok := body.(readAtSeeker)
	if !ok {
		if requireSeekable {
			return nil, 0, errors.Wrap(ErrInvalidOptions, "s3 multipart checksum headers require a seekable source")
		}
		return nil, 0, nil
	}
	start, err := source.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, errors.Wrap(err, "read s3 multipart source position")
	}
	return source, start, nil
}

func (s *S3Store) multipartPartReader(
	body io.Reader,
	source readAtSeeker,
	offset int64,
	partBytes int64,
	fullSum io.Writer,
) (io.Reader, string, error) {
	if source == nil {
		return io.TeeReader(io.LimitReader(body, partBytes), fullSum), "", nil
	}
	if s.disableChecksumHeaders {
		return io.TeeReader(io.NewSectionReader(source, offset, partBytes), fullSum), "", nil
	}
	partSum := sha256.New()
	if _, err := io.Copy(partSum, io.NewSectionReader(source, offset, partBytes)); err != nil {
		return nil, "", errors.Wrap(err, "hash s3 multipart part")
	}
	checksum := base64.StdEncoding.EncodeToString(partSum.Sum(nil))
	return io.TeeReader(io.NewSectionReader(source, offset, partBytes), fullSum), checksum, nil
}

func (s *S3Store) completeMultipartUpload(
	ctx context.Context,
	key string,
	uploadID string,
	size int64,
	parts []types.CompletedPart,
	ifAbsent bool,
) error {
	input := &s3.CompleteMultipartUploadInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		MpuObjectSize: aws.Int64(size),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	}
	if ifAbsent {
		input.IfNoneMatch = aws.String("*")
	}
	_, err := s.client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return errors.Wrap(err, "complete s3 multipart upload")
	}
	return nil
}

func (s *S3Store) verifyS3PutObject(ctx context.Context, key string, opts PutOptions) (ObjectInfo, error) {
	info, ok, err := s.HeadObject(ctx, key)
	if err != nil {
		return ObjectInfo{}, errors.Wrap(err, "head s3 object after put")
	}
	if !ok {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s missing after put", key)
	}
	if info.Size != opts.Size || (info.SHA256 != "" && info.SHA256 != opts.SHA256) {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s remote integrity mismatch", key)
	}
	if err := s.validateS3ObjectEncryption(key, info); err != nil {
		return ObjectInfo{}, err
	}
	if info.SHA256 == "" {
		verified, err := s.verifyS3ObjectBytes(ctx, key, opts)
		if err != nil {
			return ObjectInfo{}, err
		}
		info = verified
	}
	return info, nil
}

func (s *S3Store) putObjectInput(key string, body io.Reader, opts PutOptions) (*s3.PutObjectInput, error) {
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(opts.Size),
		IfNoneMatch:   aws.String("*"),
		Metadata: map[string]string{
			s3MetadataSHA256: opts.SHA256,
		},
	}
	if stringsTrim(opts.ContentType) != "" {
		input.ContentType = aws.String(stringsTrim(opts.ContentType))
	}
	if s.serverSideEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(s.serverSideEncryption)
	}
	if s.sseKMSKeyID != "" {
		input.SSEKMSKeyId = aws.String(s.sseKMSKeyID)
	}
	if !s.disableChecksumHeaders {
		checksum, err := sha256HexToBase64(opts.SHA256)
		if err != nil {
			return nil, err
		}
		input.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
		input.ChecksumSHA256 = aws.String(checksum)
	}
	return input, nil
}

func (s *S3Store) GetObject(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	normalized, err := validateStoreObjectKey(key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(normalized),
	}
	if !s.disableChecksumHeaders {
		input.ChecksumMode = types.ChecksumModeEnabled
	}
	out, err := s.client.GetObject(ctx, input)
	if err != nil {
		if isS3NotFound(err) {
			return nil, ObjectInfo{}, errors.Wrapf(ErrObjectNotFound, "object %s", normalized)
		}
		return nil, ObjectInfo{}, errors.Wrap(err, "get s3 object")
	}
	if out.Body == nil {
		return nil, ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s returned empty body", normalized)
	}
	info, err := s3ObjectInfo(
		normalized,
		out.ContentLength,
		out.Metadata,
		out.ChecksumSHA256,
		out.ChecksumType,
		out.LastModified,
		out.ServerSideEncryption,
		out.SSEKMSKeyId,
	)
	if err != nil {
		_ = out.Body.Close()
		return nil, ObjectInfo{}, err
	}
	if err := s.validateS3ObjectEncryption(normalized, info); err != nil {
		_ = out.Body.Close()
		return nil, ObjectInfo{}, err
	}
	info.ETag = aws.ToString(out.ETag)
	return out.Body, info, nil
}

func (s *S3Store) HeadObject(ctx context.Context, key string) (ObjectInfo, bool, error) {
	normalized, err := validateStoreObjectKey(key)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(normalized),
	}
	if !s.disableChecksumHeaders {
		input.ChecksumMode = types.ChecksumModeEnabled
	}
	out, err := s.client.HeadObject(ctx, input)
	if err != nil {
		if isS3NotFound(err) {
			return ObjectInfo{}, false, nil
		}
		return ObjectInfo{}, false, errors.Wrap(err, "head s3 object")
	}
	info, err := s3ObjectInfo(
		normalized,
		out.ContentLength,
		out.Metadata,
		out.ChecksumSHA256,
		out.ChecksumType,
		out.LastModified,
		out.ServerSideEncryption,
		out.SSEKMSKeyId,
	)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	if err := s.validateS3ObjectEncryption(normalized, info); err != nil {
		return ObjectInfo{}, false, err
	}
	// Carried so retention can use it as the compare-and-delete
	// precondition on DeleteObjectIfUnmodified.
	info.ETag = aws.ToString(out.ETag)
	return info, true, nil
}

func (s *S3Store) verifyS3ExistingObject(ctx context.Context, key string, opts PutOptions) (ObjectInfo, error) {
	info, ok, err := s.HeadObject(ctx, key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if !ok {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s conflicted but is not visible", key)
	}
	if err := s.validateS3ObjectEncryption(key, info); err != nil {
		return ObjectInfo{}, err
	}
	if info.Size != opts.Size {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s already exists with different content", key)
	}
	if info.SHA256 == opts.SHA256 {
		return info, nil
	}
	if info.SHA256 == "" {
		return s.verifyS3ObjectBytes(ctx, key, opts)
	}
	return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s already exists with different content", key)
}

func (s *S3Store) verifyS3ObjectBytes(ctx context.Context, key string, opts PutOptions) (ObjectInfo, error) {
	body, info, err := s.GetObject(ctx, key)
	if err != nil {
		return ObjectInfo{}, errors.Wrap(err, "get s3 object for integrity verification")
	}
	defer func() { _ = body.Close() }()
	sum := sha256.New()
	n, err := io.Copy(sum, contextReader{ctx: ctx, reader: body})
	if err != nil {
		return ObjectInfo{}, errors.WithStack(err)
	}
	gotSHA := hex.EncodeToString(sum.Sum(nil))
	if n != opts.Size || gotSHA != opts.SHA256 {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s already exists with different content", key)
	}
	info.Size = n
	info.SHA256 = gotSHA
	if err := s.validateS3ObjectEncryption(key, info); err != nil {
		return ObjectInfo{}, err
	}
	return info, nil
}

func (s *S3Store) validateS3ObjectEncryption(key string, info ObjectInfo) error {
	if info.ServerSideEncryption != s.serverSideEncryption {
		return errors.Wrapf(ErrIntegrity, "s3 object %s encryption %q, expected %q",
			key, info.ServerSideEncryption, s.serverSideEncryption)
	}
	if s.serverSideEncryption != string(types.ServerSideEncryptionAwsKms) {
		return nil
	}
	if !kmsKeyIDsMatch(info.SSEKMSKeyID, s.sseKMSKeyID) {
		return errors.Wrapf(ErrIntegrity, "s3 object %s kms key %q, expected %q",
			key, info.SSEKMSKeyID, s.sseKMSKeyID)
	}
	return nil
}

func readerPosition(body io.Reader) (int64, io.Seeker) {
	seeker, ok := body.(io.Seeker)
	if !ok {
		return 0, nil
	}
	position, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, nil
	}
	return position, seeker
}

func multipartPartSize(totalBytes, configured int64) (int64, error) {
	if totalBytes <= 0 || configured <= 0 {
		return 0, errors.Wrap(ErrInvalidOptions, "s3 multipart length and part size must be positive")
	}
	if totalBytes > s3MaxObjectBytes {
		return 0, errors.Wrapf(ErrInvalidOptions, "s3 object exceeds multipart limit: bytes=%d", totalBytes)
	}
	partSize := max(configured, (totalBytes+s3MaxMultipartParts-1)/s3MaxMultipartParts)
	if partSize > s3MaxMultipartPart {
		return 0, errors.Wrapf(ErrInvalidOptions, "s3 multipart object is too large: bytes=%d", totalBytes)
	}
	return partSize, nil
}

func requireNoTrailingBytes(reader io.Reader) error {
	var extra [1]byte
	n, err := reader.Read(extra[:])
	if n > 0 {
		return errors.Wrap(ErrIntegrity, "source has trailing bytes after declared length")
	}
	if err == nil {
		return errors.Wrap(ErrIntegrity, "source did not report EOF after declared length")
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return errors.WithStack(err)
}

func validateStoreObjectKey(key string) (string, error) {
	normalized := normalizeObjectKey(key)
	if normalized == "" || normalized == "." || normalized == ".." || strings.HasPrefix(normalized, "../") {
		return "", errors.Wrapf(ErrInvalidOptions, "invalid object key %q", key)
	}
	return normalized, nil
}

func s3ObjectInfo(
	key string,
	contentLength *int64,
	metadata map[string]string,
	checksumSHA256 *string,
	checksumType types.ChecksumType,
	updatedAt *time.Time,
	encryption types.ServerSideEncryption,
	kmsKeyID *string,
) (ObjectInfo, error) {
	if contentLength == nil || *contentLength < 0 {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s missing content length", key)
	}
	sha, err := s3ObjectSHA256(metadata, checksumSHA256, checksumType)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Key:                  key,
		Size:                 *contentLength,
		UpdatedAt:            aws.ToTime(updatedAt),
		SHA256:               sha,
		ServerSideEncryption: string(encryption),
		SSEKMSKeyID:          aws.ToString(kmsKeyID),
	}, nil
}

func s3ObjectSHA256(metadata map[string]string, checksumSHA256 *string, checksumType types.ChecksumType) (string, error) {
	metadataSHA, err := s3MetadataSHA(metadata)
	if err != nil {
		return "", err
	}
	checksumSHA, err := s3ChecksumSHA(checksumSHA256, checksumType)
	if err != nil {
		return "", err
	}
	if metadataSHA != "" && checksumSHA != "" && metadataSHA != checksumSHA {
		return "", errors.Wrap(ErrIntegrity, "s3 object sha256 metadata does not match full-object checksum")
	}
	switch {
	case metadataSHA != "":
		return metadataSHA, nil
	case checksumSHA != "":
		return checksumSHA, nil
	default:
		return "", nil
	}
}

func s3MetadataSHA(metadata map[string]string) (string, error) {
	if len(metadata) == 0 {
		return "", nil
	}
	for key, value := range metadata {
		if strings.EqualFold(key, s3MetadataSHA256) {
			sha := stringsTrim(value)
			if sha == "" {
				return "", nil
			}
			if !isSHA256Hex(sha) {
				return "", errors.Wrap(ErrIntegrity, "s3 object sha256 metadata is invalid")
			}
			return sha, nil
		}
	}
	return "", nil
}

func s3ChecksumSHA(checksum *string, checksumType types.ChecksumType) (string, error) {
	raw := stringsTrim(aws.ToString(checksum))
	if raw == "" {
		return "", nil
	}
	if checksumType == types.ChecksumTypeComposite {
		return "", nil
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", errors.Wrap(ErrIntegrity, "s3 object sha256 checksum is invalid base64")
	}
	if len(decoded) != sha256.Size {
		return "", errors.Wrapf(ErrIntegrity, "s3 object sha256 checksum has %d bytes", len(decoded))
	}
	return hex.EncodeToString(decoded), nil
}

func sha256HexToBase64(sha string) (string, error) {
	decoded, err := hex.DecodeString(sha)
	if err != nil || len(decoded) != sha256.Size {
		return "", errors.Wrap(ErrInvalidOptions, "object sha256 must be 64 lowercase hex characters")
	}
	return base64.StdEncoding.EncodeToString(decoded), nil
}

func ValidateS3StoreEncryption(encryption, kmsKeyID string) error {
	return validateS3EncryptionConfig(encryption, kmsKeyID)
}

func validateS3EncryptionConfig(encryption, kmsKeyID string) error {
	encryption = stringsTrim(encryption)
	kmsKeyID = stringsTrim(kmsKeyID)
	switch {
	case encryption == "":
		return errors.Wrap(ErrInvalidOptions, "s3 server-side encryption is required")
	case encryption != string(types.ServerSideEncryptionAes256) && encryption != string(types.ServerSideEncryptionAwsKms):
		return errors.Wrap(ErrInvalidOptions, "s3 server-side encryption must be AES256 or aws:kms")
	case encryption == string(types.ServerSideEncryptionAwsKms) && kmsKeyID == "":
		return errors.Wrap(ErrInvalidOptions, "s3 KMS key id is required for aws:kms")
	case encryption != string(types.ServerSideEncryptionAwsKms) && kmsKeyID != "":
		return errors.Wrap(ErrInvalidOptions, "s3 KMS key id requires aws:kms encryption")
	}
	if encryption == string(types.ServerSideEncryptionAwsKms) {
		if err := ValidateKMSKeyID(kmsKeyID); err != nil {
			return errors.Wrap(ErrInvalidOptions, err.Error())
		}
	}
	return nil
}

// ValidateKMSKeyID accepts canonical key ARNs and bare key IDs. Aliases are
// rejected because S3 reports the resolved key identity, not the alias string.
func ValidateKMSKeyID(value string) error {
	_, err := parseKMSKeyIdentity(value)
	return err
}

type kmsKeyIdentity struct {
	arn   string
	keyID string
}

func parseKMSKeyIdentity(value string) (kmsKeyIdentity, error) {
	value = stringsTrim(value)
	switch {
	case value == "":
		return kmsKeyIdentity{}, errors.Wrap(ErrInvalidOptions, "KMS key id is required")
	case strings.HasPrefix(value, "alias/"):
		return kmsKeyIdentity{}, errors.Wrap(ErrInvalidOptions, "KMS aliases are not supported; use a key ARN or bare key ID")
	case arn.IsARN(value):
		return parseKMSKeyARN(value)
	case strings.ContainsAny(value, ":/"):
		return kmsKeyIdentity{}, errors.Wrap(ErrInvalidOptions, "KMS key id must be a key ARN or bare key ID")
	default:
		return kmsKeyIdentity{keyID: value}, nil
	}
}

func parseKMSKeyARN(value string) (kmsKeyIdentity, error) {
	parsed, err := arn.Parse(value)
	if err != nil || parsed.Service != "kms" || parsed.Region == "" || parsed.AccountID == "" ||
		!strings.HasPrefix(parsed.Resource, "key/") || len(parsed.Resource) == len("key/") {
		return kmsKeyIdentity{}, errors.Wrap(ErrInvalidOptions, "KMS key ARN is invalid or identifies an alias")
	}
	return kmsKeyIdentity{arn: value, keyID: strings.TrimPrefix(parsed.Resource, "key/")}, nil
}

func kmsKeyIDsMatch(actual, expected string) bool {
	actualIdentity, actualErr := parseKMSKeyIdentity(actual)
	expectedIdentity, expectedErr := parseKMSKeyIdentity(expected)
	if actualErr != nil || expectedErr != nil {
		return false
	}
	if actualIdentity.arn != "" && expectedIdentity.arn != "" {
		return actualIdentity.arn == expectedIdentity.arn
	}
	return actualIdentity.keyID == expectedIdentity.keyID
}

func isS3NotFound(err error) bool {
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return true
		}
	}
	return false
}

func isS3PreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "PreconditionFailed":
		return true
	default:
		return false
	}
}

func isS3ConditionalConflict(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "ConditionalRequestConflict"
}

var _ RetentionStore = (*S3Store)(nil)

// listObjectsPageLimit bounds a single ListObjectsV2 page. The AWS
// maximum is 1000; naming it keeps the mnd linter satisfied and the
// intent legible.
const listObjectsPageLimit int32 = 1000

// ListObjects pages through every object under prefix.
//
// Per the RetentionStore contract this is all-or-error: any page
// failure returns an error and no partial slice, because §5 makes
// "an incomplete scan performs no deletes" a safety property. A
// truncated listing would make a live payload look unreferenced and
// let phase 2 delete data a committed manifest still points at.
func (s *S3Store) ListObjects(ctx context.Context, prefix string) ([]ObjectRef, error) {
	if s == nil || s.client == nil {
		return nil, errors.Wrap(ErrInvalidOptions, "object store is required")
	}
	listPrefix := s3ListSubtreePrefix(prefix)

	var (
		refs       []ObjectRef
		token      *string
		seenTokens = make(map[string]struct{})
	)
	for {
		out, err := s.listObjectsPage(ctx, listPrefix, token)
		if err != nil {
			return nil, errors.Wrapf(err, "list objects under %q", prefix)
		}
		refs, err = appendListedObjects(refs, out, listPrefix)
		if err != nil {
			return nil, err
		}

		next, more, err := nextListPageToken(out, prefix)
		if err != nil {
			return nil, err
		}
		if !more {
			return refs, nil
		}
		if err := rememberListToken(seenTokens, prefix, next); err != nil {
			return nil, err
		}
		token = next
	}
}

func s3ListSubtreePrefix(prefix string) string {
	listPrefix := cleanObjectPrefix(prefix)
	if listPrefix == "." {
		return ""
	}
	return listPrefix + "/"
}

func (s *S3Store) listObjectsPage(
	ctx context.Context,
	prefix string,
	token *string,
) (*s3.ListObjectsV2Output, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(s.bucket),
		Prefix:            aws.String(prefix),
		ContinuationToken: token,
		MaxKeys:           aws.Int32(listObjectsPageLimit),
	})
	if err != nil {
		return nil, errors.Wrap(err, "list s3 objects page")
	}
	return out, nil
}

func rememberListToken(seen map[string]struct{}, prefix string, token *string) error {
	if _, ok := seen[*token]; ok {
		return errors.Wrapf(ErrIntegrity,
			"list objects under %q returned non-advancing continuation token %q", prefix, *token)
	}
	seen[*token] = struct{}{}
	return nil
}

// appendListedObjects converts one ListObjectsV2 page into ObjectRefs.
func appendListedObjects(refs []ObjectRef, out *s3.ListObjectsV2Output, listPrefix string) ([]ObjectRef, error) {
	for _, obj := range out.Contents {
		if obj.Key == nil {
			continue
		}
		key := *obj.Key
		if listPrefix != "" && !strings.HasPrefix(key, listPrefix) {
			return nil, errors.Wrapf(ErrIntegrity, "listed object %q outside prefix %q", key, listPrefix)
		}
		if normalizeObjectKey(key) != key {
			return nil, errors.Wrapf(ErrIntegrity, "listed object key %q is not canonical", key)
		}
		ref := ObjectRef{Key: key}
		if obj.Size != nil {
			ref.Size = *obj.Size
		}
		if obj.LastModified != nil {
			ref.UpdatedAt = *obj.LastModified
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// nextListPageToken reports whether another page follows and returns
// its continuation token.
//
// A truncated page with no continuation token cannot be continued and
// would otherwise loop forever re-reading page one. §5 requires
// pagination failure to fail closed, so it becomes an error rather
// than a silently short listing.
func nextListPageToken(out *s3.ListObjectsV2Output, prefix string) (*string, bool, error) {
	if out.IsTruncated == nil || !*out.IsTruncated {
		// A page that says "complete" while still handing back a
		// continuation token is self-contradictory, and believing the
		// flag discards every later page. For a GC live-set scan that is
		// not a cosmetic truncation: a manifest missed here leaves the
		// payload it references unprotected, and retention reclaims data
		// a restore still needs. Fail closed like every other pagination
		// failure in §5 rather than returning a short listing.
		if out.NextContinuationToken != nil && *out.NextContinuationToken != "" {
			return nil, false, errors.Wrapf(ErrIntegrity,
				"list objects under %q reported a complete page with a continuation token", prefix)
		}
		return nil, false, nil
	}
	if out.NextContinuationToken == nil || *out.NextContinuationToken == "" {
		return nil, false, errors.Wrapf(ErrIntegrity,
			"list objects under %q returned a truncated page with no continuation token", prefix)
	}
	return out.NextContinuationToken, true, nil
}

// DeleteObject removes one object. S3 delete is idempotent, so an
// already-absent key is not an error.
//
// Versioned buckets: a delete without a VersionId only writes a delete
// marker, so the bytes survive as a noncurrent version that later
// ListObjectsV2 scans cannot see. Retention would then report
// successful reclamation while storage kept growing. Reclaiming those
// versions requires enumerating them (ListObjectVersions) and deleting
// each VersionId, which this store deliberately does not do — whether
// to enumerate versions, refuse versioned buckets outright, or require
// a noncurrent-version lifecycle policy is a deployment decision. Until
// that is settled, a versioned backup bucket MUST carry a
// noncurrent-version expiration lifecycle rule.
func (s *S3Store) DeleteObject(ctx context.Context, key string) error {
	if s == nil || s.client == nil {
		return errors.Wrap(ErrInvalidOptions, "object store is required")
	}
	normalized, err := validateStoreObjectKey(key)
	if err != nil {
		return err
	}
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(normalized),
	}); err != nil {
		return errors.Wrapf(err, "delete object %s", key)
	}
	return nil
}

// DeleteObjectIfUnmodified deletes key only when it still matches
// cond, mapping S3's 412 precondition failure to ErrObjectModified.
//
// When cond carries an ETag this is an exact compare-and-delete via
// If-Match. Without one it falls back to If-Match-Last-Modified-Time
// plus If-Match-Size, which is the same contract at coarser
// resolution. A store that honours neither would silently degrade to
// an unconditional delete, so an empty condition is refused instead.
func (s *S3Store) DeleteObjectIfUnmodified(ctx context.Context, key string, cond DeletePrecondition) error {
	if s == nil || s.client == nil {
		return errors.Wrap(ErrInvalidOptions, "object store is required")
	}
	normalized, err := validateStoreObjectKey(key)
	if err != nil {
		return err
	}
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(normalized),
	}
	switch {
	case strings.TrimSpace(cond.ETag) != "":
		input.IfMatch = aws.String(cond.ETag)
	case !cond.UpdatedAt.IsZero():
		input.IfMatchLastModifiedTime = aws.Time(cond.UpdatedAt)
		input.IfMatchSize = aws.Int64(cond.Size)
	default:
		return errors.Wrapf(ErrInvalidOptions,
			"conditional delete of %s requires an etag or a last-modified time", key)
	}
	if _, err := s.client.DeleteObject(ctx, input); err != nil {
		if isPreconditionFailed(err) {
			return errors.Wrapf(ErrObjectModified,
				"object %s changed since it was validated for deletion", key)
		}
		return errors.Wrapf(err, "conditional delete object %s", key)
	}
	return nil
}

// isPreconditionFailed reports whether err is S3's 412 response to a
// failed If-Match on delete.
func isPreconditionFailed(err error) bool {
	var responseErr interface{ HTTPStatusCode() int }
	if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusPreconditionFailed {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "PreconditionFailed"
	}
	return false
}

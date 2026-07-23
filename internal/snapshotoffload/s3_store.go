package snapshotoffload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
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
	if err := s.putObjectIfAbsent(ctx, normalized, body, opts); err != nil {
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

func (s *S3Store) putObjectIfAbsent(ctx context.Context, key string, body io.Reader, opts PutOptions) error {
	return s.putObjectWithRetry(key, body, func() error {
		if opts.Size > s.multipartThreshold {
			return s.putMultipartIfAbsent(ctx, key, body, opts)
		}
		input, err := s.putObjectInput(key, body, opts)
		if err != nil {
			return err
		}
		if _, err = s.client.PutObject(ctx, input); err != nil {
			return errors.Wrap(err, "put s3 object")
		}
		return nil
	})
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

func (s *S3Store) putMultipartIfAbsent(
	ctx context.Context,
	key string,
	body io.Reader,
	opts PutOptions,
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
	parts, err := s.uploadParts(ctx, key, uploadID, body, opts.Size, partSize)
	if err != nil {
		return err
	}
	if err := s.completeMultipartUpload(ctx, key, uploadID, opts.Size, parts); err != nil {
		return err
	}
	completed = true
	return nil
}

func (s *S3Store) createMultipartUpload(ctx context.Context, key string, opts PutOptions) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		Metadata: map[string]string{
			s3MetadataSHA256: opts.SHA256,
		},
		ServerSideEncryption: types.ServerSideEncryption(s.serverSideEncryption),
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
	totalBytes int64,
	partSize int64,
) ([]types.CompletedPart, error) {
	parts := make([]types.CompletedPart, 0, (totalBytes+partSize-1)/partSize)
	remaining := totalBytes
	for partNumber := int32(1); remaining > 0; partNumber++ {
		partBytes := min(remaining, partSize)
		part := make([]byte, int(partBytes))
		if _, err := io.ReadFull(body, part); err != nil {
			return nil, errors.Wrap(err, "read s3 multipart source")
		}
		sum := sha256.Sum256(part)
		checksum := base64.StdEncoding.EncodeToString(sum[:])
		out, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:            aws.String(s.bucket),
			Key:               aws.String(key),
			UploadId:          aws.String(uploadID),
			PartNumber:        aws.Int32(partNumber),
			Body:              bytes.NewReader(part),
			ContentLength:     aws.Int64(partBytes),
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			ChecksumSHA256:    aws.String(checksum),
		})
		if err != nil {
			return nil, errors.Wrap(err, "upload s3 multipart part")
		}
		if stringsTrim(aws.ToString(out.ETag)) == "" {
			return nil, errors.Wrapf(ErrIntegrity, "s3 multipart part %d returned no etag", partNumber)
		}
		parts = append(parts, types.CompletedPart{
			ETag:           out.ETag,
			PartNumber:     aws.Int32(partNumber),
			ChecksumSHA256: aws.String(checksum),
		})
		remaining -= partBytes
	}
	if err := requireNoTrailingBytes(body); err != nil {
		return nil, errors.Wrap(err, "s3 multipart source length differs from declared length")
	}
	return parts, nil
}

func (s *S3Store) completeMultipartUpload(
	ctx context.Context,
	key string,
	uploadID string,
	size int64,
	parts []types.CompletedPart,
) error {
	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		IfNoneMatch:   aws.String("*"),
		MpuObjectSize: aws.Int64(size),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
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
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(normalized),
	})
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
	return out.Body, info, nil
}

func (s *S3Store) HeadObject(ctx context.Context, key string) (ObjectInfo, bool, error) {
	normalized, err := validateStoreObjectKey(key)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(normalized),
	})
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
		out.ServerSideEncryption,
		out.SSEKMSKeyId,
	)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	if err := s.validateS3ObjectEncryption(normalized, info); err != nil {
		return ObjectInfo{}, false, err
	}
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
		return 0, errors.Wrapf(ErrInvalidOptions, "s3 object exceeds 5 TiB: bytes=%d", totalBytes)
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
	encryption types.ServerSideEncryption,
	kmsKeyID *string,
) (ObjectInfo, error) {
	if contentLength == nil || *contentLength < 0 {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s missing content length", key)
	}
	sha, err := s3ObjectSHA256(key, metadata, checksumSHA256)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Key:                  key,
		Size:                 *contentLength,
		SHA256:               sha,
		ServerSideEncryption: string(encryption),
		SSEKMSKeyID:          aws.ToString(kmsKeyID),
	}, nil
}

func s3ObjectSHA256(key string, metadata map[string]string, checksumSHA256 *string) (string, error) {
	metadataSHA, err := s3MetadataSHA(metadata)
	if err != nil {
		return "", err
	}
	checksumSHA, err := s3ChecksumSHA(checksumSHA256)
	if err != nil {
		return "", err
	}
	switch {
	case metadataSHA != "" && checksumSHA != "" && metadataSHA != checksumSHA:
		return "", errors.Wrapf(ErrIntegrity, "s3 object %s sha256 metadata/checksum mismatch", key)
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

func s3ChecksumSHA(checksum *string) (string, error) {
	raw := stringsTrim(aws.ToString(checksum))
	if raw == "" {
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

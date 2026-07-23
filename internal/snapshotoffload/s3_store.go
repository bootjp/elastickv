package snapshotoffload

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
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
)

type S3ObjectClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
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
}

func NewS3Store(ctx context.Context, cfg S3StoreConfig) (*S3Store, error) {
	if stringsTrim(cfg.Bucket) == "" {
		return nil, errors.Wrap(ErrInvalidOptions, "s3 bucket is required")
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
	input, err := s.putObjectInput(normalized, body, opts)
	if err != nil {
		return ObjectInfo{}, err
	}
	if _, err := s.client.PutObject(ctx, input); err != nil {
		if !isS3PreconditionFailed(err) {
			return ObjectInfo{}, errors.Wrap(err, "put s3 object")
		}
		return s.verifyS3ExistingObject(ctx, normalized, opts)
	}
	return s.verifyS3PutObject(ctx, normalized, opts)
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
	info, err := s3ObjectInfo(normalized, out.ContentLength, out.Metadata, out.ChecksumSHA256)
	if err != nil {
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
	info, err := s3ObjectInfo(normalized, out.ContentLength, out.Metadata, out.ChecksumSHA256)
	if err != nil {
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
	return info, nil
}

func validateStoreObjectKey(key string) (string, error) {
	normalized := normalizeObjectKey(key)
	if normalized == "" || normalized == "." || normalized == ".." || strings.HasPrefix(normalized, "../") {
		return "", errors.Wrapf(ErrInvalidOptions, "invalid object key %q", key)
	}
	return normalized, nil
}

func s3ObjectInfo(key string, contentLength *int64, metadata map[string]string, checksumSHA256 *string) (ObjectInfo, error) {
	if contentLength == nil || *contentLength < 0 {
		return ObjectInfo{}, errors.Wrapf(ErrIntegrity, "s3 object %s missing content length", key)
	}
	sha, err := s3ObjectSHA256(key, metadata, checksumSHA256)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{Key: key, Size: *contentLength, SHA256: sha}, nil
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
	case "PreconditionFailed", "ConditionalRequestConflict":
		return true
	default:
		return false
	}
}

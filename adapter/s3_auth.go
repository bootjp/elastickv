package adapter

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const (
	s3SigV4Algorithm   = "AWS4-HMAC-SHA256"
	s3UnsignedPayload  = "UNSIGNED-PAYLOAD"
	s3EmptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	s3DateHeaderFormat = "20060102T150405Z"
)

type S3ServerOption func(*S3Server)

type s3AuthError struct {
	Status  int
	Code    string
	Message string
}

type s3AuthorizationHeader struct {
	Algorithm   string
	AccessKeyID string
	Date        string
	Region      string
	Service     string
}

func WithS3Region(region string) S3ServerOption {
	return func(server *S3Server) {
		if server == nil || strings.TrimSpace(region) == "" {
			return
		}
		server.region = strings.TrimSpace(region)
	}
}

func WithS3StaticCredentials(creds map[string]string) S3ServerOption {
	return func(server *S3Server) {
		if server == nil || len(creds) == 0 {
			return
		}
		server.staticCreds = make(map[string]string, len(creds))
		for accessKeyID, secretAccessKey := range creds {
			accessKeyID = strings.TrimSpace(accessKeyID)
			secretAccessKey = strings.TrimSpace(secretAccessKey)
			if accessKeyID == "" || secretAccessKey == "" {
				continue
			}
			server.staticCreds[accessKeyID] = secretAccessKey
		}
		if len(server.staticCreds) == 0 {
			server.staticCreds = nil
		}
	}
}

func WithS3ActiveTimestampTracker(tracker *kv.ActiveTimestampTracker) S3ServerOption {
	return func(server *S3Server) {
		if server == nil {
			return
		}
		server.readTracker = tracker
	}
}

//nolint:cyclop // SigV4 validation must preserve AWS-compatible error ordering.
func (s *S3Server) authorizeRequest(r *http.Request) *s3AuthError {
	if s == nil || r == nil || len(s.staticCreds) == 0 {
		return nil
	}
	if strings.TrimSpace(r.URL.Query().Get("X-Amz-Algorithm")) != "" {
		return &s3AuthError{
			Status:  http.StatusNotImplemented,
			Code:    "NotImplemented",
			Message: "presigned URLs are not supported yet",
		}
	}

	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if authHeader == "" {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AccessDenied",
			Message: "missing Authorization header",
		}
	}
	payloadHash := normalizeS3PayloadHash(r.Header.Get("X-Amz-Content-Sha256"))
	if payloadHash == "" {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AccessDenied",
			Message: "missing x-amz-content-sha256 header",
		}
	}

	parsed, err := parseS3AuthorizationHeader(authHeader)
	if err != nil {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AccessDenied",
			Message: "invalid Authorization header",
		}
	}
	if parsed.Algorithm != s3SigV4Algorithm {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AccessDenied",
			Message: "unsupported signature algorithm",
		}
	}
	secretAccessKey, ok := s.staticCreds[parsed.AccessKeyID]
	if !ok {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "InvalidAccessKeyId",
			Message: "unknown access key",
		}
	}
	if parsed.Service != "s3" {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationHeaderMalformed",
			Message: "credential scope service must be s3",
		}
	}
	if parsed.Region != s.effectiveRegion() {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationHeaderMalformed",
			Message: "credential scope region does not match server region",
		}
	}

	amzDate := strings.TrimSpace(r.Header.Get("X-Amz-Date"))
	if amzDate == "" {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AccessDenied",
			Message: "missing x-amz-date header",
		}
	}
	signingTime, err := time.Parse(s3DateHeaderFormat, amzDate)
	if err != nil {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AccessDenied",
			Message: "invalid x-amz-date header",
		}
	}
	if parsed.Date != signingTime.UTC().Format("20060102") {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationHeaderMalformed",
			Message: "credential scope date does not match x-amz-date",
		}
	}

	expectedAuth, err := buildS3AuthorizationHeader(r, parsed.AccessKeyID, secretAccessKey, s.effectiveRegion(), signingTime, payloadHash)
	if err != nil {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AccessDenied",
			Message: "failed to verify request signature",
		}
	}
	if subtle.ConstantTimeCompare([]byte(authHeader), []byte(expectedAuth)) != 1 {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "SignatureDoesNotMatch",
			Message: "request signature does not match",
		}
	}
	return nil
}

func buildS3AuthorizationHeader(r *http.Request, accessKeyID string, secretAccessKey string, region string, signingTime time.Time, payloadHash string) (string, error) {
	if r == nil {
		return "", errors.New("request is required")
	}
	clone := r.Clone(context.Background())
	clone.Host = r.Host
	clone.Header = clone.Header.Clone()
	clone.Header.Del("Authorization")

	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Source:          "elastickv-s3",
	}
	if err := signer.SignHTTP(context.Background(), creds, clone, payloadHash, "s3", region, signingTime.UTC()); err != nil {
		return "", errors.WithStack(err)
	}
	return strings.TrimSpace(clone.Header.Get("Authorization")), nil
}

//nolint:cyclop // AWS authorization parsing is branchy because malformed scopes must be rejected precisely.
func parseS3AuthorizationHeader(raw string) (s3AuthorizationHeader, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return s3AuthorizationHeader{}, errors.New("authorization header is required")
	}
	algorithm, rest, ok := strings.Cut(raw, " ")
	if !ok {
		return s3AuthorizationHeader{}, errors.New("authorization header is malformed")
	}
	out := s3AuthorizationHeader{Algorithm: strings.TrimSpace(algorithm)}
	params := strings.Split(rest, ",")
	for _, param := range params {
		key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
		if !ok {
			continue
		}
		if key != "Credential" {
			continue
		}
		scope := strings.Split(value, "/")
		if len(scope) != 5 || scope[4] != "aws4_request" {
			return s3AuthorizationHeader{}, errors.New("credential scope is malformed")
		}
		out.AccessKeyID = scope[0]
		out.Date = scope[1]
		out.Region = scope[2]
		out.Service = scope[3]
		break
	}
	if out.AccessKeyID == "" || out.Date == "" || out.Region == "" || out.Service == "" {
		return s3AuthorizationHeader{}, errors.New("credential scope is required")
	}
	return out, nil
}

func normalizeS3PayloadHash(raw string) string {
	return strings.TrimSpace(raw)
}

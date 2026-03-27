package adapter

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const (
	s3SigV4Algorithm     = "AWS4-HMAC-SHA256"
	s3UnsignedPayload    = "UNSIGNED-PAYLOAD"
	s3EmptyPayloadHash   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	s3DateHeaderFormat   = "20060102T150405Z"
	s3RequestTimeMaxSkew = 15 * time.Minute
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
		return s.authorizePresignedRequest(r)
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
	skew := time.Now().UTC().Sub(signingTime.UTC())
	if skew < 0 {
		skew = -skew
	}
	if skew > s3RequestTimeMaxSkew {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "RequestTimeTooSkewed",
			Message: "The difference between the request time and the server's time is too large",
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
	// Compare only the Signature component to avoid false rejections caused by
	// equivalent Authorization headers that differ in whitespace or parameter
	// ordering (but carry the same cryptographic signature).
	gotSig := extractS3Signature(authHeader)
	expectedSig := extractS3Signature(expectedAuth)
	if gotSig == "" || subtle.ConstantTimeCompare([]byte(gotSig), []byte(expectedSig)) != 1 {
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

const s3PresignMaxExpiry = 7 * 24 * 60 * 60 // 604800 seconds (7 days)

// checkPresignExpiry validates the expiry of a presigned request.
// It returns an *s3AuthError if X-Amz-Expires is missing, invalid, or the URL has expired.
func checkPresignExpiry(expiresStr string, signingTime time.Time) *s3AuthError {
	if expiresStr == "" {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationQueryParametersError",
			Message: "X-Amz-Expires is required for presigned URLs",
		}
	}
	expires, err := strconv.Atoi(expiresStr)
	if err != nil || expires <= 0 || expires > s3PresignMaxExpiry {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationQueryParametersError",
			Message: "X-Amz-Expires must be between 1 and 604800",
		}
	}
	if time.Now().UTC().After(signingTime.UTC().Add(time.Duration(expires) * time.Second)) {
		return &s3AuthError{Status: http.StatusForbidden, Code: "AccessDenied", Message: "presigned URL has expired"}
	}
	return nil
}

//nolint:cyclop // Presigned URL validation must check multiple parameters precisely.
func (s *S3Server) authorizePresignedRequest(r *http.Request) *s3AuthError {
	query := r.URL.Query()
	algorithm := strings.TrimSpace(query.Get("X-Amz-Algorithm"))
	if algorithm != s3SigV4Algorithm {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationQueryParametersError",
			Message: "unsupported signature algorithm",
		}
	}
	credential := strings.TrimSpace(query.Get("X-Amz-Credential"))
	amzDate := strings.TrimSpace(query.Get("X-Amz-Date"))
	expiresStr := strings.TrimSpace(query.Get("X-Amz-Expires"))
	signedHeaders := strings.TrimSpace(query.Get("X-Amz-SignedHeaders"))
	signature := strings.TrimSpace(query.Get("X-Amz-Signature"))

	if credential == "" || amzDate == "" || signedHeaders == "" || signature == "" {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationQueryParametersError",
			Message: "missing required presigned URL parameters",
		}
	}

	// Parse credential scope: AKID/date/region/s3/aws4_request
	scope := strings.Split(credential, "/")
	if len(scope) != 5 || scope[4] != "aws4_request" || scope[3] != "s3" {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationQueryParametersError",
			Message: "credential scope is malformed",
		}
	}
	accessKeyID := scope[0]
	scopeRegion := scope[2]

	secretAccessKey, ok := s.staticCreds[accessKeyID]
	if !ok {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "InvalidAccessKeyId",
			Message: "unknown access key",
		}
	}
	if scopeRegion != s.effectiveRegion() {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationQueryParametersError",
			Message: "credential scope region does not match server region",
		}
	}

	signingTime, err := time.Parse(s3DateHeaderFormat, amzDate)
	if err != nil {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationQueryParametersError",
			Message: "invalid X-Amz-Date",
		}
	}
	if scope[1] != signingTime.UTC().Format("20060102") {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "AuthorizationQueryParametersError",
			Message: "credential scope date does not match X-Amz-Date",
		}
	}

	// Reject if signing time is too far in the future (matches AWS behavior).
	// For presigned URLs, past signing times are expected and validated via expiry.
	if signingTime.UTC().After(time.Now().UTC().Add(s3RequestTimeMaxSkew)) {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "RequestTimeTooSkewed",
			Message: "The difference between the request time and the server's time is too large",
		}
	}

	if authErr := checkPresignExpiry(expiresStr, signingTime); authErr != nil {
		return authErr
	}

	// Use the SDK's PresignHTTP to rebuild the expected presigned URL.
	// Strip all presign query params to get a clean base request,
	// then presign it with the same parameters.
	verifyURL := *r.URL
	q := verifyURL.Query()
	for _, param := range []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-SignedHeaders", "X-Amz-Signature", "X-Amz-Security-Token"} {
		q.Del(param)
	}
	verifyURL.RawQuery = q.Encode()

	verifyReq, err := http.NewRequestWithContext(context.Background(), r.Method, verifyURL.String(), nil) //nolint:gosec // G704: URL is derived from the incoming request's own URL to rebuild and verify its signature; no outbound network request is made.
	if err != nil {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "SignatureDoesNotMatch",
			Message: "failed to verify presigned request signature",
		}
	}
	verifyReq.Host = r.Host
	// Copy only the signed headers from the original request.
	for _, h := range strings.Split(signedHeaders, ";") {
		h = strings.TrimSpace(h)
		if h == "" || strings.EqualFold(h, "host") {
			continue
		}
		verifyReq.Header.Set(h, r.Header.Get(h))
	}

	// Re-add X-Amz-Expires so PresignHTTP includes it in the canonical request,
	// matching the client's original signature computation.
	if expiresStr != "" {
		vq := verifyReq.URL.Query()
		vq.Set("X-Amz-Expires", expiresStr)
		verifyReq.URL.RawQuery = vq.Encode()
	}

	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Source:          "elastickv-s3-presign",
	}
	presignedURL, _, err := signer.PresignHTTP(context.Background(), creds, verifyReq, s3UnsignedPayload, "s3", s.effectiveRegion(), signingTime)
	if err != nil {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "SignatureDoesNotMatch",
			Message: "failed to verify presigned request signature",
		}
	}
	expectedSig := extractPresignedSignature(presignedURL)
	if expectedSig == "" || subtle.ConstantTimeCompare([]byte(signature), []byte(expectedSig)) != 1 {
		return &s3AuthError{
			Status:  http.StatusForbidden,
			Code:    "SignatureDoesNotMatch",
			Message: "presigned URL signature does not match",
		}
	}
	return nil
}

func extractPresignedSignature(presignedURL string) string {
	idx := strings.Index(presignedURL, "X-Amz-Signature=")
	if idx < 0 {
		return ""
	}
	sig := presignedURL[idx+len("X-Amz-Signature="):]
	if ampIdx := strings.IndexByte(sig, '&'); ampIdx >= 0 {
		sig = sig[:ampIdx]
	}
	return sig
}

// extractS3Signature returns the hex signature value from a SigV4
// Authorization header (the "Signature=<hex>" component).
func extractS3Signature(auth string) string {
	_, params, ok := strings.Cut(auth, " ")
	if !ok {
		return ""
	}
	for _, param := range strings.Split(params, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
		if ok && key == "Signature" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

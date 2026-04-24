package adapter

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"time"
)

// Service name used in the SigV4 credential scope.
const sqsSigV4Service = "sqs"

// sqsAuthError is the SQS-flavored counterpart to s3AuthError. The outer
// handler turns these into the AWS JSON-1.0 error envelope via
// writeSQSError.
type sqsAuthError struct {
	Status  int
	Code    string
	Message string
}

// WithSQSRegion configures the signing region the adapter expects inside
// the Credential scope. Empty values retain the previous setting.
func WithSQSRegion(region string) SQSServerOption {
	return func(s *SQSServer) {
		if s == nil || strings.TrimSpace(region) == "" {
			return
		}
		s.region = strings.TrimSpace(region)
	}
}

// WithSQSStaticCredentials supplies the access-key → secret map the
// adapter will accept. Passing an empty map disables authorization
// entirely (open endpoint), matching the S3 adapter's behavior for
// unit-test friendliness.
func WithSQSStaticCredentials(creds map[string]string) SQSServerOption {
	return func(s *SQSServer) {
		if s == nil || len(creds) == 0 {
			return
		}
		s.staticCreds = make(map[string]string, len(creds))
		for accessKeyID, secretAccessKey := range creds {
			accessKeyID = strings.TrimSpace(accessKeyID)
			secretAccessKey = strings.TrimSpace(secretAccessKey)
			if accessKeyID == "" || secretAccessKey == "" {
				continue
			}
			s.staticCreds[accessKeyID] = secretAccessKey
		}
		if len(s.staticCreds) == 0 {
			s.staticCreds = nil
		}
	}
}

func (s *SQSServer) effectiveRegion() string {
	if s == nil || strings.TrimSpace(s.region) == "" {
		return "us-east-1"
	}
	return s.region
}

// authorizeSQSRequest verifies SigV4 on an SQS JSON-protocol request. The
// flow mirrors S3's header-based path (no presigned URLs or streaming
// payloads) but restricts the credential scope service to "sqs".
//
// Returns nil when authorization is either unconfigured (open endpoint) or
// the signature matches. Returns *sqsAuthError otherwise, suitable for
// writeSQSError.
//
// The function must consume and replace r.Body so that downstream handlers
// can still parse the JSON payload.
func (s *SQSServer) authorizeSQSRequest(r *http.Request) *sqsAuthError {
	if s == nil || r == nil || len(s.staticCreds) == 0 {
		return nil
	}
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if authHeader == "" {
		return &sqsAuthError{Status: http.StatusForbidden, Code: "MissingAuthenticationToken", Message: "missing Authorization header"}
	}
	parsed, authErr := s.validateSQSAuthScope(authHeader)
	if authErr != nil {
		return authErr
	}
	secretAccessKey, ok := s.staticCreds[parsed.AccessKeyID]
	if !ok {
		return &sqsAuthError{Status: http.StatusForbidden, Code: "InvalidClientTokenId", Message: "unknown access key"}
	}
	signingTime, authErr := validateSQSAuthDate(r, parsed)
	if authErr != nil {
		return authErr
	}
	payloadHash, authErr := drainAndHashSQSBody(r)
	if authErr != nil {
		return authErr
	}
	return verifySQSSignatureMatches(r, authHeader, parsed.AccessKeyID, secretAccessKey, s.effectiveRegion(), signingTime, payloadHash)
}

// validateSQSAuthScope parses the Credential scope and rejects scopes
// whose algorithm/service/region do not match this adapter.
func (s *SQSServer) validateSQSAuthScope(authHeader string) (sigv4AuthorizationHeader, *sqsAuthError) {
	parsed, err := parseSigV4AuthorizationHeader(authHeader)
	if err != nil {
		return sigv4AuthorizationHeader{}, &sqsAuthError{Status: http.StatusForbidden, Code: "IncompleteSignature", Message: "invalid Authorization header"}
	}
	if parsed.Algorithm != sigv4Algorithm {
		return sigv4AuthorizationHeader{}, &sqsAuthError{Status: http.StatusForbidden, Code: "IncompleteSignature", Message: "unsupported signature algorithm"}
	}
	if parsed.Service != sqsSigV4Service {
		return sigv4AuthorizationHeader{}, &sqsAuthError{Status: http.StatusForbidden, Code: "SignatureDoesNotMatch", Message: "credential scope service must be sqs"}
	}
	if parsed.Region != s.effectiveRegion() {
		return sigv4AuthorizationHeader{}, &sqsAuthError{Status: http.StatusForbidden, Code: "SignatureDoesNotMatch", Message: "credential scope region does not match server region"}
	}
	return parsed, nil
}

// validateSQSAuthDate parses X-Amz-Date and verifies it matches the
// Credential scope date and is within the allowed clock-skew window.
func validateSQSAuthDate(r *http.Request, parsed sigv4AuthorizationHeader) (time.Time, *sqsAuthError) {
	amzDate := strings.TrimSpace(r.Header.Get("X-Amz-Date"))
	if amzDate == "" {
		return time.Time{}, &sqsAuthError{Status: http.StatusForbidden, Code: "MissingAuthenticationToken", Message: "missing x-amz-date header"}
	}
	signingTime, err := time.Parse(sigv4DateHeaderFormat, amzDate)
	if err != nil {
		return time.Time{}, &sqsAuthError{Status: http.StatusForbidden, Code: "IncompleteSignature", Message: "invalid x-amz-date header"}
	}
	if parsed.Date != signingTime.UTC().Format(sigv4ScopeDateFormat) {
		return time.Time{}, &sqsAuthError{Status: http.StatusForbidden, Code: "SignatureDoesNotMatch", Message: "credential scope date does not match x-amz-date"}
	}
	skew := time.Now().UTC().Sub(signingTime.UTC())
	if skew < 0 {
		skew = -skew
	}
	if skew > sigv4RequestTimeMaxSkew {
		return time.Time{}, &sqsAuthError{Status: http.StatusForbidden, Code: "RequestTimeTooSkewed", Message: "The difference between the request time and the server's time is too large"}
	}
	return signingTime, nil
}

// drainAndHashSQSBody reads the request body so the signer reproduces the
// client's payload hash, then replaces r.Body so handler code can re-read
// it afterwards.
func drainAndHashSQSBody(r *http.Request) (string, *sqsAuthError) {
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, sqsMaxRequestBodyBytes))
	if err != nil {
		return "", &sqsAuthError{Status: http.StatusForbidden, Code: "IncompleteSignature", Message: "failed to read request body for signature verification"}
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// verifySQSSignatureMatches rebuilds the expected Authorization header and
// compares its hex signature to the one the client sent.
//
// The restricted builder is used so Go's transport-added headers
// (Accept-Encoding etc.) do not leak into the canonical request.
func verifySQSSignatureMatches(r *http.Request, authHeader, accessKeyID, secretAccessKey, region string, signingTime time.Time, payloadHash string) *sqsAuthError {
	signedHeaders := extractSigV4SignedHeaders(authHeader)
	expectedAuth, err := buildSigV4AuthorizationHeaderRestricted(r, accessKeyID, secretAccessKey, sqsSigV4Service, region, signingTime, payloadHash, signedHeaders)
	if err != nil {
		return &sqsAuthError{Status: http.StatusForbidden, Code: "SignatureDoesNotMatch", Message: "failed to verify request signature"}
	}
	gotSig := extractSigV4Signature(authHeader)
	expectedSig := extractSigV4Signature(expectedAuth)
	if gotSig == "" || subtle.ConstantTimeCompare([]byte(gotSig), []byte(expectedSig)) != 1 {
		return &sqsAuthError{Status: http.StatusForbidden, Code: "SignatureDoesNotMatch", Message: "request signature does not match"}
	}
	return nil
}

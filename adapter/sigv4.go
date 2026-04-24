package adapter

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/cockroachdb/errors"
)

// Service-agnostic SigV4 primitives shared by the S3 and SQS adapters.
// The per-adapter files wrap these with their own error ordering and
// service-specific rules (payload hashing for S3, JSON body hashing for
// SQS, etc.).

const (
	sigv4Algorithm          = "AWS4-HMAC-SHA256"
	sigv4DateHeaderFormat   = "20060102T150405Z"
	sigv4ScopeDateFormat    = "20060102"
	sigv4RequestTimeMaxSkew = 15 * time.Minute
	sigv4ScopeTerminator    = "aws4_request"
	sigv4ScopeParts         = 5
)

// sigv4AuthorizationHeader captures the Credential scope fields extracted
// from a SigV4 Authorization header ("AWS4-HMAC-SHA256 Credential=<akid>/
// <date>/<region>/<service>/aws4_request, SignedHeaders=..., Signature=...").
type sigv4AuthorizationHeader struct {
	Algorithm   string
	AccessKeyID string
	Date        string
	Region      string
	Service     string
}

// parseSigV4AuthorizationHeader decodes the Credential scope from an
// Authorization header. Other fields (SignedHeaders, Signature) are not
// parsed here because the adapter-level verifier rebuilds and compares the
// full signature through the AWS SDK v4 signer.
func parseSigV4AuthorizationHeader(raw string) (sigv4AuthorizationHeader, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return sigv4AuthorizationHeader{}, errors.New("authorization header is required")
	}
	algorithm, rest, ok := strings.Cut(raw, " ")
	if !ok {
		return sigv4AuthorizationHeader{}, errors.New("authorization header is malformed")
	}
	credentialValue, ok := findSigV4Param(rest, "Credential")
	if !ok {
		return sigv4AuthorizationHeader{}, errors.New("credential scope is required")
	}
	out, err := parseSigV4CredentialScope(credentialValue)
	if err != nil {
		return sigv4AuthorizationHeader{}, err
	}
	out.Algorithm = strings.TrimSpace(algorithm)
	return out, nil
}

// findSigV4Param returns the value of the named parameter from the
// "k=v, k=v, ..." tail of an Authorization header.
func findSigV4Param(params, name string) (string, bool) {
	for _, param := range strings.Split(params, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
		if ok && key == name {
			return strings.TrimSpace(value), true
		}
	}
	return "", false
}

// parseSigV4CredentialScope validates an
// <akid>/<date>/<region>/<service>/aws4_request scope string.
func parseSigV4CredentialScope(value string) (sigv4AuthorizationHeader, error) {
	scope := strings.Split(value, "/")
	if len(scope) != sigv4ScopeParts || scope[4] != sigv4ScopeTerminator {
		return sigv4AuthorizationHeader{}, errors.New("credential scope is malformed")
	}
	out := sigv4AuthorizationHeader{
		AccessKeyID: scope[0],
		Date:        scope[1],
		Region:      scope[2],
		Service:     scope[3],
	}
	if out.AccessKeyID == "" || out.Date == "" || out.Region == "" || out.Service == "" {
		return sigv4AuthorizationHeader{}, errors.New("credential scope is required")
	}
	return out, nil
}

// buildSigV4AuthorizationHeader re-signs r with the given credentials and
// returns the Authorization header the server expects. Used by adapter
// verifiers to compare against the client-supplied Authorization.
func buildSigV4AuthorizationHeader(
	r *http.Request,
	accessKeyID, secretAccessKey, service, region string,
	signingTime time.Time,
	payloadHash string,
) (string, error) {
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
		Source:          "elastickv-" + service,
	}
	if err := signer.SignHTTP(context.Background(), creds, clone, payloadHash, service, region, signingTime.UTC()); err != nil {
		return "", errors.WithStack(err)
	}
	return strings.TrimSpace(clone.Header.Get("Authorization")), nil
}

// extractSigV4Signature returns the hex Signature= value from an
// Authorization header, or "" if missing.
func extractSigV4Signature(auth string) string {
	return extractSigV4AuthField(auth, "Signature")
}

// extractSigV4SignedHeaders returns the semicolon-separated SignedHeaders
// list as a []string, or nil if missing. Header names are lowercased.
func extractSigV4SignedHeaders(auth string) []string {
	raw := extractSigV4AuthField(auth, "SignedHeaders")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, strings.ToLower(p))
	}
	return out
}

func extractSigV4AuthField(auth, field string) string {
	_, params, ok := strings.Cut(auth, " ")
	if !ok {
		return ""
	}
	v, _ := findSigV4Param(params, field)
	return v
}

// buildSigV4AuthorizationHeaderRestricted is a variant of
// buildSigV4AuthorizationHeader that strips any header not listed in
// signedHeaders before re-signing, so the server's re-computation matches
// the client's canonical request even when Go's transport added headers
// (Accept-Encoding, etc.) after the original signature.
func buildSigV4AuthorizationHeaderRestricted(
	r *http.Request,
	accessKeyID, secretAccessKey, service, region string,
	signingTime time.Time,
	payloadHash string,
	signedHeaders []string,
) (string, error) {
	if r == nil {
		return "", errors.New("request is required")
	}
	clone := r.Clone(context.Background())
	clone.Host = r.Host
	clone.Header = clone.Header.Clone()
	clone.Header.Del("Authorization")
	if len(signedHeaders) > 0 {
		allowed := make(map[string]struct{}, len(signedHeaders))
		for _, h := range signedHeaders {
			allowed[strings.ToLower(strings.TrimSpace(h))] = struct{}{}
		}
		for name := range clone.Header {
			if _, ok := allowed[strings.ToLower(name)]; !ok {
				clone.Header.Del(name)
			}
		}
	}

	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Source:          "elastickv-" + service,
	}
	if err := signer.SignHTTP(context.Background(), creds, clone, payloadHash, service, region, signingTime.UTC()); err != nil {
		return "", errors.WithStack(err)
	}
	return strings.TrimSpace(clone.Header.Get("Authorization")), nil
}

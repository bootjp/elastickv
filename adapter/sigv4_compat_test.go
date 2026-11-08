package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/require"
)

func TestSigV4VerificationRespectsClientSignedHeaders(t *testing.T) {
	t.Parallel()
	for _, service := range []string{"s3", "sqs"} {
		t.Run(service, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name         string
				signedLength bool
				extraHeaders bool
				tamperHeader bool
				tamperLength bool
				wantError    bool
			}{
				{name: "unsigned content length"},
				{name: "signed content length", signedLength: true},
				{name: "unsigned transport headers", extraHeaders: true},
				{name: "unsigned transport headers with signed length", signedLength: true, extraHeaders: true},
				{name: "tampered signed header", tamperHeader: true, wantError: true},
				{name: "tampered signed content length", signedLength: true, tamperLength: true, wantError: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					req := newSigV4CompatibilityRequest(t, service, tc.signedLength)
					if tc.extraHeaders {
						req.Header.Set("Amz-Sdk-Invocation-Id", "test-invocation")
						req.Header.Set("Amz-Sdk-Request", "attempt=1")
						req.Header.Set("X-Forwarded-For", "192.0.2.1")
						req.Header.Set("X-Forwarded-Host", req.Host)
						req.Header.Set("X-Forwarded-Proto", "https")
						req.Header.Set("Accept-Encoding", "gzip")
					}
					if tc.tamperHeader {
						req.Header.Set("Content-Type", "text/plain")
					}
					if tc.tamperLength {
						req.ContentLength++
					}
					require.Equal(t, tc.wantError, sigV4CompatibilityAuthRejected(service, req))
				})
			}
		})
	}
}

func newSigV4CompatibilityRequest(t *testing.T, service string, signedLength bool) *http.Request {
	t.Helper()
	body := []byte(`{"probe":"value"}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://api.example.test/", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Target", "AmazonSQS.NoSuchOperation")
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	if service == "s3" {
		req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	}
	// Botocore omits Content-Length from SignedHeaders. The wire request still
	// carries the actual body length, which Go stores outside the Header map.
	if !signedLength {
		req.ContentLength = 0
	}
	signer := v4.NewSigner(func(opts *v4.SignerOptions) { opts.DisableURIPathEscaping = true })
	require.NoError(t, signer.SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID: "compat-access", SecretAccessKey: "compat-secret",
	}, req, payloadHash, service, "us-east-1", time.Now().UTC()))
	require.Equal(t, signedLength, strings.Contains(extractSigV4AuthField(req.Header.Get("Authorization"), "SignedHeaders"), "content-length"))
	req.ContentLength = int64(len(body))
	return req
}

func sigV4CompatibilityAuthRejected(service string, req *http.Request) bool {
	creds := map[string]string{"compat-access": "compat-secret"}
	if service == "s3" {
		srv := NewS3Server(nil, "", nil, nil, nil, WithS3Region("us-east-1"), WithS3StaticCredentials(creds))
		return srv.authorizeRequest(req) != nil
	}
	srv := NewSQSServer(nil, nil, nil, WithSQSRegion("us-east-1"), WithSQSStaticCredentials(creds))
	return srv.authorizeSQSRequest(req) != nil
}

func TestSQSSignatureRejectsUnsignedOperationTarget(t *testing.T) {
	t.Parallel()
	req := newSigV4CompatibilityRequest(t, "sqs", false)
	req.Header.Del("Authorization")
	req.Header.Del("X-Amz-Target")
	req.ContentLength = 0
	sum := sha256.Sum256([]byte(`{"probe":"value"}`))
	signer := v4.NewSigner(func(opts *v4.SignerOptions) { opts.DisableURIPathEscaping = true })
	require.NoError(t, signer.SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID: "compat-access", SecretAccessKey: "compat-secret",
	}, req, hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now().UTC()))
	req.ContentLength = int64(len(`{"probe":"value"}`))
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	require.True(t, sigV4CompatibilityAuthRejected("sqs", req))
}

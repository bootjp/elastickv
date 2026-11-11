package adapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestS3ServerRejectsUnsignedACLBeforeCreatingBucket(t *testing.T) {
	t.Parallel()
	for _, presigned := range []bool{false, true} {
		name := "authorization header"
		if presigned {
			name = "presigned URL"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.NewMVCCStore()
			server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil,
				WithS3Region(testS3Region), WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}))
			req := newSignedS3Request(t, "/guard-bucket", "", currentS3SigningTime())
			if presigned {
				req = newPresignedS3CreateBucketRequest(t)
			}
			req.Header.Set("X-Amz-Acl", "public-read")
			rec := httptest.NewRecorder()
			server.handle(rec, req)
			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			buckets, err := server.AdminListBuckets(context.Background())
			require.NoError(t, err)
			require.Empty(t, buckets, "unsigned ACL must not create public state")
		})
	}
}

func TestS3AuthRequiresS3HeadersToBeSigned(t *testing.T) {
	t.Parallel()
	for _, header := range []string{"X-Amz-Acl", "X-Amz-Meta-Owner", "X-Amz-Checksum-Crc32", "X-Amz-Grant-Read", "Content-Md5"} {
		t.Run(header, func(t *testing.T) {
			t.Parallel()
			server := NewS3Server(nil, "", nil, nil, nil, WithS3Region(testS3Region),
				WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}))
			req := newSignedS3Request(t, "/guard-bucket", "", currentS3SigningTime())
			req.Header.Set(header, "added-after-signing")
			require.NotNil(t, server.authorizeRequest(req))
			req = resignS3Request(t, req, currentS3SigningTime())
			require.Nil(t, server.authorizeRequest(req), "a correctly signed header remains valid")
			req.Header.Set(header, "changed-after-signing")
			require.NotNil(t, server.authorizeRequest(req), "signed values remain protected")
		})
	}
}

func newPresignedS3CreateBucketRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, "http://localhost/guard-bucket?X-Amz-Expires=300", nil)
	require.NoError(t, err)
	signer := v4.NewSigner(func(opts *v4.SignerOptions) { opts.DisableURIPathEscaping = true })
	signedURL, _, err := signer.PresignHTTP(context.Background(), aws.Credentials{
		AccessKeyID: testS3AccessKey, SecretAccessKey: testS3SecretKey,
	}, req, s3UnsignedPayload, "s3", testS3Region, currentS3SigningTime())
	require.NoError(t, err)
	parsed, err := url.Parse(signedURL)
	require.NoError(t, err)
	out := newS3TestRequest(http.MethodPut, parsed.RequestURI(), nil)
	out.Host = "localhost"
	return out
}

func TestS3AuthAllowsPayloadHashOutsideSignedHeaders(t *testing.T) {
	t.Parallel()
	req := newSignedS3Request(t, "/guard-bucket", "", currentS3SigningTime())
	req.Header.Del("Authorization")
	req.Header.Del("X-Amz-Content-Sha256")
	signer := v4.NewSigner(func(opts *v4.SignerOptions) { opts.DisableURIPathEscaping = true })
	require.NoError(t, signer.SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID: testS3AccessKey, SecretAccessKey: testS3SecretKey,
	}, req, s3EmptyPayloadHash, "s3", testS3Region, currentS3SigningTime()))
	req.Header.Set("X-Amz-Content-Sha256", s3EmptyPayloadHash)
	server := NewS3Server(nil, "", nil, nil, nil, WithS3Region(testS3Region),
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}))
	require.Nil(t, server.authorizeRequest(req))
}

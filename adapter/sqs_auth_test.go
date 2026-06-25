package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	json "github.com/goccy/go-json"
)

const (
	testSQSAccessKey = "AKIATESTSQSAAAAAAAAA"
	testSQSSecretKey = "test-secret-key/xxxxxxxxxxxxxxxxxxxxxxxx"
	testSQSRegion    = "us-west-2"
)

// startAuthedSQSServer brings up an SQSServer with static credentials so
// we can exercise the full SigV4 verifier. No coordinator is wired in, so
// the server only executes the auth + health paths (handler requests land
// on unknown targets, returning 400 InvalidAction after auth passes).
func startAuthedSQSServer(t *testing.T) string {
	t.Helper()
	lc := &net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewSQSServer(listener, nil, nil,
		WithSQSRegion(testSQSRegion),
		WithSQSStaticCredentials(map[string]string{testSQSAccessKey: testSQSSecretKey}),
	)
	go func() {
		_ = srv.Run()
	}()
	t.Cleanup(func() {
		srv.Stop()
	})
	return "http://" + listener.Addr().String()
}

// signSQSRequest signs an SQS request with the AWS SDK v4 signer against
// the supplied credentials, exactly as a real AWS SDK client would.
func signSQSRequest(t *testing.T, base, target string, body []byte, signingTime time.Time, accessKey, secretKey string) *http.Request {
	region := testSQSRegion
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+"/", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", sqsContentTypeJSON)
	req.Header.Set("X-Amz-Target", target)
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])

	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	if err := signer.SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		Source:          "test",
	}, req, payloadHash, sqsSigV4Service, region, signingTime.UTC()); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return req
}

func doReq(t *testing.T, req *http.Request) (int, map[string]string) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]string{}
	if len(bytes.TrimSpace(raw)) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return resp.StatusCode, out
}

func TestSQSAuth_ValidSignaturePassesAuth(t *testing.T) {
	t.Parallel()
	base := startAuthedSQSServer(t)
	body := []byte(`{}`)
	req := signSQSRequest(t, base, "AmazonSQS.NoSuchOperation", body, time.Now().UTC(),
		testSQSAccessKey, testSQSSecretKey)

	// Auth should pass; then the target is unknown, so the dispatcher
	// returns InvalidAction — *not* SignatureDoesNotMatch. That precise
	// ordering is what we care about: auth runs first, dispatch second.
	status, body2 := doReq(t, req)
	if status != http.StatusBadRequest {
		t.Fatalf("status: got %d body %v", status, body2)
	}
	if body2["__type"] != sqsErrInvalidAction {
		t.Fatalf("error type: got %q want %q", body2["__type"], sqsErrInvalidAction)
	}
}

func TestSQSAuth_MissingAuthorizationHeaderRejected(t *testing.T) {
	t.Parallel()
	base := startAuthedSQSServer(t)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+"/", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", sqsContentTypeJSON)
	req.Header.Set("X-Amz-Target", sqsListQueuesTarget)

	status, body := doReq(t, req)
	if status != http.StatusForbidden {
		t.Fatalf("status: got %d body %v", status, body)
	}
	if body["__type"] != "MissingAuthenticationToken" {
		t.Fatalf("error type: got %q", body["__type"])
	}
}

func TestSQSAuth_WrongServiceScopeRejected(t *testing.T) {
	t.Parallel()
	base := startAuthedSQSServer(t)
	body := []byte(`{}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+"/", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", sqsContentTypeJSON)
	req.Header.Set("X-Amz-Target", sqsListQueuesTarget)
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	signer := v4.NewSigner()
	// Sign with service "s3" instead of "sqs".
	if err := signer.SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID:     testSQSAccessKey,
		SecretAccessKey: testSQSSecretKey,
	}, req, payloadHash, "s3", testSQSRegion, time.Now().UTC()); err != nil {
		t.Fatalf("sign: %v", err)
	}

	status, out := doReq(t, req)
	if status != http.StatusForbidden {
		t.Fatalf("status: got %d body %v", status, out)
	}
	if out["__type"] != "SignatureDoesNotMatch" {
		t.Fatalf("error type: got %q", out["__type"])
	}
}

func TestSQSAuth_UnknownAccessKeyRejected(t *testing.T) {
	t.Parallel()
	base := startAuthedSQSServer(t)
	req := signSQSRequest(t, base, sqsListQueuesTarget, []byte("{}"), time.Now().UTC(),
		"AKIAUNKNOWNEEEEEEEEE", "wrong-secret-keyxxxxxxxxxxxxxxxxxxxxxxxx")

	status, out := doReq(t, req)
	if status != http.StatusForbidden {
		t.Fatalf("status: got %d body %v", status, out)
	}
	if out["__type"] != "InvalidClientTokenId" {
		t.Fatalf("error type: got %q", out["__type"])
	}
}

func TestSQSAuth_TamperedBodyRejected(t *testing.T) {
	t.Parallel()
	base := startAuthedSQSServer(t)
	// Sign the original body, then send a different one — the server must
	// hash the body it actually receives and catch the mismatch.
	req := signSQSRequest(t, base, sqsListQueuesTarget, []byte(`{"QueueNamePrefix":"a"}`), time.Now().UTC(),
		testSQSAccessKey, testSQSSecretKey)
	req.Body = io.NopCloser(bytes.NewReader([]byte(`{"QueueNamePrefix":"b"}`)))
	req.ContentLength = int64(len(`{"QueueNamePrefix":"b"}`))

	status, out := doReq(t, req)
	if status != http.StatusForbidden {
		t.Fatalf("status: got %d body %v", status, out)
	}
	if out["__type"] != "SignatureDoesNotMatch" {
		t.Fatalf("error type: got %q body %v", out["__type"], out)
	}
}

func TestSQSAuth_ClockSkewRejected(t *testing.T) {
	t.Parallel()
	base := startAuthedSQSServer(t)
	old := time.Now().UTC().Add(-30 * time.Minute)
	req := signSQSRequest(t, base, sqsListQueuesTarget, []byte("{}"), old,
		testSQSAccessKey, testSQSSecretKey)

	status, out := doReq(t, req)
	if status != http.StatusForbidden {
		t.Fatalf("status: got %d body %v", status, out)
	}
	if out["__type"] != "RequestTimeTooSkewed" {
		t.Fatalf("error type: got %q body %v", out["__type"], out)
	}
}

func TestSQSAuth_NoCredentialsMeansOpenEndpoint(t *testing.T) {
	t.Parallel()
	// No creds configured → scaffold test server already exercises this
	// path (no Authorization header, no 403). This test explicitly
	// double-checks: the dispatch runs even without auth, landing on
	// InvalidAction for an unknown target.
	base := startTestSQSServer(t)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+"/", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", sqsContentTypeJSON)
	req.Header.Set("X-Amz-Target", "AmazonSQS.NoSuchOperation")

	status, out := doReq(t, req)
	if status != http.StatusBadRequest {
		t.Fatalf("status: got %d body %v", status, out)
	}
	if out["__type"] != sqsErrInvalidAction {
		t.Fatalf("error type: got %q", out["__type"])
	}
}

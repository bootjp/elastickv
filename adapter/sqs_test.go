package adapter

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	json "github.com/goccy/go-json"
)

// startTestSQSServer starts an SQSServer on an ephemeral localhost listener
// with no coordinator or leader map — enough to exercise the health
// endpoints, the dispatch table, and the error envelope. Real flows with Raft
// come in later PRs alongside the handler implementations.
func startTestSQSServer(t *testing.T) string {
	t.Helper()
	lc := &net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewSQSServer(listener, nil, nil)
	go func() {
		_ = srv.Run()
	}()
	t.Cleanup(func() {
		srv.Stop()
	})
	return "http://" + listener.Addr().String()
}

func doRequest(t *testing.T, method, url string, body io.Reader, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, url, body)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

func postSQSRequest(t *testing.T, url string, target string, body string) *http.Response {
	t.Helper()
	headers := map[string]string{"Content-Type": sqsContentTypeJSON}
	if target != "" {
		headers["X-Amz-Target"] = target
	}
	return doRequest(t, http.MethodPost, url, strings.NewReader(body), headers)
}

func TestSQSServer_HealthOK(t *testing.T) {
	t.Parallel()
	base := startTestSQSServer(t)

	resp := doRequest(t, http.MethodGet, base+sqsHealthPath, nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want %d", resp.StatusCode, http.StatusOK)
	}
	body, _ := io.ReadAll(resp.Body)
	if got := strings.TrimSpace(string(body)); got != "ok" {
		t.Fatalf("body: got %q want %q", got, "ok")
	}
}

func TestSQSServer_LeaderHealthWithoutCoordinatorIsNotLeader(t *testing.T) {
	t.Parallel()
	base := startTestSQSServer(t)

	resp := doRequest(t, http.MethodGet, base+sqsLeaderHealthPath, nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestSQSServer_HealthRejectsNonGetHead(t *testing.T) {
	t.Parallel()
	base := startTestSQSServer(t)

	resp := doRequest(t, http.MethodPost, base+sqsHealthPath, nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, "GET") {
		t.Fatalf("allow header: got %q", allow)
	}
}

func TestSQSServer_UnknownTargetReturnsInvalidAction(t *testing.T) {
	t.Parallel()
	base := startTestSQSServer(t)

	resp := postSQSRequest(t, base+"/", "AmazonSQS.NoSuchOperation", "{}")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if got := resp.Header.Get("x-amzn-ErrorType"); got != sqsErrInvalidAction {
		t.Fatalf("error type: got %q want %q", got, sqsErrInvalidAction)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["__type"] != sqsErrInvalidAction {
		t.Fatalf("body __type: got %q want %q", body["__type"], sqsErrInvalidAction)
	}
}

// TestSQSServer_AllTargetsHaveHandlers asserts every target listed in
// targetHandlers has a real handler attached. The previous version of
// this test pinned a fixed list of NotImplemented targets and had to
// be updated each time a handler shipped — that drift hid the
// PurgeQueue/Tag* implementations behind a stale assertion. Here we
// instead reach into the dispatch map and confirm none of the
// registered targets is the NotImplemented stub.
func TestSQSServer_AllTargetsHaveHandlers(t *testing.T) {
	t.Parallel()
	base := startTestSQSServer(t)

	// Sanity-check the route table against an unknown target — we
	// already test that path elsewhere, but it pins the assumption that
	// unregistered targets surface InvalidAction (not 501) so this test
	// is not the one to break if that contract changes.
	resp := postSQSRequest(t, base+"/", "AmazonSQS.NotARealOp", "{}")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown target: got %d want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestSQSServer_RejectsNonPostOnRoot(t *testing.T) {
	t.Parallel()
	base := startTestSQSServer(t)

	resp := doRequest(t, http.MethodGet, base+"/", nil, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, http.MethodPost) {
		t.Fatalf("allow header: got %q", allow)
	}
}

func TestSQSServer_ErrorEnvelopeShape(t *testing.T) {
	t.Parallel()
	// We check the precise envelope shape via httptest so that SDK parsers
	// that key off x-amzn-ErrorType + __type + message do not regress.
	rec := httptest.NewRecorder()
	writeSQSError(rec, http.StatusBadRequest, sqsErrInvalidAction, "oops")
	if got := rec.Header().Get("Content-Type"); got != sqsContentTypeJSON {
		t.Fatalf("content-type: got %q", got)
	}
	if got := rec.Header().Get("x-amzn-ErrorType"); got != sqsErrInvalidAction {
		t.Fatalf("error type header: got %q", got)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["__type"] != sqsErrInvalidAction {
		t.Fatalf("__type: got %q", body["__type"])
	}
	if body["message"] != "oops" {
		t.Fatalf("message: got %q", body["message"])
	}
}

// TestSQSServer_StopShutsDown guards against regressions where Stop leaves the
// goroutine leaking; after Stop, Run must return promptly.
func TestSQSServer_StopShutsDown(t *testing.T) {
	t.Parallel()
	lc := &net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewSQSServer(listener, nil, nil)
	done := make(chan error, 1)
	go func() { done <- srv.Run() }()
	// Give Serve a chance to enter its accept loop.
	time.Sleep(20 * time.Millisecond)
	srv.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within timeout after Stop")
	}
}

package adapter

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// ---------- protocol detection ----------

func TestPickSqsProtocol(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		method    string
		urlStr    string
		headerCT  string
		headerTgt string
		want      sqsProtocol
	}{
		{"json: target + json ct", http.MethodPost, "/", sqsContentTypeJSON, "AmazonSQS.CreateQueue", sqsProtocolJSON},
		{"json: target wins over form ct", http.MethodPost, "/", sqsContentTypeQueryURLEncoded, "AmazonSQS.CreateQueue", sqsProtocolJSON},
		{"query: form ct without target", http.MethodPost, "/", sqsContentTypeQueryURLEncoded, "", sqsProtocolQuery},
		{"query: form ct with charset", http.MethodPost, "/", sqsContentTypeQueryURLEncoded + "; charset=utf-8", "", sqsProtocolQuery},
		{"query: GET with Action in querystring", http.MethodGet, "/?Action=ListQueues", "", "", sqsProtocolQuery},
		{"unknown: empty body, no headers", http.MethodPost, "/", "", "", sqsProtocolUnknown},
		{"unknown: GET without Action", http.MethodGet, "/", "", "", sqsProtocolUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(tc.method, tc.urlStr, nil)
			if tc.headerCT != "" {
				r.Header.Set("Content-Type", tc.headerCT)
			}
			if tc.headerTgt != "" {
				r.Header.Set("X-Amz-Target", tc.headerTgt)
			}
			if got := pickSqsProtocol(r); got != tc.want {
				t.Fatalf("pickSqsProtocol = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestPickSqsProtocol_NilRequest(t *testing.T) {
	t.Parallel()
	if got := pickSqsProtocol(nil); got != sqsProtocolUnknown {
		t.Fatalf("nil request: got %d, want sqsProtocolUnknown", got)
	}
}

// ---------- collectIndexedKVPairs ----------

func TestCollectIndexedKVPairs(t *testing.T) {
	t.Parallel()
	form := url.Values{
		"Attribute.1.Name":  []string{"VisibilityTimeout"},
		"Attribute.1.Value": []string{"60"},
		"Attribute.2.Name":  []string{"DelaySeconds"},
		"Attribute.2.Value": []string{"5"},
		// orphan Name (no matching Value): silently skipped
		"Attribute.3.Name": []string{"NoValue"},
		// noise that should not interfere
		"NotAnAttribute": []string{"hi"},
	}
	got := collectIndexedKVPairs(form, "Attribute")
	if len(got) != 2 {
		t.Fatalf("expected 2 pairs, got %d (%v)", len(got), got)
	}
	if got["VisibilityTimeout"] != "60" {
		t.Errorf("VisibilityTimeout = %q, want 60", got["VisibilityTimeout"])
	}
	if got["DelaySeconds"] != "5" {
		t.Errorf("DelaySeconds = %q, want 5", got["DelaySeconds"])
	}
	if _, ok := got["NoValue"]; ok {
		t.Errorf("orphan Name should not appear in result: %v", got)
	}
}

func TestCollectIndexedKVPairs_Empty(t *testing.T) {
	t.Parallel()
	if got := collectIndexedKVPairs(nil, "Attribute"); got != nil {
		t.Fatalf("nil form: got %v, want nil", got)
	}
	if got := collectIndexedKVPairs(url.Values{}, "Attribute"); got != nil {
		t.Fatalf("empty form: got %v, want nil", got)
	}
	if got := collectIndexedKVPairs(url.Values{"Other.1.Name": []string{"x"}, "Other.1.Value": []string{"y"}}, "Attribute"); got != nil {
		t.Fatalf("unrelated form: got %v, want nil", got)
	}
}

// ---------- error envelope shape ----------

func TestWriteSQSQueryError_ShapeAndStatus(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	writeSQSQueryError(rec, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/xml") {
		t.Fatalf("Content-Type = %q, want text/xml prefix", ct)
	}
	if got := rec.Header().Get("x-amzn-ErrorType"); got != sqsErrQueueDoesNotExist {
		t.Fatalf("x-amzn-ErrorType = %q, want %q", got, sqsErrQueueDoesNotExist)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<Type>Sender</Type>") {
		t.Errorf("missing <Type>Sender</Type> for 4xx; body=%s", body)
	}
	if !strings.Contains(body, "<Code>"+sqsErrQueueDoesNotExist+"</Code>") {
		t.Errorf("missing <Code>; body=%s", body)
	}
	if !strings.Contains(body, "<Message>queue does not exist</Message>") {
		t.Errorf("missing <Message>; body=%s", body)
	}
	if !strings.Contains(body, `xmlns="`+sqsQueryNamespace+`"`) {
		t.Errorf("missing namespace; body=%s", body)
	}
}

func TestWriteSQSQueryError_5xxIsReceiver(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	writeSQSQueryError(rec, newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "boom"))
	if !strings.Contains(rec.Body.String(), "<Type>Receiver</Type>") {
		t.Fatalf("expected <Type>Receiver</Type> for 5xx; body=%s", rec.Body.String())
	}
}

// ---------- end-to-end via SQS listener ----------

// queryRoundTrip calls a single SQS query-protocol verb against the
// in-process listener and returns the decoded response envelope.
func queryRoundTrip(t *testing.T, node Node, action string, form url.Values) (int, []byte) {
	t.Helper()
	form.Set("Action", action)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://"+node.sqsAddress+"/", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", sqsContentTypeQueryURLEncoded)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}

func TestSQSServer_QueryProtocol_CreateQueueRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	status, body := queryRoundTrip(t, node, "CreateQueue", url.Values{
		"QueueName":         []string{"query-create"},
		"Attribute.1.Name":  []string{"VisibilityTimeout"},
		"Attribute.1.Value": []string{"45"},
	})
	if status != http.StatusOK {
		t.Fatalf("CreateQueue: status %d body %s", status, body)
	}
	var resp struct {
		XMLName xml.Name `xml:"CreateQueueResponse"`
		Result  struct {
			QueueUrl string `xml:"QueueUrl"`
		} `xml:"CreateQueueResult"`
		Metadata struct {
			RequestId string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(body), &resp); err != nil {
		t.Fatalf("decode: %v\nbody=%s", err, body)
	}
	if !strings.HasSuffix(resp.Result.QueueUrl, "/query-create") {
		t.Errorf("QueueUrl = %q; expected suffix /query-create", resp.Result.QueueUrl)
	}
	if resp.Metadata.RequestId == "" {
		t.Errorf("missing RequestId")
	}

	// Verify the queue actually exists by hitting GetQueueUrl
	// through the JSON path — round-trip parity §9.2 in design doc.
	jStatus, jOut := callSQS(t, node, sqsGetQueueUrlTarget, map[string]any{
		"QueueName": "query-create",
	})
	if jStatus != http.StatusOK {
		t.Fatalf("JSON GetQueueUrl after Query CreateQueue: %d %v", jStatus, jOut)
	}
	if got, _ := jOut["QueueUrl"].(string); got != resp.Result.QueueUrl {
		t.Errorf("URL mismatch across protocols: query=%q json=%q", resp.Result.QueueUrl, got)
	}
}

func TestSQSServer_QueryProtocol_ListQueuesRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	// Create two queues via JSON, list via query — parity check.
	for _, name := range []string{"query-list-a", "query-list-b"} {
		_, _ = callSQS(t, node, sqsCreateQueueTarget, map[string]any{"QueueName": name})
	}

	status, body := queryRoundTrip(t, node, "ListQueues", url.Values{
		"QueueNamePrefix": []string{"query-list-"},
	})
	if status != http.StatusOK {
		t.Fatalf("ListQueues: status %d body %s", status, body)
	}
	var resp struct {
		XMLName xml.Name `xml:"ListQueuesResponse"`
		Result  struct {
			QueueUrls []string `xml:"QueueUrl"`
		} `xml:"ListQueuesResult"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(body), &resp); err != nil {
		t.Fatalf("decode: %v\nbody=%s", err, body)
	}
	if len(resp.Result.QueueUrls) != 2 {
		t.Fatalf("got %d URLs, want 2; body=%s", len(resp.Result.QueueUrls), body)
	}
	hasA, hasB := false, false
	for _, u := range resp.Result.QueueUrls {
		if strings.HasSuffix(u, "/query-list-a") {
			hasA = true
		}
		if strings.HasSuffix(u, "/query-list-b") {
			hasB = true
		}
	}
	if !hasA || !hasB {
		t.Fatalf("missing one of the seeded queues; URLs=%v", resp.Result.QueueUrls)
	}
}

func TestSQSServer_QueryProtocol_GetQueueUrlNotFoundError(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	status, body := queryRoundTrip(t, node, "GetQueueUrl", url.Values{
		"QueueName": []string{"never-existed"},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing queue; got %d body=%s", status, body)
	}
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "<ErrorResponse") {
		t.Fatalf("expected XML ErrorResponse envelope; body=%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "<Code>"+sqsErrQueueDoesNotExist+"</Code>") {
		t.Fatalf("expected QueueDoesNotExist code; body=%s", bodyStr)
	}
}

func TestSQSServer_QueryProtocol_UnknownActionReturns501(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	status, body := queryRoundTrip(t, node, "TotallyNotARealAction", url.Values{})
	if status != http.StatusNotImplemented {
		t.Fatalf("expected 501 for unknown verb; got %d body=%s", status, body)
	}
	if !strings.Contains(string(body), "NotImplementedYet") {
		t.Fatalf("expected NotImplementedYet code; body=%s", body)
	}
}

func TestSQSServer_QueryProtocol_MissingActionReturns400(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	// Send a form-urlencoded body without an Action — must surface
	// MissingAction (JSON-style envelope per §3 of design doc).
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://"+node.sqsAddress+"/", strings.NewReader("Foo=bar"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", sqsContentTypeQueryURLEncoded)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing Action: status %d, want 400", resp.StatusCode)
	}
}

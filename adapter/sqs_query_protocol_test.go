package adapter

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
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
	got := collectIndexedKVPairs(form, "Attribute", "Name")
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

// TestCollectIndexedKVPairs_TagSuffix pins that the keyField argument
// distinguishes Attribute (.Name) from Tag (.Key) shapes per the AWS
// SQS query reference. CodexP1 + Gemini both flagged the prior
// hardcoded .Name path as a silent tag-loss bug.
func TestCollectIndexedKVPairs_TagSuffix(t *testing.T) {
	t.Parallel()
	form := url.Values{
		"Tag.1.Key":   []string{"env"},
		"Tag.1.Value": []string{"prod"},
		"Tag.2.Key":   []string{"team"},
		"Tag.2.Value": []string{"sre"},
		// Wrong shape for tags: must NOT be picked up.
		"Tag.3.Name":  []string{"shouldNotAppear"},
		"Tag.3.Value": []string{"nope"},
	}
	got := collectIndexedKVPairs(form, "Tag", "Key")
	if len(got) != 2 {
		t.Fatalf("expected 2 tag pairs, got %d (%v)", len(got), got)
	}
	if got["env"] != "prod" || got["team"] != "sre" {
		t.Errorf("tag map = %v, want env=prod team=sre", got)
	}
	if _, present := got["shouldNotAppear"]; present {
		t.Errorf("Tag.N.Name was incorrectly accepted as a tag key: %v", got)
	}
}

func TestCollectIndexedKVPairs_UnindexedSinglePair(t *testing.T) {
	t.Parallel()
	form := url.Values{
		"Attribute.Name":  []string{"VisibilityTimeout"},
		"Attribute.Value": []string{"60"},
		"Tag.Key":         []string{"env"},
		"Tag.Value":       []string{"prod"},
	}
	attrs := collectIndexedKVPairs(form, "Attribute", "Name")
	if attrs["VisibilityTimeout"] != "60" {
		t.Fatalf("unindexed attribute map = %v, want VisibilityTimeout=60", attrs)
	}
	tags := collectIndexedKVPairs(form, "Tag", "Key")
	if tags["env"] != "prod" {
		t.Fatalf("unindexed tag map = %v, want env=prod", tags)
	}
}

// TestCollectIndexedKVPairs_DeterministicOnDuplicates pins that two
// entries resolving to the same logical key resolve deterministically
// (lower index wins). CodexP2 flagged the previous map iteration as
// non-deterministic because Go map order is randomised.
func TestCollectIndexedKVPairs_DeterministicOnDuplicates(t *testing.T) {
	t.Parallel()
	form := url.Values{
		"Attribute.5.Name":  []string{"VisibilityTimeout"},
		"Attribute.5.Value": []string{"50"},
		"Attribute.2.Name":  []string{"VisibilityTimeout"},
		"Attribute.2.Value": []string{"20"},
	}
	// Run many times to make sure map-iteration randomness does not
	// leak through. Lower index (2) must win every iteration.
	for i := 0; i < 64; i++ {
		got := collectIndexedKVPairs(form, "Attribute", "Name")
		if got["VisibilityTimeout"] != "20" {
			t.Fatalf("iter %d: lower-index value lost; got=%v", i, got)
		}
	}
}

func TestCollectIndexedKVPairs_Empty(t *testing.T) {
	t.Parallel()
	if got := collectIndexedKVPairs(nil, "Attribute", "Name"); got != nil {
		t.Fatalf("nil form: got %v, want nil", got)
	}
	if got := collectIndexedKVPairs(url.Values{}, "Attribute", "Name"); got != nil {
		t.Fatalf("empty form: got %v, want nil", got)
	}
	if got := collectIndexedKVPairs(url.Values{"Other.1.Name": []string{"x"}, "Other.1.Value": []string{"y"}}, "Attribute", "Name"); got != nil {
		t.Fatalf("unrelated form: got %v, want nil", got)
	}
}

func TestCollectIndexedValues(t *testing.T) {
	t.Parallel()
	form := url.Values{
		"AttributeName":   []string{"All"},
		"AttributeName.3": []string{"DelaySeconds"},
		"AttributeName.1": []string{"VisibilityTimeout"},
		"AttributeName.x": []string{"ignored"},
		"TagKey.1":        []string{"not-an-attribute"},
	}
	got := collectIndexedValues(form, "AttributeName")
	want := []string{"All", "VisibilityTimeout", "DelaySeconds"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("value %d = %q, want %q (all=%v)", i, got[i], want[i], got)
		}
	}
}

// TestNewQueryRequestID_Length pins the AWS shape: 22 base32 chars.
// A regression where this function returned 26 chars contradicted
// its own doc comment, so the length is asserted explicitly.
func TestNewQueryRequestID_Length(t *testing.T) {
	t.Parallel()
	for i := 0; i < 64; i++ {
		id := newQueryRequestID()
		if len(id) != 22 {
			t.Fatalf("RequestId length = %d, want 22; id=%q", len(id), id)
		}
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

func TestWriteSQSQueryError_PurgeRateLimit(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	writeSQSQueryError(rec, &purgeRateLimitedError{remaining: 60})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if got := rec.Header().Get("x-amzn-ErrorType"); got != sqsErrPurgeInProgress {
		t.Fatalf("x-amzn-ErrorType = %q, want %q", got, sqsErrPurgeInProgress)
	}
	if body := rec.Body.String(); !strings.Contains(body, "<Code>"+sqsErrPurgeInProgress+"</Code>") {
		t.Fatalf("missing purge-in-progress code; body=%s", body)
	}
}

func TestParseOptionalQueryIntRejectsOutOfRange(t *testing.T) {
	t.Parallel()
	_, err := parseOptionalQueryInt(url.Values{
		"MaxNumberOfMessages": []string{"9223372036854775808"},
	}, "MaxNumberOfMessages")
	if err == nil {
		t.Fatal("expected range error")
	}
	apiErr := &sqsAPIError{}
	ok := errors.As(err, &apiErr)
	if !ok {
		t.Fatalf("error type = %T, want *sqsAPIError", err)
	}
	if apiErr.status != http.StatusBadRequest || apiErr.errorType != sqsErrInvalidAttributeValue {
		t.Fatalf("api error = status %d type %q, want 400 %q", apiErr.status, apiErr.errorType, sqsErrInvalidAttributeValue)
	}
	if !strings.Contains(apiErr.message, "out of range") {
		t.Fatalf("message = %q, want out of range", apiErr.message)
	}
}

// ---------- end-to-end via SQS listener ----------

// queryRoundTrip calls a single SQS query-protocol verb against the
// in-process listener and returns the decoded response envelope.
func queryRoundTrip(t *testing.T, node Node, action string, form url.Values) (int, []byte) {
	t.Helper()
	return queryRoundTripPath(t, node, "/", action, form)
}

func queryRoundTripPath(t *testing.T, node Node, requestPath, action string, form url.Values) (int, []byte) {
	t.Helper()
	form.Set("Action", action)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://"+node.sqsAddress+requestPath, strings.NewReader(form.Encode()))
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

func TestSQSServer_QueryProtocol_QueueURLFromPath(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := queryCreateQueueURL(t, node, "query-path")
	status, body := queryRoundTripPath(t, node, "/123456789012/query-path", "SendMessage", url.Values{
		"MessageBody": []string{"path-routed"},
	})
	if status != http.StatusOK {
		t.Fatalf("SendMessage path QueueUrl fallback: status %d body %s queueURL=%s", status, body, queueURL)
	}
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

func TestSQSServer_QueryProtocol_ControlPlaneRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := queryCreateQueueURL(t, node, "query-control")
	querySetVisibilityTimeout(t, node, queueURL, "12")
	attrs := queryAttributes(t, node, queueURL, []string{"VisibilityTimeout", "QueueArn"})
	if got := attrs["VisibilityTimeout"]; got != "12" {
		t.Fatalf("VisibilityTimeout = %q, want 12; attrs=%v", got, attrs)
	}
	if got := attrs["QueueArn"]; !strings.HasSuffix(got, ":query-control") {
		t.Fatalf("QueueArn = %q, want suffix :query-control; attrs=%v", got, attrs)
	}

	queryTagQueue(t, node, queueURL, map[string]string{"env": "prod", "team": "storage"})
	tags := queryTags(t, node, queueURL)
	if tags["env"] != "prod" || tags["team"] != "storage" {
		t.Fatalf("tags = %v, want env/prod and team/storage", tags)
	}

	queryUntagQueue(t, node, queueURL, []string{"env"})
	tags = queryTags(t, node, queueURL)
	if _, ok := tags["env"]; ok || tags["team"] != "storage" {
		t.Fatalf("tags after untag = %v, want only team/storage", tags)
	}

	queryOK(t, node, "PurgeQueue", url.Values{"QueueUrl": []string{queueURL}})
	queryOK(t, node, "DeleteQueue", url.Values{"QueueUrl": []string{queueURL}})
	assertJSONQueueDeleted(t, node, "query-control")
}

func TestSQSServer_QueryProtocol_MessageLifecycleRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := queryCreateQueueURL(t, node, "query-message")
	_, sendBody := queryOK(t, node, "SendMessage", url.Values{
		"QueueUrl":                             []string{queueURL},
		"MessageBody":                          []string{"hello-query"},
		"MessageAttribute.1.Name":              []string{"City"},
		"MessageAttribute.1.Value.DataType":    []string{"String"},
		"MessageAttribute.1.Value.StringValue": []string{"Tokyo"},
	})
	var sendResp struct {
		Result struct {
			MessageId              string `xml:"MessageId"`
			MD5OfMessageBody       string `xml:"MD5OfMessageBody"`
			MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes"`
		} `xml:"SendMessageResult"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(sendBody), &sendResp); err != nil {
		t.Fatalf("decode send: %v\nbody=%s", err, sendBody)
	}
	if sendResp.Result.MessageId == "" || sendResp.Result.MD5OfMessageBody == "" || sendResp.Result.MD5OfMessageAttributes == "" {
		t.Fatalf("incomplete SendMessage result: %+v body=%s", sendResp.Result, sendBody)
	}

	_, recvBody := queryOK(t, node, "ReceiveMessage", url.Values{
		"QueueUrl":               []string{queueURL},
		"MaxNumberOfMessages":    []string{"1"},
		"VisibilityTimeout":      []string{"60"},
		"MessageAttributeName.1": []string{"All"},
	})
	recvResp := decodeQueryReceiveResponse(t, recvBody)
	if len(recvResp.Result.Messages) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages, want 1; body=%s", len(recvResp.Result.Messages), recvBody)
	}
	msg := recvResp.Result.Messages[0]
	if msg.MessageId != sendResp.Result.MessageId || msg.Body != "hello-query" {
		t.Fatalf("received message mismatch: %+v send=%+v", msg, sendResp.Result)
	}
	if msg.MessageAttributes["City"].Value.StringValue != "Tokyo" {
		t.Fatalf("message attributes = %+v, want City=Tokyo; body=%s", msg.MessageAttributes, recvBody)
	}

	queryOK(t, node, "ChangeMessageVisibility", url.Values{
		"QueueUrl":          []string{queueURL},
		"ReceiptHandle":     []string{msg.ReceiptHandle},
		"VisibilityTimeout": []string{"120"},
	})
	queryOK(t, node, "DeleteMessage", url.Values{
		"QueueUrl":      []string{queueURL},
		"ReceiptHandle": []string{msg.ReceiptHandle},
	})
	_, emptyBody := queryOK(t, node, "ReceiveMessage", url.Values{
		"QueueUrl":            []string{queueURL},
		"MaxNumberOfMessages": []string{"1"},
		"WaitTimeSeconds":     []string{"0"},
	})
	emptyResp := decodeQueryReceiveResponse(t, emptyBody)
	if len(emptyResp.Result.Messages) != 0 {
		t.Fatalf("message should be deleted; got %d messages body=%s", len(emptyResp.Result.Messages), emptyBody)
	}
}

func TestSQSServer_QueryProtocol_BatchRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := queryCreateQueueURL(t, node, "query-batch")
	_, sendBody := queryOK(t, node, "SendMessageBatch", url.Values{
		"QueueUrl":                                   []string{queueURL},
		"SendMessageBatchRequestEntry.1.Id":          []string{"a"},
		"SendMessageBatchRequestEntry.1.MessageBody": []string{"alpha"},
		"SendMessageBatchRequestEntry.2.Id":          []string{"b"},
		"SendMessageBatchRequestEntry.2.MessageBody": []string{"beta"},
	})
	var sendResp struct {
		Result struct {
			Successful []struct {
				Id        string `xml:"Id"`
				MessageId string `xml:"MessageId"`
			} `xml:"SendMessageBatchResultEntry"`
			Failed []struct {
				Id string `xml:"Id"`
			} `xml:"BatchResultErrorEntry"`
		} `xml:"SendMessageBatchResult"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(sendBody), &sendResp); err != nil {
		t.Fatalf("decode batch send: %v\nbody=%s", err, sendBody)
	}
	if len(sendResp.Result.Successful) != 2 || len(sendResp.Result.Failed) != 0 {
		t.Fatalf("SendMessageBatch result = %+v body=%s", sendResp.Result, sendBody)
	}

	_, recvBody := queryOK(t, node, "ReceiveMessage", url.Values{
		"QueueUrl":            []string{queueURL},
		"MaxNumberOfMessages": []string{"2"},
		"VisibilityTimeout":   []string{"60"},
	})
	recvResp := decodeQueryReceiveResponse(t, recvBody)
	if len(recvResp.Result.Messages) != 2 {
		t.Fatalf("ReceiveMessage returned %d messages, want 2; body=%s", len(recvResp.Result.Messages), recvBody)
	}
	changeForm := url.Values{"QueueUrl": []string{queueURL}}
	deleteForm := url.Values{"QueueUrl": []string{queueURL}}
	for i, msg := range recvResp.Result.Messages {
		idx := strconv.Itoa(i + 1)
		changePrefix := "ChangeMessageVisibilityBatchRequestEntry." + idx
		changeForm.Set(changePrefix+".Id", "c"+idx)
		changeForm.Set(changePrefix+".ReceiptHandle", msg.ReceiptHandle)
		changeForm.Set(changePrefix+".VisibilityTimeout", "90")
		deletePrefix := "DeleteMessageBatchRequestEntry." + idx
		deleteForm.Set(deletePrefix+".Id", "d"+idx)
		deleteForm.Set(deletePrefix+".ReceiptHandle", msg.ReceiptHandle)
	}
	queryOK(t, node, "ChangeMessageVisibilityBatch", changeForm)
	_, deleteBody := queryOK(t, node, "DeleteMessageBatch", deleteForm)
	var deleteResp struct {
		Result struct {
			Successful []struct {
				Id string `xml:"Id"`
			} `xml:"DeleteMessageBatchResultEntry"`
			Failed []struct {
				Id string `xml:"Id"`
			} `xml:"BatchResultErrorEntry"`
		} `xml:"DeleteMessageBatchResult"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(deleteBody), &deleteResp); err != nil {
		t.Fatalf("decode batch delete: %v\nbody=%s", err, deleteBody)
	}
	if len(deleteResp.Result.Successful) != 2 || len(deleteResp.Result.Failed) != 0 {
		t.Fatalf("DeleteMessageBatch result = %+v body=%s", deleteResp.Result, deleteBody)
	}
}

func TestSQSServer_QueryProtocol_ChangeVisibilityBatchMissingTimeoutIsEntryFailure(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := queryCreateQueueURL(t, node, "query-change-missing-timeout")
	_, body := queryOK(t, node, "ChangeMessageVisibilityBatch", url.Values{
		"QueueUrl": []string{queueURL},
		"ChangeMessageVisibilityBatchRequestEntry.1.Id":            []string{"missing-timeout"},
		"ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle": []string{"not-used"},
	})
	var resp struct {
		Result struct {
			Successful []struct {
				Id string `xml:"Id"`
			} `xml:"ChangeMessageVisibilityBatchResultEntry"`
			Failed []struct {
				Id      string `xml:"Id"`
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			} `xml:"BatchResultErrorEntry"`
		} `xml:"ChangeMessageVisibilityBatchResult"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(body), &resp); err != nil {
		t.Fatalf("decode change visibility batch: %v\nbody=%s", err, body)
	}
	if len(resp.Result.Successful) != 0 || len(resp.Result.Failed) != 1 {
		t.Fatalf("result = %+v body=%s", resp.Result, body)
	}
	if got := resp.Result.Failed[0]; got.Id != "missing-timeout" || got.Code != sqsErrMissingParameter {
		t.Fatalf("failed entry = %+v, want id missing-timeout code %s", got, sqsErrMissingParameter)
	}
}

type queryReceiveTestResponse struct {
	Result struct {
		Messages []struct {
			MessageId     string `xml:"MessageId"`
			ReceiptHandle string `xml:"ReceiptHandle"`
			Body          string `xml:"Body"`
			Attributes    []struct {
				Name  string `xml:"Name"`
				Value string `xml:"Value"`
			} `xml:"Attribute"`
			MessageAttributes map[string]struct {
				Value struct {
					DataType    string `xml:"DataType"`
					StringValue string `xml:"StringValue"`
					BinaryValue string `xml:"BinaryValue"`
				} `xml:"Value"`
			}
			RawMessageAttributes []struct {
				Name  string `xml:"Name"`
				Value struct {
					DataType    string `xml:"DataType"`
					StringValue string `xml:"StringValue"`
					BinaryValue string `xml:"BinaryValue"`
				} `xml:"Value"`
			} `xml:"MessageAttribute"`
		} `xml:"Message"`
	} `xml:"ReceiveMessageResult"`
}

func decodeQueryReceiveResponse(t *testing.T, body []byte) queryReceiveTestResponse {
	t.Helper()
	var resp queryReceiveTestResponse
	if err := xml.Unmarshal(bytes.TrimSpace(body), &resp); err != nil {
		t.Fatalf("decode receive: %v\nbody=%s", err, body)
	}
	for i := range resp.Result.Messages {
		attrs := make(map[string]struct {
			Value struct {
				DataType    string `xml:"DataType"`
				StringValue string `xml:"StringValue"`
				BinaryValue string `xml:"BinaryValue"`
			} `xml:"Value"`
		}, len(resp.Result.Messages[i].RawMessageAttributes))
		for _, attr := range resp.Result.Messages[i].RawMessageAttributes {
			attrs[attr.Name] = struct {
				Value struct {
					DataType    string `xml:"DataType"`
					StringValue string `xml:"StringValue"`
					BinaryValue string `xml:"BinaryValue"`
				} `xml:"Value"`
			}{Value: attr.Value}
		}
		resp.Result.Messages[i].MessageAttributes = attrs
	}
	return resp
}

func queryCreateQueueURL(t *testing.T, node Node, name string) string {
	t.Helper()
	_, body := queryOK(t, node, "CreateQueue", url.Values{"QueueName": []string{name}})
	var createResp struct {
		Result struct {
			QueueUrl string `xml:"QueueUrl"`
		} `xml:"CreateQueueResult"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(body), &createResp); err != nil {
		t.Fatalf("decode create: %v\nbody=%s", err, body)
	}
	queueURL := createResp.Result.QueueUrl
	if queueURL == "" {
		t.Fatalf("missing QueueUrl in create response: %s", body)
	}
	return queueURL
}

func querySetVisibilityTimeout(t *testing.T, node Node, queueURL, value string) {
	t.Helper()
	queryOK(t, node, "SetQueueAttributes", url.Values{
		"QueueUrl":          []string{queueURL},
		"Attribute.1.Name":  []string{"VisibilityTimeout"},
		"Attribute.1.Value": []string{value},
	})
}

func queryAttributes(t *testing.T, node Node, queueURL string, names []string) map[string]string {
	t.Helper()
	form := url.Values{"QueueUrl": []string{queueURL}}
	for i, name := range names {
		form.Set("AttributeName."+strconv.Itoa(i+1), name)
	}
	_, body := queryOK(t, node, "GetQueueAttributes", form)
	var attrsResp struct {
		Result struct {
			Attributes []struct {
				Name  string `xml:"Name"`
				Value string `xml:"Value"`
			} `xml:"Attribute"`
		} `xml:"GetQueueAttributesResult"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(body), &attrsResp); err != nil {
		t.Fatalf("decode attrs: %v\nbody=%s", err, body)
	}
	attrs := map[string]string{}
	for _, a := range attrsResp.Result.Attributes {
		attrs[a.Name] = a.Value
	}
	return attrs
}

func queryTagQueue(t *testing.T, node Node, queueURL string, tags map[string]string) {
	t.Helper()
	form := url.Values{"QueueUrl": []string{queueURL}}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		prefix := "Tag." + strconv.Itoa(i+1)
		form.Set(prefix+".Key", k)
		form.Set(prefix+".Value", tags[k])
	}
	queryOK(t, node, "TagQueue", form)
}

func queryTags(t *testing.T, node Node, queueURL string) map[string]string {
	t.Helper()
	_, body := queryOK(t, node, "ListQueueTags", url.Values{"QueueUrl": []string{queueURL}})
	var tagsResp struct {
		Result struct {
			Tags []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Tag"`
		} `xml:"ListQueueTagsResult"`
	}
	if err := xml.Unmarshal(bytes.TrimSpace(body), &tagsResp); err != nil {
		t.Fatalf("decode tags: %v\nbody=%s", err, body)
	}
	tags := map[string]string{}
	for _, tag := range tagsResp.Result.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

func queryUntagQueue(t *testing.T, node Node, queueURL string, tagKeys []string) {
	t.Helper()
	form := url.Values{"QueueUrl": []string{queueURL}}
	for i, k := range tagKeys {
		form.Set("TagKey."+strconv.Itoa(i+1), k)
	}
	queryOK(t, node, "UntagQueue", form)
}

func queryOK(t *testing.T, node Node, action string, form url.Values) (int, []byte) {
	t.Helper()
	status, body := queryRoundTrip(t, node, action, form)
	if status != http.StatusOK {
		t.Fatalf("%s: status %d body %s", action, status, body)
	}
	return status, body
}

func assertJSONQueueDeleted(t *testing.T, node Node, name string) {
	t.Helper()
	jStatus, jOut := callSQS(t, node, sqsGetQueueUrlTarget, map[string]any{
		"QueueName": name,
	})
	if jStatus != http.StatusBadRequest || jOut["__type"] != sqsErrQueueDoesNotExist {
		t.Fatalf("JSON GetQueueUrl after query DeleteQueue: status=%d body=%v", jStatus, jOut)
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

package adapter

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// SQS query-protocol (form-encoded request, XML response) support per
// docs/design/2026_04_26_proposed_sqs_query_protocol.md. Detection
// runs on every inbound SQS request alongside the existing JSON path
// — no separate listener, no flag. The implementation deliberately
// touches as little of the JSON path as possible: the SQS handlers
// have been split into createQueueCore / listQueuesCore /
// getQueueUrlCore (in sqs_catalog.go) so both wrappers reuse the
// exact same business logic.

const (
	// sqsContentTypeQueryURLEncoded is the request Content-Type the
	// older AWS SDKs (aws-sdk-java v1, boto < 1.34, the AWS CLI in
	// query mode) send. Detected case-insensitively against the
	// prefix so charsets like ";charset=UTF-8" do not break the
	// dispatch.
	sqsContentTypeQueryURLEncoded = "application/x-www-form-urlencoded"

	// sqsContentTypeQueryXML is what we emit on every successful
	// query-protocol response. AWS itself sends "text/xml" with a
	// UTF-8 charset; matching that shape keeps SDK XML parsers
	// happy.
	sqsContentTypeQueryXML = "text/xml; charset=utf-8"

	// sqsQueryNamespace pins the XML namespace on every response
	// envelope. Older XML parsers DO validate the namespace; emitting
	// the wrong one for "compat" reasons silently breaks unmarshalling
	// in aws-sdk-java v1.
	sqsQueryNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"

	// sqsQueryRequestIDLen is the AWS-shape RequestId length: 22
	// base32 chars. Base32 emits 1 char per 5 input bits, so 22 chars
	// require ceil(22*5/8)=14 random bytes (110 bits — comfortably
	// more entropy than UUID-v4). The constant is used both to size
	// the random source and to trim the encoded output.
	sqsQueryRequestIDLen = 22
)

// sqsProtocol enumerates the two wire formats the SQS listener now
// accepts. Returned by pickSqsProtocol so the dispatcher can branch
// without re-inspecting the request.
type sqsProtocol int

const (
	sqsProtocolJSON sqsProtocol = iota
	sqsProtocolQuery
	// sqsProtocolUnknown signals a request whose Content-Type does
	// not match either codec. The dispatcher answers with the
	// existing JSON-style 400 envelope (best generic shape — query
	// clients that are mid-misconfiguration usually want to see
	// *some* error, and they understand HTTP status codes).
	sqsProtocolUnknown
)

// pickSqsProtocol decides which wire format a request is using. The
// rules are documented in §3 of the design doc:
//
//   - X-Amz-Target header set + JSON Content-Type (or no body) → JSON.
//   - form-urlencoded body → query.
//   - GET with Action in the query string (legacy ListQueues callers)
//     → query.
//   - everything else → unknown (handled as a JSON-style 400 by the
//     dispatcher so even broken probes get a useful error).
func pickSqsProtocol(r *http.Request) sqsProtocol {
	if r == nil {
		return sqsProtocolUnknown
	}
	if r.Header.Get("X-Amz-Target") != "" {
		return sqsProtocolJSON
	}
	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if strings.HasPrefix(contentType, sqsContentTypeQueryURLEncoded) {
		return sqsProtocolQuery
	}
	if r.Method == http.MethodGet && r.URL != nil && r.URL.Query().Get("Action") != "" {
		return sqsProtocolQuery
	}
	if strings.HasPrefix(contentType, sqsContentTypeJSON) {
		return sqsProtocolJSON
	}
	return sqsProtocolUnknown
}

// handleQuery is the query-protocol entry point invoked by SQSServer.handle
// once pickSqsProtocol returns sqsProtocolQuery. The dispatcher reads
// the form, looks up the Action, and routes to the per-verb handler.
// Errors at any layer are written through writeSQSQueryError so the
// envelope shape stays consistent.
func (s *SQSServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	form, err := readQueryForm(r)
	if err != nil {
		writeSQSQueryError(w, err)
		return
	}
	action := form.Get("Action")
	if action == "" {
		// AWS itself returns 400 MissingAction here. The JSON-style
		// envelope is documented as the right shape because query
		// clients hitting this branch usually have not yet picked a
		// codec; raw HTTP probe tooling typically logs `__type` from
		// JSON more readably than the ErrorResponse XML envelope.
		writeSQSError(w, http.StatusBadRequest, sqsErrMissingParameter, "Action is required")
		return
	}
	switch action {
	case "CreateQueue":
		s.handleQueryCreateQueue(w, r, form)
	case "ListQueues":
		s.handleQueryListQueues(w, r, form)
	case "GetQueueUrl":
		s.handleQueryGetQueueUrl(w, r, form)
	default:
		// Per design §4.1: every wired verb appears here; everything
		// else returns 501 NotImplementedYet so operators see the
		// gap explicitly rather than the request silently failing
		// against the JSON dispatch table.
		writeSQSQueryError(w, newSQSAPIError(
			http.StatusNotImplemented,
			"NotImplementedYet",
			"query-protocol Action "+action+" is not yet wired in elastickv (Phase 3.B follow-up)",
		))
	}
}

// readQueryForm extracts the form values from either the request
// body (POST) or the URL query string (GET / hybrid). The body read
// is bounded by the same sqsMaxRequestBodyBytes the JSON path uses,
// so the query path inherits the JSON path's DoS protection without
// separate plumbing.
func readQueryForm(r *http.Request) (url.Values, error) {
	if r.URL == nil {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrMalformedRequest, "missing request URL")
	}
	// r.URL.Query() returns a fresh map on each call so we can adopt
	// it directly as the base instead of copying entries one-by-one.
	// On GET we are done; on POST the body's form values are merged
	// into the same map.
	values := r.URL.Query()
	if r.Method == http.MethodGet {
		return values, nil
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, sqsMaxRequestBodyBytes))
	if err != nil {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrMalformedRequest, err.Error())
	}
	if len(body) == 0 {
		return values, nil
	}
	parsed, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrMalformedRequest, "malformed form body: "+err.Error())
	}
	for k, vs := range parsed {
		values[k] = append(values[k], vs...)
	}
	return values, nil
}

// ----------- per-verb handlers (CreateQueue / ListQueues / GetQueueUrl) -----------

func (s *SQSServer) handleQueryCreateQueue(w http.ResponseWriter, r *http.Request, form url.Values) {
	in, err := parseQueryCreateQueue(form)
	if err != nil {
		writeSQSQueryError(w, err)
		return
	}
	queueName, err := s.createQueueCore(r.Context(), in)
	if err != nil {
		writeSQSQueryError(w, err)
		return
	}
	writeSQSQueryResponse(w, "CreateQueue", queryCreateQueueResult{
		QueueUrl: s.queueURL(r, queueName),
	})
}

func (s *SQSServer) handleQueryListQueues(w http.ResponseWriter, r *http.Request, form url.Values) {
	in := parseQueryListQueues(form)
	page, nextToken, err := s.listQueuesCore(r.Context(), in)
	if err != nil {
		writeSQSQueryError(w, err)
		return
	}
	urls := make([]string, 0, len(page))
	for _, n := range page {
		urls = append(urls, s.queueURL(r, n))
	}
	writeSQSQueryResponse(w, "ListQueues", queryListQueuesResult{
		QueueUrl:  urls,
		NextToken: nextToken,
	})
}

func (s *SQSServer) handleQueryGetQueueUrl(w http.ResponseWriter, r *http.Request, form url.Values) {
	in := parseQueryGetQueueUrl(form)
	queueName, err := s.getQueueUrlCore(r.Context(), in)
	if err != nil {
		writeSQSQueryError(w, err)
		return
	}
	writeSQSQueryResponse(w, "GetQueueUrl", queryGetQueueUrlResult{
		QueueUrl: s.queueURL(r, queueName),
	})
}

// ----------- form parsers -----------

func parseQueryCreateQueue(form url.Values) (*sqsCreateQueueInput, error) {
	in := &sqsCreateQueueInput{
		QueueName: strings.TrimSpace(form.Get("QueueName")),
	}
	if in.QueueName == "" {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrMissingParameter, "QueueName is required")
	}
	in.Attributes = collectIndexedKVPairs(form, "Attribute", "Name")
	// Tags use Tag.N.Key / Tag.N.Value (NOT Tag.N.Name). The AWS
	// SQS query reference is explicit on this and CodexP1 / Gemini
	// both flagged the previous .Name-only parser as a silent
	// tag-loss bug. Pass the suffix in so each caller picks the AWS
	// vocabulary for that resource.
	in.Tags = collectIndexedKVPairs(form, "Tag", "Key")
	return in, nil
}

func parseQueryListQueues(form url.Values) *sqsListQueuesInput {
	in := &sqsListQueuesInput{
		QueueNamePrefix: form.Get("QueueNamePrefix"),
		NextToken:       form.Get("NextToken"),
	}
	if v := form.Get("MaxResults"); v != "" {
		// AWS docs say MaxResults must be 1–1000. clampListQueuesMaxResults
		// already enforces the range; leave parsing forgiving here so
		// a non-integer (e.g. an empty value some SDKs send) does not
		// poison the request — just falls through to default.
		if n, err := strconv.Atoi(v); err == nil {
			in.MaxResults = n
		}
	}
	return in
}

func parseQueryGetQueueUrl(form url.Values) *sqsGetQueueUrlInput {
	return &sqsGetQueueUrlInput{
		QueueName: strings.TrimSpace(form.Get("QueueName")),
	}
}

// collectIndexedKVPairs reads AWS-style indexed pairs of the form
//
//	<prefix>.1.<keyField> = key1
//	<prefix>.1.Value      = value1
//	<prefix>.2.<keyField> = key2
//	<prefix>.2.Value      = value2
//
// and returns them as a map. The keyField suffix is "Name" for
// Attributes and "Key" for Tags per the AWS SQS query reference;
// callers pass the right one. Pairs missing either side are dropped
// silently (AWS does the same — the validator in the core handler
// reports the actual problem).
//
// Iteration order: pairs are processed in ascending integer index
// order (so two clients sending the same parameters in different
// HTTP body orders see identical maps). When two distinct entries
// resolve to the *same* key (e.g. both Attribute.1.Name and
// Attribute.2.Name set to "VisibilityTimeout"), the lower index
// wins — AWS rejects this case as InvalidParameterValue, but our
// validator at the next layer is the right place for that, not
// this codec; deterministic last-write-wins-by-index is enough to
// make tests stable. (CodexP2 + Gemini high.)
func collectIndexedKVPairs(form url.Values, prefix, keyField string) map[string]string {
	if len(form) == 0 {
		return nil
	}
	pairs := gatherIndexedKVPairs(form, prefix+".", "."+keyField)
	if len(pairs) == 0 {
		return nil
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].idx < pairs[j].idx })
	out := make(map[string]string, len(pairs))
	for _, p := range pairs {
		// Lower-index wins on duplicates so iteration order does not
		// affect the result; map insertion overwrite would otherwise
		// resurface the original non-determinism.
		if _, taken := out[p.mapKey]; !taken {
			out[p.mapKey] = p.mapVal
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// indexedKVPair is an intermediate (idx, key, value) triple used to
// sort the form's indexed entries before flattening into the final
// map. Kept as a private struct so collectIndexedKVPairs and the
// gather helper share the exact same shape.
type indexedKVPair struct {
	idx    int
	mapKey string
	mapVal string
}

// gatherIndexedKVPairs walks the form once and emits every well-formed
// (idx, key, value) triple matching the wantPrefix / keySuffix shape.
// Pulled out of collectIndexedKVPairs to keep that function under
// cyclop=10 — the inner loop has too many guards to share a single
// scope. Returns the slice in the order Go's map iteration produced;
// the caller sorts.
func gatherIndexedKVPairs(form url.Values, wantPrefix, keySuffix string) []indexedKVPair {
	pairs := make([]indexedKVPair, 0)
	for k, vs := range form {
		idx, ok := indexedPairKeyToIdx(k, wantPrefix, keySuffix)
		if !ok {
			continue
		}
		if len(vs) == 0 || vs[0] == "" {
			continue
		}
		valueKey := strings.TrimSuffix(k, keySuffix) + ".Value"
		valueVs, found := form[valueKey]
		if !found || len(valueVs) == 0 {
			continue
		}
		pairs = append(pairs, indexedKVPair{idx: idx, mapKey: vs[0], mapVal: valueVs[0]})
	}
	return pairs
}

// indexedPairKeyToIdx parses "<wantPrefix><N><keySuffix>" and returns
// (N, true). Non-matching shapes return (_, false). Non-integer
// "<N>" segments (e.g. "Attribute.foo.Name") fall outside the AWS
// contract and return false rather than guess.
func indexedPairKeyToIdx(key, wantPrefix, keySuffix string) (int, bool) {
	if !strings.HasPrefix(key, wantPrefix) || !strings.HasSuffix(key, keySuffix) {
		return 0, false
	}
	idxStr := strings.TrimSuffix(strings.TrimPrefix(key, wantPrefix), keySuffix)
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return 0, false
	}
	return idx, true
}

// ----------- response shapes -----------

// queryCreateQueueResult is the inner <CreateQueueResult> body. The
// outer <CreateQueueResponse> wrapper + ResponseMetadata is added by
// writeSQSQueryResponse so every verb shares the same envelope code.
type queryCreateQueueResult struct {
	XMLName  xml.Name `xml:"CreateQueueResult"`
	QueueUrl string   `xml:"QueueUrl"`
}

type queryListQueuesResult struct {
	XMLName   xml.Name `xml:"ListQueuesResult"`
	QueueUrl  []string `xml:"QueueUrl"`
	NextToken string   `xml:"NextToken,omitempty"`
}

type queryGetQueueUrlResult struct {
	XMLName  xml.Name `xml:"GetQueueUrlResult"`
	QueueUrl string   `xml:"QueueUrl"`
}

// queryResponseEnvelope wraps the per-verb result in the AWS
// outer-envelope shape:
//
//	<{Action}Response xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
//	  <{Action}Result>...</{Action}Result>
//	  <ResponseMetadata><RequestId>...</RequestId></ResponseMetadata>
//	</{Action}Response>
//
// XMLName is set per-call so the same struct serves every verb.
type queryResponseEnvelope struct {
	XMLName  xml.Name
	XMLNS    string                `xml:"xmlns,attr"`
	Result   any                   `xml:",any"`
	Metadata queryResponseMetadata `xml:"ResponseMetadata"`
}

type queryResponseMetadata struct {
	RequestId string `xml:"RequestId"`
}

// writeSQSQueryResponse emits a 200 XML envelope. The action drives
// the outer element name; result is marshalled as the embedded
// Result element. RequestId is generated server-side; clients echo
// it back in support requests so the access log line carries the
// same value (logged in the access path which sees this writer).
func writeSQSQueryResponse(w http.ResponseWriter, action string, result any) {
	env := queryResponseEnvelope{
		XMLName:  xml.Name{Local: action + "Response"},
		XMLNS:    sqsQueryNamespace,
		Result:   result,
		Metadata: queryResponseMetadata{RequestId: newQueryRequestID()},
	}
	body, err := xml.MarshalIndent(env, "", "  ")
	if err != nil {
		// xml.MarshalIndent fails only on programming errors (cyclic
		// types, unsupported tags). Fall back to a best-effort 500;
		// the operator log gives the actual reason.
		writeSQSQueryError(w, errors.Wrap(err, "marshal query-protocol response"))
		return
	}
	w.Header().Set("Content-Type", sqsContentTypeQueryXML)
	w.Header().Set("x-amzn-RequestId", env.Metadata.RequestId)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

// writeSQSQueryError emits the AWS query-protocol error envelope:
//
//	<ErrorResponse xmlns="...">
//	  <Error>
//	    <Type>Sender|Receiver</Type>
//	    <Code>...</Code>
//	    <Message>...</Message>
//	  </Error>
//	  <RequestId>...</RequestId>
//	</ErrorResponse>
//
// HTTP status mirrors what the JSON path would have returned for the
// same sqsAPIError; SDK retry classifiers key off both <Code> and
// HTTP status, so keeping them aligned across protocols means a
// retry policy that works for the JSON client also works for the
// query client.
func writeSQSQueryError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := sqsErrInternalFailure
	message := "internal error"
	var apiErr *sqsAPIError
	if errors.As(err, &apiErr) {
		status = apiErr.status
		if apiErr.errorType != "" {
			code = apiErr.errorType
		}
		if apiErr.message != "" {
			message = apiErr.message
		}
	}
	env := queryErrorEnvelope{
		XMLName:   xml.Name{Local: "ErrorResponse"},
		XMLNS:     sqsQueryNamespace,
		Error:     queryErrorBody{Type: errorTypeForStatus(status), Code: code, Message: message},
		RequestId: newQueryRequestID(),
	}
	body, marshalErr := xml.MarshalIndent(env, "", "  ")
	if marshalErr != nil {
		// Truly unreachable for our shape; fall back to plain text
		// so the operator at least sees something useful.
		http.Error(w, code+": "+message, status)
		return
	}
	w.Header().Set("Content-Type", sqsContentTypeQueryXML)
	w.Header().Set("x-amzn-RequestId", env.RequestId)
	if code != "" {
		w.Header().Set("x-amzn-ErrorType", code)
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

type queryErrorEnvelope struct {
	XMLName   xml.Name       `xml:"ErrorResponse"`
	XMLNS     string         `xml:"xmlns,attr"`
	Error     queryErrorBody `xml:"Error"`
	RequestId string         `xml:"RequestId"`
}

type queryErrorBody struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// errorTypeForStatus maps an HTTP status to the AWS error
// classification AWS itself reports in <Type>: 4xx becomes Sender
// (the client did something wrong); everything else is Receiver
// (the server failed). Matches what aws-sdk-java v1 expects when it
// decides whether to retry.
func errorTypeForStatus(status int) string {
	if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
		return "Sender"
	}
	return "Receiver"
}

// newQueryRequestID returns a fresh per-response identifier shaped
// like the AWS RequestId: 22 chars of base32 (no padding). Base32
// emits 1 char per 5 input bits, so 22 chars require ceil(22*5/8)=14
// random bytes (110 bits, comfortably more entropy than a UUID-v4).
// A 16-byte source would produce a 26-char ID, not 22, so the byte
// count is pinned at 14 to match the documented length.
func newQueryRequestID() string {
	var raw [14]byte
	if _, err := rand.Read(raw[:]); err != nil {
		// crypto/rand.Read does not fail on supported platforms;
		// returning a constant on the unreachable error keeps the
		// signature error-free without hiding the symptom (operators
		// will notice every RequestId being identical).
		return strings.Repeat("0", sqsQueryRequestIDLen)
	}
	// Base32 of 14 bytes is 23 raw chars in the no-padding form.
	// Trim to the documented sqsQueryRequestIDLen so the ID is the
	// exact AWS shape.
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:])
	if len(encoded) > sqsQueryRequestIDLen {
		encoded = encoded[:sqsQueryRequestIDLen]
	}
	return encoded
}

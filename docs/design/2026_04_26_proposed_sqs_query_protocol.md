# SQS Query-Protocol Wire Format Support

**Status:** Proposed
**Author:** bootjp
**Date:** 2026-04-26

---

## 1. Background and Motivation

The elastickv SQS adapter currently speaks only the **AWS JSON 1.0 protocol**: `Content-Type: application/x-www-form-urlencoded` is rejected, the request must carry `X-Amz-Target: AmazonSQS.<Action>`, and the body is JSON. This matches what the modern AWS SDK v2 family (`aws-sdk-go-v2`, `boto3 ≥ 1.34`, `aws-sdk-java-v2`) emits.

A long tail of clients still emits the older **query protocol** (form-encoded request, XML response) — `aws-sdk-java` v1, older `boto`/`boto3 < 1.34`, every CLI tool that builds requests by hand, and the AWS CLI itself when used against a region that defaults to query. Today these clients fail with `400 MalformedRequest` or `415 UnsupportedMediaType` on the very first request, even though the underlying SQS feature is fully implemented.

Adding query-protocol support is the last piece needed to claim "drop-in SQS compatibility" for v1-era SDKs. Phase 3.B in [`docs/design/2026_04_24_partial_sqs_compatible_adapter.md`](2026_04_24_partial_sqs_compatible_adapter.md) §16.4 marked this as TODO; this document is the proposal that unblocks the implementation.

---

## 2. Goals and Non-Goals

### 2.1 Goals

1. Accept query-protocol requests on the **same SQS listener** that already serves JSON. Detection is based on the request shape, not a separate port.
2. Reuse every existing handler. The wire codec is the only new code; no SQS business logic moves or duplicates.
3. Emit XML responses that AWS SDK v1 / older boto unmarshal without modification.
4. Preserve the existing JSON-protocol behaviour bit-for-bit. No regression test on the JSON path may change.
5. Keep the doc-driven coverage explicit: the first PR ships a subset of verbs; later PRs widen it without further design work.

### 2.2 Non-Goals

1. **Server-side XML schema validation** of unsupported fields. AWS itself silently ignores unknown query-string keys; we mirror that and rely on per-handler validation that is already in place.
2. **EC2 query-protocol fidelity** for non-SQS services. This proposal touches only the SQS adapter.
3. **HTTP/2 negotiation tweaks**. The query protocol works over plain HTTP/1.1 just like JSON.
4. **Streaming responses**. SQS responses are bounded; chunked encoding is not needed.
5. Adding query-protocol support to S3 or DynamoDB. Those adapters have their own protocol semantics and are out of scope.

---

## 3. Detection

The dispatcher in `adapter/sqs.go` (`SQSServer.ServeHTTP`) decides which protocol to invoke per request, with no flag, no header, and no per-listener configuration. The decision is made from request-side signals only:

| Signal | Protocol |
|---|---|
| `X-Amz-Target` header is set **and** `Content-Type` starts with `application/x-amz-json-1.0` | JSON (existing path, unchanged) |
| `Content-Type` starts with `application/x-www-form-urlencoded` **and** the request carries a non-empty `Action` form field | Query (new path) |
| Anything else | `400 MissingAction` (existing JSON path's error envelope, since it is the most informative when the client can't even pick a protocol) |

Detection lives in a small `pickSqsProtocol(*http.Request) sqsProtocol` helper so unit tests can pin every edge case (mixed headers, missing `Action`, query-protocol POST with empty body, GET-with-query-string fallthrough). The two protocol branches share zero code beyond that switch.

Edge cases the detector accepts as **query-protocol**:

- `GET` with `Action` in the query string (some old clients still emit this for `ListQueues`).
- `POST` with `Action` either in the body or in the query string. AWS lets either side carry the `Action` parameter; we accept both.

Edge cases the detector **rejects as JSON-protocol Errors** (mirroring AWS's behaviour):

- `Content-Type: application/x-www-form-urlencoded` but no `Action` field anywhere → JSON-style 400 `{"__type":"MissingAction","message":"Action is required"}`. Returning XML here would force every probe / health-checker that doesn't know either protocol to learn both.

---

## 4. Internal Handler Shape

Today, each handler in `adapter/sqs_messages.go` / `adapter/sqs_catalog.go` is shaped:

```go
func (s *SQSServer) sendMessage(w http.ResponseWriter, r *http.Request)
```

— it owns request parsing **and** response writing, so the JSON wire format is hard-coded into every handler. To let the query protocol reuse the same logic without duplicating either the SQS algorithm or the SigV4 path, we factor each verb into three layers:

```
JSON wrapper (decode JSON / write JSON)   ─┐
                                            ├──► sqsHandlerCore: (in T) → (out U, error)
Query wrapper (decode form / write XML)   ─┘
```

Where `sqsHandlerCore` is **the existing handler body, minus the codec calls**. Concretely, for `SendMessage`:

```go
// adapter/sqs_messages.go (existing pattern, refactored)
func (s *SQSServer) sendMessage(w http.ResponseWriter, r *http.Request) {
    var in sqsSendMessageInput
    if err := decodeSQSJSONInput(r, &in); err != nil {
        writeSQSErrorFromErr(w, err)
        return
    }
    out, err := s.sendMessageCore(r.Context(), &in)
    if err != nil {
        writeSQSErrorFromErr(w, err)
        return
    }
    writeSQSJSON(w, out)
}

// New SigV4-and-codec-free worker. Already extractable for every existing
// verb because the per-handler logic is the body of the current function
// minus the first decode and final write.
func (s *SQSServer) sendMessageCore(ctx context.Context, in *sqsSendMessageInput) (*sqsSendMessageOutput, error)
```

Query path:

```go
// adapter/sqs_query_protocol.go (new)
func (s *SQSServer) handleQuerySendMessage(w http.ResponseWriter, r *http.Request, form url.Values) {
    in, err := parseQuerySendMessage(form)
    if err != nil {
        writeSQSQueryError(w, err)
        return
    }
    out, err := s.sendMessageCore(r.Context(), in)
    if err != nil {
        writeSQSQueryError(w, err)
        return
    }
    writeSQSQueryResponse(w, "SendMessage", out)
}
```

This refactor is mechanical and trivially reviewable: the JSON wrapper before the change is identical to the JSON wrapper after the change, except `decodeSQSJSONInput` and the in-place body have been split. Existing tests cover every verb's JSON path and pass unchanged.

### 4.1 Verb coverage in the first PR

The first PR is **architectural proof** — it ships dispatch, decoding, encoding, error envelope, and the refactor pattern, with **three verbs** wired end-to-end as concrete proof. The pattern then extends mechanically to every other verb in follow-up PRs (each follow-up adds a parser + response struct + one line in the dispatch table).

| Verb | Why it's in the proof set |
|---|---|
| `CreateQueue` | Simplest write verb: takes `QueueName` + optional `Attribute.N`, returns `QueueUrl`. Exercises the indexed-collection parser for `Attribute.N.Name`/`Attribute.N.Value`. |
| `ListQueues` | Read-only verb. Exercises the repeated-element XML shape (`<QueueUrl>...</QueueUrl>` repeated under `<ListQueuesResult>`) which is harder than the typical leaf-element response. |
| `GetQueueUrl` | Trivial round-trip verb. Pins that single-leaf XML response shape (`<GetQueueUrlResult><QueueUrl>...</QueueUrl></GetQueueUrlResult>`) and the `QueueDoesNotExist` error envelope path. |

`SendMessage` / `ReceiveMessage` / `DeleteMessage` are the highest-priority follow-ups; they need the `*Core` refactor to also reach into the FIFO send loop (`sqs_messages.go: sendMessageFifoLoop`), which is mechanical but bigger than this proof PR should swallow.

Verbs **not** in the first round (recorded as TODO in the PR description and in §16.4 of the partial doc):

- `DeleteQueue`, `GetQueueUrl`, `GetQueueAttributes`, `SetQueueAttributes`, `PurgeQueue` — single-call extensions; each is one parser + one response shape.
- `ReceiveMessage` / `DeleteMessage` / `ChangeMessageVisibility` — the in-flight message lifecycle. Each non-trivial because of `Attribute.N` plumbing on responses.
- `SendMessageBatch` / `DeleteMessageBatch` / `ChangeMessageVisibilityBatch` — query-protocol batch encoding has its own quirks (`SendMessageBatchRequestEntry.1.MessageBody=...`); deserves its own focused PR.
- `TagQueue`, `UntagQueue`, `ListQueueTags`, DLQ redrive control-plane verbs — small additions, easy to land incrementally.

The `pickSqsAction` switch returns a **501 `NotImplementedYet`** for any query-protocol Action that has not been wired yet, with an XML envelope that names the missing action. Operators see the gap explicitly rather than silently falling through to JSON-style errors. As verbs land, their entries move from the "TODO" branch to the live dispatch table — no other code changes per added verb.

---

## 5. Query-Protocol Decoding

Form parsing uses `net/url.ParseQuery` after `io.ReadAll` on the request body (capped at the existing `sqsMaxRequestBodyBytes` so the query path inherits the JSON path's DoS protection without separate plumbing). Each verb has a dedicated parser that walks the parsed `url.Values` and produces the *same* internal input struct the JSON path already uses — the parsers are the only protocol-specific code per verb.

AWS-style numeric collection encoding (`AttributeName.1=...`, `AttributeName.2=...`) is handled by a single `collectIndexedValues(form url.Values, prefix string) []string` helper that strips the dotted suffix, sorts by the integer index, and returns the values in order. All multi-value parameters (`AttributeNames`, `MessageAttribute.N.Name`, …) go through this helper, so the indexed-collection parsing logic exists once.

`MessageAttribute.N.Name` / `MessageAttribute.N.Value.DataType` / etc. is the only nested case; the code lives in `parseMessageAttributesQuery` and produces the same `[]sqsMessageAttribute` slice the JSON path consumes. No SQS handler sees the difference.

---

## 6. Query-Protocol Encoding (XML)

Response XML follows the AWS SQS QueryProtocol envelope per verb:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<SendMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <SendMessageResult>
    <MessageId>...</MessageId>
    <MD5OfMessageBody>...</MD5OfMessageBody>
  </SendMessageResult>
  <ResponseMetadata>
    <RequestId>...</RequestId>
  </ResponseMetadata>
</SendMessageResponse>
```

`encoding/xml` marshals every response struct directly. The wrapper `writeSQSQueryResponse(w, action, payload)` constructs the action-specific outer envelope (`<{Action}Response>` + `<{Action}Result>`) and the `<ResponseMetadata>` block, then streams the marshalled payload. Per-verb response struct definitions live in `adapter/sqs_query_responses.go` and use struct tags so the XML schema is grep-able.

`RequestId` is generated server-side: a 22-character base32 of 16 random bytes. The same value is logged in the access-log line so operator support requests can be cross-referenced.

### 6.1 Error envelope

Errors use the AWS QueryProtocol error format:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <Error>
    <Type>Sender</Type>
    <Code>QueueAlreadyExists</Code>
    <Message>A queue with that name already exists.</Message>
  </Error>
  <RequestId>...</RequestId>
</ErrorResponse>
```

`<Type>` is `Sender` for 4xx and `Receiver` for 5xx. The `<Code>` field reuses the existing `sqsErr*` constants (`QueueDoesNotExist`, `InvalidParameterValue`, …) — they are *already* AWS-compatible because the JSON path uses the same vocabulary.

The status code is set to the same value the JSON path returns, so client-side retry classifiers (which key off both `<Code>` and HTTP status) behave identically across protocols.

---

## 7. Authentication and SigV4

The query protocol uses the same SigV4 signature the JSON protocol does — the signed canonical request includes the form-encoded body, so SigV4 verification works against either codec without changes. The existing SigV4 middleware (`sigv4.go`) sees an `http.Request`; it does not care about the body schema.

Query-protocol clients sometimes send `Authorization: AWS4-HMAC-SHA256 Credential=...` and sometimes pass `X-Amz-Algorithm` / `X-Amz-Signature` as form parameters (presigned-URL style). The SigV4 path already accepts both shapes; the query-protocol dispatcher does not need its own auth handling.

---

## 8. Configuration

No new flags. The query protocol is enabled by being detected; deployments that want **only** JSON can set `--sqsRejectQueryProtocol` (Phase 3 follow-up flag, not in the first PR) which will short-circuit the detection.

The conservative default (accept both protocols) matches AWS itself: even regions that have switched their *default* SDK protocol still accept query for backward compatibility.

---

## 9. Testing Strategy

1. **Golden-file XML tests** (`adapter/sqs_query_protocol_test.go`):
   - For each wired verb, build a typical SDK v1 request as `url.Values`, send it through the in-process listener, and assert the XML response byte-for-byte against a stored golden file under `adapter/testdata/sqs_query/<Verb>.xml`.
   - The golden files are *exactly* what `aws-sdk-java` v1 unmarshals; updating them is a deliberate review event.

2. **Round-trip parity** (`adapter/sqs_query_protocol_parity_test.go`):
   - For each wired verb, perform the same logical operation through both the JSON and query protocols (e.g. `SendMessage` with body `"hello"` and `MessageGroupId=g1` on a FIFO queue).
   - Read back via `ReceiveMessage` on whichever protocol opposes the send protocol. Confirm body, MD5, attributes, and any sequencing fields match across the two paths.

3. **Detection edge cases** (`adapter/sqs_dispatch_test.go`):
   - `Content-Type: application/x-www-form-urlencoded` + missing `Action` → JSON-style 400 `MissingAction`.
   - `X-Amz-Target` set + form-encoded body → JSON path (the header wins).
   - GET with `Action=ListQueues` in the query string → query path.
   - Body over `sqsMaxRequestBodyBytes` → 413 from the existing limit, regardless of protocol.

4. **SigV4 fixture test**: take a known-good `aws-sdk-java` v1 request capture (saved under `testdata/`), feed it through the listener with the matching credentials, assert the signature verifies and the call succeeds. Pins that the SigV4 canonical-request derivation matches the query-protocol body encoding.

5. **Lint**: extend `.golangci.yaml` exemptions only if the XML envelopes trip cyclomatic complexity (they shouldn't — each verb's encoder is a flat struct definition).

---

## 10. Compatibility and Rollout

The protocol is purely additive. Existing JSON clients continue to hit the JSON path because their `Content-Type` differs. No flag default changes. No migration step is required.

Deployments that *want* to refuse query-protocol traffic (e.g. lock down to v2-SDK-only clients) can land the `--sqsRejectQueryProtocol` flag in a follow-up PR without affecting the default behaviour.

Rollout sequence:

1. This PR — implementation + tests for the first verb subset (§4.1).
2. Follow-up PR — batch verbs + tag verbs.
3. Follow-up PR — DLQ redrive admin / FIFO administrative verbs.
4. Eventually, when the `_partial_` doc's TODO list is empty, the SQS design doc transitions to `_implemented_`.

---

## 11. Alternatives Considered

### 11.1 Separate listener per protocol

Run JSON on the existing port and query on a new `--sqsQueryAddress`. Rejected because:

- Operators have to manage two ports + two TLS configs + two firewall rules.
- AWS's own behaviour is single-port multi-protocol.
- SigV4's canonical request is invariant across protocols, so no security boundary is gained.

### 11.2 Synthetic JSON re-dispatch

Translate the query request into a JSON request, hand it back to the existing pipeline, then transcode the JSON response to XML. Rejected because:

- Every verb pays the JSON marshal + unmarshal cost twice.
- Error mapping becomes stringly-typed (the JSON path returns `__type` strings; we'd have to parse them out for the XML envelope).
- Future verb-specific differences between protocols become tangled (AWS does sometimes diverge — the JSON path returns `Attributes` as `map<string,string>` while query returns `<Attribute><Name>...</Name><Value>...</Value></Attribute>` repeated).

### 11.3 Use a third-party library (`aws/smithy-go` etc.)

Rejected because the smithy generators are aimed at *clients*, and pulling in the dependency for a few hundred lines of XML scaffolding would be a net negative for binary size and supply-chain surface.

---

## 11.4 Known limitation — leader-proxy error envelope

`proxyToLeader` falls through to `sqsLeaderProxyErrorWriter`, which today always emits the JSON error envelope. A query-protocol client whose request lands on a follower during a leader flip will therefore see one JSON-shaped error before the next request lands on the new leader. This is acceptable because:

1. The window is short (one or two requests at most).
2. SDK retry classifiers key off HTTP status before body shape.
3. AWS itself sometimes returns JSON-shaped errors for query-protocol clients during regional failovers (observed in incident reports).

A follow-up PR threads the detected protocol onto the request context so the proxy error writer can emit the matching XML envelope. Recorded in the partial doc's §16.4 follow-ups list.

---

## 12. Open Questions

1. **Do we need to honour the `Version=2012-11-05` form parameter** to gate verbs that AWS retired? Likely no — clients always send the same version string, and the verb set we implement is stable across SQS API versions. Defer.
2. **Should `RequestId` be sourced from the existing distributed trace context** (W3C `traceparent` header) when available? Operators might appreciate the linkage. Out of scope for the first PR but a low-cost follow-up.
3. **Should the XML responses preserve `xmlns="http://queue.amazonaws.com/doc/2012-11-05/"`** even when no client tooling actually validates the namespace? Yes for compatibility — older XML parsers do enforce it; the cost is one literal string per response.

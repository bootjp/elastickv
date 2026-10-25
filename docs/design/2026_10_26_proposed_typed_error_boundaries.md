# Typed errors at data and protocol seams

Status: Proposed
Author: bootjp
Date: 2026-10-26

## Problem

The storage engine needs physical keys to diagnose conflicts and locks.
Protocol clients need a stable failure category and a truthful account of whether a mutation may have committed.
These are different data contracts.

On `main` at `9d8bbec8`, `store.WriteConflictError` and `kv.TxnLockedError` carry physical keys and include them in `Error()`.
Follower forwarding flattens errors into gRPC status messages.
Redis, DynamoDB, and filesystem paths then inspect those messages to recover error categories or display keys.
The resulting classification depends on wording, accepts key bytes as diagnostic text, and has no structured way to distinguish a definitive rejection from an unknown outcome after submission.

| Current seam | Evidence on `main` | Gap |
| --- | --- | --- |
| Storage cause | [`store/store.go`](../../store/store.go), [`kv/txn_errors.go`](../../kv/txn_errors.go) | Typed errors expose physical keys through `Error()`. |
| Follower forwarding | [`adapter/internal.go`](../../adapter/internal.go), [`kv/leader_proxy.go`](../../kv/leader_proxy.go) | A local cause becomes a status message; the follower infers rejection from text. |
| Public projection | [`adapter/redis_retry.go`](../../adapter/redis_retry.go), [`adapter/dynamodb_write_error.go`](../../adapter/dynamodb_write_error.go), [`internal/filesystem/write_conflict.go`](../../internal/filesystem/write_conflict.go) | Each caller reconstructs type or key from a different message shape. |
| Other status paths | [`adapter/route_write_fence.go`](../../adapter/route_write_fence.go), [`adapter/redis.go`](../../adapter/redis.go) | Route and leadership classification still have text fallbacks. |

This proposal defines the seams between storage, transaction coordination, internal forwarding, and each public protocol.
It also defines how errors cross those seams.
It does not change the behavior of any running server in this documentation PR.
PR #1337 contains a narrower conflict and lock correction; its code is not the completed implementation of this proposal.

## Decisions

1. A physical storage key belongs to the storage and coordination modules.
   It may be retained as a private field of a typed local error for an authorized internal diagnostic, but it does not enter an error message, a wire error detail, or a public protocol response.
2. A logical client key belongs to the adapter that accepted it.
   The adapter encodes it into a physical key for storage.
   An intentional data result, such as a key enumeration operation, uses a separately specified decoder for that result.
   An error is never decoded back into a logical key.
3. Internal error classification uses a closed reason type and `errors.Is` or `errors.As` over the local error chain.
   `Error()` is diagnostic prose and is not a source of control-flow information.
4. Transaction outcome is independent of error reason.
   A conflict, lock, route change, or leadership loss does not by itself prove whether an already submitted transaction committed.
5. The internal forwarding seam transmits a typed decision detail containing only the schema version, reason, and proof class.
   The gRPC status code and its message are presentation fields; neither is sufficient evidence for a retry.
6. Each public adapter maps a validated internal decision to its own protocol response.
   The public response contains no physical key, transaction lock owner, route bound, or raw wrapped internal error.

## Data ownership across seams

The data path narrows as it approaches a client:

```text
client request key
    -> adapter-owned logical key
    -> encoded physical key in kv/store
    -> typed local cause with private physical key
    -> coordinator decision without physical key
    -> authenticated internal forwarding detail without physical key
    -> adapter-owned public error code and fixed message
```

`store` and the KV FSM may inspect physical bytes because they implement MVCC, OCC, locks, and routes.
`kv` may inspect the key while making a transaction decision.
The forwarder needs the decision, not the key that led to it.
Redis, DynamoDB, S3, SQS, and FUSE need their own public semantics, not the storage encoding.

The Go implementation should make the distinction visible through private fields and named key types at new seams.
An `InternalKey` value must not be assignable to a `LogicalKey` without an explicit encoder or decoder.
This is a migration target, not a claim that all current `[]byte` parameters can be replaced in one change.
A bare Go type conversion is not an approved mapping between the two key domains.
An exported accessor such as `store.WriteConflictKey` remains an internal diagnostic capability during migration; protocol adapters must not call it to construct replies.

The RawKV interface deliberately accepts raw keys as data.
This proposal governs errors from RawKV as it does errors from other protocols, but does not redefine which raw-key reads or scans RawKV authorizes.
Reserved-key access and successful scan results require their own access-control review before claiming that no internal key can reach an external client by any path.

## Error model inside a process

The storage layer reports a **cause** as a typed error.
For example, a conflict error may contain a private physical key and unwrap to `store.ErrWriteConflict`.
Its `Error()` returns a fixed category message.
Internal structured logs may attach a bounded key family, shard ID, and request correlation ID under the repository's logging policy; a raw key is not implicitly logged by formatting the error.

The coordinator reports a separate **decision class** after considering admission, request phase, proposal state, and durable evidence.
The classes are:

| Class | Meaning | Retry consequence |
| --- | --- | --- |
| Rejected before submission | The receiving leader did not submit the mutation. | A new attempt may be made after routing or leadership refresh. |
| Definitively aborted | The attempted transaction is proven not to have committed. | Retry is permitted only under the operation's own replay and ordering contract. |
| Outcome unknown | The request may have been submitted or committed, and no authoritative terminal result is available. | Do not replay a non-idempotent effect; preserve identity and resolve, or surface an unknown outcome. |

A successful commit has a response carrying its result and, where required, a durable receipt.
It is not represented as an error class.
Cancellation after possible submission is not proof of abort.
A local timeout, a dropped response, or a new leader's failure to find a result is also not proof of abort.
For a multi-group transaction, one participant's rejection proves only that participant's result.
The coordinator may report the whole transaction as definitively aborted only after it has ruled out a committed primary decision and resolved prepared participants according to the transaction protocol.

The type returned to a local caller should preserve both axes without exposing physical bytes through `Error()`:

```go
type Failure struct {
    Reason   Reason
    Decision DecisionClass
    // Private diagnostic context, unavailable to protocol encoders.
}
```

This sketch specifies semantics, not the exact Go API.
The implementation may use sentinels plus a typed decision wrapper if that keeps existing `errors.Is` behavior intact.
It must not encode the decision by appending words to a message.

## Internal forwarding contract

`Internal.Forward` is the only producer of a definitive forwarding rejection for its requests.
It constructs the wire decision from its own verified admission and transaction result.
It must not pass through a nested gRPC status supplied by another module as if that status were its own proof.

A dedicated protobuf status detail should carry a schema version, a closed reason enum, and a closed decision-class enum.
The receiver compares the detail with the request phase and accepts it only from the authenticated internal Forward call to the expected peer.
The detail has no key, key encoding, lock text, route bounds, or arbitrary message field.
The status message is a fixed safe phrase selected from the reason enum.
The status code is checked for consistency with the detail, but the code alone never establishes the decision.
Enum numbers are never reused; an added reason receives a new number and an explicit decision mapping.

| Reason | Permitted decision | Additional check |
| --- | --- | --- |
| Write conflict or transaction lock | Definitively aborted, or outcome unknown while another participant remains unresolved | A one-phase rejection is terminal; a multi-group PREPARE rejection needs coordinator resolution. COMMIT and ABORT phases cannot inherit a prewrite proof for the whole transaction. |
| Route fenced or ownership changed | Rejected before submission or definitively aborted | Confirm which phase was reached and that no earlier participant decision remains unresolved. |
| Not leader before admission | Rejected before submission | The handler must generate this before timestamp admission or proposal. |
| Leadership lost, deadline, transport failure, missing participant receipt | Outcome unknown | Resolve through durable operation or transaction evidence before any replay. |

The same cause can require a different decision when the request reaches a different phase:

| Observed sequence | Decision exposed by Forward | Reason |
| --- | --- | --- |
| Leader refuses the request before timestamp admission | Rejected before submission | No mutation was proposed. |
| One-phase apply returns a known OCC rejection | Definitively aborted | The leader has the terminal apply result for this attempt. |
| PREPARE rejects while another group may hold an intent | Outcome unknown until coordinator resolution | A participant rejection does not by itself establish the whole transaction's terminal decision. |
| COMMIT reply is lost after the primary decision | Outcome unknown until durable resolution | The effect may have committed even if the caller saw a transport error. |
| Leadership changes after proposal, then the new leader has no immediate answer | Outcome unknown | Absence of an answer is not evidence of rollback. |

The receiver does not accept an absent detail, unknown schema version, unknown enum, inconsistent status code, or phase mismatch as proof of a definitive rejection.
It retains the error as an unknown outcome if submission might have occurred.
Old peers that return text-only statuses get this conservative treatment.
There is no parser for legacy phrases, including `key: ...: write conflict` or `not leader`.
An operation with a durable identity may resolve an unknown outcome and then return its recorded result; that resolution does not turn the original transport error into abort proof.
Root initialization's re-read behavior is an operation-specific reconciliation path, not a general exception to the rule.

The wire detail is evidence only at this authenticated invocation seam.
An arbitrary public gRPC request or a locally constructed `ErrorInfo` with the same reason is not proof of rejection.
Authentication identifies the peer; the receiver must still validate the detail's source, version, phase, and allowed reason and decision pair.
If the existing `ErrorInfo` detail is retained during rollout, it is a transitional encoding with these same acceptance rules and no key metadata.
The final protobuf detail must not be inferred from a matching status message.

## Public protocol projection

The adapter is the final error adapter at the public seam.
It selects a stable public code and a fixed or explicitly request-derived message from the validated reason and decision.
It does not serialize `err.Error()` from an internal error by default.

| Public surface | Projection rule |
| --- | --- |
| Redis RESP | Choose a Redis error category from typed reason and outcome; preserve the distinction between retryable rejection and indeterminate write. Do not reconstruct a user key from a physical key. |
| DynamoDB HTTP | Choose the documented DynamoDB error type and retry behavior from typed reason and outcome. Do not include the storage key or the internal gRPC message in the response body. |
| S3 and SQS HTTP | Translate the same decision through each protocol's error vocabulary. Preserve indeterminate writes instead of reporting a definite application failure. |
| Public gRPC | Emit its own public status detail and fixed message. Internal Forward details are not copied to a public service response. |
| FUSE | Map typed decisions to the operation's specified `errno`. A definitive conflict may be retried within the bounded filesystem operation; an unknown mutation outcome follows the journal's resolution and sticky-error rules. |

The FUSE operation outcome and durability rules remain those of `2026_10_25_partial_fuse_conflict_semantics.md`.
This proposal supplies an error classification seam; it does not make a mutation replayable or change the result of `fsync`.

A public message may mention a client-supplied logical key only when that protocol explicitly specifies the field and the adapter uses the original validated request value.
It must never derive that value by trimming prefixes from an internal error key.
Public messages and logs must be valid UTF-8 by construction; repairing arbitrary bytes at the last serialization step is not a data-ownership rule.

## Migration and verification

The implementation should land in reviewable phases while preserving fail-closed behavior for mixed versions.

1. Inventory every place that classifies errors by message, serializes `err.Error()` across a public seam, or puts a physical key in an error or status detail.
   Record the call site, trust level, request phase, and whether it can follow submission.
2. Introduce local typed causes and the decision class at the coordinator seam.
   Keep physical keys private and keep local `Error()` output key-free.
3. Deploy a decoder for the versioned Forward detail that is called only on the authenticated internal path.
   Treat text-only responses as indeterminate when submission is possible.
   Then enable the new producer after all active forwarders advertise the decoder capability, or gate production by peer capability.
   Verify that an older receiver cannot treat any new fixed message as a false abort before permitting a mixed-version rollout.
4. Convert each public adapter to a typed projection and remove message parsers and key cleaners after its tests pass.
   Do not widen retry behavior as a side effect of the conversion.
5. Audit successful response paths separately for intentional key decoding and raw-key access.
   Promote this document from `proposed` only after the declared error seams are implemented on `main`.

Acceptance requires tests at each seam:

- Local typed errors retain their private key for authorized diagnosis while `Error()`, `%v`, and `%+v` do not reveal it.
- Forwarding preserves reason and decision through a real gRPC round trip with binary keys, invalid UTF-8, wrapped errors, and keys containing phrases such as `not leader` and `write conflict`.
- An unmarked status, a forged detail outside the authenticated Forward path, an unknown version, and a phase mismatch never authorize replay.
- Lost replies before and after proposal, a leader change, a partial multi-group commit, and cancellation preserve the correct decision class.
- Redis, DynamoDB, S3, SQS, public gRPC, and FUSE tests assert protocol codes, retry safety, and the absence of physical keys in message, metadata, headers, and bodies.
- Jepsen histories distinguish definitive failures from indeterminate operations; no client records a possibly committed write as a definite failure.

The wire detail and public projection tests are contract tests.
They should assert enum values and observable behavior, not the exact English diagnostic sentence.
Metrics use bounded reason, decision, protocol, and key-family labels; they do not label with raw keys.

## Rejected approaches

| Approach | Why it fails |
| --- | --- |
| Clean or replace invalid UTF-8 at the gRPC writer | The internal key still crossed the seam, and the repaired text cannot prove a transaction decision. |
| Base64-encode the physical key in status metadata | The key still reaches a module that does not own it and can be decoded by a caller or log. |
| Match a prefix or suffix of `Error()` | Wrapping, localization, or a user key containing the phrase changes classification. A message is not an authenticated proof. |
| Use only a gRPC status code | `Aborted` or `Unavailable` does not say whether a particular mutation was submitted or committed. |
| Treat all transport failures as retryable | A lost success response can make a non-idempotent operation execute twice. |

## Risks and limits

The typed detail is an internal wire-format change and requires a mixed-version gate.
Older peers may cause more unknown outcomes until they are upgraded; availability must not be recovered by parsing their text.
Removing keys from errors reduces incidental diagnostics, so internal correlation IDs and bounded key-family logs must be available before deleting the old messages.

This document specifies error and key ownership at the named seams.
It does not certify that every existing successful RawKV scan, admin response, log line, or diagnostic endpoint is free of reserved keys.
Those surfaces require the separate audit in the migration plan.

# Leader proxy circuit breaker

Status: Implemented

Document type: Focused design and implementation record

Author: bootjp

Date: 2026-07-19

## 1. Problem

`LeaderProxy` already bounds one forwarding request with a five-second retry
budget. That bound does not coordinate concurrent requests. During an election
or an unreachable-leader interval, every request can independently retry the
same endpoint, repeatedly ask the shared gRPC connection to reconnect, and
amplify an availability incident into connection and scheduler pressure.

The proxy needs cross-request memory. It must suppress repeated work against a
leader identity that has just failed while preserving the existing behavior
that lets one request wait through a short election.

## 2. Scope

This change owns the data-plane `kv.LeaderProxy` forwarding breaker:

1. failure accounting across requests;
2. leader-change reset using leader ID, address, and Raft term;
3. bounded exponential backoff and one half-open probe;
4. adapter-visible transient error classification; and
5. interaction with the shared gRPC connection cache.

HTTP leader proxying is separate. Raft election policy, transport health, and
application-level retry/idempotency remain unchanged.

## 3. State machine

Each `LeaderProxy` owns one breaker because each instance fronts one Raft
group. The breaker key is `(leader_id, leader_address, term)`.

### 3.1 Closed

Requests use the existing per-request retry loop. Typed leadership errors,
wire-level leadership errors, and gRPC `Unavailable` or `DeadlineExceeded`
responses count as breaker failures. Business errors do not.

Three consecutive failures open the breaker. A successful RPC, including a
terminal application response from a reachable leader, clears the failure
count.

### 3.2 Open

The first request that reaches the threshold remains the owner of recovery. It
continues to honor the existing five-second request budget, but it does not send
RPCs while the breaker backoff is active. Other requests fail immediately with
`ErrLeaderProxyCircuitOpen` instead of starting their own retry loops.

Backoff begins at 100 ms, doubles after each failed half-open probe, and is
capped at two seconds.

### 3.3 Half-open

After backoff, exactly one request may probe the current identity. Success
closes the breaker. A transient failure reopens it with the next backoff. Late
failures from requests that were already in flight when the breaker opened do
not steal probe ownership or extend the outage. Results from an older leader
identity are ignored after a newer identity has been observed.

Caller cancellation and caller-deadline expiry are not leader health signals.
They release an acquired half-open probe without incrementing or clearing the
failure count, so another live request can probe the same identity.

Any change in leader ID, address, or term resets the breaker immediately. A
verified local-leader commit also resets it.

## 4. Error contract

`ErrLeaderProxyCircuitOpen` is a transient availability error. Redis maps it to
`NOTLEADER`; DynamoDB, SQS, S3, and Admin map it to their existing 503
surfaces, including the exact suffix used after a gRPC boundary. It is
deliberately excluded from the coordinator's transaction retry predicate: the
breaker has already made the retry decision, and an outer retry could reissue a
transaction with new timestamps or mutation contents.

The breaker does not change ambiguous-write semantics. A request that may have
reached a leader still returns through the same adapter-specific idempotency and
error handling paths as before.

## 5. gRPC connection behavior

`GRPCConnCache` continues to reuse one connection per address and replaces only
connections in `Shutdown`. It no longer calls `ResetConnectBackoff` whenever a
caller observes `TransientFailure`; doing so on every request defeats gRPC's
own reconnect backoff and recreates the retry storm below the breaker.

## 6. Verification

Unit coverage pins:

- threshold/open behavior;
- one half-open probe;
- leader/term reset;
- late in-flight failure handling;
- exponential backoff cap;
- transport and business-error classification; and
- proxy-budget deadline accounting against a blackholed endpoint;
- caller cancellation releasing a blackholed half-open probe;
- recovery-owner half-open success after transport `Unavailable` failures;
- S3 chunk-upload circuit-open mapping; and
- the existing short-election and full-budget integration cases.

The race suite covers concurrent breaker access and half-open probe admission.

## 7. Rollout and rollback

The change is local and carries no Raft, protobuf, or persisted-data format.
Mixed-version nodes are safe. Rollback restores independent per-request retry
loops and connection-backoff resets; it does not require data repair.

## 8. Non-goals

- changing Raft election timeout or leader selection;
- sharing breaker state across Raft groups;
- replacing adapter-level idempotency;
- changing HTTP reverse-proxy retries; or
- hiding persistent unavailability beyond the existing request deadline.

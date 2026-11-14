# Request-scoped read capabilities

Adapters use `kv.ReadRequest` for data reads. `Get`, `Scan`, and the group,
key-only, reverse, and route-fenced variants require an opaque `kv.ReadSnapshot`.
Passing an integer timestamp fails to compile. The storage engine's `GetAt` and
`ScanAt` remain explicit historical MVCC primitives.

Create a fresh request at protocol ingress, use its context for all operations,
and close it when the response completes:

```go
request := kv.NewReadRequest(ctx, coordinator, backend, tracker)
defer request.Close()
ctx = request.Context()
snapshot, err := request.Latest(ctx, "object read", bucketKey, objectKey)
if err != nil {
    return err
}
value, err := request.Get(ctx, snapshot, objectKey)
```

`Latest` fences the relevant committed log before choosing the applied timestamp
and obtaining the existing timestamp-authority stamp. A failed fence does not
fall back to a follower-local watermark. `LatestTargets` retains explicit
physical group identity for grouped reads. A keyed capability only permits its
fenced keys or groups; a range needs proof that all its groups are covered.

Snapshots are bound to one request and backend. Zero, foreign-context,
foreign-request, and closed-request snapshots fail before accessing storage.
Derived contexts must preserve the request context. Copying a token does not
extend its lifetime. Multiple reads within a request may intentionally share a
snapshot; it is not a single-use token. Cancellation suppresses late results.
`Close` cancels and joins in-flight reads and child scopes before releasing the
shared MVCC retention pins. S3 admin streaming transfers ownership to the reader,
which must be closed by its caller. Blocking Redis XREAD uses a child scope for
initial dollar-ID resolution and each polling read. BZPOPMIN likewise owns a child
scope through each pop attempt, including its transaction commit. Those children close before
waiting for updates, so an idle command retains cancellation ownership without
retaining completed polling snapshots or their GC pins.

## Migration boundary and historical reads

Existing private, version-aware adapter helpers retain numeric `At` parameters.
Their compatibility bridge resolves only snapshots already issued by the current
request. Unknown numbers are rejected; lookup cannot mint a latest snapshot.
Public HTTP, Redis, gRPC, and admin data entry points install fresh scopes even
when their caller already has one. Source guards restrict compatibility calls to
an explicit function allowlist and require public data entry points to own scopes.
New adapter code uses the typed ports directly.

Unscoped internal `At` helpers retain the historical contract for maintenance and
version-oriented callers. This is an incremental adapter API migration, not a
removal of the historical storage engine API. Explicit historical issuance is
limited in adapters to audited protocol choices: DynamoDB eventual reads,
committed snapshot seals and idempotency versions, S3 continuation snapshots,
and immutable blob versions. Cross-store blob reads create their own child scope.
Internal raw RPCs accept explicit historical versions and issue a local capability;
capabilities are process-local and are not network authentication credentials.

A future latest endpoint must install an ingress scope and call `Latest`, rather
than treating an arbitrary historical version as fresh. The compile-failure test,
source guards, and runtime request-lifetime tests enforce these boundaries.

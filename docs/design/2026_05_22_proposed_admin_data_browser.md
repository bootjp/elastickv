# Admin Web UI for DynamoDB Item and S3 Object CRUD

**Status:** Proposed
**Author:** bootjp
**Date:** 2026-05-22

---

## 1. Background and Motivation

The admin Web Console (`web/admin/`) now ships per-adapter detail pages for SQS (`SqsDetail.tsx`), DynamoDB (`DynamoDetail.tsx`), and S3 (`S3Detail.tsx`). All three pages handle resource-level operations (create / describe / delete the queue, table, or bucket) and SQS additionally ships message-level operations (peek + purge) after the 2026-05-16 admin-purge-queue work landed.

DynamoDB and S3 detail pages still stop at the resource level — they show table / bucket metadata but cannot inspect or modify what is **inside** the table / bucket. Two operator workflows are blocked:

1. **Inspect what is actually stored.** An operator triaging a misbehaving service needs to look at the items / objects the service is reading and writing. Today they have to drop to the AWS CLI or `cmd/elastickv-admin` and re-authenticate against whatever credentials store backs that path.

2. **Apply ad-hoc fixes.** A bad config blob, a stuck queue's parking-lot record, a stale routing entry, a corrupted DLQ message — operators need an in-Console way to delete or replace a small number of items / objects without writing a one-off script.

This proposal extends the admin Web Console to support **List / Get / Put / Delete** on DynamoDB items and S3 objects, following the same pattern the SQS Messages tab established.

---

## 2. Goals and Non-Goals

### 2.1 Goals

1. **DynamoDB item CRUD** — `AdminScanTable`, `AdminGetItem`, `AdminPutItem`, `AdminDeleteItem` admin RPCs on `*adapter.DynamoDBServer`. List uses `Scan` semantics (no `Query` / GSI / conditional ops in this phase). Cursor pagination via `LastEvaluatedKey`.
2. **S3 object CRUD** — `AdminListObjects`, `AdminGetObject`, `AdminPutObject`, `AdminDeleteObject` admin RPCs on `*adapter.S3Server`. List supports `prefix` + `delimiter` so the SPA can render the bucket as a pseudo-directory tree.
3. **HTTP endpoints** —
   - `GET    /admin/api/v1/dynamo/tables/{name}/items?cursor=C&limit=N` — Scan
   - `GET    /admin/api/v1/dynamo/tables/{name}/items/{key}` — Get item by primary key
   - `PUT    /admin/api/v1/dynamo/tables/{name}/items/{key}` — Put item
   - `DELETE /admin/api/v1/dynamo/tables/{name}/items/{key}` — Delete item
   - `GET    /admin/api/v1/s3/buckets/{name}/objects?prefix=P&delimiter=D&continuation_token=T` — List
   - `GET    /admin/api/v1/s3/buckets/{name}/objects/{key}` — Get (download)
   - `PUT    /admin/api/v1/s3/buckets/{name}/objects/{key}` — Put (upload)
   - `DELETE /admin/api/v1/s3/buckets/{name}/objects/{key}` — Delete
4. **SPA UI**:
   - **DynamoDetail Items tab**: paginated table of items, click for full JSON modal, Edit / Delete buttons per row, "Add item" button at the top.
   - **S3Detail Objects tab**: tree view (folders from `delimiter=/` collapses), Download button per row, Delete per row, "Upload" button at the top.
5. **100 MiB upload cap** for S3 PutObject; raw octet-stream body (not JSON-encoded). The DynamoDB PutItem stays JSON because items are small (DynamoDB's own item-size limit is 400 KiB).
6. **Read role can browse, write role can mutate** — peek-equivalent gate (`AllowsRead`) on every Get/List, write gate (`AllowsWrite`) on every Put/Delete. Mirrors the SQS Phase 4 division.
7. **Audit lines** — `admin.dynamo.put_item`, `admin.dynamo.delete_item`, `admin.s3.put_object`, `admin.s3.delete_object` with subject + role + table/bucket + key.

### 2.2 Non-Goals

1. **DynamoDB Query / GSI / conditional updates / partial updates (`UPDATE` action).** Scope decision: basic CRUD only. A future RFC can add Query when operators ask.
2. **DynamoDB UpdateItem (PATCH semantics).** Put-full-item replaces the whole record; partial mutations are out of scope.
3. **S3 multipart upload.** A single PUT handles up to 100 MiB; anything larger needs the AWS SDK against the public endpoint.
4. **S3 versioning operations.** Even though the underlying engine supports MVCC, the admin UI surfaces only the current version. Listing prior versions belongs to a future Versioning UI.
5. **Batch operations** (BatchGetItem, BatchWriteItem, DeleteObjects). One key per request keeps the audit shape simple and the failure handling unambiguous; bulk operations belong to a follow-up.
6. **Cross-table / cross-bucket views.** The SPA already lists tables / buckets at the index page; per-resource detail pages cover the rest.
7. **Server-side filtering on Scan** (FilterExpression). Adds wire complexity for a workflow the SPA can do client-side on the visible page. Future RFC if a 10K-row table needs it.

---

## 3. Design

### 3.1 Backend RPCs

#### 3.1.1 DynamoDB item RPCs

```go
type AdminItemKey struct {
    PartitionKey AdminAttributeValue
    SortKey      *AdminAttributeValue // nil for hash-only tables
}

type AdminAttributeValue struct {
    Type  string // "S", "N", "B", "BOOL", "NULL", "L", "M", "SS", "NS", "BS"
    Value any    // shape depends on Type; mirrors the DynamoDB wire shape
}

type AdminItem struct {
    Attributes map[string]AdminAttributeValue
}

type AdminScanResult struct {
    Items            []AdminItem
    LastEvaluatedKey *AdminItemKey // nil when scan is fully drained
}

type AdminScanOptions struct {
    Limit       int     // [1, 100], default 25
    StartKey    *AdminItemKey
    ProjectAll  bool    // false to project only key attributes; true to project full items
}

// Read-only.
AdminScanTable(ctx, principal, tableName, opts) (AdminScanResult, error)
AdminGetItem(ctx, principal, tableName, key) (*AdminItem, bool, error)

// Write.
AdminPutItem(ctx, principal, tableName, item) error
AdminDeleteItem(ctx, principal, tableName, key) error
```

**Scan implementation.** Reuse the existing internal `Scan` machinery; the new admin call is a thin wrapper that:
- Validates `tableName` against the catalog
- Calls `scan` with the cursor (LastEvaluatedKey) translated from `AdminItemKey`
- Clamps Limit to `[1, adminItemScanMaxLimit=100]`
- Returns the AdminAttributeValue-shaped items and the next cursor

**Key encoding.** `AdminItemKey` on the wire is `{partition: AttrValue, sort: AttrValue|null}`. The HTTP `{key}` path segment carries a base64-url-encoded JSON of the same shape (single round-trip per key, no need to plumb composite-key URI encoding through the routing layer).

#### 3.1.2 S3 object RPCs

```go
type AdminObject struct {
    Key          string
    Size         int64
    ETag         string
    LastModified time.Time
    StorageClass string
}

type AdminObjectListing struct {
    Objects               []AdminObject
    CommonPrefixes        []string // when delimiter is set, these are pseudo-directories
    NextContinuationToken string   // empty when fully drained
}

type AdminListObjectsOptions struct {
    Prefix            string
    Delimiter         string // typically "/" for directory-style listings
    ContinuationToken string
    MaxKeys           int    // [1, 1000], default 100
}

// Read-only.
AdminListObjects(ctx, principal, bucket, opts) (AdminObjectListing, error)
AdminGetObject(ctx, principal, bucket, key) (body io.ReadCloser, meta AdminObject, err error)

// Write.
AdminPutObject(ctx, principal, bucket, key string, body io.Reader, contentType string) error
AdminDeleteObject(ctx, principal, bucket, key string) error
```

**Streaming.** `AdminGetObject` / `AdminPutObject` use `io.Reader` so the 100 MiB body cap (see §3.3) does not force the leader to buffer the entire payload before responding. The HTTP handler relays the stream byte-for-byte.

### 3.2 Authorization gates

Same model as the SQS Peek + Purge division:

| Operation | Gate | Notes |
|-----------|------|-------|
| AdminScanTable / AdminListObjects | `principalForReadSensitive` | Payload contents are sensitive; nil principal denied. |
| AdminGetItem / AdminGetObject | `principalForReadSensitive` | Same. |
| AdminPutItem / AdminPutObject | `principalForWrite` | Live-role re-check, action string passed through (e.g. "put item", "upload object"). |
| AdminDeleteItem / AdminDeleteObject | `principalForWrite` | Same. |

The `principalForWrite` action-verb parameter (added in Phase 4 / r1 of the SQS work) carries forward — each new write handler passes its own verb so 403 bodies do not say "delete queues" for a put-object operation.

### 3.3 HTTP endpoints

#### 3.3.1 Path routing

DynamoDB and S3 handlers extend the same 6-step routing procedure the SQS handler adopted (escape-aware, dot-segment-rejecting, dispatch on segment count). Three resource families:

```
/admin/api/v1/dynamo/tables                              → list tables (existing)
/admin/api/v1/dynamo/tables/{name}                       → describe / delete (existing)
/admin/api/v1/dynamo/tables/{name}/items                 → scan / put (NEW)
/admin/api/v1/dynamo/tables/{name}/items/{key}           → get / put / delete (NEW)

/admin/api/v1/s3/buckets                                 → list buckets (existing)
/admin/api/v1/s3/buckets/{name}                          → describe / delete (existing)
/admin/api/v1/s3/buckets/{name}/objects                  → list / put (NEW)
/admin/api/v1/s3/buckets/{name}/objects/{key}            → get / put / delete (NEW)
```

`{key}` is base64-url-encoded so arbitrary bytes (Dynamo item keys, S3 object keys with `/` or non-ASCII characters) traverse the path validator's `%`-ban without ambiguity.

#### 3.3.2 Body shapes

| Endpoint | Method | Request body | Response body |
|----------|--------|--------------|---------------|
| `/dynamo/tables/{n}/items` | GET | — | `{items: AdminItem[], next_cursor?: string}` |
| `/dynamo/tables/{n}/items` | POST | `{key, attributes}` (single item) | 204 |
| `/dynamo/tables/{n}/items/{key}` | GET | — | `AdminItem` |
| `/dynamo/tables/{n}/items/{key}` | PUT | `{attributes}` | 204 |
| `/dynamo/tables/{n}/items/{key}` | DELETE | — | 204 |
| `/s3/buckets/{n}/objects` | GET | — | `{objects: AdminObject[], common_prefixes: string[], next_continuation_token?: string}` |
| `/s3/buckets/{n}/objects/{key}` | GET | — | raw octet-stream + `Content-Type` + `Last-Modified` + `ETag` headers |
| `/s3/buckets/{n}/objects/{key}` | PUT | raw octet-stream | 204 |
| `/s3/buckets/{n}/objects/{key}` | DELETE | — | 204 |

S3 GET responses stream the body directly (no JSON envelope, no base64 wrapper) so the browser can offer a Save-As dialog via `Content-Disposition: attachment`.

#### 3.3.3 Upload cap

S3 PutObject body is capped at `adminS3UploadMaxBytes = 100 MiB` (100 × 1024 × 1024 = 104857600 bytes). Enforced via `http.MaxBytesReader`. Exceeding the cap returns `413 Payload Too Large` with a structured `{"error": "payload_too_large", "message": "object exceeds 100 MiB admin upload cap"}` body. DynamoDB PutItem stays under DynamoDB's own item-size limit (400 KiB) so the JSON-body cap inherits the existing `BodyLimit` middleware (1 MiB).

### 3.4 SPA pages

#### 3.4.1 DynamoDetail Items tab

Below the table's metadata / counters sections, a new "Items" section:

- Top bar: "Add item" button (full role only).
- Paginated table (default 25 rows, server cap 100): columns Partition Key, Sort Key (if defined), Attributes (truncated preview), Created/Modified time if exposed.
- Row click → full-item JSON modal with Edit (replaces the item) and Delete buttons.
- Next / Refresh buttons driven by `next_cursor`.

#### 3.4.2 S3Detail Objects tab

Below the bucket's metadata / ACL section, a new "Objects" section:

- Top bar: "Upload" button (file picker → `PUT /objects/{key}`); current prefix path (breadcrumb) so the operator can navigate up.
- Paginated table (default 100 rows): columns Key (truncated to filename when in a prefix), Size, Last Modified, ETag.
- Row click on a folder (CommonPrefix) navigates into it (`prefix=foo/bar/`); row click on an object opens a detail modal with Download and Delete.
- Delimiter is fixed to `/` so the bucket renders directory-style. A future "flat view" toggle can expose `delimiter=""` for operators who want the raw list.

#### 3.4.3 SPA types

`web/admin/src/api/client.ts` gains the new wire types and four × two = eight new methods (`scanTable` / `getItem` / `putItem` / `deleteItem`, `listObjects` / `getObject` / `putObject` / `deleteObject`). Upload progress is reported via the `fetch` `Request` `body` stream's existing `XMLHttpRequest`-fallback if a future enhancement adds progress indication; the MVP does not show a progress bar (operators upload 1–10 MiB config files typically, well under any threshold worth visualising).

### 3.5 Audit + observability

Four new audit lines (parallel to `admin.sqs.purge_queue` shape):

```
admin.dynamo.put_item    subject role table key
admin.dynamo.delete_item subject role table key
admin.s3.put_object      subject role bucket key bytes
admin.s3.delete_object   subject role bucket key
```

Read-side calls (scan / get / list) DO NOT generate audit lines (the SPA polls; per-poll audit would drown the log). The admin request-log line covers them at the `route` / `subject` / `status_code` level.

Two new Prometheus counters per adapter:

```
elastickv_admin_dynamo_item_writes_total{outcome ∈ ok,forbidden,not_found,validation,internal_error}
elastickv_admin_s3_object_writes_total{outcome ∈ ok,forbidden,not_found,validation,payload_too_large,internal_error}
```

Reads are bounded by Limit / MaxKeys, so a separate counter has low marginal value; the request-log line covers their cost analysis.

---

## 4. Failure Modes

1. **Concurrent writes.** Both backends already serialise writes through Raft; the admin RPCs use the same commit path. No new TOCTOU class.
2. **Large object download mid-stream.** If a follower loses leadership while streaming a 100 MiB Get, the connection is closed by the leader-step-down path. The operator's browser shows a partial download error; safest re-try is from scratch (the partial bytes have an unknown integrity boundary).
3. **Truncated upload.** `http.MaxBytesReader` cuts the stream at the cap; the leader receives a partial body and returns 413 without committing. No state change.
4. **DynamoDB item with binary attributes.** `B` / `BS` types arrive as base64 in the JSON wire shape (mirroring AWS); the admin RPC decodes lazily where needed.
5. **S3 versioning leakage.** This admin UI uses the latest-version path only; pre-version operations from another client are visible only via the underlying SDK.
6. **Invalid base64-url key.** Path validator rejects with 400 `invalid_path` before dispatch.

---

## 5. Testing Plan

1. **Unit (adapter)**: each new admin RPC gets a focused unit test for happy path + forbidden + missing-table-or-bucket + validation. Mirrors the SQS Phase 2 / Phase 3 pattern.
2. **Integration (`internal/admin`)**: HTTP handler tests cover routing (the 6-step path validation, table-level keys with `/` and binary attributes), role gates, 429-shaped errors where applicable, upload cap enforcement.
3. **SPA**: `npm run lint` (`tsc -b --noEmit`) plus an explicit "render after queue change clears state" test mirroring the SQS r3 fix.
4. **Jepsen**: out of scope. CRUD on existing primitives runs through the same OCC commit path Jepsen workloads already exercise.

---

## 6. Rollout Plan

| Phase | Content |
|-------|---------|
| 1 | This doc lands. |
| 2 | Backend `Admin{Scan/Get/Put/Delete}{Item,Object}` RPCs + sentinel errors + tests. Two adapters; can split into 2a (Dynamo) + 2b (S3) if review surface gets large. |
| 3 | HTTP handlers + bridges + integration tests (also potentially split per adapter). |
| 4 | SPA: DynamoDetail Items tab. |
| 5 | SPA: S3Detail Objects tab + Upload. |
| 6 | Doc rename `_proposed_` → `_implemented_`. |

Each phase ships as a separate PR. Phase 2 (backend) lands first because the HTTP and SPA layers depend on the RPCs being callable. Phases 4 and 5 are SPA-only and can land in either order — 5 is slightly larger (upload + streaming) so it lands second.

---

## 7. Open Questions

1. **Should we expose `Query` on DynamoDB in this round?** The user opted for basic CRUD only; a follow-up RFC can revisit when operators ask for GSI inspection.
2. **Should S3 Get bypass the JSON envelope?** Yes — the design above commits to raw octet-stream so the browser's Save-As works without a base64 wrapper. The trade-off is that errors on the get path can't render as JSON; they use `text/plain` bodies on non-2xx like the existing S3 SigV4 path does.
3. **What's the threshold for a multipart upload?** Out of scope for this MVP. If operators routinely need > 100 MiB through the admin UI, a future RFC adds chunked PUT via the same endpoint signature with a `Content-Range` header.

---

## 8. Alternatives Considered

### 8.1 Build only DynamoDB now, defer S3 to a separate RFC

Rejected: the SPA modal / pagination patterns the two pages share are nearly identical to the SQS Messages tab; building both at once amortises that work. Code-sharing happens at the helper level (`MessagesTable` style components) and the bridge / handler layer.

### 8.2 Use the existing SigV4 paths through an admin proxy

Rejected: forces the SPA's TypeScript client to construct SigV4-flavored bodies for what is otherwise plain JSON. The cost of the dedicated admin RPCs is one short wrapper per operation, and the wire shape stays consistent with the rest of the admin surface.

### 8.3 Item-level update via PATCH instead of full PUT replace

Rejected for the MVP: PATCH semantics in DynamoDB (UpdateItem) carry conditional-expression baggage that this round opted out of. A future RFC can add UpdateItem with a constrained expression vocabulary.

# S3 Public Bucket Access Design

## 1. Background

The current S3-compatible adapter requires AWS Signature Version 4 authentication for all requests. There is no bucket-level or object-level access control; once authentication succeeds, the client has full access to all resources.

For use cases such as serving static assets or sharing public data sets, we need the ability to make specific buckets readable without authentication. This document designs bucket-level public distribution functionality.

## 2. Goals and Non-goals

### 2.1 Goals

1. Provide a mechanism to switch each bucket between public and private.
2. Allow unauthenticated `GetObject`, `HeadObject`, and `ListObjectsV2` requests against public buckets.
3. Require authentication for all write operations (`PutObject`, `DeleteObject`, multipart uploads, etc.), even on public buckets.
4. Keep all existing buckets private by default (to preserve backward compatibility).
5. Allow changing and inspecting the public access setting via the `PutBucketAcl` / `GetBucketAcl` APIs.

### 2.2 Non-goals

1. Object-level ACLs (`x-amz-acl` per object).
2. Bucket policy JSON (IAM-style conditional policies).
3. IAM / STS / cross-account authorization.
4. S3 static website hosting (automatic index.html resolution, custom error pages, etc.).
5. CORS configuration.
6. Anonymous writes to public buckets.

## 3. ACL Model

### 3.1 Supported ACL values

Among AWS S3 canned ACLs, we only support the following two:

| Canned ACL | Read | Write |
|---|---|---|
| `private` (default) | Auth required | Auth required |
| `public-read` | No auth required | Auth required |

All other canned ACLs (`public-read-write`, `authenticated-read`, `aws-exec-read`, `bucket-owner-read`, `bucket-owner-full-control`, etc.) return a `NotImplemented` error.

### 3.2 Why bucket-level only

- Object-level ACLs are discouraged even by AWS itself (S3 Object Ownership with `BucketOwnerEnforced` is the default).
- We can cover the main public-distribution use cases while keeping the implementation substantially simpler.
- If we introduce bucket policies in the future, bucket-level ACLs will coexist with them naturally.

## 4. Data Model Changes

### 4.1 Bucket metadata extension

Add an `Acl` field to `s3BucketMeta`:

```go
type s3BucketMeta struct {
    BucketName   string `json:"bucket_name"`
    Generation   uint64 `json:"generation"`
    CreatedAtHLC uint64 `json:"created_at_hlc"`
    Owner        string `json:"owner,omitempty"`
    Region       string `json:"region,omitempty"`
    Acl          string `json:"acl,omitempty"`   // "private" or "public-read"
}
```

- When `Acl` is empty or unset, the bucket is treated as `private` (backward compatibility).
- Valid values are `"private"` and `"public-read"` only.

### 4.2 Key layout

No additional keys are required. ACL information is stored in the existing `!s3|bucket|meta|<bucket-esc>` entry.

### 4.3 Migration

Existing bucket metadata does not contain the `acl` field. Upon JSON deserialization, `omitempty` produces an empty string, which is treated as `private`. No migration is required.

## 5. API Changes

### 5.1 PutBucketAcl

```
PUT /<bucket>?acl HTTP/1.1
x-amz-acl: public-read
```

- **Authentication**: Required (SigV4).
- **Request**: Specify the canned ACL with the `x-amz-acl` header. ACL specification via XML body is not supported (`NotImplemented` is returned).
- **Processing**:
  1. Read the bucket metadata.
  2. Update the `Acl` field.
  3. Write the metadata back in an OCC transaction.
- **Response**: `200 OK` (empty body).
- **Errors**:
  - `NoSuchBucket`: bucket does not exist.
  - `NotImplemented`: unsupported ACL value.
  - `AccessDenied`: authentication failed.

### 5.2 GetBucketAcl

```
GET /<bucket>?acl HTTP/1.1
```

- **Authentication**: Required (SigV4).
- **Response**: AWS-compatible XML:

```xml
<AccessControlPolicy>
  <Owner>
    <ID>owner-id</ID>
    <DisplayName>owner-id</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:type="CanonicalUser">
        <ID>owner-id</ID>
        <DisplayName>owner-id</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
    <!-- added only when public-read -->
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:type="Group">
        <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
      </Grantee>
      <Permission>READ</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>
```

### 5.3 CreateBucket with ACL

```
PUT /<bucket> HTTP/1.1
x-amz-acl: public-read
```

Add `x-amz-acl` header handling to the existing `createBucket`. Defaults to `private` when omitted.

### 5.4 PutObject ACL header

The `x-amz-acl` header on `PutObject` requests is **ignored** (per-object ACLs are not supported). If support is added in the future, the behavior can be changed to return `NotImplemented`.

## 6. Authentication Flow Changes

### 6.1 Current flow

```
handle(w, r)
  → authorizeRequest(r)        // SigV4 validation on all requests
  → parseS3Path(r)
  → route to handler
```

### 6.2 Proposed flow

```
handle(w, r)
  → parseS3Path(r)             // parse path first (lightweight)
  → resolveAuth(r, bucket, objectKey, method)
    → isPublicReadRequest(bucket, method, query)?
      → yes: skip auth
      → no:  authorizeRequest(r)
  → route to handler
```

### 6.3 resolveAuth details

```go
func (s *S3Server) resolveAuth(r *http.Request, bucket, objectKey string) *s3AuthError {
    // ACL / admin APIs always require authentication
    if isAclRequest(r) || isWriteMethod(r) {
        return s.authorizeRequest(r)
    }

    // Empty bucket (ListBuckets) always requires authentication
    if bucket == "" {
        return s.authorizeRequest(r)
    }

    // Check the bucket's ACL
    meta, err := s.loadBucketMeta(r.Context(), bucket)
    if err != nil || meta == nil {
        // If the bucket is not found, fall through to the auth flow
        // (NoSuchBucket is handled later in the pipeline)
        return s.authorizeRequest(r)
    }

    if meta.Acl == "public-read" && isReadOnlyRequest(r) {
        return nil  // skip authentication
    }

    return s.authorizeRequest(r)
}
```

### 6.4 Definition of a read-only request

A request is considered read-only when all of the following conditions are met:

- HTTP method is `GET` or `HEAD`.
- The query string does not contain `acl`, `uploads`, or `uploadId`.
- The operation is one of `ListObjectsV2`, `GetObject`, `HeadObject`, or `HeadBucket`.

### 6.5 Performance impact

One additional metadata read is added before authentication to determine whether the bucket is public. However:

- Bucket metadata is small (~100 bytes).
- `GetObject` and `ListObjectsV2` already read bucket metadata downstream, so a cache can eliminate the duplicate.
- For private buckets, the code follows the same auth path as before, keeping overhead minimal.

An in-memory ACL cache with a TTL can be introduced in the future to reduce metadata reads, but is not required for the initial implementation.

## 7. Request Flow Examples

### 7.1 Public bucket: anonymous GetObject

```
Client                    S3 Server
  |  GET /public-bucket/file.txt    |
  |  (no Authorization header)      |
  |-------------------------------->|
  |                                 | parseS3Path → bucket="public-bucket"
  |                                 | loadBucketMeta → acl="public-read"
  |                                 | isReadOnlyRequest → true
  |                                 | → skip authentication
  |                                 | getObject()
  |  200 OK + body                  |
  |<--------------------------------|
```

### 7.2 Public bucket: anonymous PutObject (rejected)

```
Client                    S3 Server
  |  PUT /public-bucket/file.txt    |
  |  (no Authorization header)      |
  |-------------------------------->|
  |                                 | parseS3Path → bucket="public-bucket"
  |                                 | isWriteMethod → true
  |                                 | authorizeRequest → AccessDenied
  |  403 AccessDenied               |
  |<--------------------------------|
```

### 7.3 Private bucket: anonymous GetObject (rejected)

```
Client                    S3 Server
  |  GET /private-bucket/file.txt   |
  |  (no Authorization header)      |
  |-------------------------------->|
  |                                 | parseS3Path → bucket="private-bucket"
  |                                 | loadBucketMeta → acl="" (private)
  |                                 | authorizeRequest → AccessDenied
  |  403 AccessDenied               |
  |<--------------------------------|
```

### 7.4 Changing ACL

```
Client                    S3 Server
  |  PUT /my-bucket?acl             |
  |  x-amz-acl: public-read        |
  |  Authorization: AWS4-HMAC-...   |
  |-------------------------------->|
  |                                 | authorizeRequest → OK
  |                                 | putBucketAcl()
  |                                 |   load meta → update acl → txn write
  |  200 OK                         |
  |<--------------------------------|
```

## 8. Implementation Plan

### Phase 1: Core ACL infrastructure

1. Add `Acl` field to `s3BucketMeta`.
2. Add ACL validation function (`validateCannedAcl`).
3. Add `isPublicReadBucket` / `isReadOnlyRequest` helpers.

### Phase 2: Auth flow changes

1. Change the auth flow in `handle()` to introduce `resolveAuth()`.
2. Move path parsing before authentication.
3. Skip authentication for read-only requests on public buckets.

### Phase 3: ACL API endpoints

1. Implement `PutBucketAcl` handler.
2. Implement `GetBucketAcl` handler.
3. Add `x-amz-acl` header handling to `CreateBucket`.
4. Add `?acl` query parameter routing in `handleBucket`.

### Phase 4: Proxy integration

1. Verify that public-bucket logic works correctly when proxying via `maybeProxyToLeader`.
2. Validate metadata read consistency on follower nodes.

## 9. Security Considerations

### 9.1 Write protection

Even for public buckets, write operations always require authentication. `resolveAuth` checks the HTTP method first and routes `PUT`, `POST`, and `DELETE` to the auth flow immediately.

### 9.2 ACL change protection

`PutBucketAcl` requires authentication. To prevent accidental public exposure, a `--s3DenyPublicBuckets` server flag can be added in the future (not required for the initial implementation).

### 9.3 Listing protection

`ListObjectsV2` on a public bucket is allowed without authentication, matching the behavior of AWS S3's `public-read` ACL. This is intended only for use cases where exposing the key listing of a bucket is acceptable.

### 9.4 ListBuckets

`ListBuckets` (`GET /`) always requires authentication even when public buckets exist. The bucket list is never exposed publicly.

## 10. Testing Plan

### 10.1 Unit tests

1. `validateCannedAcl` validation (private, public-read, invalid values).
2. `isReadOnlyRequest` decision logic (GET/HEAD/POST/PUT/DELETE × query parameters).
3. `resolveAuth` branching (public bucket × read / public bucket × write / private bucket × read).
4. `GetBucketAcl` XML response generation.

### 10.2 Integration tests

1. Create bucket with `x-amz-acl: public-read` → anonymous GET succeeds.
2. Anonymous PUT → 403.
3. Anonymous ListObjectsV2 on public bucket → succeeds.
4. Anonymous GetObject on private bucket → 403.
5. `PutBucketAcl` changes public-read → private → anonymous GET now returns 403.
6. Backward compatibility test: bucket with no `acl` field behaves as private.

### 10.3 Compatibility tests

1. `aws s3api put-bucket-acl --acl public-read`
2. `aws s3api get-bucket-acl`
3. Anonymous access via `curl` (no Authorization header).
4. Anonymous download via `aws s3 cp --no-sign-request`.

## 11. Future Extensions

- **Bucket policy**: fine-grained access control via JSON-based conditional policies.
- **`--s3DenyPublicBuckets`**: a cluster-level guardrail that prevents creating public buckets.
- **ACL cache**: reduce metadata reads with an in-memory cache with TTL.
- **Static website hosting**: automatic index.html resolution and custom error pages.
- **CORS configuration**: direct browser access support.

// HTTP client for /admin/api/v1/*. Two contracts the Go server enforces
// that the client side has to honour:
//   1. Session is delivered via the HttpOnly `admin_session` cookie. We
//      never read or store it; the browser attaches it automatically.
//   2. CSRF defence is double-submit cookie: the readable `admin_csrf`
//      cookie value MUST be echoed in the X-Admin-CSRF header on every
//      POST/PUT/DELETE. Mismatch → 403.
//
// Read /admin/api/v1/auth/login first to mint both cookies. After that,
// every call goes through `apiFetch` which decorates mutations with the
// CSRF header and surfaces errors as ApiError so callers can branch on
// status without reparsing.

const apiBase = "/admin/api/v1";

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  constructor(status: number, code: string, message: string) {
    super(message || code);
    this.status = status;
    this.code = code;
  }
}

type Json = Record<string, unknown> | unknown[] | string | number | boolean | null;

type HttpMethod = "GET" | "HEAD" | "POST" | "PUT" | "DELETE";

interface ApiOptions {
  method?: HttpMethod;
  body?: Json;
  query?: Record<string, string | number | undefined>;
  signal?: AbortSignal;
}

function readCsrfCookie(): string | undefined {
  // Cookies are stored unordered; use String.split rather than a regex
  // that would have to escape the cookie name. The CSRF cookie is set
  // without HttpOnly precisely so this code can read it.
  const raw = document.cookie;
  if (!raw) return undefined;
  for (const part of raw.split(";")) {
    const [name, ...rest] = part.trim().split("=");
    if (name === "admin_csrf") {
      return decodeURIComponent(rest.join("="));
    }
  }
  return undefined;
}

// base64UrlEncodeBytes turns a UTF-8 string into the unpadded
// base64-url alphabet the admin S3 object routes accept for the
// {key-b64url} URL segment. Used by the S3 object methods to
// build the URL path safely for arbitrary key strings (including
// non-ASCII, slashes, control chars).
function base64UrlEncodeBytes(input: string): string {
  const bytes = new TextEncoder().encode(input);
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/u, "");
}

// encodeAdminItemKey turns a primary-key attribute map into the
// base64-url segment the admin Dynamo items routes expect.
// Wire-shape sibling of base64UrlEncodeBytes: same alphabet, same
// padding stripped, just JSON-stringifies the input first. Both
// LastEvaluatedKey and the URL {key} segment use this shape, so
// DynamoItemsTab also imports it for the scan-cursor encoder.
export function encodeAdminItemKey(key: Record<string, AdminAttributeValue>): string {
  return base64UrlEncodeBytes(JSON.stringify(key));
}

function buildURL(path: string, query?: ApiOptions["query"]): string {
  const url = new URL(apiBase + path, window.location.origin);
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      if (v === undefined) continue;
      url.searchParams.set(k, String(v));
    }
  }
  return url.pathname + url.search;
}

export async function apiFetch<T>(path: string, opts: ApiOptions = {}): Promise<T> {
  const method = opts.method ?? "GET";
  const headers: Record<string, string> = { Accept: "application/json" };
  let body: BodyInit | undefined;
  if (opts.body !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(opts.body);
  }
  if (method !== "GET" && method !== "HEAD") {
    const csrf = readCsrfCookie();
    if (csrf) headers["X-Admin-CSRF"] = csrf;
  }
  const res = await fetch(buildURL(path, opts.query), {
    method,
    headers,
    body,
    credentials: "same-origin",
    signal: opts.signal,
  });
  if (res.status === 204) {
    return undefined as T;
  }
  const ct = res.headers.get("content-type") ?? "";
  // The server promises JSON for both success and error bodies under
  // /admin/api/v1/*; treat anything else as an unexpected proxy/edge
  // response and surface it without trying to JSON.parse HTML.
  if (!ct.includes("application/json")) {
    if (res.ok) {
      throw new ApiError(res.status, "non_json_response", `unexpected ${ct || "no"} content-type`);
    }
    throw new ApiError(res.status, "non_json_response", `HTTP ${res.status}`);
  }
  const payload = (await res.json()) as Record<string, unknown>;
  if (!res.ok) {
    const code = typeof payload.error === "string" ? payload.error : "request_failed";
    const msg = typeof payload.message === "string" ? payload.message : "";
    throw new ApiError(res.status, code, msg);
  }
  return payload as T;
}

// Resource shapes mirror what the Go handlers emit. Keep them in sync
// with internal/admin/cluster_handler.go (and future dynamo/s3 handlers).
// Fields the backend may not yet populate are typed optional so the
// SPA can render gracefully against the partially-built P1/P2 server.

export interface GroupInfo {
  group_id: number;
  leader_id: string;
  members: string[];
  is_leader: boolean;
}

export interface ClusterInfo {
  node_id: string;
  version: string;
  timestamp: string;
  groups: GroupInfo[];
}

export type Role = "read_only" | "full";

export interface LoginResponse {
  role: Role;
  expires_at: string;
}

// Server emits keys as flat strings (just the attribute name) plus a
// generation counter. Describe and Create return the same shape.
// See internal/admin/dynamo_handler.go DynamoTableSummary.
export interface DynamoGSISummary {
  name: string;
  partition_key: string;
  sort_key?: string;
  projection_type: string;
}

export interface DynamoTable {
  name: string;
  partition_key: string;
  sort_key?: string;
  generation: number;
  global_secondary_indexes?: DynamoGSISummary[];
}

// ListTables returns just the names; SPA fetches Describe per row when
// the user opens the detail page. Matches AWS DynamoDB's ListTables /
// DescribeTable split and lets the list endpoint stay cheap.
export interface DynamoTableList {
  tables: string[];
  next_token?: string;
}

export interface CreateTableAttribute {
  name: string;
  type: "S" | "N" | "B";
}

export interface CreateTableProjection {
  type?: "ALL" | "KEYS_ONLY" | "INCLUDE";
  non_key_attributes?: string[];
}

export interface CreateTableGSI {
  name: string;
  partition_key: CreateTableAttribute;
  sort_key?: CreateTableAttribute;
  projection: CreateTableProjection;
}

export interface CreateTableRequest {
  table_name: string;
  partition_key: CreateTableAttribute;
  sort_key?: CreateTableAttribute;
  gsi?: CreateTableGSI[];
}

// AdminAttributeValue mirrors internal/admin/dynamo_items_handler.go.
// One field set per value per the DynamoDB AttributeValue contract.
// B / BS arrive as base64-encoded strings (encoding/json convention).
// L and M serialize with the type tag present even when empty
// (Gemini medium on PR #813) so the SPA can distinguish "no L field"
// from "empty list".
export interface AdminAttributeValue {
  S?: string;
  N?: string;
  B?: string;
  BOOL?: boolean;
  NULL?: boolean;
  SS?: string[];
  NS?: string[];
  BS?: string[];
  L?: AdminAttributeValue[];
  M?: Record<string, AdminAttributeValue>;
}

export interface AdminItem {
  attributes: Record<string, AdminAttributeValue>;
}

export interface AdminScanItemsResult {
  items: AdminItem[];
  last_evaluated_key?: Record<string, AdminAttributeValue>;
}

export interface AdminScanItemsOptions {
  limit?: number;
  next_cursor?: string;
}

export interface S3Bucket {
  bucket_name: string;
  acl?: string;
  created_at?: string;
}

export interface S3BucketList {
  buckets: S3Bucket[];
  next_token?: string;
}

export interface CreateBucketRequest {
  bucket_name: string;
  acl?: "private" | "public-read";
}

// AdminObject mirrors internal/admin/s3_objects_handler.go. The
// per-object metadata projection list pages return.
export interface AdminObject {
  key: string;
  size: number;
  content_type: string;
  etag: string;
  last_modified: string;
  storage_class: string;
}

export interface AdminObjectListing {
  objects: AdminObject[];
  common_prefixes?: string[];
  next_continuation_token?: string;
}

export interface AdminListObjectsOptions {
  prefix?: string;
  delimiter?: string;
  continuation_token?: string;
  max_keys?: number;
}

// SQS queue admin DTOs (Section 16.2 of the SQS partial design doc).
// `attributes` mirrors the AWS GetQueueAttributes "All" set with
// snake_case keys; `counters` is the typed projection of the three
// Approximate* counters added in Phase 3.A.
export interface SqsQueueCounters {
  visible: number;
  not_visible: number;
  delayed: number;
}

export interface SqsQueueSummary {
  name: string;
  is_fifo: boolean;
  generation: number;
  created_at?: string;
  attributes?: Record<string, string>;
  counters: SqsQueueCounters;
  // True when another queue's RedrivePolicy points at this one.
  is_dlq: boolean;
  // Source queue names that point at this DLQ, sorted lex.
  dlq_sources?: string[];
}

export interface SqsQueueList {
  queues: string[];
}

// SqsPeekedAttribute mirrors AWS's typed MessageAttribute shape;
// binary_value arrives base64-encoded.
export interface SqsPeekedAttribute {
  data_type: string;
  string_value?: string;
  binary_value?: string;
}

export interface SqsPeekedMessage {
  message_id: string;
  body: string;
  body_truncated: boolean;
  body_original_size: number;
  sent_timestamp: string;
  receive_count: number;
  group_id?: string;
  deduplication_id?: string;
  attributes?: Record<string, SqsPeekedAttribute>;
}

export interface SqsPeekResult {
  messages: SqsPeekedMessage[];
  // Omitted when the walk has fully completed for this MVCC snapshot.
  next_cursor?: string;
}

export interface SqsPeekOptions {
  limit?: number;
  cursor?: string;
  body_max_bytes?: number;
}

// KeyViz wire shapes mirror internal/admin/keyviz_handler.go
// (KeyVizMatrix / KeyVizRow). Go []byte fields arrive as
// base64-encoded strings via encoding/json — keep them as `string` on
// the client and decode lazily where preview labels need raw bytes.
export type KeyVizSeries = "reads" | "writes" | "read_bytes" | "write_bytes";

export interface KeyVizRow {
  bucket_id: string;
  start: string;
  end: string;
  aggregate: boolean;
  route_ids?: number[];
  route_ids_truncated?: boolean;
  route_count: number;
  values: number[];
  // Phase 2-C row-level conflict flag (always present on the wire,
  // defaults to false). True when ≥2 nodes reported a non-zero
  // value for the same cell — typically a leadership flip mid-
  // window. Per design 4.2 / PR-1, this is a row-level signal; it
  // moves to per-cell when the proto extension lands in 2-C+.
  conflict?: boolean;
}

// Per-node entry in the KeyVizMatrix.fanout block. OK=true means
// the node returned a parseable matrix; OK=false carries the
// reason (timeout, refused, 5xx body, JSON decode failure). Self
// always reports ok=true since the local matrix is computed in-
// process.
export interface KeyVizFanoutNode {
  node: string;
  ok: boolean;
  error?: string;
}

// Per-response fan-out summary attached to KeyVizMatrix.fanout
// when the operator configured --keyvizFanoutNodes. Absent when
// fan-out is disabled. nodes is ordered self-first, then in
// --keyvizFanoutNodes order. Responded counts ok=true entries.
export interface KeyVizFanoutResult {
  nodes: KeyVizFanoutNode[];
  responded: number;
  expected: number;
}

export interface KeyVizMatrix {
  column_unix_ms: number[];
  rows: KeyVizRow[];
  series: KeyVizSeries;
  generated_at: string;
  fanout?: KeyVizFanoutResult;
}

export interface KeyVizParams {
  series?: KeyVizSeries;
  from_unix_ms?: number;
  to_unix_ms?: number;
  rows?: number;
}

export const api = {
  login: (access_key: string, secret_key: string) =>
    apiFetch<LoginResponse>("/auth/login", {
      method: "POST",
      body: { access_key, secret_key },
    }),
  logout: () => apiFetch<void>("/auth/logout", { method: "POST" }),
  cluster: (signal?: AbortSignal) =>
    apiFetch<ClusterInfo>("/cluster", { signal }),
  listTables: (next_token?: string, signal?: AbortSignal) =>
    apiFetch<DynamoTableList>("/dynamo/tables", { query: { next_token }, signal }),
  describeTable: (name: string, signal?: AbortSignal) =>
    apiFetch<DynamoTable>(`/dynamo/tables/${encodeURIComponent(name)}`, { signal }),
  createTable: (req: CreateTableRequest) =>
    apiFetch<DynamoTable>("/dynamo/tables", { method: "POST", body: req as unknown as Json }),
  deleteTable: (name: string) =>
    apiFetch<void>(`/dynamo/tables/${encodeURIComponent(name)}`, { method: "DELETE" }),
  scanItems: (table: string, opts?: AdminScanItemsOptions, signal?: AbortSignal) =>
    apiFetch<AdminScanItemsResult>(
      `/dynamo/tables/${encodeURIComponent(table)}/items`,
      { query: { limit: opts?.limit, next_cursor: opts?.next_cursor }, signal },
    ),
  // Items use base64-url-encoded JSON of the primary-key attribute
  // map as the URL segment. Base64-url is the same encoding the rest
  // of the admin API uses for arbitrary-bytes path segments, so
  // PathEscape on the segment is a no-op.
  getItem: (table: string, key: Record<string, AdminAttributeValue>, signal?: AbortSignal) =>
    apiFetch<AdminItem>(
      `/dynamo/tables/${encodeURIComponent(table)}/items/${encodeAdminItemKey(key)}`,
      { signal },
    ),
  putItem: (table: string, key: Record<string, AdminAttributeValue>, item: AdminItem) =>
    apiFetch<void>(
      `/dynamo/tables/${encodeURIComponent(table)}/items/${encodeAdminItemKey(key)}`,
      { method: "PUT", body: item as unknown as Json },
    ),
  deleteItem: (table: string, key: Record<string, AdminAttributeValue>) =>
    apiFetch<void>(
      `/dynamo/tables/${encodeURIComponent(table)}/items/${encodeAdminItemKey(key)}`,
      { method: "DELETE" },
    ),
  listBuckets: (next_token?: string, signal?: AbortSignal) =>
    apiFetch<S3BucketList>("/s3/buckets", { query: { next_token }, signal }),
  describeBucket: (name: string, signal?: AbortSignal) =>
    apiFetch<S3Bucket>(`/s3/buckets/${encodeURIComponent(name)}`, { signal }),
  createBucket: (req: CreateBucketRequest) =>
    apiFetch<S3Bucket>("/s3/buckets", { method: "POST", body: req as unknown as Json }),
  putBucketAcl: (name: string, acl: "private" | "public-read") =>
    apiFetch<void>(`/s3/buckets/${encodeURIComponent(name)}/acl`, {
      method: "PUT",
      body: { acl },
    }),
  deleteBucket: (name: string) =>
    apiFetch<void>(`/s3/buckets/${encodeURIComponent(name)}`, { method: "DELETE" }),
  listObjects: (bucket: string, opts?: AdminListObjectsOptions, signal?: AbortSignal) =>
    apiFetch<AdminObjectListing>(
      `/s3/buckets/${encodeURIComponent(bucket)}/objects`,
      {
        query: {
          prefix: opts?.prefix,
          delimiter: opts?.delimiter,
          continuation_token: opts?.continuation_token,
          max_keys: opts?.max_keys,
        },
        signal,
      },
    ),
  // The object body endpoints carry raw bytes (application/octet-
  // stream), so they bypass the JSON-only apiFetch and use a direct
  // fetch with the CSRF header. base64UrlEncodeBytes turns the key
  // into the {key-b64url} segment the server's
  // decodeAdminObjectKeySegment expects.
  downloadObjectURL: (bucket: string, key: string): string =>
    `${apiBase}/s3/buckets/${encodeURIComponent(bucket)}/objects/${base64UrlEncodeBytes(key)}`,
  putObject: async (
    bucket: string,
    key: string,
    body: Blob,
    contentType?: string,
  ): Promise<void> => {
    const headers: Record<string, string> = {};
    if (contentType) headers["Content-Type"] = contentType;
    const csrf = readCsrfCookie();
    if (csrf) headers["X-Admin-CSRF"] = csrf;
    const res = await fetch(
      `${apiBase}/s3/buckets/${encodeURIComponent(bucket)}/objects/${base64UrlEncodeBytes(key)}`,
      { method: "PUT", body, headers, credentials: "same-origin" },
    );
    // Treat any 2xx as success rather than just 204. The current
    // Go handler returns 204 (no body) but the contract intent is
    // any successful upload; a future change that returns the
    // metadata on 200 would otherwise be misclassified as a
    // non-JSON error (Gemini medium on PR #816).
    if (res.ok) return;
    const ct = res.headers.get("content-type") ?? "";
    if (!ct.includes("application/json")) {
      throw new ApiError(res.status, "non_json_response", `HTTP ${res.status}`);
    }
    const payload = (await res.json()) as Record<string, unknown>;
    throw new ApiError(
      res.status,
      typeof payload.error === "string" ? payload.error : "request_failed",
      typeof payload.message === "string" ? payload.message : "",
    );
  },
  deleteObject: (bucket: string, key: string) =>
    apiFetch<void>(
      `/s3/buckets/${encodeURIComponent(bucket)}/objects/${base64UrlEncodeBytes(key)}`,
      { method: "DELETE" },
    ),
  listQueues: (signal?: AbortSignal) =>
    apiFetch<SqsQueueList>("/sqs/queues", { signal }),
  describeQueue: (name: string, signal?: AbortSignal) =>
    apiFetch<SqsQueueSummary>(`/sqs/queues/${encodeURIComponent(name)}`, { signal }),
  deleteQueue: (name: string) =>
    apiFetch<void>(`/sqs/queues/${encodeURIComponent(name)}`, { method: "DELETE" }),
  // Non-destructive peek of currently-visible messages. Server clamps
  // limit to [1, 100] and body_max_bytes to [256, 262144].
  peekQueue: (name: string, opts?: SqsPeekOptions, signal?: AbortSignal) =>
    apiFetch<SqsPeekResult>(`/sqs/queues/${encodeURIComponent(name)}/messages`, {
      query: {
        limit: opts?.limit,
        cursor: opts?.cursor,
        body_max_bytes: opts?.body_max_bytes,
      },
      signal,
    }),
  // Drains the queue's messages while leaving meta/ARN/RedrivePolicy intact.
  // 60-second rate limit per queue: second purge inside the window → 429.
  purgeQueue: (name: string) =>
    apiFetch<void>(`/sqs/queues/${encodeURIComponent(name)}/messages`, { method: "DELETE" }),
  keyVizMatrix: (params: KeyVizParams, signal?: AbortSignal) =>
    apiFetch<KeyVizMatrix>("/keyviz/matrix", {
      query: {
        series: params.series,
        from_unix_ms: params.from_unix_ms,
        to_unix_ms: params.to_unix_ms,
        rows: params.rows,
      },
      signal,
    }),
};

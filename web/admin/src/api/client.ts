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

export const api = {
  login: (access_key: string, secret_key: string) =>
    apiFetch<LoginResponse>("/auth/login", {
      method: "POST",
      body: { access_key, secret_key },
    }),
  logout: () => apiFetch<void>("/auth/logout", { method: "POST" }),
  cluster: (signal?: AbortSignal) =>
    apiFetch<ClusterInfo>("/cluster", { signal }),
  listTables: (next_token?: string) =>
    apiFetch<DynamoTableList>("/dynamo/tables", { query: { next_token } }),
  describeTable: (name: string) =>
    apiFetch<DynamoTable>(`/dynamo/tables/${encodeURIComponent(name)}`),
  createTable: (req: CreateTableRequest) =>
    apiFetch<DynamoTable>("/dynamo/tables", { method: "POST", body: req as unknown as Json }),
  deleteTable: (name: string) =>
    apiFetch<void>(`/dynamo/tables/${encodeURIComponent(name)}`, { method: "DELETE" }),
  listBuckets: (next_token?: string) =>
    apiFetch<S3BucketList>("/s3/buckets", { query: { next_token } }),
  describeBucket: (name: string) =>
    apiFetch<S3Bucket>(`/s3/buckets/${encodeURIComponent(name)}`),
  createBucket: (req: CreateBucketRequest) =>
    apiFetch<S3Bucket>("/s3/buckets", { method: "POST", body: req as unknown as Json }),
  putBucketAcl: (name: string, acl: "private" | "public-read") =>
    apiFetch<void>(`/s3/buckets/${encodeURIComponent(name)}/acl`, {
      method: "PUT",
      body: { acl },
    }),
  deleteBucket: (name: string) =>
    apiFetch<void>(`/s3/buckets/${encodeURIComponent(name)}`, { method: "DELETE" }),
};

# elastickv admin dashboard — operator guide

This document covers configuration and day-2 operation of the admin
HTTP listener. Architecture and design rationale live in
[docs/design/2026_04_24_proposed_admin_dashboard.md](design/2026_04_24_proposed_admin_dashboard.md);
read that first if you're touching the code.

## What the admin dashboard is

A separate HTTP listener (default `127.0.0.1:8080`) that exposes a
React SPA + JSON API for inspecting the cluster and managing
DynamoDB tables / S3 buckets without having to construct SigV4
requests. It is **disabled by default**: set `-adminEnabled` to turn
it on.

The listener is independent of the data-plane DynamoDB
(`-dynamoAddress`) and S3 (`-s3Address`) endpoints — credentials,
TLS, and auth are configured separately.

## Quick start (loopback dev)

The minimum invocation that produces a working dashboard:

```sh
./elastickv \
  -raftId=n1 -raftBootstrap \
  -dynamoAddress=127.0.0.1:8000 \
  -s3Address=127.0.0.1:9000 \
  -s3CredentialsFile=/path/to/creds.json \
  -adminEnabled \
  -adminSessionSigningKeyFile=/path/to/admin-hs256.b64 \
  -adminFullAccessKeys=AKIA_ADMIN
```

Then open `http://127.0.0.1:8080/admin/` in a browser and log in
with the access key + secret pair from the credentials file.

## Configuration reference

### Required when `-adminEnabled=true`

| Flag | Description |
|---|---|
| `-adminEnabled` | Master on/off switch. Default `false`. |
| `-adminSessionSigningKey` *or* `-adminSessionSigningKeyFile` *or* `ELASTICKV_ADMIN_SESSION_SIGNING_KEY` | Cluster-shared base64-encoded HS256 key — **exactly 64 raw bytes** (88 base64 chars with standard padding, or 86 with `RawURLEncoding`). The validator rejects any other length at startup with a precise error message. **Must be the same on every node** — JWTs minted by node A are verified by node B during follower→leader forwarding, so a mismatch breaks the dashboard's read paths on follower nodes. The `*File` / env-var forms keep the secret out of `/proc/<pid>/cmdline`. |
| `-s3CredentialsFile` | JSON file with at least one access key + secret key pair. Same file the S3 adapter uses for SigV4; the admin dashboard reuses it for login authentication. |
| `-adminFullAccessKeys` *and/or* `-adminReadOnlyAccessKeys` | Comma-separated allow-lists. Only access keys listed here may log into the dashboard, even if their SigV4 secret validates against the credentials file. Keys must not appear in both lists. |

### Optional

| Flag | Description |
|---|---|
| `-adminListen` | host:port for the admin listener. Defaults to `127.0.0.1:8080`. |
| `-adminTLSCertFile` / `-adminTLSKeyFile` | PEM cert + key. Both must be set together; a partial config fails validation at startup. |
| `-adminAllowPlaintextNonLoopback` | Explicit opt-out for the non-loopback-without-TLS startup hard-error. **Strongly discouraged** — lets the listener accept plaintext on a non-loopback bind. **Does not** affect the cookie `Secure` attribute (that is `-adminAllowInsecureDevCookie` below); a deployment that sets only this flag will mint `Secure` cookies that the browser refuses to send over the plaintext channel, breaking session lifetime end-to-end. Pair it with `-adminAllowInsecureDevCookie` if the goal is a working plaintext rig. |
| `-adminSessionSigningKeyPrevious` *or* `-adminSessionSigningKeyPreviousFile` *or* `ELASTICKV_ADMIN_SESSION_SIGNING_KEY_PREVIOUS` | Previous HS256 key used only for verification during a rotation window. New tokens always use the primary key; existing tokens minted under the previous key continue to verify until they expire. |
| `-adminAllowInsecureDevCookie` | Mints session cookies without `Secure` for local plaintext development. Do not set on any deployment that touches a network. |

### Hard-error startup conditions

The process fails to start (non-zero exit) when:

- `-adminEnabled=true` but `-s3CredentialsFile` is empty or missing, or its parsed map has zero entries — without credentials every login is rejected, and "locked-down admin" is `-adminEnabled=false`.
- `-adminEnabled=true` but `-adminSessionSigningKey` (and the `*File` / env var) all decode to empty.
- `-adminEnabled=true` but `-adminListen` is empty or not a valid host:port.
- `-adminTLSCertFile` xor `-adminTLSKeyFile` is set (partial TLS config).
- `-adminListen` is bound to a non-loopback address, TLS is not configured, **and** `-adminAllowPlaintextNonLoopback` is not set. The error message names the flag combinations that resolve it.
- `-adminFullAccessKeys` and `-adminReadOnlyAccessKeys` overlap (the same access key listed in both).

These are deliberate — silent fallbacks to "auth disabled" or "TLS
off" would downgrade security guarantees the operator is unaware of.

## TLS setup

Two supported topologies:

### A. Loopback only (`127.0.0.1` / `::1`)

No TLS required. By default the dashboard mints cookies with
`Secure=true`, which most modern browsers accept on the loopback
origin even without TLS (the loopback-is-trusted policy). If a
specific browser refuses the cookie in this configuration, set
`-adminAllowInsecureDevCookie` to mint without `Secure` — the flag
is intentionally distinct from `-adminAllowPlaintextNonLoopback`
because the listener can be plaintext for entirely separate
reasons (loopback) than the cookie needing to drop `Secure`.

### B. Reachable address with TLS

Set `-adminListen` to the public bind, plus `-adminTLSCertFile` and
`-adminTLSKeyFile`. TLS 1.2+ is enforced. Cookies are issued with
`Secure; SameSite=Strict; HttpOnly`.

Cert renewal: the listener picks up the cert files at startup only;
restart the process after rotating certs. Hot-reload is not
implemented (out of scope for the dashboard's maintenance model).

### Discouraged: plaintext non-loopback

`-adminAllowPlaintextNonLoopback` exists as an escape hatch for
short-lived test deployments. The session JWT and its bearer cookie
travel in clear text in this mode; anyone on the path can replay
the token until it expires. Do not enable on a long-running
deployment.

A working plaintext rig also needs `-adminAllowInsecureDevCookie` —
otherwise the dashboard mints cookies with `Secure=true` and the
browser refuses to send them back over plaintext, so login appears
to succeed but every subsequent request 401s. The two flags are
deliberately separate so a misconfigured deployment fails closed
on either axis (TLS guard or cookie attribute) rather than
silently downgrading both at once.

## Roles

Two roles, both checked against the live `-adminFullAccessKeys` /
`-adminReadOnlyAccessKeys` lists on **every** state-changing
request (not just at login):

- **read-only** — may list / describe Dynamo tables and S3 buckets, view cluster status. Cannot create, mutate ACL, or delete.
- **full** — adds POST / PUT / DELETE on `/dynamo/tables` and `/s3/buckets`.

A key revoked from `-adminFullAccessKeys` immediately loses
write access on the next request — the dashboard does not wait for
the token to expire. The token's role claim is treated as a hint;
the live role index is authoritative.

## API surface

All endpoints are under `/admin/api/v1/`. Authentication: cookie
session minted by `POST /auth/login`; CSRF: double-submit token in
`admin_csrf` cookie + `X-Admin-CSRF` header on every state-changing
method.

| Method | Path | Role | Notes |
|---|---|---|---|
| `POST` | `/auth/login` | none | Body `{access_key, secret_key}`. Sets `admin_session` and `admin_csrf` cookies. |
| `POST` | `/auth/logout` | any | Invalidates the session cookie. |
| `GET` | `/cluster` | any | Node ID, Raft leader, version. |
| `GET` | `/dynamo/tables` | any | Paginated list. `?limit=` (default 100, max 1000). |
| `POST` | `/dynamo/tables` | full | Body schema in design 4.2. |
| `GET` | `/dynamo/tables/{name}` | any | Schema + GSI summary. |
| `DELETE` | `/dynamo/tables/{name}` | full | 204 on success. |
| `GET` | `/s3/buckets` | any | Paginated list with the same `?limit=` semantics. |
| `POST` | `/s3/buckets` | full | Body `{bucket_name, acl?}`. ACL omitted defaults to `private`. |
| `GET` | `/s3/buckets/{name}` | any | Bucket meta + ACL. |
| `PUT` | `/s3/buckets/{name}/acl` | full | Body `{acl}`. Only `private` and `public-read` are accepted. |
| `DELETE` | `/s3/buckets/{name}` | full | 204 on success. The bucket must be empty (no objects); a non-empty bucket returns 409 `bucket_not_empty`. |

## Follower → leader forwarding

Writes (`POST` / `PUT` / `DELETE`) require the local node to be the
Raft leader. When the SPA's request hits a follower, the dashboard
transparently forwards the call to the leader over an internal
gRPC service (`AdminForward`). The leader re-validates the
principal against its own `adminFullAccessKeys` list before
acting — a follower cannot smuggle a downgraded key past the
leader's view.

This means there is **no need to point the SPA at a specific
node**: any node with `-adminEnabled` can serve the dashboard.
Operators that fan out behind a load balancer get the same
behaviour as a single-node cluster, with one caveat below.

### Follower forwarding caveat: rolling configuration changes

A configuration change (e.g. adding `AKIA_NEW` to
`-adminFullAccessKeys`) must propagate to **every node** before
the new key works against any follower's dashboard. During the
rollout window:

- A login against a node that has not yet been restarted with the new flags fails with 403.
- A token minted by an updated node, replayed against a not-yet-updated node, will be re-validated against that node's stale role list. If the key is missing on the older node, the request fails with 403 even though the token is structurally valid.

The dashboard does not have an automatic role-refresh path — restart
each node after editing the access-key flags.

### Election-period 503

When the leader steps down mid-write (or has not yet been elected
after a fresh start), the forwarder cannot reach a leader and the
SPA receives `503 Service Unavailable` with a `Retry-After: 1`
header. The current SPA client (`web/admin/src/api/client.ts`)
makes a single `fetch` call with no automatic retry, so the user
sees the 503 surfaced directly and must re-issue the action. The
`Retry-After: 1` header is still emitted so a future client (or an
external operator script driving the JSON API) can implement the
one-second back-off the server is asking for. Operators
investigating "intermittent 503s" should look at Raft leader-churn
logs first.

## Audit log

Every state-changing admin request emits structured slog lines at
`INFO` level under the `admin_audit` key on the leader's stdout (or
wherever the process slog handler is wired). A protected-chain
mutation (Dynamo / S3 / cluster / keyviz writes) typically produces
**two** audit lines: one operation-specific line from the source
that performed the mutation, plus one generic HTTP-shaped line from
the `Audit` middleware. Auth endpoints (`/auth/login`, `/auth/logout`)
produce **one** line — the action-specific one from `AuthService` —
because the generic middleware is intentionally not wrapped around
them (see the per-shape section below for why). The shapes differ
by source — log parsers should treat the `admin_audit` key as a
union and dispatch on the fields present.

**`Audit` middleware** — emitted for non-GET/HEAD/OPTIONS requests
on the **protected mux chain** (Dynamo, S3, cluster, keyviz) after
`SessionAuth` accepts the session, but **before** `CSRFDoubleSubmit`
runs. That ordering is deliberate: a CSRF-rejected protected
request still produces an audit line because the actor is already
known, but an unauthenticated request (no / invalid session) is
rejected at `SessionAuth` and never reaches the middleware. The
following endpoints are **not** wrapped by this middleware and rely
on their own `admin_audit` emission instead:

- `/auth/login` — runs without a pre-existing session, so the
  generic middleware cannot identify the actor; `AuthService`
  emits `admin_audit action=login` (success and failure) directly.
- `/auth/logout` — runs through `protectNoAudit` so logout produces
  exactly one `admin_audit action=logout` line from `AuthService`
  rather than two (a generic line plus the action-specific one).

For requests that *do* reach the middleware, the line is always
present on the node that received the HTTP request — which may be
a follower if the request was then forwarded:

```
admin_audit actor=AKIA_ADMIN role=full method=POST path=/admin/api/v1/buckets status=201 remote=10.0.0.7:51234 duration=8.2ms
```

**`S3Handler` operation line** — emitted on the leader after a
successful bucket mutation. Only the S3 admin path emits these; the
DynamoDB admin path relies on the middleware line plus the forwarded
line below for its audit trail:

```
admin_audit actor=AKIA_ADMIN role=full operation=create_bucket bucket=my-bucket
admin_audit actor=AKIA_ADMIN role=full operation=put_bucket_acl bucket=my-bucket acl=public-read
admin_audit actor=AKIA_ADMIN role=full operation=delete_bucket bucket=my-bucket
```

**`ForwardServer` operation line** — emitted on the leader when a
follower forwarded the request via `AdminForward`. Carries the
originating follower's node ID in `forwarded_from`. Covers both
DynamoDB and S3 admin operations:

```
admin_audit actor=AKIA_ADMIN role=full forwarded_from=n2 operation=create_table table=orders
admin_audit actor=AKIA_ADMIN role=full forwarded_from=n2 operation=delete_table table=orders
admin_audit actor=AKIA_ADMIN role=full forwarded_from=n2 operation=put_bucket_acl bucket=my-bucket acl=public-read
```

CR and LF in `forwarded_from` are stripped at the entry point — a
hostile follower cannot split a single audit line into two by
smuggling control characters into its node ID.

Login and logout emit their own `admin_audit` lines with
`action=login` / `action=logout` (plus `actor`, `claimed_actor`,
`remote`, `status`) so the JWT's lifetime can be correlated with the
mutations it authorised.

## Troubleshooting

### "admin listener is enabled but no static credentials are configured"

Either `-s3CredentialsFile` is unset or the file parses to an empty
map. Check the file exists and contains at least one entry:
```json
{"credentials":[{"access_key_id":"AKIA_ADMIN","secret_access_key":"..."}]}
```

### "is not loopback but TLS is not configured"

Default-deny safety net. Either set `-adminTLSCertFile` +
`-adminTLSKeyFile`, or pass `-adminAllowPlaintextNonLoopback` (and
read the TLS section above before doing so).

### Login returns 401 invalid_credentials

The access key + secret pair did not match an entry in
`-s3CredentialsFile`. Either the access key is unknown or the secret
is wrong. Verify the credentials file is the one the running process
loaded (it is read once at startup) and that the secret matches
exactly — secrets are compared with `subtle.ConstantTimeCompare`, so
trailing whitespace counts.

### Login returns 403 forbidden

The credentials matched, but the access key is not listed in either
`-adminFullAccessKeys` or `-adminReadOnlyAccessKeys`. This is a
distinct case from the 401 above: the operator has valid SigV4
credentials for the data plane but no admin role assignment. Add the
key to one of the role flags and **restart every node** so each
node's live role index picks up the change.

### Write returns 403 forbidden

The principal's role is read-only. Move the access key into
`-adminFullAccessKeys` (and remove it from
`-adminReadOnlyAccessKeys`), then **restart every node** so each
node's live role index picks up the change.

### Write returns 503 leader_unavailable

The Raft cluster is mid-election. Re-issue the request after the
`Retry-After: 1` header tells you to. If it persists past one or
two seconds, check Raft leader status via the admin
`/admin/api/v1/cluster` endpoint or `cmd/elastickv-admin`.

### `bucket_not_empty` on DELETE

The dashboard cannot force a recursive delete by design — the
SPA's job is to surface the error and guide the operator to clean
up first. Use the SigV4 S3 path (`aws s3 rm s3://<bucket> --recursive`)
to drain the bucket, then retry the DELETE on the dashboard.

### Stuck SPA / blank screen

The dashboard ships a placeholder `internal/admin/dist/index.html`
that renders a "bundle missing" page when `make` was run without
the SPA build step. Run `cd web/admin && npm install && npm run build`
to populate the embedded `dist` directory, then rebuild the binary.

## Cross-references

- Design rationale: [docs/design/2026_04_24_proposed_admin_dashboard.md](design/2026_04_24_proposed_admin_dashboard.md) (renamed to `_partial_` in PR #675; this link will follow once that lands)
- Architecture overview: [docs/architecture_overview.md](architecture_overview.md)
- AdminForward RPC contract: `proto/admin_forward.proto`

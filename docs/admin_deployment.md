# elastickv admin dashboard — deployment runbook

This document covers **operational procedures** for the admin
dashboard: how to deploy with admin enabled, how operators log in,
how to roll out role / key / TLS changes safely, and which
runbooks fire when something breaks.

For the configuration reference (per-flag semantics, hard-error
startup conditions, audit log shape, troubleshooting catalogue),
read [`docs/admin.md`](admin.md) first — this doc assumes you have
already skimmed it.

For design rationale, see
[`docs/design/2026_04_24_partial_admin_dashboard.md`](design/2026_04_24_partial_admin_dashboard.md).

---

## 1. Pre-deployment checklist

Before turning the admin listener on, prepare the following on a
machine you control (not on a shared bastion):

### 1.1 HS256 session signing key

Exactly **64 raw bytes**. The validator rejects any other length at
startup with a precise error.

```sh
# Generate 64 random bytes, base64-encode with standard padding (88 chars).
openssl rand 64 | base64 > admin-hs256.b64

# Or with RawURLEncoding (86 chars, no padding) — both are accepted.
openssl rand 64 | base64 | tr '+/' '-_' | tr -d '=' > admin-hs256.b64
```

Verify the length:

```sh
# Standard padding: 88 chars including newline trailer = 89 bytes total
wc -c admin-hs256.b64
# Decoded byte count:
base64 -d admin-hs256.b64 | wc -c   # → 64
```

Distribute the same file to every node — under `/etc/elastickv/`
on each remote host with `0400` permissions owned by the runtime
user. The dashboard read paths re-validate JWTs locally on each
node, so a key mismatch breaks follower → leader forwarding the
moment a follower is elected leader.

### 1.2 SigV4 credentials file

The same `-s3CredentialsFile` the data-plane S3 adapter consumes.
Add an entry per admin operator (each operator gets a distinct
access key):

```json
{
  "credentials": [
    {"access_key_id": "AKIA_ADMIN_ALICE",   "secret_access_key": "..."},
    {"access_key_id": "AKIA_OBSERVER_BOB",  "secret_access_key": "..."}
  ]
}
```

The dashboard authenticates against this file using the same
`subtle.ConstantTimeCompare` the SigV4 path uses; secrets must
match exactly (trailing whitespace counts).

### 1.3 Role allow-lists

Decide who gets `full` (mutate) vs `read-only` (inspect) and write
the access keys into two comma-separated lists. The same key must
NOT appear in both — startup rejects an overlap with a hard error.

```
ADMIN_FULL_ACCESS_KEYS="AKIA_ADMIN_ALICE"
ADMIN_READ_ONLY_ACCESS_KEYS="AKIA_OBSERVER_BOB"
```

The role index is checked **on every state-changing request**, not
only at login. Promotions and demotions take effect on the next
admin write — but the JWT freezes the role at login (the live store
can only narrow the JWT's role, never widen it; see
[`docs/admin.md`](admin.md) §Roles).

### 1.4 Admin TLS material (only if non-loopback)

If the listener will bind to anything other than `127.0.0.1` /
`::1`, prepare a PEM cert + key pair on each remote host. Both
files must be readable by the runtime user. Hot-reload is **not**
supported — rotating certs requires a full container restart, so
prefer cert lifetimes that align with your existing rolling-update
cadence.

If you genuinely need plaintext on a non-loopback bind for a
short-lived test rig, see §3.3 below for the two-flag escape hatch
(both `ADMIN_ALLOW_PLAINTEXT_NON_LOOPBACK=true` and
`ADMIN_ALLOW_INSECURE_DEV_COOKIE=true` are needed — the former
without the latter is a broken-end-to-end configuration).

---

## 2. First-time deployment

Use `scripts/rolling-update.sh` for both first-time deploys and
subsequent rollouts. The script bind-mounts the referenced files
into the container read-only, validates that each path exists on
the remote host before stopping the previous container, and passes
the corresponding flags to the daemon.

### 2.1 Configure the deploy environment

Copy `scripts/rolling-update.env.example` and fill in the
`ADMIN_*` block:

```sh
# admin-cluster.env
NODES="n1=raft-1.example,n2=raft-2.example,n3=raft-3.example"
IMAGE="ghcr.io/bootjp/elastickv:v0.42.0"
SSH_USER="deploy"
S3_CREDENTIALS_FILE="/etc/elastickv/s3-credentials.json"

ADMIN_ENABLED="true"
ADMIN_ADDRESS="0.0.0.0:8080"
ADMIN_FULL_ACCESS_KEYS="AKIA_ADMIN_ALICE"
ADMIN_READ_ONLY_ACCESS_KEYS="AKIA_OBSERVER_BOB"
ADMIN_SESSION_SIGNING_KEY_FILE="/etc/elastickv/admin-hs256.b64"
ADMIN_TLS_CERT_FILE="/etc/elastickv/admin-tls.crt"
ADMIN_TLS_KEY_FILE="/etc/elastickv/admin-tls.key"
```

Place the credentials file, signing key, and TLS pair on every
remote host at the configured paths *before* invoking the script —
the validation pass aborts the rollout on the first node where any
referenced path is missing, so you do not get partial-cluster
states.

### 2.2 Run the rollout

```sh
ROLLING_UPDATE_ENV_FILE=admin-cluster.env ./scripts/rolling-update.sh
```

The script rolls one node at a time:

1. Transfers Raft leadership away from the node (if it is leader).
2. Stops the existing container.
3. Starts a new container with the admin flags + bind-mounts.
4. Waits for the gRPC health endpoint to come up.
5. Moves on to the next node after `ROLLING_DELAY_SECONDS`.

If a node fails the health check, the script exits non-zero
**before** touching the next node — the cluster is left with at
most one partially-updated node, which preserves quorum on a
3-node deployment.

### 2.3 Verify the listener

After the rollout finishes, hit the listener from a host that can
reach the admin port:

```sh
# Loopback (TLS optional)
curl -sf http://127.0.0.1:8080/admin/healthz

# Non-loopback (TLS required by default)
curl -sf https://raft-1.example:8080/admin/healthz
```

The endpoint returns `204 No Content` when the listener is up.

---

## 3. Operator login workflow

### 3.1 Browser flow (the primary path)

1. Open `https://<admin-host>:<admin-port>/admin/` in a browser.
2. Enter the access key + secret pair from
   `-s3CredentialsFile`. The SPA POSTs to `/admin/api/v1/auth/login`,
   the server validates the credentials with constant-time
   comparison, and on success sets two cookies — the session JWT
   and the CSRF double-submit token, both `Secure; HttpOnly;
   SameSite=Strict; Path=/admin/`.
3. The dashboard becomes available. Every state-changing request
   carries the CSRF token in a header so a cross-site page cannot
   force an action against the session.

### 3.2 Login failure modes (operator-visible)

| HTTP code | Cause | Remediation |
|---|---|---|
| `401 invalid_credentials` | access key + secret pair did not match `-s3CredentialsFile` | Verify the credentials file is the one the running process loaded (it is read once at startup) and that the secret matches exactly. |
| `403 forbidden` (login) | credentials matched, but the access key is not in `-adminFullAccessKeys` / `-adminReadOnlyAccessKeys` | Add the key to one of the role flags and **restart every node**. |
| `403 forbidden` (write) | session is read-only | Move the access key into `-adminFullAccessKeys`, then restart every node. |
| `503 leader_unavailable` | Raft mid-election | Re-issue after a moment; the SPA does not auto-retry today (see `docs/admin.md` §Election-period 503). |

The leader's audit log distinguishes 401 vs 403 with the precise
reason; the dashboard does not surface it for security. See
[`docs/admin.md`](admin.md) §Audit log for the slog shapes.

### 3.3 Plaintext development setups

**Loopback only** (the default `127.0.0.1:8080` bind): no TLS
required. Cookies are minted with `Secure=true`; modern browsers
accept them on the loopback origin without TLS via the
loopback-is-trusted policy. If a specific browser refuses the
cookie, set `ADMIN_ALLOW_INSECURE_DEV_COOKIE=true` to drop the
`Secure` flag.

**Plaintext non-loopback** (test rig only): set both
`ADMIN_ALLOW_PLAINTEXT_NON_LOOPBACK=true` AND
`ADMIN_ALLOW_INSECURE_DEV_COOKIE=true`. The former bypasses the
startup TLS guard; the latter drops `Secure` from the cookie. Each
flag is independent on purpose so a misconfigured deployment fails
closed on either axis instead of silently downgrading both.

---

## 4. Day-2 operations

### 4.1 Adding or removing an admin key

1. Edit `ADMIN_FULL_ACCESS_KEYS` / `ADMIN_READ_ONLY_ACCESS_KEYS`
   in your env file. The key must NOT appear in both lists —
   startup will reject the overlap.
2. If the key is being added, also add the access key + secret to
   `S3_CREDENTIALS_FILE` on every node. (Removing a key only
   requires the role-flag edit; the credentials file can be
   trimmed later.)
3. Re-run `scripts/rolling-update.sh`.
4. The change takes effect on each node immediately at restart.
   During the rollout window, sessions whose JWT was minted on a
   not-yet-restarted node will see role enforcement against the
   stale list on those nodes — operators may briefly see 403s on
   read-only operations replayed against a not-yet-updated node.

If you cannot tolerate the rollout-window inconsistency, drain
admin traffic before starting the rollout (close all dashboard
tabs and wait for the JWT TTL to expire on every issued token).

### 4.2 Rotating the HS256 signing key

The cluster supports a **two-key verification window** so a
rotation does not invalidate existing sessions.

```
                     primary           previous
  before rotation:   key A             (none)
  rotation step 1:   key B             key A     ← roll out
  rotation step 2:   key B             (none)    ← roll out (after JWT TTL)
```

Procedure:

1. Generate the new key:
   `openssl rand 64 | base64 > admin-hs256-new.b64`.
2. Copy `admin-hs256-new.b64` to every node at a stable path
   (e.g. `/etc/elastickv/admin-hs256.next.b64`) and the previous
   key remains at its existing path.
3. Edit the env file:
   ```
   ADMIN_SESSION_SIGNING_KEY_FILE="/etc/elastickv/admin-hs256.next.b64"
   ADMIN_SESSION_SIGNING_KEY_PREVIOUS_FILE="/etc/elastickv/admin-hs256.b64"
   ```
4. Roll the cluster.
   New tokens are minted with the new key; existing tokens minted
   under the previous key continue to verify until they expire.
5. Wait at least one full session TTL (default 1h — see
   `sessionTTL` in `internal/admin/auth_handler.go`) to ensure
   every previously-issued JWT has expired.
6. Promote the new key to be the only key — clear the previous-
   file variable and rename the path on each node:
   ```
   ADMIN_SESSION_SIGNING_KEY_FILE="/etc/elastickv/admin-hs256.b64"
   # ADMIN_SESSION_SIGNING_KEY_PREVIOUS_FILE=
   ```
7. Roll the cluster again. The previous-key file can now be
   deleted from every node.

### 4.3 Rotating admin TLS certificates

Hot-reload is not implemented (out of scope for the dashboard's
maintenance model). Rotation steps:

1. Place the new cert + key at a temporary path on every node
   (e.g. `/etc/elastickv/admin-tls.next.crt` /
   `admin-tls.next.key`).
2. Edit the env file to point at the new paths.
3. Roll the cluster.
4. Once all nodes have started with the new cert, delete the old
   files at your leisure.

### 4.4 Switching the admin listener to a different port / bind

A bind change is a regular config change — edit `ADMIN_ADDRESS`
and roll. Operators with the dashboard open will see their next
request fail (the old listener is gone); reload the SPA.

If you are switching from loopback-only to a reachable bind, you
**must** prepare TLS material (§1.4) before flipping the bind.
The startup hard-error fires the moment a non-loopback bind is
configured without TLS or without the
`ADMIN_ALLOW_PLAINTEXT_NON_LOOPBACK` flag. The script's pre-flight
validation will catch a missing TLS pair before stopping the
existing container.

### 4.5 Disabling the dashboard

Set `ADMIN_ENABLED="false"` (or remove `ADMIN_ENABLED` from the
env file entirely) and roll. The script emits no admin flags and
no admin bind-mounts; the daemon comes up with the listener off.

The signing key file and TLS material can stay on disk — they
only have effect when `--adminEnabled` is passed.

### 4.6 Deleting an S3 bucket

`AdminDeleteBucket` (and the SigV4 `DeleteBucket` path) follows
the standard S3 contract: the bucket must be empty. The dashboard
returns `409 BucketNotEmpty` if any object exists.

**Race-window contract change**: a `PutObject` that returns 200
OK during the empty-probe → commit window of an `AdminDeleteBucket`
running concurrently can have its data swept along with the
bucket meta. The delete commits a `DEL_PREFIX` safety net across
every per-bucket key family in the same atomic
`OperationGroup`, so any object that landed in the race window
is tombstoned at the same `commitTS` rather than left as an
unreachable orphan. The behaviour is intentional — the
alternative was orphan objects that no API can enumerate or
remove. See
[`docs/design/2026_04_28_proposed_admin_delete_bucket_safety_net.md`](design/2026_04_28_proposed_admin_delete_bucket_safety_net.md)
for the full race analysis.

Operationally:

- For **planned bucket deletes**, pause writes against the bucket
  before issuing the delete. The 5-second window between the
  empty-probe and the commit is small but non-zero; pausing
  writes guarantees no in-flight `PutObject` is swept.
- For **emergency bucket deletes** (e.g. cleaning up a
  compromised bucket while traffic is still live), the safety
  net guarantees the cluster reaches a clean state immediately
  — but accept that any client whose `PutObject` was in flight
  may have received `200 OK` for data that no longer exists.
- Re-creating a bucket with the same name after delete is safe.
  `BucketGenerationKey` survives the delete, so the new bucket
  starts at a strictly higher generation; any object that ever
  escaped a previous delete (under the old generation prefix)
  stays invisible to the new bucket.

---

## 5. Failure-mode runbooks

### 5.1 "admin listener is enabled but no static credentials are configured"

Daemon hard-errored at startup. Fix on the affected node:

1. Verify `S3_CREDENTIALS_FILE` is set in the env file.
2. SSH to the node, confirm the file exists and contains at least
   one entry.
3. Re-run the rollout.

### 5.2 "is not loopback but TLS is not configured"

Daemon hard-errored. Either supply
`ADMIN_TLS_CERT_FILE`/`ADMIN_TLS_KEY_FILE`, or accept the
plaintext escape hatch in §3.3.

### 5.3 Sessions intermittently 403 during a rollout

Expected during a role-flag change rollout (§4.1). The dashboard
does not auto-refresh the role index — operators need to log in
again on a node with the updated list. Wait for the rollout to
finish, then ask the affected operator to refresh the SPA.

### 5.4 Login succeeds but every subsequent request 401s

Almost always a cookie-Secure mismatch:

- Listener is plaintext (loopback or
  `ADMIN_ALLOW_PLAINTEXT_NON_LOOPBACK=true`)
- But the cookie is `Secure=true` (default — the browser refuses
  to send it back over HTTP)

Fix by setting `ADMIN_ALLOW_INSECURE_DEV_COOKIE=true` and rolling.
For a production deployment, use TLS instead.

### 5.5 Forwarded write returns 503 leader_unavailable

The follower's `LeaderForwarder` could not reach a leader. This is
expected during Raft elections — see `docs/admin.md` §Election-period 503.
Investigate Raft leader-churn logs first; persistent 503s usually
mean the cluster has lost quorum.

---

## 6. Cross-references

- [`docs/admin.md`](admin.md) — per-flag configuration reference,
  audit log shapes, troubleshooting catalogue.
- [`docs/design/2026_04_24_partial_admin_dashboard.md`](design/2026_04_24_partial_admin_dashboard.md) —
  design rationale, acceptance criteria, outstanding items.
- [`scripts/rolling-update.sh`](../scripts/rolling-update.sh) —
  the rollout driver this doc references throughout.
- [`scripts/rolling-update.env.example`](../scripts/rolling-update.env.example) —
  starting point for your `<env>.env` file.

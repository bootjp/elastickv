# Physical Snapshot Object Offload — Operations

Runbook for the physical snapshot offload subsystem: continuous backup of
Raft snapshots to an S3-compatible object store, and disaster recovery from
those artifacts.

Design: [`design/2026_07_19_partial_physical_snapshot_object_offload.md`](design/2026_07_19_partial_physical_snapshot_object_offload.md).

> **Note:** §4 (Retention) describes the retention/GC subsystem, which lands
> in a separate change. Everything else here is live once this change ships.

## Scope

Use this runbook to:

1. enable continuous snapshot offload on a cluster,
2. verify that backups are actually being produced,
3. restore a node from a published snapshot,
4. configure retention, and understand what it will and will not delete.

This is **physical** backup: it ships the Raft snapshot the engine already
produced. It does not force an extra state-machine snapshot, so backup
freshness is bounded by the engine's own snapshot cadence.

## 1. What gets written

Two object kinds under the configured prefix:

```
<prefix>/v1/groups/<group>/snapshots/<index>-<term>.json   manifest
<prefix>/v1/payloads/sha256/<xx>/<sha256>.fsm              payload
```

Payloads are **content-addressed and shared**: two groups (or two generations)
whose snapshots hash identically converge on one object. This matters for
retention — see §4.

Manifests are immutable and self-hashing. A manifest names exactly one payload.

## 2. Enabling offload

Offload is opt-in. A node with no destination configured does no offload work
and cannot fail startup on offload settings.

```bash
elastickv \
  --snapshotOffloadBucket=my-backup-bucket \
  --snapshotOffloadRegion=ap-northeast-1 \
  --snapshotOffloadSourceCluster=prod-tokyo \
  --snapshotOffloadPrefix=elastickv \
  --snapshotOffloadServerSideEncryption=aws:kms \
  --snapshotOffloadSSEKMSKeyId=arn:aws:kms:ap-northeast-1:123456789012:key/abcd
```

| Flag | Meaning |
|---|---|
| `--snapshotOffloadBucket` | S3 bucket. Enables offload. |
| `--snapshotOffloadLocalDir` | Filesystem root instead of S3. **Mutually exclusive** with the bucket. |
| `--snapshotOffloadSourceCluster` | Cluster identity recorded in every manifest. Required. |
| `--snapshotOffloadPrefix` | Key prefix for all artifacts. |
| `--snapshotOffloadRegion` / `--snapshotOffloadEndpoint` / `--snapshotOffloadProfile` / `--snapshotOffloadForcePathStyle` | S3 addressing and credentials. |
| `--snapshotOffloadServerSideEncryption` / `--snapshotOffloadSSEKMSKeyId` | `AES256` or `aws:kms`. KMS aliases are rejected; pass an ARN or bare key ID. |
| `--snapshotOffloadInterval` | Scan cadence. Default 15m. |
| `--snapshotOffloadJitter` | Spread across groups. Default: a quarter of the interval. |
| `--snapshotOffloadConcurrency` | Concurrent uploads per process. Default 1. |
| `--snapshotOffloadSpoolDir` | Where payloads are spooled before upload. Needs room for the largest snapshot. |

**A misconfigured offload refuses to start the node.** That is deliberate: an
operator who configured a backup destination and silently received no backups
is worse off than one whose node failed loudly.

### Security requirements

The bucket holds physical keys and metadata. Storage-envelope encryption
protects *values*, not all keys and metadata, so the bucket itself must be
protected:

- private ACLs — anonymous read or write is a deployment failure,
- TLS,
- server-side encryption (SSE-S3 or SSE-KMS),
- credentials scoped to `list`/`get`/`put`/`delete` **below the prefix only**,
- secrets supplied by file or environment, never in process arguments.

## 3. Verifying that backups are happening

Only the current leader of a group publishes; followers skip. On a healthy
three-node group, exactly one node reports publishes and two report
`not_leader`.

```promql
# Backup freshness — the number that matters. Alert if it stops advancing.
elastickv_snapshot_offload_last_published_index

# Backups are failing. Any sustained rate is paging-grade.
rate(elastickv_snapshot_offload_failed_total[15m])

# Routine skips. Expected on followers and unchanged snapshots.
rate(elastickv_snapshot_offload_skipped_total[15m])
```

Skip reasons and what they mean:

| Reason | Meaning | Action |
|---|---|---|
| `not_leader` | This node does not lead the group. | None — expected on followers. |
| `already_published` | Snapshot unchanged since this process last published it. | None. |
| `no_persisted_snapshot` | The group has not produced a snapshot yet. | None on a young cluster. Investigate if it persists on a busy group. |
| `already_in_flight` | Another scan is publishing this group. | None. |
| `leadership_unknown` | Engine unavailable, typically during shutdown. | None if the node is stopping. |

**A group whose `last_published_index` never advances has no backups**, even
though nothing is failing. Alert on staleness, not only on errors.

## 4. Retention

Retention is per group, and runs in two phases.

**Phase 1 — manifests.** Keeps `MinGenerations` newest per group plus anything
inside `MaxAge`, and always keeps a group's newest valid manifest regardless of
both. A group can never be left with no restore point.

**Phase 2 — payloads.** Rebuilds the live set from **every surviving manifest
in the whole prefix** — not per group, because payloads are shared — then
reclaims only unreferenced objects, using **two-pass mark-and-sweep**: a pass
marks an eligible payload, and only a later pass, with the object unchanged and
the mark older than `MinMarkAge`, deletes it.

The second pass exists because a publisher reusing a payload rewrites identical
bytes, which no general-purpose S3 precondition can detect (`If-Match` compares
a content-derived ETag; `IfMatchLastModifiedTime` is directory-buckets only).
**`MinMarkAge` must exceed your longest plausible publish.**

Retention refuses to delete anything when it cannot prove the live set:

- a malformed manifest anywhere in the prefix → payload reclamation is skipped
  entirely, and the malformed object is preserved for inspection,
- a listing or pagination failure → no deletes at all,
- an object under the payload prefix that does not parse as a payload key →
  left alone.

If `PayloadPhaseSkipped` is set with malformed manifests reported, fix or
remove the malformed object; storage will not be reclaimed until you do.

### Versioned buckets

Retention deletes by key. On a bucket with **S3 versioning enabled**, a keyed
delete only writes a delete marker: the bytes survive as a noncurrent version
that later listings cannot see, so GC reports successful reclamation while
storage grows without bound.

**A versioned backup bucket requires a noncurrent-version expiration lifecycle
rule.** Whether to instead enumerate versions directly, or refuse versioned
buckets at startup, is an open decision.

## 5. Restore

Restore is **offline** and targets an **absent** data directory. It refuses to
overwrite an existing one — that guard is what protects an operator who
mistakenly points a restore at a live node.

```bash
# 1. Find the generation to restore.
elastickv-snapshot-offload publish --help   # same store flags as below

# 2. Restore into a fresh directory.
elastickv-snapshot-offload restore \
  --store=s3 --s3-bucket=my-backup-bucket --s3-region=ap-northeast-1 \
  --manifest-key='elastickv/v1/groups/1/snapshots/00000000000000004211-00000000000000000007.json' \
  --data-dir=/var/lib/elastickv/n1 \
  --peers='n1=10.0.0.1:50051,n2=10.0.0.2:50051,n3=10.0.0.3:50051'

# 3. Start the node normally against the restored directory.
```

Restore verifies exact length and SHA-256 before the payload is accepted, then
fsyncs and atomically renames it into place. Any integrity failure leaves the
destination **absent** rather than half-written.

Target membership (`--peers`) is explicit operator input, not copied from the
source. That is what makes recovery onto replacement addresses possible, while
the source membership stays in the manifest for audit.

Exit codes: `0` success, `1` invalid invocation, `2` missing or invalid
snapshot data. Automation should distinguish these.

## 6. Failure modes

| Symptom | Cause | Action |
|---|---|---|
| `last_published_index` frozen, no failures | Node is not the leader, or the engine has produced no new snapshot. | Confirm which node leads the group; check the engine's snapshot cadence. |
| Sustained `failed_total` | Object store unreachable, credentials expired, bucket policy denies writes. | Check the scheduler's log line — it carries the error the metric deliberately omits. |
| Storage grows despite retention | Versioned bucket without a lifecycle rule (§4), or reclamation blocked by a malformed manifest. | Add the lifecycle rule; inspect reported malformed manifests. |
| Restore fails with an integrity error | Payload truncated, over-length, or the manifest was edited. | Restore an older generation; the destination was left absent, so nothing was damaged. |
| Restore refuses to run | Destination directory already exists. | Restore into a fresh path. Never delete a live data dir to make room. |

## 7. Limits

- Backup freshness is bounded by the Raft engine's snapshot cadence; offload
  never forces an extra snapshot.
- Losing a group's leadership mid-publish can leave an unreferenced payload,
  which retention reclaims. It can never leave a committed manifest.
- Mark state is per-process and in memory. A restart delays reclamation by one
  pass; it never advances it.

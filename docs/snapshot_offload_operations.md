# Physical Snapshot Object Offload — Operations

Runbook for the physical snapshot offload subsystem: continuous backup of
Raft snapshots to an S3-compatible object store, and disaster recovery from
those artifacts.

Design: [`design/2026_07_19_partial_physical_snapshot_object_offload.md`](design/2026_07_19_partial_physical_snapshot_object_offload.md).

Dependency order: merge #1245, then #1222, then this runtime integration (#1224).
Retention reuses #1222's GC and claim protocol. Publication and reclamation
have separate opt-ins; configuring a backup destination never enables deletion.

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

Manifests are self-hashing. Reusing a matching manifest advances its generation
under a claim; snapshot identity and payload remain unchanged.

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
| `--snapshotOffloadPublishTimeout` | Deadline for each publication, including spooling and store operations. Default 10m; must be positive. |
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
| `object_claimed` | A publisher or GC holds the same object claim. | Retry on the next scan; inspect persistent orphan claims before recovering ownership. |
| `leadership_unknown` | Engine unavailable, typically during shutdown. | None if the node is stopping. |

**A group whose `last_published_index` never advances has no backups**, even
though nothing is failing. Alert on staleness, not only on errors.

## 4. Retention

GC is off by default. Upgrade every server and standalone CLI publisher sharing
the destination/prefix to the claims protocol, stop and drain all legacy
publications, then enable one designated retention worker with both:

```text
--snapshotOffloadGCEnabled=true
--snapshotOffloadGCClaimsRolloutComplete=true
```

The second setting is an explicit operator assertion about the entire
destination/prefix, not automatic membership discovery. Without it, enabling GC
fails startup. The runtime supplies #1222's mandatory verifier before every GC
pass. Before starting any legacy publisher or rolling back, disable GC, stop
and drain its worker, and only then resume legacy publication. Stopping the
server cancels publication and retention, waits for both workers, then closes
the local store descriptor.

| Flag | Default | Meaning |
|---|---|---|
| `--snapshotOffloadGCInterval` | `1h` | Delay after each serial GC pass. |
| `--snapshotOffloadGCTimeout` | `10m` | Deadline for a pass; failures log and retry on the next interval. |
| `--snapshotOffloadGCMinGenerations` | `3` | Minimum retained generations per group. |
| `--snapshotOffloadGCMaxAge` | `336h` | Retain manifests inside this age window too. |
| `--snapshotOffloadGCPayloadGrace` | `24h` | Delay before an unreferenced payload can be marked. |
| `--snapshotOffloadGCMinMarkAge` | `24h` | Delay between marking and reclaiming an unchanged payload. |
| `--snapshotOffloadAllowVersionedWithLifecycle` | `false` | Assert noncurrent-version expiration is configured on a versioned bucket. |

All policy values, interval and timeout must be positive. GC also requires an
offload destination. Startup validates bucket policy when GC is enabled; every
pass validates it again before scanning or deleting. Read-only restore and
publication-only workers do not require `s3:GetBucketVersioning`.

Retention is per group, and runs in two phases.

**Phase 1 — manifests.** Keeps `MinGenerations` newest per group plus anything
inside `MaxAge`, and always keeps a group's newest valid manifest regardless of
both. A group can never be left with no restore point.

**Phase 2 — payloads.** Rebuilds the live set from **every surviving manifest
in the whole prefix** — not per group, because payloads are shared — then
reclaims only unreferenced objects, using **two-pass mark-and-sweep**: a pass
marks an eligible payload, and only a later pass, with the object unchanged and
the mark older than `MinMarkAge`, deletes it.

Publishers and GC coordinate with storage-visible object claims because
content-preserving reuse cannot be detected by a general-purpose S3 ETag
precondition. The two-pass delay adds protection against stale evidence.
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

GC refuses Enabled/Suspended versioning unless noncurrent-version expiration
has been configured and `--snapshotOffloadAllowVersionedWithLifecycle=true`
acknowledges it. Unknown status or missing `s3:GetBucketVersioning` permission
aborts GC. Restore requires object reads and applicable KMS decrypt permission,
not lifecycle acknowledgement or bucket-versioning access.

## 5. Restore

Restore is **offline** and targets an **absent** data directory. It refuses to
overwrite an existing one — that guard is what protects an operator who
mistakenly points a restore at a live node.

```bash
# 1. Find the generation to restore.
elastickv-snapshot-offload publish --help   # same store flags as below

# 2. Restore each group into ITS OWN directory (see the path rule below).
elastickv-snapshot-offload restore \
  --store=s3 --s3-bucket=my-backup-bucket --s3-region=ap-northeast-1 \
  --manifest-key='elastickv/v1/groups/1/snapshots/00000000000000004211-00000000000000000007.json' \
  --data-dir=/var/lib/elastickv/n1/group-1 \
  --expect-group=1 \
  --expect-source-cluster=prod-tokyo \
  --peers='n1=10.0.0.1:50051,n2=10.0.0.2:50051,n3=10.0.0.3:50051'

# 3. Repeat for every group the node hosts, then start it normally.
```

### Every flag that names the target must name the SAME target

`--manifest-key`, `--data-dir`, `--expect-group`, `--expect-source-cluster`
and `--peers` are five independent statements about what you are restoring.
Most of them used to be uncheckable against each other:

- **`--expect-source-cluster` is required.** The group check is not enough
  when one bucket holds backups from several clusters, even under
  different prefixes: another cluster's manifest for the *same* group id
  passes it, and the restore produces a valid-looking directory that
  startup loads as this cluster's FSM. Nothing downstream records the
  source cluster, so this is the only place that mistake is detectable.
  It must match the `--source-cluster` the publish used.

- **`--expect-group` is required and is the only cross-check.** Nothing in
  the restored directory records which group the data came from — the
  artifacts carry index, term, peers and payload hash, and startup derives
  the group from the directory layout. So pasting group 2's manifest key
  into a group-1 restore produced a valid-looking directory that startup
  then loaded as group 1: the wrong physical FSM under another group's
  routing identity, with no error anywhere. The restore is now refused,
  before the download and before the destination is created, so a mistaken
  key costs nothing and leaves nothing behind.
- **`--peers` must be that group's peer map, not group 1's.** Each group
  has its own listener addresses from `--raftGroups` / `--raftGroupPeers`,
  and restore persists the supplied peers into that group's data
  directory. Copying the `:50051` endpoints from the example above into
  every invocation leaves the other groups trying to reach the wrong Raft
  endpoints, and they never form a quorum. Re-derive the peer list per
  group the same way you re-derive the manifest key and the data dir.

### The `--data-dir` path must match what the server will open

`--data-dir` is the **per-group** directory, not the node's `--raftDir`.
The server derives it as:

The rule the server actually applies (`groupDataDir`) is:

- **group 0 always** gets `<raftDir>/<raftID>/group-0`.
- **every other group** gets `group-<G>` **only when the node hosts more
  than one _data_ group**, and `<raftDir>/<raftID>` otherwise.

"More than one data group" is the exact condition, because
`dataGroupsNeedMultiDirs` counts data groups and **excludes group 0**:

| Deployment (`--raftGroups`) | Group | Directory |
|---|---|---|
| two or more data groups, e.g. `1,2` | any group *G* | `<raftDir>/<raftID>/group-<G>` |
| a single data group, e.g. `1` | 1 | `<raftDir>/<raftID>` |
| dedicated TSO **plus one** data group, e.g. `0,1` | 0 | `<raftDir>/<raftID>/group-0` |
| dedicated TSO **plus one** data group, e.g. `0,1` | 1 | `<raftDir>/<raftID>` — **not** `group-1` |
| dedicated TSO plus two or more data groups, e.g. `0,1,2` | any group *G* | `<raftDir>/<raftID>/group-<G>` |

The fourth row is the one that catches people out. A node running the
dedicated TSO group alongside a single data group *looks* multi-group —
two entries in `--raftGroups` — but only group 0 is parked under
`group-0`; the data group still opens `<raftDir>/<raftID>` directly.
Restoring it into `group-1` puts the data where startup never looks.

That is the general failure in both directions: a directory the server
does not open is not an error, it is an empty group. Startup finds
nothing, the restore is silently ignored, and the node comes back with
only the groups you happened to place correctly. **Check the table per
group before each invocation** rather than assuming one rule for the
whole node.

### Transport and spool hygiene

- **Snapshot offload requests must use HTTPS.** The node validates an explicit
  `--snapshotOffloadEndpoint` and also rejects plaintext URLs resolved by the
  AWS SDK configuration chain. Redirects must preserve both HTTPS and the
  endpoint host. For a local MinIO during development, pass
  `--snapshotOffloadAllowInsecureEndpoint` explicitly; host-changing redirects
  remain prohibited. The AWS SDK's CA bundle and credential-provider settings
  are preserved.
- **Stale spool files are cleaned at startup.** A publish spools the
  payload to `<parent of data dir>/.snapshot-offload-spool` (or
  `--snapshotOffloadSpoolDir`) and removes it on completion; a process
  killed mid-publish leaves the file behind. Offload startup removes
  leftovers older than 24 hours before scheduling. Use separate spool
  directories when a publish can take longer than 24 hours. Budget the spool volume for
  one in-flight payload per concurrent upload
  (`--snapshotOffloadConcurrency`).

Restore verifies exact length and SHA-256 before the payload is accepted, then
fsyncs and atomically renames it into place. Any integrity failure leaves the
destination **absent** rather than half-written.

Target membership (`--peers`) is explicit operator input, not copied from the
source. That is what makes recovery onto replacement addresses possible, while
the source membership stays in the manifest for audit.

Exit codes: `0` success, `1` invalid invocation, `2` missing or invalid
snapshot data. Automation should distinguish these.

Backups created by older versions may have an empty `source_cluster` for
nonzero groups. They remain fail-closed by default because their source cannot
be verified. After independently verifying provenance, an operator may add
`--allow-legacy-source-clusterless` to one restore invocation; the command
logs a warning and still requires `--expect-source-cluster`.

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

## 8. Bounded local recovery acceptance

`TestSnapshotOffloadThreeNodeRecoveryAcceptance` reuses the real bootstrap
fixture, not mocked leadership or seeded snapshot files. It runs three etcd/Raft
nodes with Pebble FSMs, loopback gRPC adapters and one data group. The durable
filesystem object root is independent of all source and replacement data dirs.

The test starts the actual periodic offload runtime on the source nodes,
observes a committed generation, stops one follower, and keeps writing through
the surviving quorum. It restarts the publishers and enables runtime retention
on one worker with the claims-rollout acknowledgement. Retention must actually
remove the old manifest **and** its payload; writes continue after reclamation.
No direct publication, forced snapshot, fake timestamp or direct deletion is
used. A few checkpoint writes advance the fixture's normal snapshot threshold
past the asserted user data; offload itself never forces a snapshot.

After stopping all publishers/GC and source nodes, the test reopens the object
root and restores the retained latest generation into absent data dirs for an
isolated three-node cluster with new IDs and addresses. Group/source identity,
target peer metadata and payload SHA are checked. All replacement nodes must
expose the data in their **local FSM**, report Raft and durable FSM applied indexes
at least the manifest index, pass RawKV adapter reads, and apply a new replicated
write. Local checks are necessary because an adapter can forward to the leader.

Reproduce with bounded concurrency and a hard test-process deadline:

```sh
GOMAXPROCS=4 go test -race . \
  -run '^TestSnapshotOffloadThreeNodeRecoveryAcceptance$' \
  -count=3 -timeout=3m -v
```

Local receipt, 2026-10-23 JST, Go 1.26.6/darwin-arm64: all three race runs passed
in 28.769 seconds total. Each observed periodic snapshot indexes `17 -> 33 -> 49`,
reclaimed the first generation, and verified 36 user keys (in addition to
checkpoint writes). Each replacement restored index 49, initially reported
Raft applied index 50 and durable FSM applied index 49, and read all 37 asserted
keys after the post-restore replicated write. Per-run elapsed times were
9.37, 8.53 and 8.99 seconds.

All data, object roots and sockets are ephemeral/local; the test does not
deploy, contact production, delete production data or provision paid cloud
resources. It is **not** a 30-day burn-in, remote-S3 acceptance, a multi-group/TSO
topology drill or operational closure. The design remains `partial`.

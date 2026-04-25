# S3 raft blob offload — keep large object payloads out of the Raft log

> **Status: Proposed**
> Author: bootjp
> Date: 2026-04-25
>
> Companion to PR #636 (`s3ChunkBatchOps = 4`, Raft entry size aligned
> with `MaxSizePerMsg = 4 MiB` per PR #593) and to the S3 PUT
> admission-control proposal
> (`docs/design/2026_04_25_proposed_s3_admission_control.md`).
>
> PR #636 caps the *per-entry* size; admission control caps the
> *aggregate in-flight* memory; this doc removes large blob payloads
> from the *Raft log itself* so that snapshots and follower catch-up
> stay bounded as the data set grows.

---

## 1. Problem

Today every byte of an S3 object travels through the Raft log:

```
HTTP PUT body  ─►  s3ChunkSize (1 MiB) chunks
                 ─►  s3ChunkBatchOps × 1 MiB Raft entry
                 ─►  Raft log entry (Pebble WAL on every node)
                 ─►  applyLoop → s3keys.BlobKey(...) → MVCCStore Put
```

`s3keys.BlobKey(bucket, generation, objectKey, uploadID, partNo, chunkNo)`
is the key actually written to Pebble; the log entry that proposed it
contains the full chunk value. Two consequences:

1. **WAL & snapshot growth scales with object bytes.** A node that
   serves 100 GiB of S3 PUT traffic ends up with a 100 GiB Pebble WAL
   plus the same 100 GiB persisted in the engine's state machine.
   Snapshot transfer for a falling-behind follower carries the
   payload twice — once as Raft log replication (during catch-up
   inside the WAL window) and once as a snapshot dump if the leader
   has already truncated.
2. **Follower catch-up after a long absence is expensive.** Right now
   a follower that misses 5 GiB of PUT traffic re-applies it as Raft
   log entries, each going through the full `applyRequests` →
   `MVCCStore.Put` path on a single thread. The ApplyLoop becomes a
   bottleneck that holds the rest of the cluster waiting for the
   `commitIndex` advance.

S3 blob payloads are special: they are **idempotent, content-
addressable, large, and rarely re-read inside the leader's apply
window**. They look more like attachments than like Raft log records.
Treating them as Raft entries is overkill — Raft only needs to
linearise *what changed* (the manifest), not *the chunk bytes
themselves*.

## 2. Goals & non-goals

**Goals**

- Raft log entries for S3 PUT carry a *reference* to the chunk
  payload, not the payload itself. Replication traffic and WAL
  size become O(manifest size), not O(object size).
- Followers fetch blob payloads out-of-band when they apply a
  manifest reference. Apply order remains Raft-defined; only the
  bytes are pulled lazily.
- Snapshot transfer for a falling-behind follower is O(manifest
  count + small blob index), not O(stored bytes).
- The new path is opt-in and lives alongside the current direct-Raft
  path until parity is proven. Existing S3 traffic is unaffected
  during rollout.

**Non-goals**

- No external object store dependency (S3, MinIO, Ceph). The blob
  offload uses Pebble itself plus a peer-to-peer fetch protocol;
  introducing an external dependency would re-create the operational
  surface elastickv exists to replace.
- No *user-facing* deduplication API or storage-accounting credit.
  The `chunkblob` keyspace is content-addressed by SHA-256 (§3.1)
  and reference-counted (§3.5), which means two distinct objects
  whose chunks happen to hash identically will share one
  `chunkblob` row at the storage layer — that is a structural
  property of content addressing, not a feature we expose. We do
  *not*: surface dedup ratios, charge storage by post-dedup bytes,
  rebalance dedup credit across tenants, or treat dedup hits as
  semantically observable from S3 verbs. The reference layer
  (`chunkref`) keeps `(bucket, objectKey, uploadID, partNo,
  chunkNo)` granular and per-object, so DELETE / lifecycle still
  reason about objects independently. Authorisation enforcement
  remains on `chunkref` reads, never on `chunkblob` reads
  (§3.3 covers the proxy-on-miss path; ACL checks fire before the
  blob fetch is initiated, so a tenant cannot dereference a peer's
  `chunkblob` by guessing a SHA — see §6 *Cross-tenant blob fetch*).
- No change to MVCC semantics. Manifest commits remain the
  serialisation point; blob fetch is a side channel that does not
  change visibility rules.
- No removal of the existing `BlobKey` path in this doc. We will
  ship in two stages (manifest-only writes through Raft, blob
  payload via a side channel), and the legacy path stays available
  until enough operational evidence accumulates.

## 3. Design

### 3.1 New keyspace

```
!s3|chunkref|<bucket>|<gen>|<objectKey>|<uploadID>|<partNo>|<chunkNo>
    → ChunkRef{
        ContentSHA256 [32]byte
        Size          uint64
        // Optional: leader-locality hints. Followers without the
        // payload locally fetch from a peer that advertises the
        // chunk in its catalog.
        SourcePeer    NodeID
      }

!s3|chunkblob|<contentSHA256>
    → raw bytes (the chunk payload)
```

Two separate keyspaces:

- `chunkref` is replicated through Raft. Cheap (32 B + small header
  per chunk) and ordered against the manifest commit.
- `chunkblob` is **not** written through Raft. It is written
  directly to Pebble on the receiving node and pulled by peers via
  the new fetch protocol when they apply the corresponding
  `chunkref`.

### 3.2 PUT path

```
client ─► HTTP PUT body
        ─► chunk loop (s3ChunkSize):
             1. compute SHA-256 of chunk
             2. write chunk to LOCAL Pebble at !s3|chunkblob|<SHA>
             3. *** synchronously replicate to ≥ chunkBlobMinReplicas peers ***
             4. queue ChunkRef into pendingBatch
        ─► flushBatch:
             coordinator.Dispatch(OperationGroup{
                 Elems: [ chunkref Puts ... + manifest Put ],
             })
        ─► HTTP 200 OK once Dispatch acks
```

Step 3 — synchronous chunkblob replication before the chunkref
commit — is the difference between "Raft-equivalent durability"
and "leader-only durability." Without it, a leader crash between
the chunkref commit and the eventual async fetch would leave a
committed manifest pointing at a chunkblob nobody else has — Raft's
quorum guarantees the chunkref but tells you nothing about the
blob payload. We close that gap by treating the chunkblob like a
mini-Raft entry of its own with **semi-synchronous quorum**:

1. Leader writes the chunkblob to local Pebble (fsync).
2. Leader pushes the chunkblob to `chunkBlobMinReplicas - 1`
   followers via the `S3BlobFetch.PushChunkBlob` RPC and waits for
   each follower's "fsync ack." (`PushChunkBlob` is the leader-
   initiated counterpart to the follower-initiated `FetchChunkBlob`
   defined in §3.6.)
3. Only after the chunkblob is durable on a quorum of nodes does
   the leader propose the chunkref through Raft.

`chunkBlobMinReplicas` defaults to **2** on a 3-node cluster (= a
quorum of 2 includes the leader and one follower). For larger
clusters the floor is `(N/2)+1` to match Raft's quorum size; the
operator can opt into N for stronger-than-Raft durability. A
follower that crashes after acking the push but before the chunkref
commits is fine — the chunkref will be retried by the leader on the
next attempt because it has not yet entered the Raft log.

**Behaviour during cluster shrink / partial outage.** A controlled
decommission (5 → 3 nodes) or a transient partition can leave the
leader with fewer reachable peers than `(N/2)+1` configured at the
last membership commit. Blocking PUTs until the configured minimum
is reachable would surface as an indefinite hang, which is worse
than the legacy "every byte through Raft" path (which fails when
Raft itself loses quorum, but otherwise succeeds). The contract is
therefore degraded availability with a hard floor:

- If reachable peers ≥ `chunkBlobMinReplicas`, normal path: ack
  after the configured minimum.
- If reachable peers < `chunkBlobMinReplicas` but ≥ `floor(N/2)+1`
  (Raft quorum is intact), degrade to "as many as currently
  available, but never fewer than 2 — i.e. leader + at least one
  follower." Emit `s3_chunkblob_replication_degraded_total` so
  operators see the degradation. This matches Raft's own behaviour
  during the same window: Raft would still commit at quorum, just
  with one fewer redundant ack.
- If reachable peers < 2 (leader-only durability), fail the PUT
  with 503 — single-node durability is what `BlobKey`-on-Raft
  already loses on leader crash, and is the regression this design
  exists to prevent.

The floor of 2 is the strict invariant: leader-only writes are the
case the design refuses to accept regardless of operator
configuration. Tuning `chunkBlobMinReplicas` higher trades PUT
availability for stronger durability; tuning lower than 2 is
rejected at config-load.

The trade-off is PUT latency: a PUT now blocks on
`chunkBlobMinReplicas - 1` follower fsyncs in addition to the Raft
quorum write of the chunkref. Empirically the chunkblob fsync is
the dominant cost (1 MiB write, ~5–10 ms on consumer SSD), so PUT
p99 is roughly equivalent to today's "every byte through Raft"
latency — we are paying the same fsync cost, just to a different
keyspace.

The `chunkref` keys are < 100 B each. A 1 GiB PUT generates 1024
of them = ~100 KiB of Raft log payload. Compared with today's
1 GiB through Raft, that is a **10⁴× reduction** in Raft log
write amplification — even with semi-synchronous chunkblob
replication, the Raft log itself is unaffected by chunk size, so
log replay time, snapshot transfer, and follower catch-up still
collapse to O(manifest count).

### 3.3 Follower apply path

When a follower's apply loop sees a `chunkref` Put:

1. Stage the `chunkref` key in MVCCStore as usual.
2. Schedule an async fetch of `!s3|chunkblob|<SHA>` from
   `SourcePeer` (or a quorum-style fanout to all known peers).
3. The fetch worker writes the chunk to local Pebble at
   `!s3|chunkblob|<SHA>` once the body arrives and verifies the
   SHA-256 (mismatch → drop and retry from another peer).

`SourcePeer` is a **best-effort hint** captured at write time
(§3.1). It is *not* authoritative — the recorded peer may have
crashed, restarted, evicted the blob via §3.5 GC, or simply lost
the local copy to disk failure. Callers MUST treat
`FetchChunkBlob → NOT_FOUND` from `SourcePeer` as a normal fallback
trigger, not an error: drop to fanout against the rest of the
peer set and accept the first peer that returns the bytes (with
SHA-256 verification at the receiving end). Treating
`NOT_FOUND` as a hard failure would make a single peer's GC tick
pin clients on a bad source.

GET / range-read on the follower checks the local `chunkblob`
first; if absent (because the async fetch is still pending), it
either:

- proxies the read to a peer that holds the chunk (using the
  `SourcePeer` hint, falling back to fanout on `NOT_FOUND`), or
- replies 503 with `Retry-After`, identical to S3's behaviour
  during a region failover.

The choice is per-deployment; Phase-1 ships proxy-on-miss.

#### 3.3.1 GET vs. GC delete race

Even with the §3.5 grace window, a GET that proxies to peer X for
a chunkblob can lose a race against peer X's sweeper if the
sweeper deletes the local copy *between* the GET arriving on the
caller's node and the proxy RPC reaching peer X. The blob remains
reachable globally (other peers still hold it; the chunkref is
unchanged), so the right behaviour is for the caller to fall back
to fanout. We therefore mandate:

- `FetchChunkBlob` on a peer whose local sweeper just removed the
  blob returns `NOT_FOUND`, **not** an internal error.
- The caller's GET handler treats both `NOT_FOUND` and
  `INVALID_ARGUMENT (sha mismatch)` as fallback triggers and
  cycles through the remaining peers in randomised order.
- If the entire peer set returns `NOT_FOUND` for an SHA whose
  `chunkref` is still present, that is a genuine durability
  failure (every replica including the leader's GC raced); the
  GET surfaces 500 *and* the read path emits a
  `s3_chunkblob_unrecoverable_total` metric so operators detect
  the underlying GC bug. With the §3.2 quorum-write durability
  and the §3.5 grace window, this case requires a coincident
  failure across a quorum of nodes within a 1-hour window — vastly
  more unlikely than the per-peer 404 the fanout absorbs as a
  matter of course.

### 3.4 Snapshot

Today a follower snapshot dump includes every `BlobKey` Pebble has
ever stored. Under the new design:

- The Raft snapshot serialises only `chunkref` keys plus the rest
  of the MVCC state (manifests, bucket meta, ACLs).
- A separate **blob catalog snapshot** lists every locally-held
  `chunkblob` SHA. This is included in the snapshot stream as a
  manifest of "blobs you should fetch from me on demand."
- Once the follower has consumed the Raft snapshot and the blob
  catalog, it begins serving GET / HEAD by proxying chunkblob
  fetches to peers as in 3.3.

A 100 GiB cluster's Raft snapshot drops from ~100 GiB to a few
megabytes (one `chunkref` per chunk, plus the manifest set). The
blob catalog adds 32 B × N_chunks = ~3 MiB per 100 GiB. Snapshot
stream time falls by orders of magnitude; the recovery cost
shifts from "leader's WAL dump" to "follower's lazy blob fetch
amortised across reads."

### 3.5 Garbage collection

A blob whose `chunkref` has been deleted (DELETE, lifecycle policy,
object version pruned, manifest aborted) is reclaimable. The
sweeper needs to know not just *that* the RC reached zero but
*when*, otherwise the documented grace window is unimplementable
(a plain counter at zero carries no time signal). We make the
"became eligible at T" fact a first-class Raft entry:

1. **Reference counting** via `!s3|chunkref-rc|<SHA>`, a counter
   updated inside the same Raft txn that adds / removes a
   `chunkref`. The atomic `(chunkref change, RC update)` pair is
   the linearisation point for "this blob is now / no longer
   reachable."
2. **GC eligibility queue.** When the txn that decrements an RC
   would drive it to zero, the *same* txn additionally writes
   `!s3|chunkblob-gc-queue|<commitTS-nanos>|<SHA>` → empty. The
   commitTS-prefixed key is the time signal: the queue is
   naturally sorted by elegibility-start time, and any node can
   determine the grace-period boundary by scanning the queue with
   `endKey = !s3|chunkblob-gc-queue|<now-gracePeriod>|`. If a
   subsequent txn re-references the same SHA before the sweeper
   runs (e.g. an upload reuses a content hash), that txn deletes
   the queue entry as part of incrementing the RC; the queue
   therefore reflects "currently RC==0" rather than "ever was zero."
3. **Node-local sweeper.** Each node runs an independent sweeper
   every `chunkBlobGCInterval` (proposed default 5 minutes) that:
   a. scans the queue range `[!s3|chunkblob-gc-queue|, !s3|chunkblob-gc-queue|<now-gracePeriod>|)`
      for entries whose grace window has elapsed,
   b. for each `<SHA>` returned, re-checks the RC counter at the
      sweeper's read timestamp; if the RC is still 0, deletes the
      local `!s3|chunkblob|<SHA>` AND deletes the queue entry —
      both as a single Raft txn so the queue stays consistent with
      the keyspace,
   c. if the RC has bounced above 0 in the meantime, the queue
      entry is stale (a re-reference txn forgot to remove it, or
      the sweeper raced) and the sweeper deletes only the queue
      entry, leaving the chunkblob in place.

The queue is the authoritative "blob is GC-eligible since T"
signal; the RC is the authoritative "is reachable" signal. Both
are Raft-replicated, so every node arrives at the same set of
sweepable SHAs and the same eligibility window. Different nodes
running sweepers concurrently is safe because step (3b) commits
through Raft and the txn fails with a write-write conflict on the
second sweeper, leaving the first sweeper's deletion authoritative.

`chunkBlobGCGracePeriod` defaults to 1 hour. The grace window
absorbs in-flight reads (a peer that has already started fetching
the blob completes its fetch before the sweeper runs), in-flight
upload aborts (a multipart abort flips RCs to zero, then a retry
creates a new manifest with the same chunks and bumps them back),
and clock skew between nodes (we use the Raft `commitTS` from the
RC-update txn, not wall clock, so skew is bounded to the
HLC-physical-shift the cluster already tolerates).

### 3.6 Fetch protocol

Two RPCs on the existing internal raft transport service — one
follower-initiated (lazy fetch on miss), one leader-initiated
(synchronous replication before chunkref commit, see §3.2 step 3):

```protobuf
service S3BlobFetch {
  // FetchChunkBlob returns the bytes of a chunkblob this peer holds
  // locally. Caller must verify SHA-256. Used by followers on the
  // proxy-on-miss GET path (§3.3) and during snapshot catch-up.
  rpc FetchChunkBlob(FetchChunkBlobRequest) returns (stream FetchChunkBlobResponse);

  // PushChunkBlob streams a chunkblob from the leader to a follower
  // and acks once the bytes are durable in the receiver's Pebble.
  // Used by §3.2 step 3 to make chunkblob writes survive a leader
  // crash without depending on the async fetch path catching up.
  // The receiver SHOULD verify SHA-256 against the request header
  // before fsync; mismatch fails the RPC and the leader retries.
  rpc PushChunkBlob(stream PushChunkBlobRequest) returns (PushChunkBlobResponse);
}

message FetchChunkBlobRequest {
  bytes  content_sha256 = 1;
}

message FetchChunkBlobResponse {
  bytes  payload = 1;
  bool   eof     = 2;
}

message PushChunkBlobRequest {
  bytes  content_sha256 = 1; // sent in the first frame only
  bytes  payload        = 2;
  bool   eof            = 3;
}

message PushChunkBlobResponse {
  bool   durable        = 1; // true == fsynced
}
```

Streamed because a chunkblob is up to `s3ChunkSize = 1 MiB`. The
existing gRPC `MaxRecvMsgSize = 64 MiB` (PR #593 → `internal.GRPCCallOptions`)
already covers this in a single RPC, but streaming keeps the
implementation symmetric with how the future Raft streaming
transport (proposed under
`docs/design/2026_04_18_proposed_raft_grpc_streaming_transport.md`)
handles large payloads.

### 3.7 Backwards compatibility & rollout

The legacy `BlobKey` path remains available. New PUTs use the
offload path when `ELASTICKV_S3_BLOB_OFFLOAD=true`; existing data
keeps reading through the legacy `BlobKey` path until a background
migrator (separate proposal) rewrites it. Mixed keyspace coexistence
works because `!s3|chunkblob|*` and the legacy
`!s3|blob|<bucket>|<gen>|...` namespaces are disjoint.

The opt-in flag stays for at least one full release cycle so we can
revert by flipping a single env var if any of the following surface:

- a SHA-256 collision (~zero probability but a hard kill criterion),
- a follower fetch storm overwhelming peer-to-peer bandwidth,
- a GC bug that leaks reachable blobs,
- semi-synchronous `PushChunkBlob` latency exceeding the legacy
  PUT p99 by an unacceptable margin (the soak-test acceptance
  criterion in §5).

### 3.8 Mixed-version cluster behaviour

Until *every* node in the cluster speaks the offload protocol, PUTs
on the offload path cannot proceed safely: a node that does not
implement `PushChunkBlob` cannot ack a quorum write, and a follower
that does not implement `FetchChunkBlob` cannot resolve a chunkref
on apply. We therefore gate the offload path on cluster-level
feature negotiation rather than a single env var:

- A node advertises offload capability by setting
  `feature_s3_blob_offload=true` in the `AdminServer.GetClusterOverview`
  response (alongside the existing role / version metadata).
- The leader inspects every peer's advertised capabilities at PUT
  admission time. If any peer is missing the capability, the PUT
  falls back to the legacy `BlobKey` path for that request — even
  if the leader has the env var enabled.
- During an upgrade window the leader continues to emit legacy
  writes; once the last peer rolls and re-advertises, subsequent
  PUTs flip to offload automatically. A roll-back works the same
  way in reverse: the first downgraded peer drops its capability
  flag and the leader resumes legacy emission within the next
  capability-refresh interval (default 30 s).
- Reads always succeed regardless of mixed state, because both
  keyspaces are namespaced and the GET path checks legacy then
  offload (or vice-versa) and serves whichever resolves.

This gives operators a **strict two-step rolling upgrade** with no
PUT data path that depends on a half-upgraded cluster:

1. Roll out the new binary with `ELASTICKV_S3_BLOB_OFFLOAD=false`
   on every node. PUTs continue on the legacy path. Validate
   stability for a soak window (24 h on the canary cluster in
   §5's M0 acceptance criteria).
2. Flip `ELASTICKV_S3_BLOB_OFFLOAD=true` on the leader, then on
   followers. Once every node advertises capability, PUTs switch
   to the offload path.

Roll-back: flip the env var to `false` on any node; the leader's
capability check sees the disagreement and falls back to legacy
within ≤ refresh interval. The migrator (M4) is independently
gated and never runs during a roll-back window.

A node with `ELASTICKV_S3_BLOB_OFFLOAD=true` running against a
cluster where offload is disabled (e.g. a stuck rollout) is safe
— it advertises capability but the leader's per-PUT capability
check sees other peers missing it and routes through legacy. No
data is written into the offload keyspace until a quorum of
capability-advertising peers exists.

## 4. Interaction with related subsystems

- **PR #636 + admission control.** The admission control budget
  drops in importance under the offload path because Raft entries
  are tiny (~100 B per chunkref). However the *body bytes still
  flow through HTTP* and `prepareStreamingPutBody` continues to
  hold them in memory until the local Pebble write returns. The
  admission cap must stay; only the per-peer Raft-side worst-case
  bound (`MaxInflight × MaxSizePerMsg = 4 GiB`) gets *much* easier
  to honour.
- **PR #589 (snapshot tuning) and PR #614 (etcd-snapshot-disk-offload).**
  Already implemented. The offload path makes those tunables more
  effective by reducing the per-snapshot byte count.
- **`docs/design/2026_04_18_proposed_raft_grpc_streaming_transport.md`.**
  The blob-fetch RPC reuses the same chunked-streaming approach
  proposed for Raft transport. We can land both behind the same
  abstraction.
- **Lease read & MVCC snapshot reads.** No change. Manifests remain
  the linearisation point; chunk bytes are immutable once committed
  (content-addressable), so a stale local copy on a follower is
  still correct.

## 5. Implementation plan

| Milestone | Scope | Risk |
|---|---|---|
| M0 | Spike: prove the chunkref + chunkblob keyspaces under a feature flag with 1 % traffic. Measure local Pebble write amp & blob fetch latency. | Low (observability only). |
| M1 | PUT path emits chunkrefs through Raft; chunkblob writes go directly to local Pebble. **`FetchChunkBlob` and `PushChunkBlob` RPCs ship in this milestone** because both M1 PUT (semi-synchronous push) and M1 GET (proxy-on-miss) depend on them — without them M1 GET could only serve local-hit or 503. | Medium (race ordering, RPC plumbing on the request goroutine). |
| M2 | Async fetch worker pool for follower apply (catch-up after a long absence). Independent of M1's synchronous `FetchChunkBlob` use on the GET path. SHA verification + retry from alternate peer on mismatch. | Medium (fanout cost on snapshot apply). |
| M3 | Reference-count + grace-period GC (the queue-based scheme in §3.5). | Medium (correctness of RC under concurrent ops). |
| M4 | Migrator: rewrite legacy `BlobKey` data in the background. Off by default until M0–M3 burn in for 30 days in production. | High (long-running batch over live traffic). |

Acceptance criteria for M3 (the milestone that flips `ELASTICKV_S3_BLOB_OFFLOAD=true` by default):

- WAL growth per GiB of S3 PUT < 1 MiB on a one-week soak test.
- Snapshot transfer for a 100 GiB-cluster follower restart completes
  in < 60 s on a 1 Gbps interconnect.
- No regression in PUT p99 latency or GET p99 latency vs. the legacy
  path (measured on the 24 h pre-cutover window).

## 6. Risks

- **Race between local chunkblob write and the chunkref commit.**
  Mitigated by writing the chunkblob to a local Pebble batch with
  fsync *before* the chunkref enters `coordinator.Dispatch`. The
  manifest commit is the linearisation point; if the chunkblob is
  durable on the leader, peers can fetch it as soon as they apply.
- **Follower fetch storm.** A new follower that catches up sees a
  flood of `chunkref` Puts and could DDoS the source peer with
  fetches. Mitigation: bounded fetch worker pool + token bucket
  per-source. The Raft apply loop does *not* block on the fetch —
  it stages `chunkref` and lets the fetch lag — so apply latency
  stays bounded.
- **SHA-256 collision.** Operationally improbable; shipped with a
  metric (`s3_chunkblob_sha_mismatch_total`) and a hard-fail option
  for paranoid operators.
- **Leader-only durability before chunkref commit.** Without
  intervention, a leader crash between writing the chunkblob to its
  own Pebble and the eventual async fetch on followers would leave
  a Raft-committed chunkref pointing at a chunkblob no surviving
  node has. Mitigation: §3.2 step 3 — synchronous semi-quorum
  replication via `PushChunkBlob` before the chunkref enters Raft.
  `chunkBlobMinReplicas` defaults to a Raft-quorum-equivalent
  floor; operators who want N-way durability bump it explicitly.
  This restores end-to-end durability parity with the legacy
  "every byte through Raft" path at the cost of one extra fsync
  per chunkblob on the followers in the quorum.
- **Cross-tenant blob fetch via SHA-256 guessing.** Because
  `chunkblob` keys are SHA-256-addressed, *if* a malicious tenant
  could (a) guess a victim tenant's chunk SHA and (b) bypass
  authorisation, they could exfiltrate the chunk. SHA-256 guessing
  is computationally infeasible for non-trivial content, but we
  remove the second prerequisite by enforcing authorisation
  *exclusively at the `chunkref` layer*. The `S3BlobFetch.FetchChunkBlob`
  RPC is internal-only (raft-transport credentials, not exposed to
  S3 clients); user-facing GET resolves through `chunkref` first,
  which carries the bucket / key tenancy context, and only after
  the ACL check does the server proxy to a peer for the
  corresponding `chunkblob`. A future design that exposes
  blob fetch on a public surface would need to reintroduce
  tenant-scoped authorisation at the blob layer; this proposal
  intentionally does not.

## 7. Out of scope (future work)

- Cross-cluster blob replication (CRR / disaster recovery).
- Tiered storage (cold blobs to S3-IA / Glacier-equivalent).
- Erasure coding for blob payloads.
- Compression. The current S3 spec is "the bytes the client sent";
  any compression layer is a separate negotiated feature.

## 8. Open questions

- Do we need a per-follower bandwidth cap on blob fetch? If the
  cluster network is constrained, a runaway catch-up could starve
  user-path GET / Raft heartbeat traffic. Probably yes — defer to
  the workload-isolation rollout.
- Is content-addressing at the chunk granularity (`chunkSize = 1 MiB`)
  the right unit, or should we content-address whole objects and
  range-fetch sub-chunks? The chunk granularity matches what
  `prefetchObjectChunks` already does and keeps content addressing
  predictable; whole-object addressing would require re-hashing on
  partial reads. Tentatively: chunk granularity for v1.

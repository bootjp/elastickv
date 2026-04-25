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
- No deduplication across objects. Each `(bucket, objectKey,
  uploadID, partNo, chunkNo)` keeps its own key. Real S3 also does
  not deduplicate at this layer.
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
             3. queue ChunkRef into pendingBatch
        ─► flushBatch:
             coordinator.Dispatch(OperationGroup{
                 Elems: [ chunkref Puts ... + manifest Put ],
             })
        ─► HTTP 200 OK once Dispatch acks
```

The order matters: the chunk *must* be persisted in local Pebble
before its `chunkref` is committed through Raft, otherwise a
follower that applies the `chunkref` and immediately re-fetches
might race the leader's local write. Pebble's WAL fsync gives the
ordering guarantee; the chunk write returns only after the data is
durable.

The `chunkref` keys are < 100 B each. A 1 GiB PUT generates 1024
of them = ~100 KiB of Raft log payload. Compared with today's
1 GiB through Raft, that is a **10⁴× reduction** in Raft log
write amplification.

### 3.3 Follower apply path

When a follower's apply loop sees a `chunkref` Put:

1. Stage the `chunkref` key in MVCCStore as usual.
2. Schedule an async fetch of `!s3|chunkblob|<SHA>` from
   `SourcePeer` (or a quorum-style fanout to all known peers).
3. The fetch worker writes the chunk to local Pebble at
   `!s3|chunkblob|<SHA>` once the body arrives and verifies the
   SHA-256 (mismatch → drop and retry from another peer).

GET / range-read on the follower checks the local `chunkblob`
first; if absent (because the async fetch is still pending), it
either:

- proxies the read to a peer that *does* have the chunk (using the
  `SourcePeer` hint and falling back to a fanout), or
- replies 503 with `Retry-After`, identical to S3's behaviour
  during a region failover.

The choice is per-deployment; Phase-1 ships proxy-on-miss.

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
object version pruned, manifest aborted) is reclaimable. We handle
this two-staged:

1. Reference counting via a `!s3|chunkref-rc|<SHA>` counter
   updated inside the same Raft txn that adds / removes a
   `chunkref`. RC == 0 marks the blob as eligible.
2. A node-local sweeper periodically (e.g. every 5 minutes)
   scans `!s3|chunkblob|*` and deletes blobs whose RC is 0 and
   whose RC has been 0 for at least `chunkBlobGCGracePeriod`
   (proposed default 1 hour). The grace window covers in-flight
   reads and avoids deleting a blob a peer is just about to fetch.

Sweeper deletion is local — it does not pass through Raft, because
the authoritative state ("is this blob unreachable?") is the
already-committed RC. Followers run independent sweepers and arrive
at the same conclusion.

### 3.6 Fetch protocol

A new gRPC method on the existing internal raft transport service:

```protobuf
service S3BlobFetch {
  // FetchChunkBlob returns the bytes of a chunkblob this peer holds
  // locally. Caller must verify SHA-256.
  rpc FetchChunkBlob(FetchChunkBlobRequest) returns (stream FetchChunkBlobResponse);
}

message FetchChunkBlobRequest {
  bytes  content_sha256 = 1;
}

message FetchChunkBlobResponse {
  bytes  payload = 1;
  bool   eof     = 2;
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
migrator (separate proposal) rewrites it. Mixed clusters work
because both keyspaces are namespaced.

The opt-in flag stays for at least one full release cycle so we can
revert by flipping a single env var if any of the following surface:

- a SHA-256 collision (~zero probability but a hard kill criterion),
- a follower fetch storm overwhelming peer-to-peer bandwidth,
- a GC bug that leaks reachable blobs.

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
| M1 | PUT path emits chunkrefs through Raft; chunkblob writes go directly to local Pebble. GET path checks chunkblob locally with proxy-on-miss. | Medium (race ordering). |
| M2 | Follower fetch protocol + async fetch worker pool. SHA verification + retry from alternate peer on mismatch. | Medium (fanout cost on snapshot apply). |
| M3 | Reference-count + grace-period GC. | Medium (correctness of RC under concurrent ops). |
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
- **Local-blob-only on a single node.** If only one node has a
  given chunkblob and that node fails before peers fetch it, data
  is lost. Mitigation: PUT path replicates the chunkblob to *N*
  peers asynchronously before returning 200 (e.g. quorum write
  outside of Raft). N defaults to 2 (one extra copy = parity with
  Raft's quorum durability).

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

# Logical Backup: Per-Adapter Decomposed Snapshot Format

Status: Proposed
Author: bootjp
Date: 2026-04-29

## Background and Problem

### What we have today

The FSM snapshot path (`store/snapshot_pebble.go`, see
`2026_04_14_implemented_etcd_snapshot_disk_offload.md`) writes the entire
keyspace as a single opaque stream:

```
[magic "EKVPBBL1" :8]
[lastCommitTS    :8]
([keyLen:8][key][valLen:8][val])*
[CRC32C footer  :4]
```

That format is fine for Raft log compaction and follower catch-up — it is
loaded back by `kvFSM.Restore` into the same Pebble store that produced it —
but it is **fundamentally a one-way artifact**:

- Keys are raw byte slices in elastickv's internal layout
  (`!ddb|item|<table>|<gen>|<orderedKey>`, `!s3|blob|<bucket><gen><obj>...`,
  `!hs|fld|<keyLen><key><field>`, `!sqs|msg|data|<queue><gen><msgID>`, …).
- Values are protobuf or JSON envelopes wrapped with internal magic prefixes
  (`0x00 'D' 'I' 0x01` for DynamoDB items in `adapter/dynamodb_storage_codec.go`,
  `0x00 'S' 'M' 0x01` for SQS records in `adapter/sqs_messages.go`, etc.).
- S3 objects are split into per-chunk blob keys keyed by `(bucket, generation,
  object, uploadID, partNo, chunkNo, partVersion)` (see
  `internal/s3keys/keys.go`); the original byte stream only exists as a sequence
  reassembled by `streamObjectChunks` at GET time.
- Internal control-plane keys (route catalog, txn keys, HLC ceiling, write
  conflict counter prefixes) are intermixed with user data with no separation
  marker.

If the user takes that snapshot off-cluster — to S3, tape, a sidecar disk,
another cluster — they have a backup that **only this elastickv binary, on
exactly the version that wrote it, can read**.

### Why that is unsafe

Backups are an insurance policy. The condition under which a user reaches for
a backup is by definition the worst case: corrupted store, lost cluster,
botched migration, rolled-back schema, or a vendor-side failure that leaves
elastickv itself unusable. A backup format that requires the same vendor's
binary to extract is not insurance — it is a single point of failure
collocated with the thing it is supposed to protect against.

Concrete failure modes the current snapshot format does not survive:

1. **The cluster is gone and the binary cannot be rebuilt** (lost source,
   broken toolchain, vendor end-of-life). The user holds bytes they cannot
   read.
2. **Format drift between versions.** Internal magic prefixes
   (`storedDynamoItemProtoPrefix`, `storedSQSMsgPrefix`, the Redis stream
   entry envelope, the `!hs|fld|<keyLen>...` wide-column layout) have already
   changed twice in this codebase (Redis hash/set/zset legacy → wide-column
   migrations, see `redis_compat_commands.go:1881-1950`). A snapshot from an
   older format cannot be restored into a newer binary without running the
   in-tree migration code paths first.
3. **Cross-system migration.** The user wants to leave elastickv and feed
   DynamoDB tables into real DynamoDB, Redis keys into a real Redis, S3
   objects into MinIO/AWS, SQS messages into AWS SQS. Today they would have
   to write a custom snapshot-stream parser per adapter, including reverse-
   engineering all the wide-column layouts.
4. **Partial or selective restore.** "I just want to recover the
   `users` DynamoDB table" or "I just want this one S3 bucket back". The
   current snapshot is monolithic.

### What we want instead

A backup whose physical layout *is* the logical structure of the data, in
formats every operating system, scripting language, and competing storage
product already understands:

- Directory hierarchy by adapter, then by user-visible scope (table /
  bucket / queue / db).
- Records in self-describing formats (JSON for keyed records, JSONL for
  ordered streams, raw bytes for blobs).
- Filenames that match the user's primary key — so `find`, `ls`, `grep`,
  `aws s3 sync`, `redis-cli --pipe` can operate on the dump directly.
- A versioned manifest at the root so future tools can round-trip if they
  want to, without that being a precondition for reading.

This is what is meant by "not a one-way ticket": the user can recover, audit,
or migrate the data **without elastickv being part of the recovery path**.

## Design Goals

| Goal | Detail |
|------|--------|
| **Vendor-independent restore** | `cat`, `jq`, `find`, `aws s3 cp`, `redis-cli` are sufficient to reconstruct any single record without running elastickv |
| **Adapter-shaped layout** | Top level partitions by `dynamodb/`, `s3/`, `redis/`, `sqs/`; each adapter's subtree mirrors how that protocol exposes data |
| **Self-describing per record** | A `dynamodb/<table>/items/<pk>.json` file is a complete DynamoDB item, decodable in isolation; an `s3/<bucket>/<key>` file is the original object body byte-for-byte |
| **Crash-consistent across adapters** | All records in a single backup come from one cluster-wide read timestamp, so cross-adapter invariants (e.g., DynamoDB conditional write outcomes) are preserved |
| **Selective backup and restore** | The dump and restore tools can be scoped to one adapter, one table, one bucket, or one queue without reading or rewriting unrelated data |
| **Format stability across elastickv versions** | The on-disk record format does not embed elastickv's internal magic prefixes or wide-column key encoding; format changes are versioned in `MANIFEST.json` |
| **Forward-compatible restore** | Older dumps remain restorable on newer binaries; per-adapter restore handles dropped/added optional fields |
| **No change to the in-cluster snapshot path** | This is a separate output produced by a backup tool, not a replacement for the FSM snapshot used by Raft compaction |

### Non-Goals

- Replacing the FSM/Raft snapshot pipeline. The on-the-wire and on-disk
  snapshot format described in `2026_04_14_implemented_etcd_snapshot_disk_offload.md`
  is unaffected.
- Continuous/incremental backup or PITR. This proposal is the full-snapshot
  baseline; incremental/CDC is left for a follow-up.
- Encryption-at-rest or signed manifests. Both are valuable but are layered
  on top — the format described here is the payload for them.
- Backing up internal control-plane state (route catalog, HLC ceiling,
  Raft term/index, txn intent keys, GSI projections). These are
  re-derivable from user data plus configuration (see "Internal-State
  Handling" below).

## Top-Level Layout

```
backup-<utc-timestamp>-<cluster-id>-<commit-ts>/
├── MANIFEST.json
├── CHECKSUMS                         # SHA-256 of every regular file under the root
├── dynamodb/
│   └── <table-name>/
│       ├── _schema.json
│       └── items/
│           └── <pk-segment>/[<sk-segment>.]json
├── s3/
│   └── <bucket-name>/
│       ├── _bucket.json
│       └── <object-key-path>          # original object bytes, original hierarchy
├── redis/
│   └── db_<n>/
│       ├── strings/<key>.bin
│       ├── hashes/<key>.json
│       ├── lists/<key>.json
│       ├── sets/<key>.json
│       ├── zsets/<key>.json
│       └── streams/<key>.jsonl
└── sqs/
    └── <queue-name>/
        ├── _queue.json
        └── messages.jsonl
```

The directory **is** the index. There is no other side-table that must be
parsed before the user can find a record.

### Filename Encoding

User keys (table names, bucket names, S3 object keys, Redis keys, SQS
message IDs) can contain bytes that are illegal or ambiguous on common
filesystems: `/`, NUL, `\`, control characters, names ending in `.`, names
that case-collide on Windows/HFS+, names longer than `NAME_MAX`.

Rules:

- **S3 object keys preserve their `/` separators** as filesystem path
  separators. `s3://photos/2026/04/29/img.jpg` becomes
  `s3/photos/2026/04/29/img.jpg`. This is the whole point — `aws s3 sync`
  on the dump directory works.
- **All other key segments** (DynamoDB partition/sort key bytes, Redis keys,
  SQS queue names and message IDs) are encoded with the following scheme,
  which is unambiguous and reversible:
  - Bytes in `[A-Za-z0-9._-]` pass through.
  - Every other byte is rendered as `%HH` (uppercase hex), like
    `application/x-www-form-urlencoded` but applied to every non-allowlisted
    byte.
  - If the resulting segment exceeds 240 bytes, the segment is replaced with
    `<sha256-hex-prefix-32>__<truncated-original>` and the full original
    bytes are recorded in `<adapter>/<scope>/_long_keys.jsonl`.
- **DynamoDB binary partition / sort keys** (the `B` attribute type) are
  rendered as `b64.<base64url>` so a binary key never collides with a
  string key whose hex encoding happens to look like base64.
- **Stream messages** within an SQS queue are not represented by file
  per message (would be millions of small files); they live in a single
  `messages.jsonl`. Same rationale for Redis stream entries.

A single `KEYMAP` file at each adapter scope root translates the encoded
filename back to the exact original bytes, in case the user needs to feed
the data into a system that requires the verbatim key. The translation is
also losslessly recoverable from the encoded filename alone — `KEYMAP` is a
convenience, not a correctness dependency.

## Per-Adapter Format

### DynamoDB

```
dynamodb/
└── orders/
    ├── _schema.json
    └── items/
        ├── customer-7421/
        │   ├── 2026-04-29T12:00:00Z.json
        │   └── 2026-04-29T13:15:42Z.json
        ├── customer-7422/
        │   └── 2026-04-29T09:00:00Z.json
        └── b64.AAECAw../single.json     # binary partition key
```

`_schema.json`:

```json
{
  "format_version": 1,
  "table_name": "orders",
  "creation_time_iso": "2026-03-12T08:14:00Z",
  "primary_key": {
    "hash_key": {"name": "customer_id", "type": "S"},
    "range_key": {"name": "order_ts", "type": "S"}
  },
  "global_secondary_indexes": [
    {
      "name": "by_status",
      "key_schema": [
        {"name": "status", "type": "S"},
        {"name": "order_ts", "type": "S"}
      ],
      "projection": {"type": "INCLUDE", "non_key_attributes": ["total_amount"]}
    }
  ],
  "attribute_definitions": [
    {"name": "customer_id", "type": "S"},
    {"name": "order_ts",   "type": "S"},
    {"name": "status",     "type": "S"},
    {"name": "total_amount", "type": "N"}
  ],
  "billing_mode": "PAY_PER_REQUEST"
}
```

This is structurally identical to what `DescribeTable` returns; tools that
already speak DynamoDB JSON ingest it without translation.

`items/<pk>/<sk>.json`:

```json
{
  "customer_id": {"S": "customer-7421"},
  "order_ts":    {"S": "2026-04-29T12:00:00Z"},
  "total_amount": {"N": "129.50"},
  "status":       {"S": "shipped"},
  "items":        {"L": [
    {"M": {"sku": {"S": "abc"}, "qty": {"N": "2"}}}
  ]}
}
```

This is the **wire-format DynamoDB item** — the same shape `GetItem` returns
to a real DynamoDB SDK. Restoring into AWS DynamoDB is a literal
`PutItem` per file. A failed table can be partially recovered by hand-
editing one item and re-feeding it.

GSIs are **not materialized** under the dump because they are derivable
from `_schema.json` plus the base item set. Re-creating the table from
`_schema.json` and replaying the items rebuilds the GSI; this is what AWS
itself does on table import. (Materializing GSIs would also create
consistency hazards: the dump's GSI rows would be at the same read TS as
the base rows but rewriting them on restore could conflict with the
restore-side OCC, see "Read-Side Consistency".)

### S3

```
s3/
└── photos/
    ├── _bucket.json
    ├── 2026/
    │   └── 04/
    │       └── 29/
    │           ├── img.jpg
    │           ├── img.jpg.metadata.json
    │           ├── thumbnails/
    │           │   └── img-128x128.jpg
    │           └── thumbnails/img-128x128.jpg.metadata.json
    └── archive/
        └── manifest.csv
```

The S3 object body sits at its natural path — every byte that the
elastickv S3 adapter would have streamed through `streamObjectChunks` for a
GET is reassembled in order and written out as a single regular file.

A sidecar `<object>.metadata.json` carries the parts of `s3ObjectManifest`
that S3 itself exposes via headers (`Content-Type`, `Content-Encoding`,
`Cache-Control`, `Content-Disposition`, user-defined `x-amz-meta-*`,
`ETag`, `LastModified`):

```json
{
  "format_version": 1,
  "etag":             "\"d41d8cd98f00b204e9800998ecf8427e\"",
  "size_bytes":       1024576,
  "last_modified":    "2026-04-29T12:34:56.789Z",
  "content_type":     "image/jpeg",
  "content_encoding": "",
  "cache_control":    "max-age=3600",
  "user_metadata":    {"camera": "fx30"}
}
```

Multipart parts are **flattened on dump**: the user gets the assembled
object, not the per-part fragments. Tools like `aws s3 cp --recursive`
work on the dump directory tree directly. In-flight multipart uploads
(`!s3|upload|meta|`, `!s3|upload|part|`, blob chunks not yet committed by
`CompleteMultipartUpload`) are excluded by default; an
`--include-incomplete-uploads` dump flag emits them under
`s3/<bucket>/_incomplete_uploads/<uploadID>/`.

`_bucket.json`:

```json
{
  "format_version": 1,
  "name": "photos",
  "creation_time_iso": "2026-01-15T10:00:00Z",
  "policy_json": null,
  "acl": "private",
  "versioning": "Disabled"
}
```

Generation handling: only the live (latest) generation per bucket is
included. Pre-generation orphans (objects under
`!s3|blob|<bucket>|<oldGen>|...`) are deliberately omitted — they exist on
disk only because GC has not run yet, and replaying them into a fresh
elastickv would resurrect deleted state. They land under
`_orphans/<oldGen>/` only when `--include-orphans` is passed.

### Redis

```
redis/
└── db_0/
    ├── strings/
    │   ├── session%3Aabc123.bin             # SET session:abc123 …
    │   └── counter%3Aglobal.bin             # SET counter:global "42"
    ├── strings_ttl.json                     # { "session%3Aabc123": 1735689600000, ... }
    ├── hashes/
    │   └── user%3A1.json
    ├── lists/
    │   └── queue%3Ajobs.json
    ├── sets/
    │   └── tags%3Apost1.json
    ├── zsets/
    │   └── leaderboard.json
    └── streams/
        └── events.jsonl
```

Redis values are encoded so that `redis-cli --pipe` (or the equivalent in
any client library) can replay them without elastickv.

- `strings/<key>.bin` is the **raw value bytes** — Redis strings are
  binary-safe, so JSON wrapping would force base64 and lose the property
  that `cat strings/<key>.bin` produces the original payload. TTL is in
  the sidecar `strings_ttl.json` keyed by the same encoded filename;
  values are absolute Unix-millis expirations to avoid clock skew on
  restore.
- `hashes/<key>.json`:
  ```json
  {"format_version": 1, "fields": {"name": "alice", "age": "30"}, "expire_at_ms": null}
  ```
- `lists/<key>.json`:
  ```json
  {"format_version": 1, "items": ["job-1", "job-2", "job-3"], "expire_at_ms": null}
  ```
  Order is left-to-right (LPUSH `[3,2,1]` produces `["3","2","1"]`).
- `sets/<key>.json`:
  ```json
  {"format_version": 1, "members": ["red", "green", "blue"], "expire_at_ms": null}
  ```
- `zsets/<key>.json`:
  ```json
  {"format_version": 1, "members": [
    {"member": "alice", "score": 100.0},
    {"member": "bob",   "score":  85.5}
  ], "expire_at_ms": null}
  ```
- `streams/<key>.jsonl`: one entry per line, in stream order:
  ```
  {"id": "1714400000000-0", "fields": {"event": "login",  "user": "alice"}}
  {"id": "1714400000001-0", "fields": {"event": "logout", "user": "alice"}}
  ```
  followed (optionally) by a trailing meta line:
  ```
  {"_meta": true, "length": 2, "last_ms": 1714400000001, "last_seq": 0}
  ```
  The meta line lets a restore tool seed `XADD` IDs without re-deriving
  them from the entries.
- HyperLogLog (`!redis|hll|<key>`) is a binary opaque sketch; written under
  `hll/<key>.bin` byte-for-byte. A non-elastickv consumer that does not
  know HLL can still copy the bytes; a restore back into elastickv (or a
  Redis-compatible HLL implementation) reads them as-is.

Bitmaps and binary strings flow through the `strings/` path and remain
raw bytes.

### SQS

```
sqs/
└── orders-fifo.fifo/
    ├── _queue.json
    └── messages.jsonl
```

`_queue.json`:

```json
{
  "format_version":            1,
  "name":                      "orders-fifo.fifo",
  "fifo":                      true,
  "content_based_deduplication": false,
  "visibility_timeout_seconds": 30,
  "message_retention_seconds": 345600,
  "delay_seconds":             0,
  "redrive_policy":            null
}
```

`messages.jsonl` — one record per line, ordered by `(SendTimestampMillis,
SequenceNumber, MessageId)`:

```
{"message_id":"abc-1","body":"hello","md5_of_body":"…","sender_id":"…","send_timestamp_millis":1714400000000,"available_at_millis":1714400000000,"visible_at_millis":0,"receive_count":0,"first_receive_millis":0,"current_receipt_token":null,"queue_generation":7,"message_group_id":"orders","message_deduplication_id":"abc-1","sequence_number":42,"dead_letter_source_arn":null,"message_attributes":{}}
```

The schema is the dump-time projection of `sqsMessageRecord`
(`adapter/sqs_messages.go:80`) with **all visibility-state fields present
but zeroed**. A restored queue starts with every message visible — which
matches what AWS SQS does when a queue is rehydrated from a backup. If the
operator explicitly requests `--preserve-visibility`, the live
`visible_at_millis` / `current_receipt_token` are kept, but this is opt-in
because resuming with a stale receipt token mid-flight is almost never
the right behavior.

JSONL was chosen over per-message files for two reasons: (1) production
queues commonly hold tens of thousands of messages, and one file per
message inflates inode pressure and `tar` time; (2) message order is
intrinsic to the queue's semantics (especially FIFO), and a single
ordered file makes that order explicit and trivially preservable.

In-flight side records (`!sqs|msg|dedup|`, `!sqs|msg|group|`,
`!sqs|msg|byage|`, `!sqs|msg|vis|`, `!sqs|queue|tombstone|`) are derivable
from the queue config + message records and are not dumped. The dedup
window will reset on restore; this is documented as expected behavior.
Operators who need exactness pass `--include-sqs-side-records`, which
emits an `_internals/` subdirectory of newline-delimited records.

## MANIFEST.json

```json
{
  "format_version":   1,
  "elastickv_version": "v1.7.3",
  "cluster_id":       "ek-prod-us-east-1",
  "commit_ts":        4517352099840000,
  "wall_time_iso":    "2026-04-29T15:42:11.094Z",
  "shards": [
    {"group_id": "default", "raft_leader_at_dump": "n2", "applied_index": 18432021}
  ],
  "adapters": {
    "dynamodb": {"tables":  ["orders", "users", "audit"]},
    "s3":       {"buckets": ["photos", "logs"]},
    "redis":    {"databases": [0, 1]},
    "sqs":      {"queues":  ["orders-fifo.fifo", "notifications"]}
  },
  "exclusions": {
    "include_incomplete_uploads": false,
    "include_orphans":            false,
    "preserve_sqs_visibility":    false,
    "include_sqs_side_records":   false
  },
  "checksum_algorithm": "sha256",
  "encoded_filename_charset": "rfc3986-unreserved-plus-percent",
  "key_segment_max_bytes": 240
}
```

`MANIFEST.json` is **the only file a restore tool must read first**.
Everything else is decoded from its on-disk path and contents.

## Read-Side Consistency

The dump must capture state at a single cluster-wide commit-ts so that:

- A DynamoDB item and the GSI row it would produce, if both were dumped,
  would agree.
- An S3 object's manifest at `!s3|obj|head|` and its blob chunks at
  `!s3|blob|...` correspond to the same upload generation.
- A Redis transaction's writes (multiple keys committed together) all
  appear or all do not appear.
- An SQS message's `data` row and its `vis`/`byage` index entries
  (when included) reflect the same FIFO sequence number.

Implementation:

1. The backup tool calls a new admin RPC `Admin.BeginBackup` that returns
   a cluster-wide `read_ts` chosen by the same path that lease reads use
   (`kv/lease_state.go`, see also
   `2026_04_20_implemented_lease_read.md`). The `read_ts` is held open
   for the duration of the dump by registering it with the **active
   timestamp tracker** (`kv/active_timestamp_tracker.go`) so
   `kv/compactor.go` cannot retire MVCC versions the dump still depends
   on.
2. Every per-adapter scan (`kv.ShardedCoordinator.ScanRange`) is issued
   `at_ts = read_ts`. This is exactly the path
   lease reads already use; no new code path through MVCC is introduced.
3. `Admin.EndBackup` releases the tracker registration. A
   tool-side crash leaves the tracker entry behind for at most
   `backup_ts_ttl` (default 30 minutes, configurable via the same admin
   RPC), after which the registration auto-expires so the compactor is
   not blocked indefinitely.

The dump is therefore a **point-in-time snapshot** of the user-visible
keyspace, not a streaming tail.

### Cross-shard consistency

A multi-shard deployment serves different key ranges from different Raft
groups. `BeginBackup` propagates the same `read_ts` to every group;
`ScanRange` at `read_ts` is the same OCC visibility check on every group.
Because HLC physical ceilings are coordinated through the default group
(`kv/hlc.go`), the chosen `read_ts` is admissible on every group as long
as it is below each group's last-applied commit-ts at the moment of the
call — which `BeginBackup` enforces by re-reading
`max(group_commit_ts)` and adding a small skew buffer (default 50 ms)
matching the existing TSO buffer.

## Internal-State Handling

Internal keys are partitioned into three classes:

| Class | Examples | Backup behavior |
|---|---|---|
| **Re-derivable from user data + config** | DynamoDB GSI rows; S3 route catalog (`!s3route|`); Redis TTL scan index (`!redis|ttl|`); SQS visibility / age / dedup / group / tombstone indexes; route catalog under default group | Excluded by default. Restore re-builds them as user data is replayed. |
| **Per-cluster operational state** | HLC physical ceiling; Raft term/index/conf state; FSM marker files (`raft-engine`, `EKVR`/`EKVM`/`EKVW` magic files in `internal/raftengine/etcd/persistence.go`); write conflict counter | Never dumped. They belong to the cluster, not the data. A restore initializes them fresh. |
| **In-flight transactional state** | `!txn|` intent / lock / resolver records (`kv/txn_keys.go`, `kv/txn_codec.go`); pending S3 multipart uploads | Excluded by default. Optionally dumped under `_internals/` for forensics, but never re-applied by the restore path — replaying intents from a stale read_ts can resurrect aborted transactions. |

This split is what makes the backup safe across versions: the only thing
encoded in the dump is information the user could in principle have
reconstructed from API responses (DescribeTable, GetObject, KEYS *, etc.).
Anything that depends on elastickv's internal physics stays in elastickv.

## Tooling

### Producer: `cmd/elastickv-backup`

A new CLI under `cmd/elastickv-backup/`. Streams the dump out as a
directory tree on local disk, or as `tar` / `tar+zstd` to stdout for
piping straight to S3 / GCS / a tape device.

```
elastickv-backup dump \
  --address     127.0.0.1:50051 \
  --output-dir  /backups/2026-04-29 \
  [--adapter dynamodb,s3,redis,sqs] \
  [--scope    dynamodb=orders,users] \
  [--scope    s3=photos] \
  [--include-incomplete-uploads] \
  [--include-orphans] \
  [--preserve-sqs-visibility] \
  [--include-sqs-side-records] \
  [--checksums sha256]
```

Internally it runs:

```
BeginBackup → ListAdaptersAndScopes → ScanRange (per scope, at read_ts)
            → encode-and-write per adapter → CHECKSUMS → MANIFEST.json
            → EndBackup
```

The producer **does not need access to the leader** — it issues lease
reads. It does need network reach to every Raft group; for multi-shard
deployments it scans groups in parallel.

### Consumer: `cmd/elastickv-restore`

```
elastickv-restore apply \
  --address    127.0.0.1:50051 \
  --input-dir  /backups/2026-04-29 \
  [--adapter dynamodb,s3,redis,sqs] \
  [--scope    dynamodb=orders] \
  [--mode replace|merge|skip-existing] \
  [--rate-limit 5000ops/s]
```

`replace` deletes the target scope before re-importing; `merge`
upserts; `skip-existing` only writes keys not already present.

The consumer talks to the same public adapter endpoints (PutItem,
PutObject, SET / HSET / RPUSH / SADD / ZADD / XADD, SendMessage). It does
**not** write internal keys directly. This is what guarantees that a
restore exercises the same code paths a real client would, and that any
future change to internal layout does not break old backups: as long as
the public protocol still accepts the operation, the restore still works.

### External tools

The format is designed to be extractable without elastickv:

- DynamoDB: `aws dynamodb create-table --cli-input-json _schema.json` and
  then `aws dynamodb put-item --item @items/<pk>/<sk>.json` per file.
- S3: `aws s3 sync s3/<bucket>/ s3://target-bucket/`. The metadata
  sidecars are reapplied with a one-pass script that maps
  `<obj>.metadata.json` to `--metadata` / `--content-type` / etc.
- Redis: a 100-line shell script over `find redis/db_0/strings -name '*.bin' -exec redis-cli -x SET …`,
  with similar one-liners per type.
- SQS: `jq -c . messages.jsonl | xargs -n1 aws sqs send-message --message-body …`.

These are intentionally one-liners. If they require a 500-line bespoke
parser, the format has failed its goal.

## Trade-offs

### Benefits

- **Vendor-independent recovery**. The cluster could be permanently
  unavailable and the user can still get their data back.
- **Format auditability**. A regulator or operator can `find | jq` over a
  dump and verify what is there, in human-readable form.
- **Cross-system migration**. Migrating off elastickv (or onto it from
  AWS-shaped sources) is now a per-adapter sync, not a custom rewrite.
- **Selective restore**. Recovering one DynamoDB item is editing one
  file and running one PutItem.
- **Stable across elastickv versions**. The dump format is decoupled
  from internal magic prefixes and wide-column key layouts, which have
  changed before and will change again.

### Costs

- **Decoded dumps are larger than the FSM stream.** JSON wrapping,
  per-record sidecars, percent-encoded filenames, and one-file-per-record
  for DynamoDB items all bloat on-disk size relative to the Pebble
  snapshot. Expected overhead: 1.5×–3× depending on adapter and average
  record size. Acceptable: backups are written rarely and often compress
  well (tar+zstd on JSON is dense).
- **Dump time is longer than copying the FSM file.** A logical dump must
  scan the live keyspace at `read_ts` and re-encode every record, vs. a
  physical snapshot which is essentially `cp`. Mitigations: per-adapter
  parallel scans, configurable concurrency, no-op if nothing changed
  since the last dump (Phase 2).
- **Restore exercises the public adapter API rather than direct FSM
  apply.** Slower than restoring an FSM snapshot, but correct under
  schema/version drift in a way that direct FSM apply is not.
- **GSIs and side indexes are recomputed on restore.** A correctness win,
  not a regression — but operators should expect restore to take longer
  than dump for large secondary-index footprints.
- **Encoded filenames are not always recognizable.** A SQS message ID
  containing percent-signs has its filename percent-encoded twice. The
  `KEYMAP` file mitigates, and JSONL/JSON record contents always carry
  the original key bytes.

### Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Dump duration exceeds MVCC retention horizon | `read_ts` registered with `active_timestamp_tracker`; compactor cannot retire its versions; producer exits with a typed error if registration is rejected |
| Filesystem path collisions between distinct keys | Per-adapter encoding rule documented; SHA-256 prefix fallback for very long segments; `KEYMAP` records the original bytes |
| Restore writes conflict with live traffic on the target cluster | Restore tool defaults to refusing if the target scope is not empty; `--mode replace` requires explicit confirmation; rate limiter caps OCC pressure |
| Format drift in the dump format itself | `format_version` on every record schema; restore tool reads `MANIFEST.json` first and refuses to operate on unknown major versions |
| Sensitive data in dumps | Out of scope; layered with at-rest encryption / signing on top of the dump tarball, not inside record format |
| Partial dumps from producer crash | Producer writes `MANIFEST.json` **last**; restore refuses dumps without a manifest; a partial directory tree is recognizable as such and ignored by tooling |

## Implementation Phases

### Phase 1 — Dump producer for read paths

Scope: backup-side only. No restore. Targeted at giving operators a
trustworthy off-cluster artifact even before the restore tool is fully
written.

- New admin RPC pair `BeginBackup` / `EndBackup` on `proto/admin.proto`
  registering with `kv/active_timestamp_tracker.go`.
- New tool `cmd/elastickv-backup/` performing `BeginBackup` →
  per-adapter `ScanRange(at_ts)` → encode → write directory tree → emit
  `MANIFEST.json` and `CHECKSUMS` last → `EndBackup`.
- Per-adapter encoders:
  - `internal/backup/dynamodb.go` — items + `_schema.json`.
  - `internal/backup/s3.go` — manifest reassembly into single object
    files, sidecar metadata.
  - `internal/backup/redis.go` — strings/hashes/lists/sets/zsets/streams
    with the layout above.
  - `internal/backup/sqs.go` — `_queue.json` + `messages.jsonl`.
- Filename encoding lives in `internal/backup/filename.go` with shared
  unit tests for round-trip safety.
- Documentation: `docs/operations/backup_restore.md` runbook (separate
  PR after this design lands).

### Phase 2 — Restore consumer

Scope: re-apply a Phase 1 dump into a running cluster.

- `cmd/elastickv-restore/` driving public adapter endpoints.
- Mode flags `replace` / `merge` / `skip-existing`.
- Per-adapter restore drivers reuse the existing client SDKs
  (`cmd/client/`, the AWS SDK for DynamoDB/S3/SQS, `redis-cli`-style
  pipelined writer for Redis).
- Idempotence under retry — restore is a stream of public mutations,
  each of which the adapter already protects with OCC; producer-side
  ordering of operations within a single record (e.g., `XADD` order
  within a stream) is preserved by the dump format.

### Phase 3 — Incremental / scoped backups

Scope: out of this proposal; mentioned only to draw the boundary.

- CDC-style incremental backup: a follow-up design that records mutations
  between two `read_ts` values and dumps the delta in the same
  per-adapter format under a `delta/<from-ts>-<to-ts>/` subtree.
- Concurrent multi-cluster fan-out (one logical backup spanning shards
  on different physical clusters) — depends on the `Distribution`
  control plane being fan-out-aware (see
  `2026_04_27_proposed_keyviz_cluster_fanout.md`).

## Required Tests

### P0

| Test | Verifies |
|---|---|
| `TestFilenameEncodingRoundTrip` | Random bytes → encode → decode → original; long segments overflow into SHA-256 fallback; binary keys take the `b64.` path |
| `TestDynamoDumpItemMatchesGetItem` | An item dumped to `items/<pk>/<sk>.json` matches what the DynamoDB adapter would return for `GetItem` at the same `read_ts` |
| `TestS3DumpReassemblesObject` | Multipart object stored across N parts and M chunks per part round-trips bytewise to a single dump file; metadata sidecar matches manifest |
| `TestRedisDumpAllTypes` | One key per type (string, hash, list, set, zset, stream, hll) round-trips through dump + the Phase 2 restore back to a live store |
| `TestSQSDumpFifoOrderPreserved` | Messages with interleaved `MessageGroupId` are emitted in `(send_ts, sequence_number, message_id)` order; visibility-state fields zeroed by default |
| `TestManifestVersionGate` | Restore with `format_version > current` fails fast with a typed error; same-major-newer-minor allowed; older-major refused with a clear message |
| `TestBeginBackupBlocksCompactor` | Open a `BeginBackup`, force a compaction round, confirm MVCC versions for the registered `read_ts` are retained |

### P1

| Test | Verifies |
|---|---|
| `TestProducerCrashLeavesNoManifest` | Killing the producer mid-dump leaves the partial tree without `MANIFEST.json`; restore refuses; `EndBackup` TTL releases the tracker |
| `TestRestoreReplaceMode` | `replace` empties the target scope before re-importing; live traffic is rejected during the replace window |
| `TestCrossAdapterConsistency` | Dump captures a DynamoDB write and an S3 PutObject committed in the same transaction at TS T; both appear in the dump or neither does |
| `TestExternalToolReplay` | Generated `aws s3 sync` / `aws dynamodb put-item` / `redis-cli --pipe` scripts (run in CI against MinIO / a local DynamoDB / a real Redis) reproduce the dumped state on a non-elastickv target |
| `TestLongKeySHA256Fallback` | A 1 KiB key encodes to `<sha256-prefix-32>__<truncated>`; `KEYMAP` records the original; round-trip restore still hits the right key |
| `TestSQSPreserveVisibilityFlag` | Default leaves messages immediately visible on restore; `--preserve-visibility` retains in-flight receipts |

### P2

| Test | Verifies |
|---|---|
| `FuzzFilenameEncoding` | Filename encoder/decoder never panics and is bijective on arbitrary byte input |
| `BenchmarkDumpThroughputPerAdapter` | Establishes baselines so the Phase 3 incremental design has a regression target |

## References

- `2026_04_14_implemented_etcd_snapshot_disk_offload.md` — the FSM
  snapshot path this proposal *does not* touch.
- `2026_04_20_implemented_lease_read.md` — the consistent read primitive
  used to pin `read_ts`.
- `kv/active_timestamp_tracker.go` — the mechanism that prevents the
  compactor from retiring versions an in-flight backup depends on.
- `internal/s3keys/keys.go`, `kv/shard_key.go`, `adapter/sqs_keys.go`,
  `store/{hash,list,set,zset,stream}_helpers.go` — the internal key
  layouts that this format intentionally hides from the backup consumer.
- `adapter/dynamodb_storage_codec.go`, `adapter/sqs_messages.go`,
  `adapter/redis_storage_codec.go` — the internal value envelopes
  (magic-prefixed protobuf/JSON) that the dump format strips before
  emitting records.

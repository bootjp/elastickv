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
├── CHECKSUMS                         # sha256sum(1)-compatible: "<64-hex>  <relative-path>" per line
├── dynamodb/
│   └── <table-name>/
│       ├── _schema.json
│       ├── KEYMAP.jsonl                            # per-scope (omitted when empty)
│       └── items/
│           └── <pk-segment>/[<sk-segment>.]json
├── s3/
│   └── <bucket-name>/
│       ├── _bucket.json
│       ├── KEYMAP.jsonl                            # per-scope (omitted when empty)
│       ├── <object-key-path>                       # original object bytes
│       └── <object-key-path>.elastickv-meta.json   # sidecar (reserved suffix)
├── redis/
│   └── db_<n>/
│       ├── KEYMAP.jsonl                            # per-scope (omitted when empty)
│       ├── strings/<key>.bin
│       ├── strings_ttl.jsonl
│       ├── hashes/<key>.json
│       ├── lists/<key>.json
│       ├── sets/<key>.json
│       ├── zsets/<key>.json
│       ├── streams/<key>.jsonl
│       ├── hll/<key>.bin                           # HyperLogLog opaque sketch (!redis|hll|<key>)
│       └── hll_ttl.jsonl                           # HLL TTL sidecar (omitted when empty)
└── sqs/
    └── <queue-name>/
        ├── _queue.json
        ├── KEYMAP.jsonl                            # per-scope (omitted when empty)
        └── messages.jsonl
```

`CHECKSUMS` is exact `sha256sum(1)` output so verification works without
elastickv: `sha256sum -c CHECKSUMS` from the dump root succeeds on a
clean dump and fails (with file-level diagnostics) on tampering.

The `.elastickv-meta.json` suffix is **reserved**: a user S3 object
whose key happens to end in `.elastickv-meta.json` is rejected at dump
time with a typed error rather than silently colliding with its own
sidecar. Operators who hold such keys pass `--rename-collisions` to have
them emitted as `<obj>.elastickv-meta.json.user-data` (the rename is
recorded in `KEYMAP`).

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

`KEYMAP.jsonl` at each adapter scope root translates the encoded
filename back to the exact original bytes, in case the user needs to
feed the data into a system that requires the verbatim key. JSONL
(one mapping per line, never loaded fully) is used so the file scales
to millions of keys without becoming a memory bottleneck on either the
producer or a downstream consumer. The translation is also losslessly
recoverable from the encoded filename alone — `KEYMAP.jsonl` is a
convenience, not a correctness dependency. A consumer that does not
need it can ignore it entirely.

### S3 path collisions (file vs. directory)

S3 permits two objects whose keys are `path/to` and `path/to/obj`
simultaneously. POSIX filesystems cannot represent both — `path/to`
cannot be both a regular file and a directory. The dump producer
detects this case at scan time:

- **Pure-leaf case** (the more common case): the key without `/obj`
  suffix is the only object → write at the natural path, no collision.
- **Collision case**: both `path/to` and `path/to/obj` exist in the
  same bucket at the same generation. The shorter key (the one that
  would force a regular file on a path that must also be a directory)
  is renamed to `path/to.elastickv-leaf-data`, with the rename
  recorded in `s3/<bucket>/KEYMAP.jsonl`. The sidecar follows the
  same renamed base: `path/to.elastickv-leaf-data.elastickv-meta.json`.
- The collision-handling rule is documented at dump time in
  `MANIFEST.json` (`s3_collision_strategy: "leaf-data-suffix"`) so a
  restore tool reverses it without guessing.

The producer emits a structured warning per collision so operators
can spot dataset shapes that benefit from object-key normalization.

## Per-Adapter Format

### DynamoDB

```
dynamodb/
└── orders/                                        # composite-key table (hash + range)
    ├── _schema.json
    └── items/
        ├── customer-7421/                          # <pk>/
        │   ├── 2026-04-29T12:00:00Z.json           # <sk>.json
        │   └── 2026-04-29T13:15:42Z.json
        ├── customer-7422/
        │   └── 2026-04-29T09:00:00Z.json
        └── b64.AAECAw../                           # binary partition key (B attribute)
            └── 2026-04-29T10:00:00Z.json
└── sessions/                                      # hash-only table
    ├── _schema.json
    └── items/
        ├── sess-abc123.json                        # <pk>.json directly under items/
        └── sess-def456.json
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

#### One-file-per-item vs. bundle mode

The default — one item per file under `items/<pk>/<sk>.json` — is a
**deliberate, requirement-driven choice**, not an oversight. It is
what makes "recover one row by editing one file and running one
PutItem" trivially true, which is the whole point of a
vendor-independent dump. The cost is filesystem overhead: a 50-million-
item table emits 50 million inodes. On most modern filesystems
(ext4/xfs/zfs/apfs) this is fine for write-once-and-tar dumps but
slow for live filesystem operations.

For tables where the inode count is the binding constraint, the
producer accepts an opt-in `--dynamodb-bundle-mode jsonl` (paired
with `--dynamodb-bundle-size 64MiB`, defaulting to that value) that
emits items as `items/data-<part-id>.jsonl` instead, packed up to the
configurable per-file size budget:

```
dynamodb/orders/
├── _schema.json
└── items/
    ├── data-00001.jsonl   # one item per line, items/data-00001.jsonl ≤ 64 MiB
    ├── data-00002.jsonl
    └── data-00003.jsonl
```

`MANIFEST.json` records the choice (`dynamodb_layout: "per-item" |
"jsonl"`) so a restore tool dispatches the right reader. The bundle
mode is an explicit operational decision; the default stays per-file
because that is the layout the user asked for and the layout that
makes one-line recovery scripts trivial. Operators are also free to
post-process a per-item dump into bundles (`find … | xargs cat`)
without losing any information, so the choice is not load-bearing on
the format itself.

The producer emits a structured warning when an unbundled scope
exceeds 1 million items so operators can decide whether to switch
modes for that table.

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
    │           ├── img.jpg.elastickv-meta.json
    │           ├── thumbnails/
    │           │   ├── img-128x128.jpg
    │           │   └── img-128x128.jpg.elastickv-meta.json
    └── archive/
        ├── manifest.csv
        └── manifest.csv.elastickv-meta.json
```

The S3 object body sits at its natural path — every byte that the
elastickv S3 adapter would have streamed through `streamObjectChunks` for a
GET is reassembled in order and written out as a single regular file.

A sidecar `<object>.elastickv-meta.json` carries the parts of
`s3ObjectManifest` that S3 itself exposes via headers (`Content-Type`,
`Content-Encoding`, `Cache-Control`, `Content-Disposition`, user-defined
`x-amz-meta-*`, `ETag`, `LastModified`). The suffix is reserved (see
"Top-Level Layout" above): a user S3 object whose key ends in
`.elastickv-meta.json` is rejected at dump time unless
`--rename-collisions` is passed.

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
    ├── strings_ttl.jsonl                     # one line per TTL'd key
    ├── hashes/
    │   └── user%3A1.json
    ├── lists/
    │   └── queue%3Ajobs.json
    ├── sets/
    │   └── tags%3Apost1.json
    ├── zsets/
    │   └── leaderboard.json
    ├── streams/
    │   └── events.jsonl
    ├── hll/
    │   └── pfcount%3Auniques.bin
    └── hll_ttl.jsonl                         # one line per TTL'd HLL key
```

Redis values are encoded so that `redis-cli --pipe` (or the equivalent in
any client library) can replay them without elastickv.

- `strings/<key>.bin` is the **raw value bytes** — Redis strings are
  binary-safe, so JSON wrapping would force base64 and lose the property
  that `cat strings/<key>.bin` produces the original payload. TTL is in
  the sidecar `strings_ttl.jsonl`, one record per line:
  ```
  {"key":"session%3Aabc123","expire_at_ms":1735689600000}
  {"key":"cache%3Akv99","expire_at_ms":1735689610500}
  ```
  Values are absolute Unix-millis expirations to avoid clock skew on
  restore. JSONL (not a single JSON object) is used so producers and
  consumers stream the file line-by-line; a Redis instance with tens
  of millions of TTL'd strings would exceed practical memory limits
  for a single-object form, and Redis itself has no upper bound on
  the number of TTL'd keys.
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
  {"_meta": true, "length": 2, "last_ms": 1714400000001, "last_seq": 0, "expire_at_ms": null}
  ```
  The meta line lets a restore tool seed `XADD` IDs without re-deriving
  them from the entries. `expire_at_ms` is null when the stream has no
  TTL and is the absolute Unix-millis expiry otherwise — Redis streams
  can have TTLs set with `EXPIRE` / `PEXPIRE` / `PEXPIREAT`, kept in the
  `!redis|ttl|` scan index by `buildTTLElems` (`adapter/redis.go:2471`).
  Without `expire_at_ms` here, restoring a TTL'd stream would silently
  produce a permanent stream.
- HyperLogLog (`!redis|hll|<key>`) is a binary opaque sketch; written under
  `hll/<key>.bin` byte-for-byte. A non-elastickv consumer that does not
  know HLL can still copy the bytes; a restore back into elastickv (or a
  Redis-compatible HLL implementation) reads them as-is. HLL keys can
  also carry a TTL — the adapter reports HLL as `redisTypeString` but
  stores the TTL in `!redis|ttl|` rather than inline (see comment at
  `adapter/redis.go:2319-2320`). The TTL therefore lives in a
  `hll_ttl.jsonl` sidecar at the `db_<n>` root, in the same shape as
  `strings_ttl.jsonl`:
  ```
  {"key":"pfcount%3Auniques","expire_at_ms":1735689600000}
  ```
  A key with no TTL has no entry in the sidecar; the sidecar file is
  absent entirely when no HLL key in the database has a TTL.

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
  "checksum_format":    "sha256sum",
  "encoded_filename_charset": "rfc3986-unreserved-plus-percent",
  "key_segment_max_bytes": 240,
  "backup_ts_ttl_ms":   1800000,
  "s3_meta_suffix":     ".elastickv-meta.json",
  "s3_collision_strategy": "leaf-data-suffix",
  "dynamodb_layout":    "per-item",
  "max_active_backup_pins": 4
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

### Scan primitive

`ShardStore.ScanAt(ctx, start, end, limit, ts)` already exists today
(`kv/shard_store.go:106`, returning `[]*store.KVPair`); it shards the
range across `RouteEngine` routes and runs `MVCCStore.ScanAt`
(`store/store.go:117`) on the resulting per-group sub-ranges. The
backup producer reuses it as-is — there is no new MVCC code path. What
the proposal does add is a thin **iterator wrapper** so a multi-million
key range does not need to be materialized in one call:

```go
// kv/shard_store.go (new method on *ShardStore — same file as ScanAt)
type BackupScanner interface {
    // Next advances the iterator. Returns (nil, false, nil) at end-of-range.
    // The returned KVPair is owned by the caller until the next call to Next.
    Next(ctx context.Context) (*store.KVPair, bool, error)
    Close() error
}

// NewBackupScanner returns a forward iterator over [start, end) at ts.
// Internally calls ScanAt in pages of pageSize, chaining
// `start = lastReturnedKey + \x00` between pages so no lock is held
// across pages and the compactor's MVCC retention budget is bounded.
func (s *ShardStore) NewBackupScanner(start, end []byte, ts uint64, pageSize int) BackupScanner
```

`NewBackupScanner` lives on `*ShardStore` (its natural home, sharing a
file with `ScanAt`) rather than `*ShardedCoordinator`. The coordinator
type today (`kv/sharded_coordinator.go:124`) has no `*ShardStore`
field — only `engine`, `router`, `groups`, `clock`, and a raw
`store.MVCCStore` — so placing the scanner on the coordinator would
force either an awkward extra field plus `main.go` wiring or a
roundabout coordinator → router → ShardStore lookup. Putting it on
`*ShardStore` keeps the scan primitive cohesive with `ScanAt` and
lets the admin server reach it directly via the existing
`AdminServer` → `ShardStore` wiring used by `GetRaftGroups`.

> **Phase 1 also updates `ScanAt`'s doc comment**
> (`kv/shard_store.go:101–105`) which today warns _"this method is
> NOT a globally consistent snapshot; the caller must implement a
> cross-shard snapshot fence"_. Under the backup contract the caller
> *is* fencing — `BeginBackup` waits for `applied_index ≥ f(ts)` on
> every group before any `Next` call. The new comment must say
> something like: _"consistent across groups when the caller has
> fenced `applied_index ≥ f(ts)` cluster-wide before calling, as
> `BeginBackup` does. Without that fence, results are eventually-
> consistent across groups."_ Without this comment update, code
> review of the Phase 1 producer will flag the `ScanAt` use as
> contradicting the warning.

`pageSize` is the same `ScanAt` `limit` parameter; the producer's
default is 1024 and is exposed as a `--scan-page-size` CLI flag.
Per-adapter encoders consume `BackupScanner` and emit one record per
`Next` (or batch records into the same JSONL file for SQS / streams).

### BeginBackup / EndBackup RPCs

```protobuf
// proto/admin.proto additions
service Admin {
  rpc BeginBackup(BeginBackupRequest) returns (BeginBackupResponse) {}
  rpc EndBackup(EndBackupRequest) returns (EndBackupResponse) {}
  rpc RenewBackup(RenewBackupRequest) returns (RenewBackupResponse) {}
  rpc ListAdaptersAndScopes(ListAdaptersAndScopesRequest)
      returns (ListAdaptersAndScopesResponse) {}
}

message BeginBackupRequest {
  // Time-to-live for the read_ts pin on the active timestamp tracker.
  // If neither EndBackup nor RenewBackup is called within this window
  // the pin is auto-released. Range: 60s–24h. Default: 30m.
  uint64 ttl_ms = 1;
}
message BeginBackupResponse {
  uint64 read_ts            = 1;
  bytes  pin_token          = 2;  // opaque, must be passed back to RenewBackup / EndBackup
  uint64 ttl_ms_effective   = 3;
  repeated ShardApplied shards = 4;  // group_id, applied_index at pin time
}

// RenewBackup extends the deadline for an existing pin. A long-running
// dump calls this every ttl_ms/3 (producer default) so a multi-hour
// scan never relies on a single TTL window. The read_ts is preserved
// across renewals; only the deadline shifts.
message RenewBackupRequest {
  bytes  pin_token = 1;
  // Same range constraint as BeginBackupRequest.ttl_ms:
  // 60s–24h, bounded above by backup_max_ttl_ms. Out-of-range values
  // are rejected with InvalidArgument.
  uint64 ttl_ms    = 2;
}
message RenewBackupResponse { uint64 ttl_ms_effective = 1; }

message EndBackupRequest  { bytes pin_token = 1; }
message EndBackupResponse {}

// ListAdaptersAndScopes runs the per-adapter metadata-prefix scan at
// the read_ts associated with pin_token, so the returned scope list
// is a precise match for what BackupScanner will surface. A new scope
// created by a concurrent client between BeginBackup and this call is
// invisible at the pinned read_ts and therefore not listed.
message ListAdaptersAndScopesRequest  { bytes pin_token = 1; }
message ListAdaptersAndScopesResponse {
  repeated string dynamodb_tables = 1;  // from !ddb|meta|table| scan at read_ts
  repeated string s3_buckets       = 2;  // from !s3|bucket|meta| scan at read_ts
  repeated uint32 redis_databases  = 3;  // {0} until multi-db lands
  repeated string sqs_queues       = 4;  // from !sqs|queue|meta| scan at read_ts
}
```

`ListAdaptersAndScopes` is a thin wrapper over per-adapter metadata
prefix scans run at `read_ts`; it has no new state of its own.

### TTL on the active timestamp tracker

`kv/active_timestamp_tracker.go` today exposes only `Pin(ts) → token`
and `token.Release()`. To support BeginBackup's deadline, this design
extends the tracker:

```go
// kv/active_timestamp_tracker.go (extended)
func (t *ActiveTimestampTracker) PinWithDeadline(ts uint64, deadline time.Time) (*ActiveTimestampToken, error)
func (t *ActiveTimestampToken) Extend(newDeadline time.Time) error
```

`PinWithDeadline` records `(id → ts, deadline)`. A single sweeper
goroutine started by `NewActiveTimestampTracker` wakes once per second
(or when notified by `PinWithDeadline`) and drops entries whose deadline
has passed. `Pin(ts)` keeps its current zero-deadline behavior (no
expiry) and is unaffected; only `BeginBackup` uses the deadline path.
The sweeper logs a structured warning (`backup_pin_expired`) when it
drops a stuck registration so operators see crashed-producer cases in
their existing log pipeline.

**Cluster-wide propagation.** Each elastickv node owns its own
`ActiveTimestampTracker` (`main.go:294`) and its compactor only
consults the local instance (`main.go:335`). For a multi-node
deployment, a pin recorded on the node receiving the admin RPC is not
sufficient — the producer's `BackupScanner` reads from group leaders
that may live on different nodes whose compactors are oblivious to the
local pin. Pins are therefore **propagated through each Raft group's
log** as `BackupPin{pin_id, read_ts, deadline}` / `BackupExtend` /
`BackupRelease` FSM commands. Every replica applies these to its
local tracker on log apply, so the pin set is replicated and durable
across leader changes (a newly-elected leader inherits the pin from
the same log it just applied). Compaction on each replica continues to
consult only the local tracker; the only new ingredient is the FSM
plumbing that keeps every replica's tracker in sync. The backup
admin server (the node receiving `BeginBackup`) is responsible for
fan-out: it submits the per-group entry through the existing
`ShardGroup.Propose` path and waits for commit before returning to the
producer.

**Snapshot installation invalidates a replica's pin set.**
`kvFSM.Restore` (`kv/fsm.go:264-287`) installs a Raft snapshot by
rebuilding the Pebble store from the snapshot stream — and then *only*
the Pebble store. The in-memory `ActiveTimestampTracker` is not part
of the snapshot, so a replica that installs a snapshot during a
backup window loses every `BackupPin` that was recorded before the
snapshot's `Metadata.Index`. Once the leader has compacted the log
past the original `BackupPin` entry index, that entry is gone — a
follower catching up via snapshot will never replay it. The replica's
compactor (now bounded only by other live pins, or unbounded) becomes
free to retire MVCC versions at `read_ts`.

This bounds when the design is safe:

> **Safe-bound invariant**:
> `backup_duration + worst_case_compaction_lag < mvcc_retention_horizon`.

Concretely, with a 1-hour MVCC retention and a 10-minute Raft
snapshot interval, the design tolerates dumps up to ~50 minutes
without hitting the corner case. Beyond that, a snapshot triggered
by a freshly-restarted follower can quietly unprotect a replica.

`BeginBackup` enforces a soft form of this invariant: it reads each
group's `Status.AppliedIndex` and `firstIndex` (the oldest log entry
still on the leader), and refuses to start if any group's
`appliedIndex - firstIndex` is below a configurable margin
(`--snapshot-headroom-entries`, default 10000). Operators dumping a
cluster running close to its snapshot threshold receive
`FailedPrecondition` rather than a silent corruption, and either
retry once the headroom widens or extend `mvcc_retention_horizon`
upstream of the dump.

The failure mode for replicas that go through `Restore` mid-backup is
documented explicitly: the replica's `ScanAt(at_ts=read_ts)` results
become eventually-consistent (versions at `read_ts` may already have
been compacted on that replica), and the producer surfaces the
inconsistency as a `ScanAt` returning fewer keys than the
`applied_index` baseline implied. The producer's per-scope
`expected_keys` heuristic — already needed for
`TestBeginBackupPinSurvivesLeaderChange` — catches it and fails the
dump rather than emitting a corrupted artifact.

A heavier mitigation that would let dumps run longer than the
retention horizon — snapshotting the tracker state alongside the
Pebble snapshot, or having `Restore` query siblings for active pins
— is intentionally **out of scope for Phase 1**. The Phase 1 contract
is "your dump completes within the retention horizon"; longer dumps
are a Phase 3 problem (see Incremental Backups).

**Bound on concurrent active backup pins.** To prevent a misbehaving
or malicious caller from issuing unbounded `BeginBackup` requests and
holding the compactor open across the whole MVCC retention horizon,
the tracker carries a hard cap (default `max_active_backup_pins = 4`,
configurable). When the cap is reached, `PinWithDeadline` returns
`ErrTooManyActiveBackups` and the admin RPC surfaces it as
`ResourceExhausted`. Operators raising this cap should size it
against the GC/compaction headroom — each held pin clamps the
compactor's `Oldest()` timestamp until released. The cap is
intentionally small because backups are an operator action, not
end-user traffic; if four are not enough, something is wrong with
the orchestration layer and adding pins compounds the underlying
problem rather than fixing it.

**TTL ceiling and back-pressure.** `ttl_ms` is bounded above by
`backup_max_ttl_ms` (default 1 h) — a single pin cannot block
compaction beyond that window even if a buggy caller asks for it.
For dumps that legitimately need to run longer (e.g. a 50 TiB
warehouse), the producer renews via `RenewBackup` every `ttl_ms/3`,
so total dump duration is bounded only by overall MVCC retention
budget, not by any single TTL choice. The producer surfaces a
`pin_renewals_total` metric so operators can correlate long-running
dumps with retention pressure.

### BeginBackup → EndBackup flow

1. **Pick `read_ts`**: `BeginBackup` reads the lease-read timestamp
   pipeline (`kv/lease_state.go`, see
   `2026_04_20_implemented_lease_read.md`) and snapshots
   `applied_index` per Raft group.
2. **Wait for shards to catch up**: every group is required to report
   `applied_index ≥ commit_index_at_pin` for the default group's HLC
   ceiling proposal that produced `read_ts`. `BeginBackup` polls each
   group's `Status.AppliedIndex` (already exposed via the existing
   raftengine status interface used by `AdminServer.GetRaftGroups`)
   with a 500 ms tick and a configurable deadline (default 5 s; surfaced
   as `--begin-backup-deadline` on the CLI). **This is the binding wait
   in practice** — a healthy group commits a Raft entry in <100 ms,
   while a lagging shard recovering from a leader change or restart
   can take seconds. Operators tuning `--begin-backup-deadline` are
   adjusting tolerance for shard lag; it does not need to scale with
   pin-fan-out latency. If any group fails to reach the threshold
   within the deadline, `BeginBackup` returns `FailedPrecondition`
   and the producer aborts — the dump is not started until every
   group can serve `read_ts` consistently.
3. **Pin `read_ts` cluster-wide**, not just on the node that received
   the RPC. `kv/active_timestamp_tracker.go` is per-process
   (`main.go:294` constructs one tracker per node and wires it
   exclusively to that node's compactor at `main.go:335`). A pin
   recorded on the receiving node would gate only its own compactor;
   compactors on other nodes — including any group leader serving the
   producer's `BackupScanner` — would be free to retire MVCC versions
   at `read_ts`. The pin therefore fans out to **every replica that
   could compact**, propagated via the Raft log of each group:
   `BeginBackup` proposes a `BackupPin{pin_id, read_ts, deadline}`
   entry through every group involved in the dump; each replica's
   FSM, on apply, calls `PinWithDeadline(read_ts, deadline)` on its
   local tracker. The pin is therefore replicated and durable across
   leader changes — a new leader applies the same entry from the log
   and inherits the pin. `BeginBackup` returns the resulting
   `pin_token = (pin_id, []group_id)` to the producer only after every
   group has committed the entry; if any group's leader is unreachable
   or fails to commit within `--begin-backup-deadline`, `BeginBackup`
   proposes a matching `BackupRelease{pin_id}` to every group that
   *did* commit and returns `Unavailable` so the operator retries
   cleanly without leaving stranded pins.
4. **Producer scans** all configured adapter scopes via
   `BackupScanner.Next(at_ts=read_ts)`.
5. **Renew on long dumps**: the producer calls
   `RenewBackup(pin_token, ttl_ms)` every `ttl_ms / 3`. The admin
   server proposes `BackupExtend{pin_id, deadline}` on every group
   recorded in `pin_token`. The `read_ts` is preserved across
   renewals; only the deadline shifts. A multi-hour dump never relies
   on a single 30-minute pin. Renewals are cheap (one Raft entry per
   group per renewal — at `ttl_ms/3 = 10 min`, that is 6 entries per
   hour per group, negligible alongside production traffic).

   **Per-group renewal retry**: `BackupExtend` proposes through
   `ShardGroup.Propose`, which fails transiently during a leader
   election (typically <1 s under etcd/raft defaults). The admin
   server retries each per-group proposal **up to 3 times with 500 ms
   backoff** before declaring that group's renewal failed. Only after
   the retry budget is exhausted does the producer's renewal goroutine
   log a critical alert and abort the dump — letting the dump continue
   past the TTL would silently produce a corrupted artifact (the
   compactor would have already retired versions the in-flight scan
   still depends on). If renewal succeeds on some groups but fails on
   others after retries, the producer aborts and issues `EndBackup`
   (which itself tolerates partial state — see step 6).
6. **`EndBackup(pin_token)`** proposes `BackupRelease{pin_id}` on
   every group recorded in `pin_token`. The release is idempotent: a
   group that has already swept the pin via deadline expiry treats
   the release as a no-op. A producer crash before EndBackup leaves
   the pin to be reaped by each replica's sweeper goroutine
   independently — the per-replica deadline ensures liveness even if
   no group ever sees a `BackupRelease`.

The dump is therefore a **point-in-time snapshot** of the user-visible
keyspace, not a streaming tail.

### Cross-shard consistency

Step 2 above is the mechanism. Without it, picking `read_ts` from
`max(group_commit_ts) + 50 ms` is only a *liveness* assertion: a
lagging shard might not yet have applied through `read_ts`, and
`ScanAt(at_ts=read_ts)` on that shard would either block forever or
return a partial view. The `applied_index` poll-and-wait in
`BeginBackup` makes the constraint explicit and bounded — every shard
provably has the data at `read_ts` before any scan begins.

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
  [--checksums sha256] \
  [--ttl-ms 1800000] \
  [--begin-backup-deadline 5s] \
  [--snapshot-headroom-entries 10000] \
  [--scan-page-size 1024] \
  [--dynamodb-bundle-mode per-item|jsonl] \
  [--dynamodb-bundle-size 64MiB] \
  [--rename-collisions]
```

Internally it runs:

```
BeginBackup(ttl_ms=1800000) → ListAdaptersAndScopes
                            → BackupScanner.Next* (per scope, at read_ts)
                            → encode-and-write per adapter
                            → CHECKSUMS → MANIFEST.json
                            → EndBackup(pin_token)
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
  [--rate-limit 5000ops/s] \
  [--preserve-ttl] \
  [--preserve-visibility] \
  [--stream-merge-strategy reject|auto-id]
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
- S3: `aws s3 sync --exclude '*.elastickv-meta.json' s3/<bucket>/ s3://target-bucket/`.
  The metadata sidecars are reapplied with a one-pass script that maps
  `<obj>.elastickv-meta.json` to `--metadata` / `--content-type` / etc.
  The `--exclude` is mandatory — without it, `aws s3 sync` would upload
  every sidecar as if it were a user object.
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
- **SQS FIFO deduplication window resets on restore.** Side records
  (`!sqs|msg|dedup|`) are intentionally not dumped, so a queue restored
  on a fresh cluster will accept message-deduplication-IDs that were
  previously suppressed by the live queue's dedup window. For
  `ContentBasedDeduplication=true` this reset is harmless on a clean
  restore (the body hash drives dedup; replaying the dump produces the
  same hashes). For ID-based dedup, callers replaying messages from
  the dump *and* from a still-live source concurrently can produce
  duplicates. Operators who depend on exact dedup state across a
  restore use `--include-sqs-side-records` to opt in to the
  `_internals/dedup.jsonl` artifact, then replay it through a
  follow-up tool that re-seeds the dedup keys.
- **Redis TTL keys may already be expired by the time of restore.**
  TTLs are dumped as absolute Unix-millis (`expire_at_ms`). The
  default restore behavior is **skip-expired**: keys whose
  `expire_at_ms` is in the past at restore time are not re-applied
  (they would be deleted by the TTL reaper on the next pass anyway).
  `--preserve-ttl` forces re-application with the original epoch,
  which makes sense when the goal is auditability (verifying the
  exact TTL state at backup time) rather than getting a working
  cache back.
- **Redis stream restore in `--mode merge`** can collide on entry IDs.
  `XADD <id>` fails when an entry with a higher ID already exists.
  The restore tool's stream behavior:
  - `--mode replace` — `DEL` the stream, then `XADD` every entry with
    the original ID (matches the dump's `_meta` line).
  - `--mode skip-existing` — for each entry, attempt
    `XADD <key> <id> ...` and treat the
    `ERR The ID specified in XADD is equal or smaller than the target
    stream top item` response as "already present, skip." `NOMKSTREAM`
    is **not** a skip-existing mechanism — it only suppresses stream
    creation; it does not skip entries by ID. A `XRANGE <key> <id>
    <id> COUNT 1` probe per entry is an alternative implementation,
    but is the same number of round-trips for the worst case (full
    overlap) and is slower for the common case (small overlap), so
    the `XADD`-and-handle-error form is preferred. The cost is O(N)
    in the entry count regardless.
  - `--mode merge` — refuses to operate on streams unless the target
    is empty; emits an error pointing at the conflict. There is no
    sound way to splice mid-stream without losing the original IDs.
    Operators who need merge-into-non-empty pass
    `--stream-merge-strategy=auto-id`, which falls back to `XADD *`
    and records the original-to-new ID mapping next to the source
    file at `redis/db_<n>/streams/<key>._id_remap.jsonl` (one
    `{"original_id":"…","new_id":"…"}` line per entry) so referential
    integrity can be patched out-of-band by tools that find the dump
    on disk.

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

- New admin RPCs on `proto/admin.proto`: `BeginBackup` (with `ttl_ms`),
  `EndBackup`, `RenewBackup`, `ListAdaptersAndScopes` (signatures in
  "Read-Side Consistency").
- Extend `kv/active_timestamp_tracker.go` with `PinWithDeadline`,
  `Extend`, and the per-second sweeper goroutine that reaps expired
  pins and emits the `backup_pin_expired` structured warning.
- **FSM plumbing for cluster-wide pins** (the propagation described
  in "Cluster-wide propagation" only works if these land):
  - New byte tag constants in `kv/fsm.go` alongside the existing
    `raftEncodeHLCLease = 0x02` (`kv/fsm.go:116`):
    `raftEncodeBackupPin = 0x03`, `raftEncodeBackupExtend = 0x04`,
    `raftEncodeBackupRelease = 0x05`. New
    `applyBackupPin` / `applyBackupExtend` / `applyBackupRelease`
    handlers on `kvFSM`, dispatched from `kvFSM.Apply`
    (`kv/fsm.go:60`) in the same shape as `applyHLCLease`.
  - New `*ActiveTimestampTracker` field on `kvFSM` (`kv/fsm.go:26`),
    wired via a new `NewKvFSMWithHLCAndTracker(store, hlc, tracker)`
    constructor — analogous to how `*HLC` is shared today via
    `NewKvFSMWithHLC`. The coordinator and the FSM must share the
    same tracker instance so `applyBackupPin` and the local
    compactor consult the same map. This invariant goes alongside
    the HLC-sharing invariant in `CLAUDE.md`.
  - New `kv/backup_codec.go` — wire encoding for the three entry
    types. Hand-coded fixed-layout binary (matching the HLC lease
    style):
    ```
    BackupPin     : [tag:1][pin_id:16][read_ts:8][deadline_ms:8]   = 33 bytes
    BackupExtend  : [tag:1][pin_id:16][deadline_ms:8]              = 25 bytes
    BackupRelease : [tag:1][pin_id:16]                             = 17 bytes
    ```
    `pin_id` is a UUIDv4 generated by the admin server at
    `BeginBackup` time and echoed in every subsequent `BackupExtend`
    / `BackupRelease` so the FSM can target the right tracker entry.
    Hand-coded binary (vs. proto) keeps the entry small enough to
    stay within the existing `MaxEntryBytes` budget and avoids
    pulling proto codegen into the FSM apply hot path.
- New `kv/backup_scan.go` — `BackupScanner` iterator wrapping the
  existing `ShardStore.ScanAt` (`kv/shard_store.go:106`) so multi-
  million-key ranges page through `ScanAt` calls of `--scan-page-size`
  rather than materializing in one call.
- New tool `cmd/elastickv-backup/` performing
  `BeginBackup → ListAdaptersAndScopes → BackupScanner.Next* (per scope)
  → encode → write directory tree → CHECKSUMS → MANIFEST.json
  → EndBackup`.
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
| `TestBeginBackupPinFanOutAllNodes` | A 3-node cluster: `BeginBackup` issued to node A; verify nodes B and C have applied the `BackupPin` Raft entry and their compactors retain MVCC versions at `read_ts`. Compactor on B forced to run mid-dump must not retire pinned versions |
| `TestBeginBackupPinSurvivesLeaderChange` | After `BeginBackup` on node A, force a leadership change on a group; the new leader still honors the pin (its FSM applied the same entry); subsequent `BackupScanner.Next` calls succeed |
| `TestBeginBackupGroupUnreachable` | If one group cannot commit `BackupPin` within `--begin-backup-deadline`, `BeginBackup` returns `Unavailable` and proposes `BackupRelease` on every group that did commit; no stranded pins remain |
| `TestBackupPinFSMCodecRoundTrip` | `BackupPin` / `BackupExtend` / `BackupRelease` byte layouts (33 / 25 / 17 bytes) round-trip through the FSM apply path; unknown tag bytes return `ErrUnknownRequestType` rather than panicking |
| `TestRestoreWipesLocalPins` | A replica that installs a Raft snapshot during a backup loses its `BackupPin`; the producer's per-scope `expected_keys` heuristic detects the resulting `ScanAt` shortfall and fails the dump rather than emitting a corrupted artifact |
| `TestBeginBackupRefusesNearSnapshotThreshold` | When any group's `appliedIndex - firstIndex < snapshot_headroom_entries`, `BeginBackup` returns `FailedPrecondition` rather than starting a dump that risks the snapshot-installation path |
| `TestRenewBackupRetriesLeaderElection` | Force a leader election mid-`RenewBackup`; the admin server retries `BackupExtend` up to 3 times with 500ms backoff and succeeds once the new leader is established, without aborting the dump |
| `TestPinWithDeadlineExpiry` | `PinWithDeadline(ts, now+100ms)` is auto-released by the sweeper after the deadline; compactor unblocked; `backup_pin_expired` log emitted |
| `TestBeginBackupWaitsForLaggingShard` | Force shard B's `applied_index` to lag; `BeginBackup` polls until it catches up or times out with `FailedPrecondition`; no scan starts in the timeout case |
| `TestBackupScannerPaging` | A range with > pageSize keys is returned across multiple `ScanAt` pages with no overlap, no gaps; iteration tolerates concurrent writes by completing at the pinned `read_ts` |
| `TestS3SidecarSuffixCollision` | A user S3 object key ending in `.elastickv-meta.json` is rejected without `--rename-collisions`; with the flag, the rename is recorded in `KEYMAP.jsonl` |
| `TestS3PathFileVsDirectoryCollision` | Bucket holds both `path/to` (object) and `path/to/obj`; producer renames the shorter key to `path/to.elastickv-leaf-data` and records it in `KEYMAP.jsonl`; restore tool reverses it via `MANIFEST.s3_collision_strategy` |
| `TestBeginBackupTooManyActiveBackups` | Reaching `max_active_backup_pins` returns `ResourceExhausted`; releasing one pin frees a slot for the next request |
| `TestRenewBackupExtendsDeadline` | `RenewBackup` shifts the deadline; producer's failed-renewal path aborts the dump with a critical log line rather than continuing past the TTL |
| `TestRenewBackupTTLRangeValidation` | `RenewBackup` with `ttl_ms < 60s` or `ttl_ms > backup_max_ttl_ms` returns `InvalidArgument`; in-range values succeed |
| `TestListAdaptersAndScopesAtPinTS` | A scope created (e.g. CreateTable) after `BeginBackup` is not surfaced by `ListAdaptersAndScopes(pin_token)`; pre-existing scopes are |

### P1

| Test | Verifies |
|---|---|
| `TestProducerCrashLeavesNoManifest` | Killing the producer mid-dump leaves the partial tree without `MANIFEST.json`; restore refuses; `EndBackup` TTL releases the tracker |
| `TestRestoreReplaceMode` | `replace` empties the target scope before re-importing; live traffic is rejected during the replace window |
| `TestCrossAdapterConsistency` | Dump captures a DynamoDB write and an S3 PutObject committed in the same transaction at TS T; both appear in the dump or neither does |
| `TestExternalToolReplay` | Generated `aws s3 sync` / `aws dynamodb put-item` / `redis-cli --pipe` scripts (run in CI against MinIO / a local DynamoDB / a real Redis) reproduce the dumped state on a non-elastickv target |
| `TestLongKeySHA256Fallback` | A 1 KiB key encodes to `<sha256-prefix-32>__<truncated>`; `KEYMAP` records the original; round-trip restore still hits the right key |
| `TestSQSPreserveVisibilityFlag` | Default leaves messages immediately visible on restore; `--preserve-visibility` retains in-flight receipts |
| `TestRedisTTLExpiredKeySkippedByDefault` | A key whose `expire_at_ms` is in the past at restore time is not re-applied without `--preserve-ttl`; with the flag it is re-applied with the original epoch |
| `TestRedisStreamMergeRejectsNonEmpty` | `--mode merge` on a non-empty target stream errors out; `--stream-merge-strategy=auto-id` falls back to `XADD *` and writes `_id_remap.jsonl` |
| `TestRedisStreamTTLRoundTrip` | A stream with `PEXPIREAT` round-trips through dump and restore: `expire_at_ms` is captured in the `_meta` line and re-applied so the restored stream expires at the original epoch |
| `TestRedisHLLTTLRoundTrip` | A TTL'd HLL key surfaces in `hll_ttl.jsonl` and is re-applied on restore via the same `EXPIREAT` path used for strings; no-TTL HLLs leave `hll_ttl.jsonl` absent |
| `TestRedisStreamSkipExistingHandlesERR` | The restore tool's `skip-existing` path tolerates `ERR The ID specified in XADD is equal or smaller` without aborting the restore; entries with non-conflicting IDs are still applied |

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

# Snapshot ↔ Logical-Format Decoder (Phase 0)

Status: Proposed
Author: bootjp
Date: 2026-04-29

## Background

The existing FSM snapshot path (`store/snapshot_pebble.go`, see
`2026_04_14_implemented_etcd_snapshot_disk_offload.md`) writes the
entire keyspace as a single opaque stream:

```text
[magic "EKVPBBL1" :8]
[lastCommitTS    :8]
([keyLen:8][key][valLen:8][val])*
[CRC32C footer  :4]
```

Snapshots are taken automatically every `defaultSnapshotEvery = 10000`
log entries (`internal/raftengine/etcd/engine.go:92`) and stored under
`{dataDir}/fsm-snap/<index>.fsm`. They are crash-consistent by
construction — the writer takes a Pebble snapshot at the FSM's
applied index and serializes it.

These files are excellent for what Raft uses them for (log compaction,
follower catch-up). They are useless as a **user-readable backup**:
keys are raw internal-format bytes (`!ddb|item|<table>|<gen>|...`,
`!s3|blob|...`, `!hs|fld|<keyLen>...`, `!sqs|msg|data|...`), values
are protobuf or JSON wrapped with internal magic prefixes
(`0x00 'D' 'I' 0x01`, `0x00 'S' 'M' 0x01`, …), and S3 object bodies
are split into per-chunk blob keys keyed by `(bucket, generation,
object, uploadID, partNo, chunkNo, partVersion)`.

The user holding such a `.fsm` file has bytes only the same elastickv
binary on the same version can read. A cluster failure that destroys
the binary, or a vendor end-of-life, leaves them with unreadable
bytes — this violates the basic property an operator expects of a
backup: **vendor-independent recovery**.

## Why Phase 0

The full live-cluster, point-in-time-consistent extraction pipeline
described in
`2026_04_29_proposed_logical_backup.md` (the Phase 1 design)
introduces non-trivial machinery: replicated `BackupPin` Raft FSM
commands, version-gated RPC fan-outs, expected-keys baselines,
admin-side connection caches, etc. That machinery exists to chase a
specific consistency property: a single cluster-wide `read_ts` across
all Raft groups while the cluster is running.

Many users do not need that property:

- A single-shard or single-Raft-group deployment.
- A deployment where "the most recent crash-consistent snapshot" is a
  good-enough recovery point (the gap between the last snapshot and
  "now" is bounded by `SnapshotEvery × write_rate`).
- A use case where the cluster has been shut down or destroyed and
  the only remaining artifact is the snapshot files on a sidecar
  disk — there is no live cluster to query.
- An operator who wants to perform a one-time format migration off
  elastickv: read the snapshots, convert to logical form, replay
  into the destination system. No live elastickv needed.

For those use cases, **a purely offline `.fsm` ↔ logical-format
converter delivers the vendor-independent-recovery property at a
fraction of the complexity** of the Phase 1 pipeline. Phase 0 is
that converter.

Phase 1 layers on top by producing the *same* logical format (defined
here) from a running cluster with cross-shard PIT consistency.

## Design Goals

| Goal | Detail |
|------|--------|
| **Vendor-independent recovery** | The output directory tree can be inspected and recovered from with `cat`, `jq`, `find`, `aws s3 sync`, `redis-cli` alone — no elastickv binary involved |
| **Pure offline operation** | No live cluster, no admin RPCs, no Raft, no FSM changes. Reads `.fsm` files; writes a directory tree (and the inverse) |
| **Bidirectional** | Decode (`.fsm` → directory tree) and encode (directory tree → `.fsm`) are both in scope. Encode lets an operator restore by stop-replace-restart on a node |
| **Format ownership** | Phase 0 *defines* the per-adapter directory format. Phase 1 produces the same format from a live cluster |
| **No coupling to elastickv internals** | The decoder reads the snapshot's wire format and the public adapter envelopes (DynamoDB proto, SQS JSON, …); no in-process FSM, no MVCC, no OCC paths are exercised |

### Non-Goals

- Cross-shard / cross-Raft-group point-in-time consistency. Each
  group's snapshot is independent; a Phase 0 dump that includes
  multiple groups will reflect different commit-ts per group. Phase 1
  exists to fix this.
- The Raft log tail beyond the last snapshot. Whatever has happened
  between the snapshot's `applied_index` and "now" is not in the
  snapshot, so it is not in a Phase 0 dump. Phase 1 (which scans the
  live store) does see it.
- Online backup of a running cluster as a regular operation. Phase 0
  is "snapshot exists already; convert it." Repeated dumps require
  repeated snapshots, which is a Raft-driven cadence, not a
  user-driven one.
- A new admin RPC, FSM tag, or proto change. Phase 0 is purely a
  command-line tool reading and writing files.

## Output Format (per-adapter directory tree)

This is the canonical format for both Phase 0 and Phase 1 dumps. The
shape of the tree, the filename-encoding rules, the per-adapter
record schemas, and `MANIFEST.json` are all defined here.

### Top-level layout

```text
backup-<utc-timestamp>-<cluster-id>-<applied-index>/
├── MANIFEST.json
├── CHECKSUMS                         # sha256sum(1)-compatible
├── dynamodb/
│   └── <table-name>/
│       ├── _schema.json
│       ├── KEYMAP.jsonl                            # per-scope (omitted when empty)
│       └── items/
│           └── <pk-segment>/[<sk-segment>.]json
├── s3/
│   └── <bucket-name>/
│       ├── _bucket.json
│       ├── KEYMAP.jsonl
│       ├── <object-key-path>                       # original object bytes
│       └── <object-key-path>.elastickv-meta.json   # sidecar (reserved suffix)
├── redis/
│   └── db_<n>/
│       ├── KEYMAP.jsonl
│       ├── strings/<key>.bin
│       ├── strings_ttl.jsonl
│       ├── hashes/<key>.json
│       ├── lists/<key>.json
│       ├── sets/<key>.json
│       ├── zsets/<key>.json
│       ├── streams/<key>.jsonl
│       ├── hll/<key>.bin
│       └── hll_ttl.jsonl
└── sqs/
    └── <queue-name>/
        ├── _queue.json
        ├── KEYMAP.jsonl
        └── messages.jsonl
```

`CHECKSUMS` is exact `sha256sum(1)` output so verification works
without elastickv: `sha256sum -c CHECKSUMS` from the dump root
succeeds on a clean dump and fails (with file-level diagnostics) on
tampering.

### Filename encoding

User keys (table names, bucket names, S3 object keys, Redis keys,
SQS message IDs) can contain bytes illegal or ambiguous on common
filesystems: `/`, NUL, `\`, control characters, names ending in `.`,
case-collision pairs, names longer than `NAME_MAX`.

Rules:

- **S3 object keys preserve their `/` separators** as filesystem
  path separators. `s3://photos/2026/04/29/img.jpg` becomes
  `s3/photos/2026/04/29/img.jpg`. This is the whole point — `aws s3
  sync` on the dump directory works.
- **All other key segments** are encoded with RFC3986 unreserved
  characters (`[A-Za-z0-9._-]`) passed through; every other byte
  becomes `%HH` (uppercase hex).
- **DynamoDB binary partition / sort keys** (the `B` attribute type)
  are rendered as `b64.<base64url>` so a binary key never collides
  with a string key whose hex encoding happens to look like base64.
- If the resulting segment exceeds 240 bytes, it becomes
  `<sha256-prefix-32>__<truncated-original>` and the full original
  bytes are recorded in `<adapter>/<scope>/KEYMAP.jsonl` — same file
  used for filename round-trip and S3 path-collision rename
  bookkeeping.
- **Stream messages** within an SQS queue are not represented by file
  per message (millions of small files); they live in a single
  `messages.jsonl`. Same rationale for Redis stream entries.

`KEYMAP.jsonl` at each adapter scope root translates encoded
filenames back to original bytes when needed. JSONL (one mapping per
line) so the file scales to millions of keys without becoming a
memory bottleneck.

Reversibility depends on the encoding path:

- **`%HH` percent-encoded segments and `b64.<base64url>` segments**
  (DynamoDB binary keys) are losslessly reversible from the filename
  alone. `KEYMAP.jsonl` is **convenience only** for these — a
  consumer that operates on encoded keys, or that needs only the
  human-recognizable subset, can ignore it.
- **SHA-fallback segments** (`<sha256-prefix-32>__<truncated-original>`,
  used when a key segment exceeds 240 bytes) carry only the truncated
  prefix and a hash. The full original bytes are recoverable only via
  `KEYMAP.jsonl`. For these entries `KEYMAP.jsonl` is a
  **correctness dependency** — without it, a consumer can identify
  *which* record a file came from but cannot reproduce the exact
  original key bytes.
- **S3 path-collision renames** (`<obj>.elastickv-leaf-data` for the
  shorter key when both `path/to` and `path/to/obj` exist) similarly
  require `KEYMAP.jsonl` to reverse — the renamed filename does not
  by itself encode that the original key was the un-suffixed form.

A consumer that does not need original-byte recovery for SHA-
fallback or collision-renamed entries (e.g., a migration tool that
preserves keys as-encoded into the destination system) can ignore
`KEYMAP.jsonl` entirely. A consumer that needs exact round-trip on
arbitrary inputs must consult it.

#### S3 path collisions (file vs. directory)

S3 permits two objects whose keys are `path/to` and `path/to/obj`
simultaneously. POSIX filesystems cannot represent both — `path/to`
cannot be both a regular file and a directory. The decoder detects
this case at scan time:

- **Pure-leaf case** (the more common case): the key without `/obj`
  suffix is the only object → write at the natural path, no
  collision.
- **Collision case**: both `path/to` and `path/to/obj` exist in the
  same bucket at the same generation. The shorter key (the one that
  would force a regular file on a path that must also be a
  directory) is renamed to `path/to.elastickv-leaf-data`, with the
  rename recorded in `s3/<bucket>/KEYMAP.jsonl`. The sidecar follows
  the renamed base:
  `path/to.elastickv-leaf-data.elastickv-meta.json`.
- The collision-handling rule is documented at dump time in
  `MANIFEST.json` (`s3_collision_strategy: "leaf-data-suffix"`) so
  an encoder reverses it without guessing.

### Per-adapter format

#### DynamoDB

```text
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

`_schema.json` is structurally identical to a `DescribeTable` JSON
response — tools that already speak DynamoDB JSON ingest it without
translation.

`items/<pk>/<sk>.json` is the wire-format DynamoDB item (the same
shape `GetItem` returns). Restoring into AWS DynamoDB is a literal
`PutItem` per file. A failed table can be partially recovered by
hand-editing one item and re-feeding it.

GSIs are **not materialized** in the dump because they are derivable
from `_schema.json` plus the base item set. Re-creating the table
from `_schema.json` and replaying the items rebuilds the GSI; this
is what AWS itself does on table import.

**Bundle mode for very large tables.** The default — one item per
file under `items/<pk>/<sk>.json` — is what makes "recover one row
by editing one file" trivially true. For tables where inode count
is the binding constraint (50M+ items), the decoder accepts an
opt-in `--dynamodb-bundle-mode jsonl` (paired with
`--dynamodb-bundle-size 64MiB`, defaulting to that value) that
emits items as `items/data-<part-id>.jsonl` instead. `MANIFEST.json`
records the choice (`dynamodb_layout: "per-item" | "jsonl"`) so an
encoder dispatches the right reader.

#### S3

The S3 object body sits at its natural path — every byte that the
elastickv S3 adapter would have streamed through `streamObjectChunks`
for a GET is reassembled in order and written out as a single
regular file.

A sidecar `<object>.elastickv-meta.json` carries the parts of
`s3ObjectManifest` that S3 itself exposes via headers
(`Content-Type`, `Content-Encoding`, `Cache-Control`,
`Content-Disposition`, user-defined `x-amz-meta-*`, `ETag`,
`LastModified`):

```json
{
  "format_version":   1,
  "etag":             "\"d41d8cd98f00b204e9800998ecf8427e\"",
  "size_bytes":       1024576,
  "last_modified":    "2026-04-29T12:34:56.789Z",
  "content_type":     "image/jpeg",
  "content_encoding": "",
  "cache_control":    "max-age=3600",
  "user_metadata":    {"camera": "fx30"}
}
```

The `.elastickv-meta.json` suffix is **reserved**: a user S3 object
whose key ends in `.elastickv-meta.json` is rejected at decode time
unless `--rename-collisions` is passed.

Multipart parts are flattened on decode: the user gets the assembled
object, not the per-part fragments. In-flight multipart uploads
(`!s3|upload|meta|`, `!s3|upload|part|`, blob chunks not yet committed
by `CompleteMultipartUpload`) are excluded by default; an
`--include-incomplete-uploads` flag emits them under
`s3/<bucket>/_incomplete_uploads/<uploadID>/`.

Generation handling: only the live (latest) generation per bucket
is included. Pre-generation orphans land under `_orphans/<oldGen>/`
only when `--include-orphans` is passed.

#### Redis

- `strings/<key>.bin` is the **raw value bytes** — Redis strings are
  binary-safe. TTL is in the sidecar `strings_ttl.jsonl`, one record
  per line:
  ```
  {"key":"session%3Aabc123","expire_at_ms":1735689600000}
  {"key":"cache%3Akv99","expire_at_ms":1735689610500}
  ```
  Values are absolute Unix-millis expirations. JSONL (not a single
  JSON object) is used so producers and consumers stream the file
  line-by-line.
- `hashes/<key>.json`:
  `{"format_version": 1, "fields": {"name": "alice"}, "expire_at_ms": null}`
- `lists/<key>.json`:
  `{"format_version": 1, "items": ["job-1", "job-2"], "expire_at_ms": null}`
  Order is left-to-right (LPUSH `[3,2,1]` produces `["3","2","1"]`).
- `sets/<key>.json`:
  `{"format_version": 1, "members": ["red", "green"], "expire_at_ms": null}`
- `zsets/<key>.json`:
  `{"format_version": 1, "members": [{"member":"alice","score":100}], "expire_at_ms": null}`
- `streams/<key>.jsonl`: one entry per line, in stream order:
  ```
  {"id": "1714400000000-0", "fields": {"event": "login", "user": "alice"}}
  {"_meta": true, "length": 2, "last_ms": 1714400000001, "last_seq": 0, "expire_at_ms": null}
  ```
  `expire_at_ms` is non-null when the stream has TTL set via
  `EXPIRE`/`PEXPIRE`/`PEXPIREAT` (kept in `!redis|ttl|` by
  `buildTTLElems` — `adapter/redis.go:2471`). Without this field, a
  TTL'd stream would be silently restored as permanent.
- `hll/<key>.bin` is the binary opaque HLL sketch. HLL TTLs go in a
  `hll_ttl.jsonl` sidecar (same shape as `strings_ttl.jsonl`) at the
  `db_<n>` root because HLL is reported as `redisTypeString` but
  stores its TTL in the legacy scan index (`adapter/redis.go:2319`).

#### SQS

`_queue.json` has the queue configuration that AWS SQS exposes
(name, FIFO, content-based-dedup, visibility-timeout, retention,
delay-seconds, redrive-policy).

`messages.jsonl` is one record per line, ordered by
`(SendTimestampMillis, SequenceNumber, MessageId)`. The schema is
the dump-time projection of `sqsMessageRecord`
(`adapter/sqs_messages.go:80`) with **all visibility-state fields
present but zeroed**. A restored queue starts with every message
visible — which matches what AWS SQS does when a queue is rehydrated
from a backup. `--preserve-visibility` keeps the live values for
operators who need exactness.

JSONL was chosen over per-message files for two reasons: production
queues commonly hold tens of thousands of messages (one file per
message inflates inode pressure and `tar` time), and message order
is intrinsic to FIFO semantics.

In-flight side records (`!sqs|msg|dedup|`, `!sqs|msg|group|`,
`!sqs|msg|byage|`, `!sqs|msg|vis|`, `!sqs|queue|tombstone|`) are
derivable from the queue config + message records and are not
dumped by default. Operators who need exactness pass
`--include-sqs-side-records` to opt in to the
`_internals/dedup.jsonl` artifact.

### MANIFEST.json

```json
{
  "format_version":   1,
  "phase":            "phase0-snapshot-decode",
  "elastickv_version": "v1.7.3",
  "cluster_id":       "ek-prod-us-east-1",
  "snapshot_index":   18432021,
  "last_commit_ts":   4517352099840000,
  "wall_time_iso":    "2026-04-29T15:42:11.094Z",
  "source": {
    "fsm_path":       "/data/fsm-snap/0000000000000064.fsm",
    "fsm_crc32c":     "deadbeef"
  },
  "adapters": {
    "dynamodb": {"tables":  ["orders", "users"]},
    "s3":       {"buckets": ["photos"]},
    "redis":    {"databases": [0]},
    "sqs":      {"queues":  ["orders-fifo.fifo"]}
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
  "s3_meta_suffix":     ".elastickv-meta.json",
  "s3_collision_strategy": "leaf-data-suffix",
  "dynamodb_layout":    "per-item"
}
```

`phase` is `"phase0-snapshot-decode"` for offline-decoded dumps and
`"phase1-live-pinned"` for Phase 1 live-extracted dumps. Restorers
that care about cross-shard PIT consistency check this field and
warn (or refuse) on Phase 0 inputs.

`snapshot_index` is the FSM `applied_index` that produced the source
snapshot; `last_commit_ts` is the HLC commit-ts at that index. Phase 1
dumps use a `read_ts` field instead.

## Internal-State Handling

Internal keys are partitioned into three classes:

| Class | Examples | Decode behavior |
|---|---|---|
| **Re-derivable from user data + config** | DynamoDB GSI rows; S3 route catalog (`!s3route\|`); Redis TTL scan index (`!redis\|ttl\|`); SQS visibility / age / dedup / group / tombstone indexes | Excluded by default. Restore re-builds them by replaying user data through the public adapter API. |
| **Per-cluster operational state** | HLC physical ceiling; Raft term/index/conf state; FSM marker files; write conflict counter | Never decoded. Belong to the cluster, not the data. |
| **In-flight transactional state** | `!txn\|` intent / lock / resolver records (`kv/txn_keys.go`, `kv/txn_codec.go`); pending S3 multipart uploads | Excluded by default. Optionally dumped under `_internals/` for forensics, but never re-applied — replaying intents from a stale snapshot can resurrect aborted transactions. |

This is what makes Phase 0 backups safe across versions: only
information the user could have reconstructed from API responses
(DescribeTable, GetObject, KEYS *, etc.) is encoded. Anything
depending on elastickv's internal physics stays in elastickv.

## Tooling

### Decoder: `cmd/elastickv-snapshot-decode`

```text
elastickv-snapshot-decode \
  --input  <fsm-file>          # or --input-snap-dir <data-dir>/fsm-snap/
  --output <directory-root> \
  [--adapter dynamodb,s3,redis,sqs] \
  [--scope dynamodb=orders,users] \
  [--include-incomplete-uploads] \
  [--include-orphans] \
  [--preserve-sqs-visibility] \
  [--include-sqs-side-records] \
  [--dynamodb-bundle-mode per-item|jsonl] \
  [--dynamodb-bundle-size 64MiB] \
  [--rename-collisions] \
  [--checksums sha256]
```

Pipeline:

```text
open .fsm                                       # verifies CRC32C footer
parse EKVPBBL1 magic + lastCommitTS
stream ([keyLen:8][key][valLen:8][val])* entries:
  dispatch by leading prefix:
    "!ddb|item|"  → DynamoDB item encoder
    "!ddb|meta|"  → DynamoDB schema encoder
    "!s3|obj|"    → S3 manifest encoder (buffer; emit when blob seen)
    "!s3|blob|"   → S3 blob writer (append to assembled object body)
    "!redis|str|" → Redis string encoder
    "!hs|"        → Redis hash encoder
    "!lst|"       → Redis list encoder
    "!st|"        → Redis set encoder
    "!zs|"        → Redis zset encoder
    "!stream|"    → Redis stream encoder
    "!sqs|queue|" → SQS queue meta encoder
    "!sqs|msg|"   → SQS message encoder
    "!txn|"       → drop (or emit to _internals/ if requested)
    other "!..."  → drop (internal state)
emit MANIFEST.json + CHECKSUMS last
```

Memory: streaming. The decoder holds at most one assembled S3
object's manifest entries in memory at a time (one per active
bucket × generation; bounded by the number of in-flight objects in
the snapshot). Other adapters emit per-record without buffering.

`--input-snap-dir` mode points at a `fsm-snap/` directory and
processes the newest `.fsm` (or every `.fsm` in chronological order
with `--all`) so an operator does not need to know which file is
current.

### Encoder: `cmd/elastickv-snapshot-encode`

```text
elastickv-snapshot-encode \
  --input  <directory-root> \
  --output <fsm-file> \
  [--last-commit-ts <unix-ms>]
```

Pipeline:

```text
read MANIFEST.json (refuse on unknown major format_version)
walk per-adapter subtrees:
  DynamoDB → emit !ddb|meta|table| then !ddb|item| KV pairs
  S3       → emit !s3|bucket|meta| then !s3|obj|head| then !s3|blob| pairs
              (split assembled bodies into chunks at the same chunk-size
              the live cluster uses; chunk_size from MANIFEST.json)
  Redis    → emit per-type wide-column or simple keys
  SQS      → emit !sqs|queue|meta| then !sqs|msg|data| pairs
verify the resulting key-set has no duplicates
write EKVPBBL1 header + lastCommitTS + sorted KV stream + CRC32C footer
```

Output is a valid `.fsm` file in the same wire format the live FSM
emits.

### Restore via stop-replace-restart

Phase 0 does not include a "live load this snapshot" RPC — that is
non-trivial (would need pause/resume Apply, install snapshot at the
right index, etc.). Operators restore by:

1. Stop the target node.
2. Generate the matching `.snap` token file (see
   `2026_04_14_implemented_etcd_snapshot_disk_offload.md` for the
   17-byte EKVT format) using a small `cmd/elastickv-snap-token`
   helper that takes the `.fsm` path and emits the token file.
3. Place the new `.fsm` in `{dataDir}/fsm-snap/<index>.fsm` and the
   token in `{dataDir}/snap/<index>.snap` (atomic rename of both,
   then `syncDir` of both directories).
4. Start the node. It loads the snapshot at startup as if it had
   just installed it from a leader.

For multi-node clusters: do this on one node first, then re-add the
others as fresh members so they snapshot-install from the seeded
node.

For a *fresh cluster* (zero state, just-bootstrapped), the encoder
output can be placed directly under `fsm-snap/` and the cluster
opens to it as its initial state. This is the cleanest restore
path.

The runbook for both is documented in
`docs/operations/snapshot_restore.md` (separate PR after this
design lands).

### External tools

- DynamoDB: `aws dynamodb create-table --cli-input-json _schema.json` and
  then `aws dynamodb put-item --item @items/<pk>/<sk>.json` per file.
- S3: `aws s3 sync --exclude '*.elastickv-meta.json' s3/<bucket>/ s3://target-bucket/`,
  followed by a one-pass script that maps
  `<obj>.elastickv-meta.json` to `--metadata` / `--content-type`.
- Redis: a 100-line shell script over
  `find redis/db_0/strings -name '*.bin' -exec redis-cli -x SET …`,
  with similar one-liners per type.
- SQS: `jq -c . messages.jsonl | xargs -n1 aws sqs send-message --message-body …`.

These are intentionally one-liners. If they require a 500-line
bespoke parser, the format has failed its goal.

## Trade-offs

### Benefits

- **Minimal surface area.** Two CLI tools, one shared internal package,
  no proto changes, no FSM changes, no admin RPCs, no Raft involvement.
- **Works without a running cluster.** A `.fsm` file pulled from S3 /
  tape / a sidecar disk is all the input the decoder needs.
- **Deterministic and offline-testable.** A unit test feeds a
  synthetic snapshot and asserts the resulting directory tree.
- **Format owner.** Phase 1 produces the same format; the format
  itself is defined here, in one place, decoupled from the
  live-cluster machinery.

### Costs

- **Multi-shard inconsistency.** Each Raft group's snapshot is at an
  independent `applied_index` and therefore an independent
  `commit_ts`. A multi-shard dump produced by running the decoder
  over each group's `.fsm` shows shard-A's state at one ts and
  shard-B's at another — cross-shard transactions can split.
- **Staleness.** Whatever was written between the snapshot's
  `applied_index` and "now" is not in the snapshot, so it is not in
  the decoded output. The gap is bounded by `SnapshotEvery × write_rate`
  (default 10000 entries; for a write-heavy cluster, seconds; for a
  quiet one, hours).
- **Cadence is not user-controlled.** "Snapshot now" requires a Raft
  trigger; the decoder cannot create a fresh snapshot, only consume
  existing ones. Phase 1 takes a fresh PIT on demand.
- **The encoder must reproduce internal layouts.** Encoding a
  decoded directory tree back into a `.fsm` requires re-emitting the
  exact internal key encodings (`!ddb|item|<table>|<gen>|<orderedKey>`
  with the right `generation` value, `!s3|blob|<bucket><gen><obj>...`
  with the right chunk boundaries). This means the encoder has a
  hard dependency on the live key-format version: encoding a v1
  dump as a v2 `.fsm` requires per-version logic. The decoded
  directory tree itself is version-stable; only the encoder side
  takes on this coupling.

### When Phase 0 is sufficient

| Need | Phase 0 enough? |
|---|---|
| Vendor-independent recovery format on disk | ✓ |
| One-time export off elastickv (migration) | ✓ |
| Single-shard cluster backup | ✓ |
| Per-shard backup of long-lived data | ✓ |
| Recovering one DynamoDB row by hand | ✓ |
| Multi-shard cross-group point-in-time consistency | ✗ — Phase 1 |
| "Take a backup right now" with bounded staleness | ✗ — Phase 1 |
| Online running-cluster backup | ✗ — Phase 1 |
| Cross-cluster live replication | ✗ — Phase 1 / Phase 3 |

### Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Stale `.fsm` (old snapshot, much newer cluster state) | Decoder records `snapshot_index` and `wall_time_iso` in `MANIFEST.json`; operators reading the dump know how stale it is |
| Encoder produces a `.fsm` the running cluster cannot load | Encoder runs `cmd/elastickv-snapshot-decode` on its own output and asserts a byte-identical round-trip before emitting the final `.fsm` |
| Format drift between Phase 0 dumps from different elastickv versions | `format_version` major-version gate on `MANIFEST.json`; decoder refuses unknown major versions; minor-version drift handled through optional fields |
| In-flight S3 multipart uploads referenced by the snapshot but not committed | Excluded by default; `--include-incomplete-uploads` opts in and emits them under a clearly-marked `_incomplete_uploads/` subtree |
| Restoring an encoded `.fsm` onto a node with mismatched cluster_id | Encoder emits `cluster_id` in `MANIFEST.json`; the restore-runbook step checks it matches the target node before placing the file |

## Implementation Phases

### Phase 0a — Decoder

- New binary `cmd/elastickv-snapshot-decode/`.
- `internal/backup/` package (filename encoding, per-adapter
  encoders, `MANIFEST.json` writer, CHECKSUMS writer). Shared with
  Phase 1 — Phase 1 imports the same package and produces the same
  format.
- Per-adapter encoders:
  - `internal/backup/dynamodb.go` — items + `_schema.json`.
  - `internal/backup/s3.go` — manifest reassembly into single object
    files, sidecar metadata, multipart flattening.
  - `internal/backup/redis.go` — strings/hashes/lists/sets/zsets/streams/hll
    with TTL sidecars.
  - `internal/backup/sqs.go` — `_queue.json` + `messages.jsonl`.
- Filename encoding lives in `internal/backup/filename.go` with
  shared unit tests for round-trip safety.
- `cmd/elastickv-snap-token` helper for synthesizing the matching
  `.snap` EKVT token file from a `.fsm`.
- Documentation: `docs/operations/snapshot_restore.md` runbook
  (separate PR after this design lands).

### Phase 0b — Encoder

- New binary `cmd/elastickv-snapshot-encode/`.
- Reverse-direction encoders mirroring the Phase 0a code path.
- Self-test: encode → decode round-trip asserts byte-identical
  output for the user-visible directory tree (modulo wall-time and
  encoder version metadata).
- End-to-end test: encode a synthetic dump, place output in a
  fresh single-node cluster's `fsm-snap/` + `snap/`, start the
  cluster, verify reads return the original data via the
  DynamoDB / S3 / Redis / SQS adapters.

### Phase 0c — Operator integration

- Stop-replace-restart runbook in `docs/operations/`.
- Cluster-id and version-compatibility guards.
- Tar / tar+zstd helpers for shipping decoded directory trees off
  the host.

## Required Tests

### P0

| Test | Verifies |
|------|---|
| `TestFilenameEncodingRoundTrip` | Random bytes → encode → decode → original; long segments overflow into SHA-256 fallback; binary keys take the `b64.` path |
| `TestSnapshotDecodeAllAdapters` | A synthetic `.fsm` containing one record per adapter (DynamoDB, S3, Redis × all types, SQS) decodes to the documented directory shape; record contents match the inputs |
| `TestS3DecodeReassemblesObject` | Multipart object stored across N parts and M chunks per part round-trips bytewise to a single decoded file; `.elastickv-meta.json` matches manifest |
| `TestRedisDecodeAllTypes` | One key per type round-trips through decode + the Phase 0b encode back to a `.fsm` whose Pebble image matches the original |
| `TestSQSDecodeFifoOrderPreserved` | Messages with interleaved `MessageGroupId` are emitted in `(send_ts, sequence_number, message_id)` order; visibility-state fields zeroed by default |
| `TestStreamTTLPreserved` | A stream with `PEXPIREAT` round-trips: `expire_at_ms` is captured in the `_meta` line and re-applied by the encoder |
| `TestHLLTTLPreserved` | TTL'd HLL key surfaces in `hll_ttl.jsonl` and is re-applied by the encoder; no-TTL HLLs leave `hll_ttl.jsonl` absent |
| `TestS3PathFileVsDirectoryCollision` | Bucket holds both `path/to` (object) and `path/to/obj`; decoder renames the shorter to `path/to.elastickv-leaf-data` and records it in `KEYMAP.jsonl`; `MANIFEST.s3_collision_strategy` set |
| `TestS3SidecarSuffixCollision` | A user S3 object key ending in `.elastickv-meta.json` is rejected without `--rename-collisions`; with the flag the rename is recorded |
| `TestEncodeDecodeRoundTrip` | Encoded `.fsm` decodes back to a directory tree byte-identical to the original (wall-time fields excluded) |
| `TestManifestVersionGate` | Decoder refuses inputs with `format_version > current_major`; same-major-newer-minor allowed; older-major refused with a clear message |
| `TestDecoderRejectsTruncatedFSM` | A `.fsm` whose CRC32C footer fails verification is rejected with a typed error before any record is emitted |

### P1

| Test | Verifies |
|------|---|
| `TestEncoderProducesLoadableSnapshot` | Encoder output placed under a fresh node's `fsm-snap/` + `snap/` is loaded successfully on `Open`; the resulting cluster serves the original data via every adapter |
| `TestLongKeySHA256Fallback` | A 1 KiB key encodes to `<sha256-prefix-32>__<truncated>`; `KEYMAP.jsonl` records the original; encoder reverses correctly |
| `TestExternalToolReplay` | Generated `aws s3 sync` / `aws dynamodb put-item` / `redis-cli --pipe` scripts (run in CI against MinIO / a local DynamoDB / a real Redis) reproduce the decoded state on a non-elastickv target |
| `TestEncoderClusterIDGuard` | Encoder writes `cluster_id` in `MANIFEST.json`; the restore runbook step refuses to place the file if the target node's `cluster_id` differs |
| `TestPhaseFieldDistinguishesPhase0And1` | A Phase 1 dump and a Phase 0 dump containing the same logical state both pass the decoder's structural validation but `MANIFEST.phase` distinguishes them |

### P2

| Test | Verifies |
|------|---|
| `FuzzFilenameEncoding` | Encoder/decoder never panics and is bijective on arbitrary byte input |
| `BenchmarkSnapshotDecodeThroughput` | Establishes baseline throughput per adapter so Phase 1 has a regression target |

## References

- `2026_04_14_implemented_etcd_snapshot_disk_offload.md` — the
  `.fsm` file format Phase 0 reads and writes.
- `2026_04_29_proposed_logical_backup.md` — Phase 1 (live PIT
  extraction). Phase 1 produces dumps in the same format defined
  here.
- `internal/s3keys/keys.go`, `kv/shard_key.go`, `adapter/sqs_keys.go`,
  `store/{hash,list,set,zset,stream}_helpers.go` — the internal key
  layouts the decoder dispatches on and the encoder reproduces.
- `adapter/dynamodb_storage_codec.go`, `adapter/sqs_messages.go`,
  `adapter/redis_storage_codec.go` — internal value envelopes
  (magic-prefixed protobuf/JSON) that the decoder unwraps and the
  encoder re-wraps.

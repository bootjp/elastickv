# Snapshot Logical Encoder (Phase 0b)

Status: Proposed
Author: bootjp
Date: 2026-05-25

## Background

Phase 0a (`2026_04_29_partial_snapshot_logical_decoder.md`,
milestones all merged: PRs #790, #791, #792, #806, #810) shipped the
**decoder**: an offline tool that reads a native Pebble `.fsm`
snapshot and writes a vendor-independent, per-adapter directory tree
(`internal/backup/`, `cmd/elastickv-snapshot-decode`).

Phase 0b is the **inverse**: `cmd/elastickv-snapshot-encode` reads a
Phase 0a/Phase 1 directory tree and produces a native `.fsm` file
that a stopped node can load by stop-replace-restart (parent doc
§"Restore via stop-replace-restart"). This doc pins down the encoder
specifics the parent doc left at sketch level (parent §"Encoder:
`cmd/elastickv-snapshot-encode`" and §"Phase 0b — Encoder").

The parent doc is the format owner and remains authoritative for the
directory-tree shape, filename encoding, and `MANIFEST.json`. This
doc owns the **reverse-direction wire-format reconstruction** and the
decisions that only arise on the encode side. On landing this
proposal, the parent doc is promoted `proposed` → `partial` (Phase 0a
shipped) via `git mv`.

## Why a separate design doc

The encoder is not a mechanical mirror of the decoder. Three problems
surface only on the encode side, and each is a wire-format decision:

1. **The directory tree is lossy w.r.t. the internal keyspace.** The
   decoder *drops* every re-derivable internal index (Redis TTL scan
   index, DynamoDB GSI rows, SQS visibility / dedup / group / by-age
   side records, per-scope generation counters). A loadable `.fsm`
   must contain those rows, or the restored node serves wrong results
   (a TTL'd key never expires; a GSI query returns nothing; an SQS
   FIFO dedup window is empty). The encoder must **reconstruct the
   full internal keyspace**, not just re-wrap the user-visible subset.
   This is the central Phase 0b decision (§"Internal-index
   reconstruction").

2. **The directory tree carries no per-key commit timestamp.** The
   decoder reads each entry's MVCC `commit_ts` but discards it — only
   the snapshot-global `last_commit_ts` survives, in `MANIFEST.json`.
   The encoder must therefore choose what invTS suffix to stamp on
   every reconstructed key (§"MVCC re-encoding").

3. **The native `.fsm` format has no CRC32C footer.** The parent doc's
   format sketch (parent §"Background") shows a trailing
   `[CRC32C footer :4]`; that framing belongs to the *MVCC streaming
   restore* path (`store/lsm_store.go` `readStreamingMVCCRestoreHeader`,
   `crc32.NewIEEE`), **not** the native Pebble snapshot the decoder
   reads and the encoder must emit. The authoritative target format is
   defined in §"Target `.fsm` format" below.

## Target `.fsm` format (authoritative)

The encoder emits exactly what `store/snapshot_pebble.go`
`(*pebbleSnapshot).WriteTo` produces and what
`internal/backup/snapshot_reader.go` `ReadSnapshot` consumes:

```text
[8 bytes]   magic "EKVPBBL1"
[8 bytes]   lastCommitTS              (LittleEndian uint64)
repeated (sorted ascending by encoded key):
  [8 bytes]   keyLen                  (LittleEndian uint64)
  [keyLen]    encoded key  = <userKey><invTS>          invTS = ^commitTS, 8 bytes BigEndian
  [8 bytes]   valLen                  (LittleEndian uint64)
  [valLen]    encoded value = <flags:1><expireAt:8 LE><body>
              flags bit0      = tombstone
              flags bits1-2   = encryption_state (00 = cleartext)
              flags bits3-7   = reserved (must be zero)
```

There is **no** trailing checksum. Termination is a clean EOF at the
start of a key-length field (`snapshot_reader.go` `readEntryLen`).

Constraints the encoder must honor (all enforced by the reader, so a
violation is caught by the round-trip self-test):

- **Sorted order.** The live writer iterates a Pebble snapshot, which
  yields keys in ascending byte order. The loader
  (`store/lsm_store.go` native restore path) feeds entries into a
  Pebble batch, which does not *require* sorted input for correctness
  — but emitting sorted output is required for a deterministic,
  byte-stable `.fsm` and matches what the live FSM produces. The
  encoder sorts the fully-materialized encoded-key set before writing.
- **Per-entry size caps.** `keyLen ≤ MaxSnapshotEncodedKeySize`
  (1 MiB + 8), `valLen ≤ MaxSnapshotEncodedValueSize` (256 MiB + 9 +
  34). The encoder rejects any reconstructed entry that exceeds these
  with a typed error rather than emitting an unloadable file.
- **Cleartext only (Phase 0b).** The encoder emits `encryption_state =
  00`. Encrypted snapshots are out of scope (the decoder already
  refuses them: `ErrSnapshotEncryptedEntry`).

## MVCC re-encoding

Every reconstructed `(userKey, userValue, expireAt, tombstone=false)`
tuple is encoded as:

- `encKey = userKey || BigEndian(^commitTS)`
- `encVal = flags || LittleEndian(expireAt) || userValue`, `flags = 0`
  (cleartext, live, no tombstone).

**Choice of `commitTS`.** The directory tree does not preserve
per-key commit timestamps, and the decoder discards them, so any
single value round-trips identically through decode. The encoder
stamps **every** reconstructed key with the snapshot-global
`last_commit_ts` from `MANIFEST.json`:

```text
commitTS := manifest.last_commit_ts
invTS    := ^commitTS                       // same suffix on every key
```

Rationale:

- The loaded node's HLC physical ceiling is seeded from the snapshot's
  `lastCommitTS` (header field); stamping keys at exactly that ts
  keeps every restored row at-or-below the ceiling, so the first
  leader-issued read timestamp after restart sees all of them
  (CLAUDE.md HLC invariant — reads use a leader-issued ts ≥ ceiling).
- Using a single ts means the encoder never has to invent a *relative*
  ordering between keys that the dump does not record. MVCC visibility
  for a restored-then-read key only requires `commitTS ≤ read_ts`;
  equality at `last_commit_ts` satisfies that for every key.
- Tombstones are never emitted: Phase 0a dumps reflect live
  user-visible state (parent §"Internal-State Handling"), so the
  encoder only writes live rows. A restored node starts with a clean
  MVCC history (one version per key).

The `last_commit_ts` written into both the file header and every
key's invTS is the same value the decoder will read back, so the
self-test (§"Round-trip self-test") is exact.

**`last_commit_ts` is a 64-bit HLC value, not Unix-ms.** It is the
same encoding the live store carries (`store/lsm_store.go`
`lastCommitTS`): a 48-bit physical half (Unix-ms) in the upper bits
and a 16-bit logical counter in the lower bits (CLAUDE.md "Timestamp
Oracle"). The encoder reads it verbatim from `MANIFEST.json` and
writes it verbatim into the file header and every invTS — it does not
reinterpret or rescale it.

**`--last-commit-ts T` override semantics.** The override exists only
for the rare "the dump's recorded ceiling is too low for the target
cluster's HLC" recovery case; **it is forbidden to diverge from the
manifest value by default**, and when supplied it is applied as a
single atomic substitution everywhere `last_commit_ts` appears:

- The chosen `T` (a 64-bit HLC value, same encoding as above)
  **replaces** `manifest.last_commit_ts` as the source for *both* the
  EKVPBBL1 header *and* every key's `invTS = ^T`. There is never a
  state where the header and the per-key suffix disagree — the whole
  point of the uniform-stamping rule (above) is preserved.
- **Validation (fail-closed):** the encoder rejects `T <
  manifest.last_commit_ts`. A lower ceiling would seed the restored
  node's HLC below timestamps already durable in the dump, letting a
  post-restart leader re-issue a ts ≤ a restored row's commit ts —
  the exact HLC-ceiling regression the invariant forbids. `T ≥
  manifest.last_commit_ts` is the only accepted direction (raising the
  ceiling is always safe). Equality is the default (no override).
- **Self-test (§"Round-trip self-test") compares against the
  effective `T`, not the manifest value.** The round-trip re-decode
  reads `T` back as the dump's `last_commit_ts`, so the comparison is
  exact when the encoder also stamps `MANIFEST.last_commit_ts := T` in
  the round-trip's intermediate manifest. (Per-key `invTS` is
  discarded by decode, so only the header value participates in the
  comparison — and it is `T` on both sides.)

The directory tree records no per-key write timestamp, so there is no
*per-key* monotonicity check to perform; the single ceiling check
above is the complete monotonicity guarantee.

## Internal-index reconstruction

This is the load-bearing decision. The decoder partitions internal
keys into three classes (parent §"Internal-State Handling"). On
encode they map as follows:

| Class | Examples | Encode behavior |
|---|---|---|
| **User-visible records** | Redis strings/hashes/.../streams; DynamoDB items + `_schema.json`; S3 object bodies + sidecars; SQS `messages.jsonl` + `_queue.json` | Reconstructed from the directory tree — the direct inverse of each Phase 0a `Handle*` encoder. |
| **Re-derivable indexes** | Redis TTL scan index (`!redis\|ttl\|`); DynamoDB GSI rows (`!ddb\|gsi\|`); SQS vis/byage/dedup/group/seq side records; per-scope generation counters (`!s3\|bucket\|gen\|`, `!ddb\|table\|gen\|`, `!sqs\|queue\|gen\|`) | **Reconstructed by the encoder** from the user records + config it just read. Required for a correct loadable image. |
| **Per-cluster operational / in-flight transactional** | HLC ceiling, Raft term/index/conf, FSM markers, write-conflict counter; `!txn\|` intents/locks; `!dist\|`, `!encryption\|` rows | **Never emitted.** They belong to the receiving cluster, not the data. The restore runbook seeds HLC/Raft state via the `.snap` token, not via FSM rows. |

The middle row is what makes Phase 0b larger than "reverse the
handlers." The encoder must own the **index-derivation logic** for
each adapter, mirroring the live adapter's index builders:

- **Redis TTL scan index.** For every reconstructed key whose sidecar
  (`strings_ttl.jsonl`, `hashes/<k>.json` `expire_at_ms`, stream
  `_meta.expire_at_ms`, `hll_ttl.jsonl`, ...) carries a non-null
  expiry, emit the matching `!redis|ttl|` row that the live adapter's
  `buildTTLElems` (`adapter/redis.go`) would have written. Without it
  the restored node never expires the key.
- **DynamoDB GSI rows.** Re-derive from `_schema.json`'s GSI
  definitions applied to each base item — exactly what the live
  adapter does on `PutItem`. (Parent doc: "Re-creating the table from
  `_schema.json` and replaying the items rebuilds the GSI.") The
  encoder performs that derivation offline.
- **SQS side records.** Re-derive `dedup`, `group`, `byage`, `vis`,
  `seq` rows from `messages.jsonl` + `_queue.json` using the same
  rules as `adapter/sqs_messages.go` / `sqs_keys.go`. By default
  messages restore fully visible (vis rows zeroed), matching parent
  §"SQS".
- **Generation counters.** Each scope's live generation is recorded in
  its `_bucket.json` / `_schema.json` / `_queue.json` (the decoder
  captured it). The encoder re-emits the `gen` counter row at that
  value so the live adapter's next allocation continues from the right
  point.

To keep the offline-tool boundary the parent doc requires
(`internal/backup` links no live-cluster machinery), the
index-derivation helpers are **duplicated** into `internal/backup`
with the same staleness-review discipline already used for the
snapshot-reader constants (`snapshot_reader.go` documents this
pattern). Each duplicated builder cites its live counterpart and is
covered by a cross-check test that asserts the encoder's derived index
rows are byte-identical to the live adapter's output for a shared
fixture.

> **Scope note / open question.** GSI and SQS-side-record derivation
> are the heaviest pieces. If the cross-check tests show the live
> builders are impractical to mirror offline within Phase 0b, the
> fallback is to emit only the user records + cheap indexes (TTL,
> generation) and document that GSI/SQS-side-state rebuild lazily on
> first adapter access after restart. The recommended path is full
> reconstruction; the fallback is called out in §"Milestones" as a
> per-adapter decision gate.
>
> **The fallback is not zero-cost transparency.** A missing GSI row
> makes a DynamoDB GSI query return empty *silently* (no error); a
> missing SQS dedup window lets a FIFO queue redeliver an
> already-received message post-restore. Neither degrades gracefully.
> So if lazy-rebuild is chosen for an adapter, the restore runbook
> MUST require a post-restore admin `SCAN+REBUILD` pass on the target
> cluster to complete *before the adapter serves traffic* — the
> milestone that picks the fallback owns adding that runbook step and
> the admin command to drive it. Absent that, the fallback is
> incorrect, not merely slower.

## Per-adapter reverse encoders

Each mirrors a set of Phase 0a `Handle*` methods, reversed. The
encoder walks the directory tree, and for each adapter emits the
internal `(userKey, userValue)` pairs (then MVCC-encodes them per
§"MVCC re-encoding"). Prefixes/key layouts come from the same sources
the decoder dispatches on (`internal/s3keys/keys.go`, `kv/shard_key.go`,
`adapter/sqs_keys.go`, `store/{hash,list,set,zset,stream}_helpers.go`)
and the value envelopes from the same codecs
(`adapter/dynamodb_storage_codec.go`, `adapter/sqs_messages.go`,
`adapter/redis_storage_codec.go`).

- **Redis** (`internal/backup/encode_redis_*.go`): strings → `!redis|str|`;
  hashes → `!hs|meta|` + `!hs|fld|`; lists → `!lst|meta|` + items;
  sets, zsets (incl. score index `!zs|score|`), streams
  (`!stream|meta|` + entries); HLL → `!redis|str|` body + `!redis|ttl|`.
  Plus the TTL scan index for every expiring key.
- **DynamoDB** (`encode_dynamodb.go`): `_schema.json` → `!ddb|meta|`;
  items → `!ddb|item|<table>|<gen>|<orderedKey>`; generation counter;
  derived `!ddb|gsi|` rows. Reads both per-item and `--bundle jsonl`
  layouts (`MANIFEST.dynamodb_layout`).
- **S3** (`encode_s3.go`): `_bucket.json` → `!s3|bucket|meta|` + gen
  counter; each object body re-split into `!s3|blob|...` chunks at the
  chunk size recorded in the manifest sidecar / `MANIFEST.json`;
  `!s3|obj|...` manifest row from the sidecar. Reverses the
  collision-rename and reserved-suffix rules via `KEYMAP.jsonl`.
- **SQS** (`encode_sqs.go`): `_queue.json` → `!sqs|queue|meta|` + gen;
  `messages.jsonl` → `!sqs|msg|data|` rows in stored order; derived
  side records.

A shared `encode.go` orchestrates: read+validate `MANIFEST.json`,
fan out to enabled adapters, collect all `(encKey, encVal)` pairs,
**sort**, then stream-write the EKVPBBL1 file. Memory: the sort
requires materializing the encoded-key set; for very large dumps the
encoder spills to an on-disk external sort keyed by encoded key (a
later milestone — Phase 0b v1 sorts in memory and documents the
bound, mirroring how Phase 0a bounded per-entry allocations first and
optimized later). Worst-case in-memory bound for v1 is roughly
`N × (avg_encoded_key_size + small_slice_header)` for the key index
plus the value bytes if held — e.g. a 10M-key snapshot at a 256-byte
average key is ~2.5 GiB just for the keys. The external-sort
follow-up milestone uses this as its baseline target.

## Version coupling and the format gate

The encoder reproduces *internal* key encodings, so it has a hard
dependency on the live key-format version (parent §"Costs"). Guards:

- Refuse `MANIFEST.format_version` with a major greater than the
  encoder's supported major (mirrors the decoder's
  `TestManifestVersionGate`).
- Record an `encoder_key_format_version` in the emitted file's
  provenance (a sidecar `ENCODE_INFO.json` next to the output `.fsm`,
  not in the `.fsm` itself — the `.fsm` byte format is fixed) so a
  restore operator can confirm the encoder matched the target
  cluster's key-format version.
- `cluster_id` from `MANIFEST.json` is surfaced in `ENCODE_INFO.json`;
  the restore runbook step refuses to place a file whose `cluster_id`
  differs from the target node (parent §"Risks").

## Round-trip self-test

The encoder's correctness gate (parent §"Risks": "Encoder runs
`cmd/elastickv-snapshot-decode` on its own output and asserts a
round-trip"):

```text
dirTree  --encode-->  .fsm  --decode-->  dirTree'
assert dirTree == dirTree'   (excluding wall-time + encoder-provenance fields)
```

This is a **directory-level** round-trip and is exact because:

- per-key `commit_ts` is discarded by decode, so the encoder's uniform
  `last_commit_ts` stamping is invisible to the comparison;
- the derived internal indexes are dropped again by decode, so they do
  not appear in `dirTree'`;
- `last_commit_ts` itself survives in both `MANIFEST.json` files.

The reverse direction (`.fsm` → dirTree → `.fsm`) is **not**
byte-identical (per-key `commit_ts` is unrecoverable) and is
explicitly not a goal. The self-test is wired as a library call
(decode the just-written buffer) before the encoder finalizes the
output file, so a node never receives an unloadable `.fsm`.

## Milestones (per-adapter PRs, mirroring Phase 0a)

Doc-first: this proposal lands as its own PR before any code. Then,
in order (each its own PR, each with the cross-check + round-trip
tests for that adapter):

1. **Encoder core** — `encode.go`: MANIFEST read/validate, MVCC
   re-encoding, in-memory sort, EKVPBBL1 writer, in-process round-trip
   harness. Lands with a trivial single-Redis-string fixture.
2. **Redis** — all types + TTL scan-index reconstruction.
3. **DynamoDB** — items + `_schema.json` + generation; **GSI
   derivation decision gate** (full vs. lazy-rebuild fallback).
4. **S3** — bodies re-chunked + manifest + collision/suffix reversal.
5. **SQS** — queue + messages + **side-record derivation decision
   gate**.
6. **CLI** — `cmd/elastickv-snapshot-encode` flag parsing
   (`--input`, `--output`, optional `--last-commit-ts` override with
   the fail-closed `T ≥ manifest.last_commit_ts` semantics from
   §"MVCC re-encoding"), `ENCODE_INFO.json` provenance, end-to-end
   test that loads the output into a fresh single-node cluster and
   reads back every adapter's data.

Each milestone follows the project convention: a review-found defect
gets a failing test first, then the fix, in the same PR.

## Test plan

P0 (per the parent doc's Phase 0b rows, made concrete here):

| Test | Verifies |
|---|---|
| `TestEncodeMVCCReEncoding` | `(userKey, expireAt)` → encKey/encVal matches the live `fillEncodedKey`/`fillEncodedValue` layout for a fixture; invTS = `^last_commit_ts` |
| `TestEncodeSortedOutput` | Emitted entries are strictly ascending by encoded key; reader accepts the stream |
| `TestEncodeRedisAllTypes` | One key per Redis type round-trips dir→fsm→dir; TTL'd keys produce the `!redis\|ttl\|` index row (cross-checked vs. live `buildTTLElems`) |
| `TestEncodeDynamoDBItemsAndGSI` | Items + schema round-trip; derived GSI rows match the live adapter for a composite-key fixture |
| `TestEncodeS3Rechunk` | Object body re-split into blob chunks reassembles bytewise; manifest + sidecar match |
| `TestEncodeSQSMessagesAndSideRecords` | `messages.jsonl` → data rows in order; derived dedup/group/vis rows match the live adapter; vis zeroed by default |
| `TestEncodeManifestVersionGate` | Refuses `format_version` major > supported; same-major-newer-minor allowed |
| `TestEncodeRoundTripExact` | dir→fsm→dir equality (wall-time/provenance excluded) across an all-adapter fixture |
| `TestEncodeRejectsOversizeEntry` | A reconstructed entry exceeding the key/value cap fails closed before any byte is written |

P1:

| Test | Verifies |
|---|---|
| `TestEncoderProducesLoadableSnapshot` | Output placed under a fresh node's `fsm-snap/` + `snap/` loads on `Open`; every adapter serves the original data |
| `TestEncoderClusterIDProvenance` | `ENCODE_INFO.json` carries `cluster_id` + key-format version; mismatch is detectable by the runbook step |

P2:

| Test | Verifies |
|---|---|
| `BenchmarkEncodeThroughput` | Per-adapter encode throughput baseline; flags the in-memory-sort memory bound for the external-sort follow-up |

## References

- `2026_04_29_partial_snapshot_logical_decoder.md` — Phase 0 format
  owner; Phase 0a (decoder) shipped. Promoted to `partial` on landing
  this doc.
- `2026_04_29_proposed_logical_backup.md` — Phase 1 (live PIT
  extraction); produces the same directory format this encoder reads.
- `2026_04_14_implemented_etcd_snapshot_disk_offload.md` — the `.fsm`
  / `.snap` EKVT token format the restore runbook seeds.
- `store/snapshot_pebble.go` (`WriteTo`),
  `internal/backup/snapshot_reader.go` (`ReadSnapshot`) — the
  authoritative native `.fsm` format this encoder must reproduce.
- `internal/backup/decode.go` — the dispatch table this encoder
  reverses, route-for-route.
